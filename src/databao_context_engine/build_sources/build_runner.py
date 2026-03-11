import logging
from datetime import datetime

from pydantic import TypeAdapter

import databao_context_engine.perf.core as perf
from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.export_results import (
    delete_all_results_file,
    export_build_result,
)
from databao_context_engine.build_sources.types import (
    BuildDatasourceResult,
    DatasourceStatus,
    EnrichContextResult,
    IndexDatasourceResult,
)
from databao_context_engine.datasources.datasource_context import (
    DatasourceContext,
    hash_context_file,
    read_datasource_type_from_context,
)
from databao_context_engine.datasources.datasource_discovery import discover_datasources, prepare_source
from databao_context_engine.datasources.types import PreparedConfig, PreparedDatasource
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.progress.progress import ProgressCallback, ProgressEmitter, ProgressStep
from databao_context_engine.project.layout import ProjectLayout

logger = logging.getLogger(__name__)


def _build_step_plan(*, should_index: bool, should_enrich_context: bool) -> tuple[ProgressStep, ...]:
    steps: list[ProgressStep] = [ProgressStep.PLUGIN_EXECUTION]

    if should_enrich_context:
        steps.append(ProgressStep.CONTEXT_ENRICHMENT)

    if should_index:
        steps.extend(_index_step_plan())

    return tuple(steps)


def _index_step_plan() -> tuple[ProgressStep, ...]:
    return (
        ProgressStep.EMBEDDING,
        ProgressStep.PERSISTENCE,
    )


@perf.perf_run(
    operation="build",
    attrs=lambda *, should_index, should_enrich_context, **_: {
        "should_index": should_index,
        "should_enrich_context": should_enrich_context,
    },
)
@perf.perf_span("build.total")
def build(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    should_index: bool,
    should_enrich_context: bool,
    progress: ProgressCallback | None = None,
) -> list[BuildDatasourceResult]:
    """Build the context for all datasources in the project.

    Unless you already have access to BuildService, this should not be called directly.
    Instead, internal callers should go through the build_wiring module or directly use DatabaoContextProjectManager.build_context().

    1) Load available plugins
    2) Discover sources
    3) For each source, call process_source

    Returns:
        A list of per-datasource build results.
    """
    datasource_ids = discover_datasources(project_layout)

    emitter = ProgressEmitter(progress)

    if not datasource_ids:
        logger.info("No sources discovered under %s", project_layout.src_dir)
        emitter.operation_started(operation="build", total=0)
        emitter.operation_finished(operation="build")
        return []

    emitter.operation_started(operation="build", total=len(datasource_ids))

    results: list[BuildDatasourceResult] = []
    failed = 0
    skipped = 0
    delete_all_results_file(project_layout)
    for datasource_index, datasource_id in enumerate(datasource_ids, start=1):
        emitter.datasource_started(
            datasource_id=str(datasource_id),
            index=datasource_index,
            total=len(datasource_ids),
        )
        try:
            result = _build_one_datasource(
                project_layout=project_layout,
                plugin_loader=plugin_loader,
                build_service=build_service,
                datasource_id=datasource_id,
                should_index=should_index,
                should_enrich_context=should_enrich_context,
                progress=progress,
            )
            results.append(result)
            if result.status == DatasourceStatus.SKIPPED:
                skipped += 1

            emitter.datasource_finished(
                datasource_id=str(datasource_id),
                index=datasource_index,
                total=len(datasource_ids),
                status=result.status.value,
                error=result.error,
            )
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({datasource_id.relative_path_to_config_file()}): {str(e)}")

            failed += 1
            results.append(
                BuildDatasourceResult(datasource_id=datasource_id, status=DatasourceStatus.FAILED, error=str(e))
            )
            emitter.datasource_finished(
                datasource_id=str(datasource_id),
                index=datasource_index,
                total=len(datasource_ids),
                status=DatasourceStatus.FAILED.value,
                error=str(e),
            )

    ok = sum(1 for result in results if result.status == DatasourceStatus.OK)
    logger.debug(
        "Successfully built %d/%d datasources. %s",
        ok,
        len(datasource_ids),
        f"Skipped {skipped}. Failed {failed}." if (skipped or failed) else "",
    )

    emitter.operation_finished(operation="build")
    return results


@perf.perf_span(
    "datasource.total",
    datasource_id=lambda *, datasource_id, **_: str(datasource_id),
)
def _build_one_datasource(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    datasource_id,
    should_index: bool,
    should_enrich_context: bool,
    progress: ProgressCallback | None = None,
) -> BuildDatasourceResult:
    prepared_source = prepare_source(project_layout, datasource_id)
    if not _is_datasource_enabled(prepared_source):
        logger.info(f"Skipping disabled datasource {prepared_source.datasource_id.datasource_path}")
        return BuildDatasourceResult(datasource_id=datasource_id, status=DatasourceStatus.SKIPPED)

    perf.set_attribute("datasource_type", prepared_source.datasource_type.full_type)

    logger.info(
        f'Found datasource of type "{prepared_source.datasource_type.full_type}" with name {prepared_source.datasource_id.datasource_path}'
    )

    plugin = plugin_loader.get_plugin_for_datasource_type(prepared_source.datasource_type)
    if plugin is None:
        logger.warning(
            "No plugin for '%s' (datasource=%s) — skipping.",
            prepared_source.datasource_type.full_type,
            prepared_source.datasource_id.relative_path_to_config_file(),
        )
        return BuildDatasourceResult(datasource_id=datasource_id, status=DatasourceStatus.SKIPPED)

    ProgressEmitter(progress).datasource_step_plan_set(
        datasource_id=str(datasource_id),
        step_plan=_build_step_plan(
            should_index=should_index,
            should_enrich_context=should_enrich_context,
        ),
    )

    result = build_service.build_context(
        prepared_source=prepared_source,
        plugin=plugin,
        progress=progress,
    )

    if should_enrich_context:
        result = build_service.enrich_built_context(result, plugin, progress)

    output_dir = project_layout.output_dir
    context_file_path = export_build_result(output_dir, result)

    perf.set_attribute("context_size_bytes", context_file_path.stat().st_size)

    if should_index:
        context_hash = hash_context_file(datasource_id=prepared_source.datasource_id, context_path=context_file_path)
        build_service.index_built_context(
            built_context=result, plugin=plugin, context_hash=context_hash, override=False, progress=progress
        )

    return BuildDatasourceResult(
        datasource_id=datasource_id,
        status=DatasourceStatus.OK,
        datasource_type=DatasourceType(full_type=result.datasource_type),
        context_built_at=datetime.now(),
        context_file_path=context_file_path,
    )


def _is_datasource_enabled(prepared_source: PreparedDatasource) -> bool:
    if isinstance(prepared_source, PreparedConfig):
        enabled_attribute = prepared_source.config.get("enabled", True)
        return TypeAdapter(bool).validate_python(enabled_attribute)

    return True


@perf.perf_run(
    operation="enrich_context",
    attrs=lambda *, should_index, **_: {
        "should_index": should_index,
    },
)
@perf.perf_span("enrich_context.total")
def run_enrich_context(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    contexts: list[DatasourceContext],
    should_index: bool,
) -> list[EnrichContextResult]:
    results: list[EnrichContextResult] = []
    ok = 0
    skipped = 0
    failed = 0

    for context in contexts:
        try:
            logger.info(f"Enriching context for datasource {context.datasource_id}")

            result = _enrich_one_context(
                project_layout=project_layout,
                context=context,
                plugin_loader=plugin_loader,
                build_service=build_service,
                should_index=should_index,
            )

            results.append(result)
            if result.status == DatasourceStatus.OK:
                ok += 1
            elif result.status == DatasourceStatus.SKIPPED:
                skipped += 1
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to enrich context for datasource ({context.datasource_id}): {str(e)}")
            failed += 1
            results.append(
                EnrichContextResult(datasource_id=context.datasource_id, status=DatasourceStatus.FAILED, error=str(e))
            )

    logger.debug(
        "Successfully indexed %d/%d datasource(s). %s",
        ok,
        len(contexts),
        f"Skipped {skipped}. Failed {failed}." if (skipped or failed) else "",
    )

    return results


@perf.perf_span(
    "datasource.total",
    datasource_id=lambda *, context, **_: str(context.datasource_id),
)
def _enrich_one_context(
    *,
    project_layout: ProjectLayout,
    context: DatasourceContext,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    should_index: bool,
) -> EnrichContextResult:
    perf.set_attribute("context_size_bytes", len(context.context.encode("utf-8")))

    datasource_type = read_datasource_type_from_context(context)
    perf.set_attribute("datasource_type", getattr(datasource_type, "full_type", datasource_type))

    plugin = plugin_loader.get_plugin_for_datasource_type(datasource_type)
    if plugin is None:
        logger.warning(
            "No plugin for datasource type '%s' — skipping context enrichment for datasource %s.",
            getattr(datasource_type, "full_type", datasource_type),
            context.datasource_id,
        )
        return EnrichContextResult(datasource_id=context.datasource_id, status=DatasourceStatus.SKIPPED)

    enriched_context = build_service.enrich_datasource_context(context=context, plugin=plugin)

    output_dir = project_layout.output_dir
    context_file_path = export_build_result(output_dir, enriched_context)

    if should_index:
        context_hash = hash_context_file(datasource_id=context.datasource_id, context_path=context_file_path)
        build_service.index_built_context(
            built_context=enriched_context, plugin=plugin, context_hash=context_hash, override=True
        )

    return EnrichContextResult(
        datasource_id=context.datasource_id,
        status=DatasourceStatus.OK,
        context_file_path=context_file_path,
    )


@perf.perf_run(operation="index")
@perf.perf_span("index.total")
def run_indexing(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    contexts: list[DatasourceContext],
    progress: ProgressCallback | None = None,
) -> list[IndexDatasourceResult]:
    """Index a list of built datasource contexts.

    1) Load available plugins
    2) Infer datasource type from context file
    3) For each context, call index_built_context

    Returns:
        A list of per-context indexing results.
    """
    emitter = ProgressEmitter(progress)
    emitter.operation_started(operation="index", total=len(contexts))

    results: list[IndexDatasourceResult] = []
    ok = 0
    skipped = 0
    failed = 0

    for context_index, context in enumerate(contexts, start=1):
        emitter.datasource_started(
            datasource_id=str(context.datasource_id),
            index=context_index,
            total=len(contexts),
        )
        try:
            logger.info(f"Indexing datasource {context.datasource_id}")

            result = _index_one_context(
                context=context, plugin_loader=plugin_loader, build_service=build_service, progress=progress
            )

            results.append(result)
            if result.status == DatasourceStatus.OK:
                ok += 1
            elif result.status == DatasourceStatus.SKIPPED:
                skipped += 1

            emitter.datasource_finished(
                datasource_id=str(context.datasource_id),
                index=context_index,
                total=len(contexts),
                status=result.status.value,
                error=result.error,
            )
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({context.datasource_id}): {str(e)}")
            failed += 1
            results.append(
                IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.FAILED, error=str(e))
            )
            emitter.datasource_finished(
                datasource_id=str(context.datasource_id),
                index=context_index,
                total=len(contexts),
                status=DatasourceStatus.FAILED.value,
                error=str(e),
            )

    logger.debug(
        "Successfully indexed %d/%d datasource(s). %s",
        ok,
        len(contexts),
        f"Skipped {skipped}. Failed {failed}." if (skipped or failed) else "",
    )

    emitter.operation_finished(operation="index")
    return results


@perf.perf_span(
    "datasource.total",
    datasource_id=lambda *, context, **_: str(context.datasource_id),
)
def _index_one_context(
    *,
    context: DatasourceContext,
    plugin_loader: DatabaoContextPluginLoader,
    build_service: BuildService,
    progress: ProgressCallback | None = None,
) -> IndexDatasourceResult:
    perf.set_attribute("context_size_bytes", len(context.context.encode("utf-8")))

    datasource_type = read_datasource_type_from_context(context)
    perf.set_attribute("datasource_type", getattr(datasource_type, "full_type", datasource_type))

    plugin = plugin_loader.get_plugin_for_datasource_type(datasource_type)
    if plugin is None:
        logger.warning(
            "No plugin for datasource type '%s' — skipping indexing for %s.",
            getattr(datasource_type, "full_type", datasource_type),
            context.datasource_id,
        )
        return IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.SKIPPED)

    ProgressEmitter(progress).datasource_step_plan_set(
        datasource_id=str(context.datasource_id),
        step_plan=_index_step_plan(),
    )

    build_service.index_datasource_context(context=context, plugin=plugin, progress=progress)
    return IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.OK)
