import logging

from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.export_results import (
    append_result_to_all_results,
    export_build_result,
    reset_all_results,
)
from databao_context_engine.build_sources.types import (
    BuildDatasourceResult,
    DatasourceStatus,
    IndexDatasourceResult,
)
from databao_context_engine.datasources.datasource_context import (
    DatasourceContext,
    read_datasource_type_from_context,
)
from databao_context_engine.datasources.datasource_discovery import discover_datasources, prepare_source
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import load_plugins
from databao_context_engine.project.layout import ProjectLayout

logger = logging.getLogger(__name__)


def build(
    project_layout: ProjectLayout, *, build_service: BuildService, generate_embeddings: bool = True
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
    plugins = load_plugins()

    datasource_ids = discover_datasources(project_layout)

    if not datasource_ids:
        logger.info("No sources discovered under %s", project_layout.src_dir)
        return []

    results: list[BuildDatasourceResult] = []
    failed = 0
    skipped = 0
    reset_all_results(project_layout.output_dir)
    for datasource_id in datasource_ids:
        try:
            prepared_source = prepare_source(project_layout, datasource_id)

            logger.info(
                f'Found datasource of type "{prepared_source.datasource_type.full_type}" with name {prepared_source.datasource_id.datasource_path}'
            )

            plugin = plugins.get(prepared_source.datasource_type)
            if plugin is None:
                logger.warning(
                    "No plugin for '%s' (datasource=%s) — skipping.",
                    prepared_source.datasource_type.full_type,
                    prepared_source.datasource_id.relative_path_to_config_file(),
                )
                skipped += 1
                results.append(BuildDatasourceResult(datasource_id=datasource_id, status=DatasourceStatus.SKIPPED))
                continue

            result = build_service.process_prepared_source(
                prepared_source=prepared_source, plugin=plugin, generate_embeddings=generate_embeddings
            )

            output_dir = project_layout.output_dir

            context_file_path = export_build_result(output_dir, result)
            append_result_to_all_results(output_dir, result)

            results.append(
                BuildDatasourceResult(
                    datasource_id=datasource_id,
                    status=DatasourceStatus.OK,
                    datasource_type=DatasourceType(full_type=result.datasource_type),
                    context_built_at=result.context_built_at,
                    context_file_path=context_file_path,
                )
            )
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({datasource_id.relative_path_to_config_file()}): {str(e)}")

            failed += 1
            results.append(
                BuildDatasourceResult(datasource_id=datasource_id, status=DatasourceStatus.FAILED, error=str(e))
            )

    ok = sum(1 for result in results if result.status == DatasourceStatus.OK)
    logger.debug(
        "Successfully built %d/%d datasources. %s",
        ok,
        len(datasource_ids),
        f"Skipped {skipped}. Failed {failed}." if (skipped or failed) else "",
    )

    return results


def run_indexing(
    *, project_layout: ProjectLayout, build_service: BuildService, contexts: list[DatasourceContext]
) -> list[IndexDatasourceResult]:
    """Index a list of built datasource contexts.

    1) Load available plugins
    2) Infer datasource type from context file
    3) For each context, call index_built_context

    Returns:
        A list of per-context indexing results.
    """
    plugins = load_plugins()

    results: list[IndexDatasourceResult] = []
    ok = 0
    skipped = 0
    failed = 0

    for context in contexts:
        try:
            logger.info(f"Indexing datasource {context.datasource_id}")

            datasource_type = read_datasource_type_from_context(context)

            plugin = plugins.get(datasource_type)
            if plugin is None:
                logger.warning(
                    "No plugin for datasource type '%s' — skipping indexing for %s.",
                    getattr(datasource_type, "full_type", datasource_type),
                    context.datasource_id,
                )
                skipped += 1
                results.append(
                    IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.SKIPPED)
                )
                continue

            build_service.index_built_context(context=context, plugin=plugin)
            ok += 1
            results.append(IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.OK))
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({context.datasource_id}): {str(e)}")
            failed += 1
            results.append(
                IndexDatasourceResult(datasource_id=context.datasource_id, status=DatasourceStatus.FAILED, error=str(e))
            )

    logger.debug(
        "Successfully indexed %d/%d datasource(s). %s",
        ok,
        len(contexts),
        f"Skipped {skipped}. Failed {failed}." if (skipped or failed) else "",
    )

    return results
