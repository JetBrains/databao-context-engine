import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from databao_context_engine.build_sources.internal.build_service import BuildService
from databao_context_engine.build_sources.internal.export_results import (
    append_result_to_all_results,
    create_run_dir,
    export_build_result,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import load_plugins
from databao_context_engine.project.datasource_discovery import discover_datasources, prepare_source
from databao_context_engine.project.types import DatasourceId

logger = logging.getLogger(__name__)


@dataclass
class BuildContextResult:
    datasource_id: DatasourceId
    datasource_type: DatasourceType
    context_built_at: datetime
    context_file_path: Path


def build(
    project_dir: Path,
    *,
    build_service: BuildService,
    project_id: str,
    dce_version: str,
) -> list[BuildContextResult]:
    """
    Build entrypoint.

    1) Load available plugins
    2) Discover sources
    3) Create a run
    4) For each source, call process_source
    """
    plugins = load_plugins()

    datasources = discover_datasources(project_dir)

    if not datasources:
        logger.info("No sources discovered under %s", project_dir)
        return []

    run = None
    run_dir = None

    number_of_failed_builds = 0
    build_result = []
    for discovered_datasource in datasources:
        try:
            prepared_source = prepare_source(discovered_datasource)

            logger.info(
                f'Found datasource of type "{prepared_source.datasource_type.full_type}" with name {prepared_source.path.stem}'
            )

            plugin = plugins.get(prepared_source.datasource_type)
            if plugin is None:
                logger.warning(
                    "No plugin for '%s' (datasource=%s) — skipping.",
                    prepared_source.datasource_type.full_type,
                    prepared_source.path,
                )
                number_of_failed_builds += 1
                continue

            if run is None or run_dir is None:
                # Initialiase the run as soon as we found a source
                run = build_service.start_run(project_id=project_id, dce_version=dce_version)
                run_dir = create_run_dir(project_dir, run.run_name)

            result = build_service.process_prepared_source(
                run_id=run.run_id,
                prepared_source=prepared_source,
                plugin=plugin,
            )

            context_file_path = export_build_result(run_dir, result)
            append_result_to_all_results(run_dir, result)

            build_result.append(
                BuildContextResult(
                    datasource_id=DatasourceId.from_string_repr(result.datasource_id),
                    datasource_type=DatasourceType(full_type=result.datasource_type),
                    context_built_at=result.context_built_at,
                    context_file_path=context_file_path,
                )
            )
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({discovered_datasource.path}): {str(e)}")

            number_of_failed_builds += 1

    if run is not None:
        build_service.finalize_run(run_id=run.run_id)

    logger.debug(
        "Successfully built %d datasources. %s",
        len(build_result),
        f"Failed to build {number_of_failed_builds}." if number_of_failed_builds > 0 else "",
    )

    return build_result
