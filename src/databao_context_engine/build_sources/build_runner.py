import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.export_results import (
    append_result_to_all_results,
    export_build_result,
    reset_all_results,
)
from databao_context_engine.datasources.datasource_discovery import discover_datasources, prepare_source
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import load_plugins
from databao_context_engine.project.layout import ProjectLayout

logger = logging.getLogger(__name__)


@dataclass
class BuildContextResult:
    """Result of a single datasource context build.

    Attributes:
        datasource_id: The id of the datasource.
        datasource_type: The type of the datasource.
        context_built_at: The timestamp when the context was built.
        context_file_path: The path to the generated context file.
    """

    datasource_id: DatasourceId
    datasource_type: DatasourceType
    context_built_at: datetime
    context_file_path: Path


def build(
    project_layout: ProjectLayout,
    *,
    build_service: BuildService,
) -> list[BuildContextResult]:
    """Build the context for all datasources in the project.

    Unless you already have access to BuildService, this should not be called directly.
    Instead, internal callers should go through the build_wiring module or directly use DatabaoContextProjectManager.build_context().

    1) Load available plugins
    2) Discover sources
    3) Create a run
    4) For each source, call process_source

    Returns:
        A list of all the contexts built.
    """
    plugins = load_plugins()

    datasources = discover_datasources(project_layout)

    if not datasources:
        logger.info("No sources discovered under %s", project_layout.src_dir)
        return []

    number_of_failed_builds = 0
    build_result = []
    reset_all_results(project_layout.output_dir)
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

            result = build_service.process_prepared_source(
                prepared_source=prepared_source,
                plugin=plugin,
            )

            output_dir = project_layout.output_dir

            context_file_path = export_build_result(output_dir, result)
            append_result_to_all_results(output_dir, result)

            build_result.append(
                BuildContextResult(
                    datasource_id=discovered_datasource.datasource_id,
                    datasource_type=DatasourceType(full_type=result.datasource_type),
                    context_built_at=result.context_built_at,
                    context_file_path=context_file_path,
                )
            )
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({discovered_datasource.path}): {str(e)}")

            number_of_failed_builds += 1

    logger.debug(
        "Successfully built %d datasources. %s",
        len(build_result),
        f"Failed to build {number_of_failed_builds}." if number_of_failed_builds > 0 else "",
    )

    return build_result
