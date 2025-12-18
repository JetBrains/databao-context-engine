import logging
from pathlib import Path

from nemory.build_sources.internal.build_service import BuildService
from nemory.build_sources.internal.export_results import (
    append_result_to_all_results,
    create_run_dir,
    export_build_result,
)
from nemory.plugins.plugin_loader import load_plugins
from nemory.project.datasource_discovery import discover_datasources, prepare_source

logger = logging.getLogger(__name__)


def build(
    project_dir: Path,
    *,
    build_service: BuildService,
    project_id: str,
    nemory_version: str,
) -> None:
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
        return

    run = None
    run_dir = None

    number_processed_datasources = 0
    for discovered_datasource in datasources:
        try:
            prepared_source = prepare_source(discovered_datasource)

            logger.info(f'Found datasource of type "{prepared_source.full_type}" with name {prepared_source.path.stem}')

            plugin = plugins.get(prepared_source.full_type)
            if plugin is None:
                logger.warning(
                    "No plugin for '%s' (datasource=%s) — skipping.", prepared_source.full_type, prepared_source.path
                )
                continue

            if run is None or run_dir is None:
                # Initialiase the run as soon as we found a source
                run = build_service.start_run(project_id=project_id, nemory_version=nemory_version)
                run_dir = create_run_dir(project_dir, run.run_name)

            result = build_service.process_prepared_source(
                run_id=run.run_id,
                prepared_source=prepared_source,
                plugin=plugin,
            )

            export_build_result(run_dir, result)
            append_result_to_all_results(run_dir, result)

            number_processed_datasources += 1
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Failed to build source at ({discovered_datasource.path}): {str(e)}")

    if run is not None:
        build_service.finalize_run(run_id=run.run_id)

    logger.info("Build complete. Processed %d datasources.", number_processed_datasources)
