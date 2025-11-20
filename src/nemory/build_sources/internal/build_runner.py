import logging
from pathlib import Path

import yaml

from nemory.build_sources.internal.build_service import BuildService
from nemory.build_sources.internal.export_results import (
    append_result_to_all_results,
    create_run_dir,
    export_build_result,
)
from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.build_sources.internal.datasource_discovery import (
    DatasourceDescriptor,
    DatasourceKind,
    discover_datasources,
)
from nemory.build_sources.internal.types import PreparedConfig, PreparedFile, PreparedDatasource

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

    run = build_service.start_run(project_id=project_id, nemory_version=nemory_version)
    run_dir = create_run_dir(project_dir, run.started_at)

    for datasource in datasources:
        try:
            prepared_source = _prepare_source(datasource)
            if prepared_source is None:
                continue

            plugin = plugins.get(prepared_source.full_type)
            if plugin is None:
                logger.warning(
                    "No plugin for '%s' (datasource=%s) — skipping.", prepared_source.full_type, datasource.path
                )
                continue

            result = build_service.process_prepared_source(
                run_id=run.run_id,
                prepared_source=prepared_source,
                plugin=plugin,
            )

            export_build_result(run_dir, result)
            append_result_to_all_results(run_dir, result)
        except Exception as e:
            logger.exception("Source failed (%s): %s", datasource.path, e)

    build_service.finalize_run(run_id=run.run_id)

    logger.info("Build complete. Processed %d datasources.", len(datasources))


def _prepare_source(datasource: DatasourceDescriptor) -> PreparedDatasource | None:
    """
    Convert a discovered datasource into a prepared datasource ready for plugin execution
    """
    if datasource.kind is DatasourceKind.FILE:
        file_subtype = datasource.path.suffix.lower().lstrip(".")
        full_type = f"{datasource.main_type}/{file_subtype}"
        return PreparedFile(full_type=full_type, path=datasource.path)

    else:
        try:
            with datasource.path.open("r", encoding="utf-8") as fh:
                config = yaml.safe_load(fh) or {}
        except Exception as e:
            logger.warning("Skipping invalid YAML %s: %s", datasource.path, e)
            return None
        subtype = config.get("type")
        if not subtype or not isinstance(subtype, str):
            logger.warning("Config missing 'type' at %s - skipping", datasource.path)
            return None
        full_type = f"{datasource.main_type}/{subtype}"
        return PreparedConfig(
            full_type=full_type, path=datasource.path, config=config, datasource_name=datasource.path.stem
        )
