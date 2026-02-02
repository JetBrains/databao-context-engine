import logging
import os
from pathlib import Path
from typing import Any

import yaml

from databao_context_engine.datasources.types import (
    Datasource,
    DatasourceDescriptor,
    DatasourceId,
    DatasourceKind,
    PreparedConfig,
    PreparedDatasource,
    PreparedFile,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.templating.renderer import render_template

logger = logging.getLogger(__name__)


def get_datasource_list(project_layout: ProjectLayout) -> list[Datasource]:
    result = []
    for discovered_datasource in discover_datasources(project_layout=project_layout):
        try:
            prepared_source = prepare_source(project_layout, discovered_datasource)
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Invalid source at ({discovered_datasource.path}): {str(e)}")
            continue

        result.append(
            Datasource(
                id=DatasourceId.from_datasource_config_file_path(project_layout, discovered_datasource.path),
                type=prepared_source.datasource_type,
            )
        )

    return result


def discover_datasources(project_layout: ProjectLayout) -> list[DatasourceDescriptor]:
    """Scan the project's src/ directory and return all discovered sources.

    Rules:
        - Each first-level directory under src/ is treated as a main_type
        - Unsupported or unreadable entries are skipped.
        - The returned list is sorted by directory and then filename

    Args:
        project_layout: ProjectLayout instance representing the project directory and configuration.

    Returns:
        A list of DatasourceDescriptor instances representing the discovered datasources.
    """
    datasources: list[DatasourceDescriptor] = []
    for dirpath, dirnames, filenames in os.walk(project_layout.src_dir):
        for config_file_name in filenames:
            context_file = Path(dirpath).joinpath(config_file_name)
            datasource = _load_datasource_descriptor(project_layout, context_file)
            if datasource is not None:
                datasources.append(datasource)

    return sorted(datasources, key=lambda ds: str(ds.datasource_id.relative_path_to_config_file()).lower())


def _is_datasource_file(p: Path) -> bool:
    # ignore backup files
    return p.is_file() and not p.suffix.endswith("~")


def get_datasource_descriptors(
    project_layout: ProjectLayout, datasource_ids: list[DatasourceId]
) -> list[DatasourceDescriptor]:
    datasources: list[DatasourceDescriptor] = []
    for datasource_id in sorted(datasource_ids, key=lambda id: str(id)):
        config_file_path = project_layout.src_dir.joinpath(datasource_id.relative_path_to_config_file())
        if not config_file_path.is_file():
            raise ValueError(f"Datasource config file not found: {config_file_path}")

        datasource = _load_datasource_descriptor(project_layout, config_file_path)
        if datasource is not None:
            datasources.append(datasource)

    return datasources


def _load_datasource_descriptor(project_layout: ProjectLayout, config_file: Path) -> DatasourceDescriptor | None:
    """Load a single file with src/<parent_name>/ into a DatasourceDescriptor."""
    if not config_file.is_file():
        return None

    parent_name = config_file.parent.name
    extension = config_file.suffix.lower().lstrip(".")
    relative_config_file = config_file.relative_to(project_layout.src_dir)

    if parent_name == "files" and len(relative_config_file.parts) == 2:
        datasource_id = DatasourceId.from_datasource_config_file_path(project_layout, config_file)
        return DatasourceDescriptor(datasource_id=datasource_id, path=config_file.resolve(), kind=DatasourceKind.FILE)

    if extension in {"yaml", "yml"}:
        datasource_id = DatasourceId.from_datasource_config_file_path(project_layout, config_file)
        return DatasourceDescriptor(datasource_id=datasource_id, path=config_file.resolve(), kind=DatasourceKind.CONFIG)

    if extension:
        datasource_id = DatasourceId.from_datasource_config_file_path(project_layout, config_file)
        return DatasourceDescriptor(datasource_id=datasource_id, path=config_file.resolve(), kind=DatasourceKind.FILE)

    logger.debug("Skipping file without extension: %s", config_file)
    return None


def prepare_source(project_layout: ProjectLayout, datasource: DatasourceDescriptor) -> PreparedDatasource:
    """Convert a discovered datasource into a prepared datasource ready for plugin execution."""
    if datasource.kind is DatasourceKind.FILE:
        file_subtype = datasource.path.suffix.lower().lstrip(".")
        return PreparedFile(
            datasource_id=datasource.datasource_id,
            datasource_type=DatasourceType(full_type=file_subtype),
            path=datasource.path,
        )

    config = _parse_config_file(project_layout, datasource.path)

    ds_type = config.get("type")
    if not ds_type or not isinstance(ds_type, str):
        raise ValueError("Config missing 'type' at %s - skipping", datasource.path)

    return PreparedConfig(
        datasource_id=datasource.datasource_id,
        datasource_type=DatasourceType(full_type=ds_type),
        path=datasource.path,
        config=config,
        datasource_name=datasource.path.stem,
    )


def _parse_config_file(project_layout: ProjectLayout, file_path: Path) -> dict[Any, Any]:
    rendered_file = render_template(project_layout, file_path.read_text())

    return yaml.safe_load(rendered_file) or {}
