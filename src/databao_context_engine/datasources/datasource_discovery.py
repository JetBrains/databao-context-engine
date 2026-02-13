import logging
import os
from pathlib import Path
from typing import Any

import yaml

from databao_context_engine.datasources.types import (
    ConfiguredDatasource,
    Datasource,
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


def get_datasource_list(project_layout: ProjectLayout) -> list[ConfiguredDatasource]:
    result = []
    for datasource_id in discover_datasources(project_layout=project_layout):
        datasource_path = datasource_id.relative_path_to_config_file()
        try:
            prepared_source = prepare_source(project_layout, datasource_id)
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Invalid source at ({datasource_path}): {str(e)}")
            continue

        result.append(
            ConfiguredDatasource(
                datasource=Datasource(
                    id=datasource_id,
                    type=prepared_source.datasource_type,
                ),
                config=prepared_source.config if isinstance(prepared_source, PreparedConfig) else None,
            )
        )

    return result


def discover_datasources(project_layout: ProjectLayout) -> list[DatasourceId]:
    """Scan the project's src/ directory and return all discovered sources.

    Rules:
        - Each first-level directory under src/ is treated as a main_type
        - Unsupported or unreadable entries are skipped.
        - The returned list is sorted by directory and then filename

    Args:
        project_layout: ProjectLayout instance representing the project directory and configuration.

    Returns:
        A list of DatasourceId representing the discovered datasources.
    """
    datasource_ids: list[DatasourceId] = []
    for dirpath, dirnames, filenames in os.walk(project_layout.src_dir):
        for config_file_name in filenames:
            context_file = Path(dirpath).joinpath(config_file_name)
            datasource_id = _load_datasource_id(project_layout, context_file)
            if datasource_id is not None:
                datasource_ids.append(datasource_id)

    return sorted(datasource_ids, key=lambda id: str(id.relative_path_to_config_file()).lower())


def _is_datasource_file(p: Path) -> bool:
    # ignore backup files
    return p.is_file() and not p.suffix.endswith("~")


def validate_datasource_ids(project_layout: ProjectLayout, datasource_ids: list[DatasourceId]) -> list[DatasourceId]:
    for datasource_id in sorted(datasource_ids, key=lambda id: str(id)):
        config_file_path = project_layout.src_dir.joinpath(datasource_id.relative_path_to_config_file())
        if not config_file_path.is_file():
            raise ValueError(f"Datasource config file not found: {config_file_path}")

    def _is_datasource_id_valid(datasource_id: DatasourceId) -> bool:
        config_file_path = project_layout.src_dir.joinpath(datasource_id.relative_path_to_config_file())
        return _is_valid_config_file(config_file_path)

    return [datasource_id for datasource_id in datasource_ids if _is_datasource_id_valid(datasource_id)]


def _load_datasource_id(project_layout: ProjectLayout, config_file: Path) -> DatasourceId | None:
    if not _is_valid_config_file(config_file):
        logger.debug("Skipping config file: %s", config_file)
        return None

    return DatasourceId.from_datasource_config_file_path(project_layout, config_file)


def _is_valid_config_file(config_file: Path) -> bool:
    if config_file.suffix.lower().lstrip("."):
        return config_file.is_file()
    return False


def prepare_source(project_layout: ProjectLayout, datasource_id: DatasourceId) -> PreparedDatasource:
    """Convert a discovered datasource into a prepared datasource ready for plugin execution."""
    if datasource_id.kind is DatasourceKind.FILE:
        file_subtype = datasource_id.config_file_suffix.lower().lstrip(".")
        return PreparedFile(
            datasource_id=datasource_id,
            datasource_type=DatasourceType(full_type=file_subtype),
        )

    absolute_datasource_path = datasource_id.absolute_path_to_config_file(project_layout)
    config = _parse_config_file(absolute_datasource_path)

    datasource_path = datasource_id.relative_path_to_config_file()
    ds_type = config.get("type")
    if not ds_type or not isinstance(ds_type, str):
        raise ValueError("Config missing 'type' at %s - skipping", datasource_path)

    return PreparedConfig(
        datasource_id=datasource_id,
        datasource_type=DatasourceType(full_type=ds_type),
        config=config,
        datasource_name=datasource_path.stem,
    )


def _parse_config_file(file_path: Path) -> dict[str, Any]:
    rendered_file = render_template(file_path.read_text())

    return yaml.safe_load(rendered_file) or {}
