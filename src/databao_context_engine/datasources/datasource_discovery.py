import logging
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
from databao_context_engine.project.layout import get_source_dir
from databao_context_engine.templating.renderer import render_template

logger = logging.getLogger(__name__)


def get_datasource_list(project_dir: Path) -> list[Datasource]:
    result = []
    for discovered_datasource in discover_datasources(project_dir=project_dir):
        try:
            prepared_source = prepare_source(discovered_datasource)
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info(f"Invalid source at ({discovered_datasource.path}): {str(e)}")
            continue

        result.append(
            Datasource(
                id=DatasourceId.from_datasource_config_file_path(discovered_datasource.path),
                type=prepared_source.datasource_type,
            )
        )

    return result


def discover_datasources(project_dir: Path) -> list[DatasourceDescriptor]:
    """Scan the project's src/ directory and return all discovered sources.

    Rules:
        - Each first-level directory under src/ is treated as a main_type
        - Unsupported or unreadable entries are skipped.
        - The returned list is sorted by directory and then filename

    Returns:
        A list of DatasourceDescriptor instances representing the discovered datasources.

    Raises:
        ValueError: If the src directory does not exist in the project directory.
    """
    src = get_source_dir(project_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    datasources: list[DatasourceDescriptor] = []
    for main_dir in sorted((p for p in src.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        for path in sorted((p for p in main_dir.iterdir() if _is_datasource_file(p)), key=lambda p: p.name.lower()):
            datasource = _load_datasource_descriptor(path)
            if datasource is not None:
                datasources.append(datasource)

    return datasources


def _is_datasource_file(p: Path) -> bool:
    # ignore backup files
    return p.is_file() and not p.suffix.endswith("~")


def get_datasource_descriptors(project_dir: Path, datasource_ids: list[DatasourceId]) -> list[DatasourceDescriptor]:
    src = get_source_dir(project_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    datasources: list[DatasourceDescriptor] = []
    for datasource_id in sorted(datasource_ids, key=lambda id: str(id)):
        config_file_path = src.joinpath(datasource_id.relative_path_to_config_file())
        if not config_file_path.is_file():
            raise ValueError(f"Datasource config file not found: {config_file_path}")

        datasource = _load_datasource_descriptor(config_file_path)
        if datasource is not None:
            datasources.append(datasource)

    return datasources


def _load_datasource_descriptor(path: Path) -> DatasourceDescriptor | None:
    """Load a single file with src/<parent_name>/ into a DatasourceDescriptor."""
    if not path.is_file():
        return None

    parent_name = path.parent.name
    extension = path.suffix.lower().lstrip(".")

    if parent_name == "files":
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.FILE)

    if extension in {"yaml", "yml"}:
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.CONFIG)

    if extension:
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.FILE)

    logger.debug("Skipping file without extension: %s", path)
    return None


def prepare_source(datasource: DatasourceDescriptor) -> PreparedDatasource:
    """Convert a discovered datasource into a prepared datasource ready for plugin execution."""
    if datasource.kind is DatasourceKind.FILE:
        file_subtype = datasource.path.suffix.lower().lstrip(".")
        return PreparedFile(
            datasource_type=DatasourceType.from_main_and_subtypes(main_type=datasource.main_type, subtype=file_subtype),
            path=datasource.path,
        )

    config = _parse_config_file(datasource.path)

    subtype = config.get("type")
    if not subtype or not isinstance(subtype, str):
        raise ValueError("Config missing 'type' at %s - skipping", datasource.path)

    return PreparedConfig(
        datasource_type=DatasourceType.from_main_and_subtypes(main_type=datasource.main_type, subtype=subtype),
        path=datasource.path,
        config=config,
        datasource_name=datasource.path.stem,
    )


def _parse_config_file(file_path: Path) -> dict[Any, Any]:
    rendered_file = render_template(file_path.read_text())

    return yaml.safe_load(rendered_file) or {}
