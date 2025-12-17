import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from nemory.project.layout import get_source_dir
from nemory.project.types import (
    DatasourceDescriptor,
    DatasourceKind,
    PreparedConfig,
    PreparedDatasource,
    PreparedDatasourceError,
    PreparedFile,
)
from nemory.templating.renderer import render_template
from nemory.utils.result import Err, Ok, Result

logger = logging.getLogger(__name__)


def traverse_datasources(
    project_dir: Path,
    *,
    datasources_to_traverse: list[DatasourceDescriptor] | None = None,
) -> Iterator[Result[PreparedDatasource, PreparedDatasourceError]]:
    datasources = discover_datasources(project_dir) if datasources_to_traverse is None else datasources_to_traverse

    if not datasources:
        logger.info("No sources discovered under %s", project_dir)
        return

    for datasource in datasources:
        try:
            prepared_source = _prepare_source(datasource)

            logger.info(f'Found datasource of type "{prepared_source.full_type}" with name {prepared_source.path.stem}')

            yield Ok(prepared_source)
        except Exception as e:
            logger.debug(str(e), exc_info=True, stack_info=True)
            logger.info("Failed to prepare source at (%s): %s", datasource.path, str(e))
            yield Err(PreparedDatasourceError("Failed to prepare source", datasource.path, e))

    return


def discover_datasources(project_dir: Path) -> list[DatasourceDescriptor]:
    """
    Scan the project's src/ directory and return all discovered sources.

    Rules:
        - Each first-level directory under src/ is treated as a main_type
        - Unsupported or unreadable entries are skipped.
        - The returned list is sorted by directory and then filename
    """
    src = get_source_dir(project_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    datasources: list[DatasourceDescriptor] = []
    for main_dir in sorted((p for p in src.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        main_type = main_dir.name
        for path in sorted((p for p in main_dir.iterdir() if _is_datasource_file(p)), key=lambda p: p.name.lower()):
            datasource = load_datasource_descriptor(path, main_type)
            if datasource is not None:
                datasources.append(datasource)

    return datasources


def _is_datasource_file(p: Path) -> bool:
    # ignore backup files
    return p.is_file() and not p.suffix.endswith("~")


def get_datasource_descriptors(project_dir: Path, datasource_config_files: list[str]):
    src = get_source_dir(project_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    datasources: list[DatasourceDescriptor] = []
    for datasource_config_file in datasource_config_files:
        config_segments = datasource_config_file.split("/")
        if len(config_segments) != 2:
            raise ValueError(
                f"Invalid datasource config file path: {datasource_config_file}. The path must be relative to the src folder (e.g: my-folder/my-config.yaml)"
            )

        main_type, datasource_name = config_segments

        config_file_path = src.joinpath(main_type, datasource_name)
        if not config_file_path.is_file():
            raise ValueError(f"Datasource config file not found: {config_file_path}")

        datasource = load_datasource_descriptor(config_file_path, main_type)
        if datasource is not None:
            datasources.append(datasource)

    return datasources


def load_datasource_descriptor(path: Path, parent_name: str) -> DatasourceDescriptor | None:
    """
    Load a single file with src/<parent_name>/ into a DatasourceDescriptor
    """
    if not path.is_file():
        return None

    extension = path.suffix.lower().lstrip(".")

    if parent_name == "files":
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.FILE)

    if extension in {"yaml", "yml"}:
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.CONFIG)

    if extension:
        return DatasourceDescriptor(path=path.resolve(), main_type=parent_name, kind=DatasourceKind.FILE)

    logger.debug("Skipping file without extension: %s", path)
    return None


def _prepare_source(datasource: DatasourceDescriptor) -> PreparedDatasource:
    """
    Convert a discovered datasource into a prepared datasource ready for plugin execution
    """
    if datasource.kind is DatasourceKind.FILE:
        file_subtype = datasource.path.suffix.lower().lstrip(".")
        full_type = f"{datasource.main_type}/{file_subtype}"
        return PreparedFile(full_type=full_type, path=datasource.path)

    else:
        config = _parse_config_file(datasource.path)

        subtype = config.get("type")
        if not subtype or not isinstance(subtype, str):
            raise ValueError("Config missing 'type' at %s - skipping", datasource.path)

        full_type = f"{datasource.main_type}/{subtype}"
        return PreparedConfig(
            full_type=full_type, path=datasource.path, config=config, datasource_name=datasource.path.stem
        )


def _parse_config_file(file_path: Path) -> dict[Any, Any]:
    rendered_file = render_template(file_path.read_text())

    return yaml.safe_load(rendered_file) or {}
