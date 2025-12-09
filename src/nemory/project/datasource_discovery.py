import logging
from collections.abc import Iterator
from pathlib import Path

import yaml

from nemory.project.layout import get_source_dir
from nemory.project.types import DatasourceDescriptor, DatasourceKind, PreparedConfig, PreparedDatasource, PreparedFile

logger = logging.getLogger(__name__)


def traverse_datasources(
    project_dir: Path,
) -> Iterator[PreparedDatasource]:
    datasources = discover_datasources(project_dir)
    if not datasources:
        logger.info("No sources discovered under %s", project_dir)
        return

    for datasource in datasources:
        try:
            prepared_source = _prepare_source(datasource)
            if prepared_source is None:
                continue

            logger.info(f'Found datasource of type "{prepared_source.full_type}" with name {prepared_source.path.stem}')

            yield prepared_source
        except Exception as e:
            logger.exception("Source failed (%s): %s", datasource.path, e)
            continue

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
        for path in sorted((p for p in main_dir.iterdir() if p.is_file()), key=lambda p: p.name.lower()):
            datasource = load_datasource_descriptor(path, main_type)
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
