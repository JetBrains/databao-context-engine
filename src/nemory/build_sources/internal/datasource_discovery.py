import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from nemory.project.layout import get_source_dir

logger = logging.getLogger(__name__)


class DatasourceKind(StrEnum):
    CONFIG = "config"
    FILE = "file"


@dataclass(frozen=True)
class DatasourceDescriptor:
    path: Path
    kind: DatasourceKind
    main_type: str


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
