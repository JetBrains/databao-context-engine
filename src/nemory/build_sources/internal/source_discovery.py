import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from nemory.project.layout import get_source_dir

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceDescriptor:
    path: Path
    main_type: str
    subtype: str


def discover_sources(project_dir: Path) -> list[SourceDescriptor]:
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

    sources: list[SourceDescriptor] = []
    for main_dir in sorted((p for p in src.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        main_type = main_dir.name
        for path in sorted((p for p in main_dir.iterdir() if p.is_file()), key=lambda p: p.name.lower()):
            source = load_source_descriptor(path, main_type)
            if source is not None:
                sources.append(source)

    return sources


def load_source_descriptor(path: Path, parent_name: str) -> Optional[SourceDescriptor]:
    """
    Load a single file with src/<parent_name>/ into a SourceDescriptor
    """
    if not path.is_file():
        return None

    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        except yaml.YAMLError as e:
            logger.warning("Skipping invalid YAML at %s: %s", path, e)
            return None
        except OSError as e:
            logger.warning("Skipping unreadable YAML at %s: %s", path, e)
            return None

        subtype = data.get("type") if isinstance(data, dict) else None
        if not isinstance(subtype, str) or not subtype.strip():
            logger.warning("Skipping YAML without a valid 'type' at %s", path)
            return None
        return SourceDescriptor(path=path.resolve(), main_type=parent_name, subtype=subtype.strip())

    if suffix:
        return SourceDescriptor(path=path.resolve(), main_type=parent_name, subtype=suffix[1:])

    logger.debug("Skipping file without extension: %s", path)
    return None


def full_type_of(source: SourceDescriptor) -> str:
    """
    Compute the plugin routing key for a source as <main_type>/<subtype>
    """
    return f"{source.main_type}/{source.subtype}"
