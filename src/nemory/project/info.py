from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

from nemory.project.layout import is_project_dir_valid, read_config_file
from nemory.system.properties import get_nemory_path


@dataclass(kw_only=True, frozen=True)
class NemoryProjectInfo:
    project_path: Path
    is_initialised: bool
    project_id: str | None


@dataclass(kw_only=True, frozen=True)
class NemoryInfo:
    version: str
    nemory_path: Path

    project_info: NemoryProjectInfo


def get_command_info(project_dir: Path) -> NemoryInfo:
    return NemoryInfo(
        version=get_nemory_version(),
        nemory_path=get_nemory_path(),
        project_info=_get_project_info(project_dir),
    )


def _get_project_info(project_dir: Path) -> NemoryProjectInfo:
    is_project_initialised = is_project_dir_valid(project_dir)

    return NemoryProjectInfo(
        project_path=project_dir,
        is_initialised=is_project_initialised,
        project_id=read_config_file(project_dir).project_id if is_project_initialised else None,
    )


def get_nemory_version() -> str:
    return version("nemory")
