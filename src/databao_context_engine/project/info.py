from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

from databao_context_engine.project.layout import is_project_dir_valid, read_config_file
from databao_context_engine.system.properties import get_dce_path


@dataclass(kw_only=True, frozen=True)
class DceProjectInfo:
    project_path: Path
    is_initialised: bool
    project_id: UUID | None


@dataclass(kw_only=True, frozen=True)
class DceInfo:
    version: str
    dce_path: Path

    project_info: DceProjectInfo


def get_command_info(project_dir: Path) -> DceInfo:
    return DceInfo(
        version=get_dce_version(),
        dce_path=get_dce_path(),
        project_info=_get_project_info(project_dir),
    )


def _get_project_info(project_dir: Path) -> DceProjectInfo:
    is_project_initialised = is_project_dir_valid(project_dir)

    return DceProjectInfo(
        project_path=project_dir,
        is_initialised=is_project_initialised,
        project_id=read_config_file(project_dir).project_id if is_project_initialised else None,
    )


def get_dce_version() -> str:
    return version("databao_context_engine")
