from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

from databao_context_engine.project.layout import validate_project_dir
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
    project_layout = validate_project_dir(project_dir)

    return DceProjectInfo(
        project_path=project_dir,
        is_initialised=project_layout is not None,
        project_id=project_layout.read_config_file().project_id if project_layout is not None else None,
    )


def get_dce_version() -> str:
    return version("databao_context_engine")
