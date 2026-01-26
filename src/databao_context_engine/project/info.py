from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

from databao_context_engine.project.layout import validate_project_dir
from databao_context_engine.system.properties import get_dce_path


@dataclass(kw_only=True, frozen=True)
class DceProjectInfo:
    """Information about a Databao Context Engine project.

    Attributes:
        project_path: The root directory of the Databao Context Engine project.
        is_initialized: Whether the project has been initialized.
        project_id: The UUID of the project, or None if the project has not been initialized.
    """

    project_path: Path
    is_initialized: bool
    project_id: UUID | None


@dataclass(kw_only=True, frozen=True)
class DceInfo:
    """Information about the current Databao Context Engine installation and project.

    Attributes:
        version: The version of the databao_context_engine package installed on the system.
        dce_path: The path where databao_context_engine stores its global data.
        project_info: Information about the Databao Context Engine project.
    """

    version: str
    dce_path: Path

    project_info: DceProjectInfo


def get_databao_context_engine_info(project_dir: Path) -> DceInfo:
    """Return information about the current Databao Context Engine installation and project.

    Args:
        project_dir: The root directory of the Databao Context Project.

    Returns:
        A DceInfo instance containing information about the Databao Context Engine installation and project.
    """
    return DceInfo(
        version=get_dce_version(),
        dce_path=get_dce_path(),
        project_info=_get_project_info(project_dir),
    )


def _get_project_info(project_dir: Path) -> DceProjectInfo:
    project_layout = validate_project_dir(project_dir)

    return DceProjectInfo(
        project_path=project_dir,
        is_initialized=project_layout is not None,
        project_id=project_layout.read_config_file().project_id if project_layout is not None else None,
    )


def get_dce_version() -> str:
    """Return the installed version of the databao_context_engine package.

    Returns:
        The installed version of the databao_context_engine package.
    """
    return version("databao_context_engine")
