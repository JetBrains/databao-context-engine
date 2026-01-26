from pathlib import Path

from databao_context_engine.databao_context_project_manager import DatabaoContextProjectManager
from databao_context_engine.project.init_project import InitProjectError, init_project_dir
from databao_context_engine.project.layout import is_project_dir_valid


def init_dce_project(project_dir: Path) -> DatabaoContextProjectManager:
    """Initialise a Databao Context project in the given directory.

    Args:
        project_dir: The directory where the project should be initialised.

    Returns:
        A DatabaoContextProjectManager for the initialised project.

    Raises:
        InitProjectError: If the project could not be initialised.
    """
    try:
        initialised_project_dir = init_project_dir(project_dir=project_dir)

        return DatabaoContextProjectManager(project_dir=initialised_project_dir)
    except InitProjectError as e:
        raise e


def init_or_get_dce_project(project_dir: Path) -> DatabaoContextProjectManager:
    """Initialise a Databao Context project in the given directory or get a DatabaoContextProjectManager for the project if it already exists.

    Args:
        project_dir: The directory where the project should be initialised if it doesn't exist yet..

    Returns:
        A DatabaoContextProjectManager for the project in project_dir.
    """
    if is_project_dir_valid(project_dir):
        return DatabaoContextProjectManager(project_dir=project_dir)

    return init_dce_project(project_dir=project_dir)
