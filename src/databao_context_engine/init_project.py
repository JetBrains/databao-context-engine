from pathlib import Path

from databao_context_engine.databao_context_project_manager import DatabaoContextProjectManager
from databao_context_engine.project.init_project import InitProjectError, init_project_dir
from databao_context_engine.project.layout import is_project_dir_valid


def init_dce_project(
    project_dir: Path, ollama_model_id: str | None = None, ollama_model_dim: int | None = None
) -> DatabaoContextProjectManager:
    """Initialize a Databao Context project in the given directory.

    Args:
        project_dir: The directory where the project should be initialized.
        ollama_model_id: The default value for ollama model-id which will be stored in the project config file.
        ollama_model_dim: The default value for ollama model dimenstions which will be stored in the project config file.

    Returns:
        A DatabaoContextProjectManager for the initialized project.

    Raises:
        InitProjectError: If the project could not be initialized.
    """
    try:
        initialized_project_dir = init_project_dir(
            project_dir=project_dir, ollama_model_id=ollama_model_id, ollama_model_dim=ollama_model_dim
        )

        return DatabaoContextProjectManager(project_dir=initialized_project_dir)
    except InitProjectError as e:
        raise e


def init_or_get_dce_project(project_dir: Path) -> DatabaoContextProjectManager:
    """Initialize a Databao Context project in the given directory or get a DatabaoContextProjectManager for the project if it already exists.

    Args:
        project_dir: The directory where the project should be initialized if it doesn't exist yet.

    Returns:
        A DatabaoContextProjectManager for the project in project_dir.
    """
    if is_project_dir_valid(project_dir):
        return DatabaoContextProjectManager(project_dir=project_dir)

    return init_dce_project(project_dir=project_dir)
