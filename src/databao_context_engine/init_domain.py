from pathlib import Path

from databao_context_engine.databao_context_domain_manager import DatabaoContextDomainManager
from databao_context_engine.project.init_project import InitDomainError, init_project_dir
from databao_context_engine.project.layout import is_project_dir_valid


def init_dce_domain(
    domain_dir: Path, ollama_model_id: str | None = None, ollama_model_dim: int | None = None
) -> DatabaoContextDomainManager:
    """Initialize a Databao Context domain in the given directory.

    Args:
        domain_dir: The directory where the domain should be initialized.
        ollama_model_id: The default value for ollama model-id which will be stored in the domain config file.
        ollama_model_dim: The default value for ollama model dimenstions which will be stored in the domain config file.

    Returns:
        A DatabaoContextDomainManager for the initialized domain.

    Raises:
        InitDomainError: If the project could not be initialized.
    """
    try:
        initialized_project_dir = init_project_dir(
            project_dir=domain_dir, ollama_model_id=ollama_model_id, ollama_model_dim=ollama_model_dim
        )

        return DatabaoContextDomainManager(domain_dir=initialized_project_dir)
    except InitDomainError as e:
        raise e


def init_or_get_dce_domain(project_dir: Path) -> DatabaoContextDomainManager:
    """Initialize a Databao Context project in the given directory or get a DatabaoContextProjectManager for the project if it already exists.

    Args:
        project_dir: The directory where the project should be initialized if it doesn't exist yet.

    Returns:
        A DatabaoContextProjectManager for the project in project_dir.
    """
    if is_project_dir_valid(project_dir):
        return DatabaoContextDomainManager(domain_dir=project_dir)

    return init_dce_domain(domain_dir=project_dir)
