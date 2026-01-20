from pathlib import Path

from databao_context_engine.databao_context_project_manager import DatabaoContextProjectManager
from databao_context_engine.project.init_project import init_project_dir
from databao_context_engine.project.layout import is_project_dir_valid


def init_dce_project(project_dir: Path) -> DatabaoContextProjectManager:
    initialised_project_dir = init_project_dir(project_dir=project_dir)

    return DatabaoContextProjectManager(project_dir=initialised_project_dir)


def init_or_get_dce_project(project_dir: Path) -> DatabaoContextProjectManager:
    if is_project_dir_valid(project_dir):
        return DatabaoContextProjectManager(project_dir=project_dir)

    return init_dce_project(project_dir=project_dir)
