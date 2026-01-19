from pathlib import Path

from databao_context_engine.project.init_project import init_project_dir
from databao_context_engine.project.layout import (
    DEPRECATED_CONFIG_FILE_NAME,
    ensure_project_dir,
    is_project_dir_valid,
    validate_project_dir,
)


def test_read_deprecated_config(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")
    project_dir.mkdir()

    init_project_dir(project_dir=project_dir)
    project_layout = ensure_project_dir(project_dir)
    project_layout.config_file.rename(project_layout.config_file.parent / DEPRECATED_CONFIG_FILE_NAME)

    assert is_project_dir_valid(project_dir) is True

    assert validate_project_dir(project_dir).config_file.name == DEPRECATED_CONFIG_FILE_NAME
    assert ensure_project_dir(project_dir).config_file.name == DEPRECATED_CONFIG_FILE_NAME
