from pathlib import Path

from databao_context_engine.project.init_project import init_project_dir
from databao_context_engine.project.layout import (
    DEPRECATED_CONFIG_FILE_NAME,
    ensure_project_dir,
    get_config_file,
    is_project_dir_valid,
    validate_project_dir,
)


def test_read_deprecated_config(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")
    project_dir.mkdir()

    init_project_dir(project_dir=project_dir)
    project_layout = ensure_project_dir(project_dir)
    expected_config = project_layout.project_config

    original_config_file = get_config_file(project_layout.project_dir)
    original_config_file.rename(original_config_file.parent / DEPRECATED_CONFIG_FILE_NAME)

    assert is_project_dir_valid(project_dir) is True

    validated_project = validate_project_dir(project_dir)
    assert validated_project is not None
    assert validated_project.project_config == expected_config
