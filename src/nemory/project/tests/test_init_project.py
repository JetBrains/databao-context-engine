import uuid
from pathlib import Path

import pytest

from nemory.project.init_project import init_project_dir
from nemory.project.project_config import ProjectConfig


def test_init_project_dir(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")
    project_dir.mkdir()

    assert project_dir.is_dir()

    init_project_dir(project_dir=str(project_dir))

    assert project_dir.is_dir()

    src_dir = project_dir.joinpath("src")
    assert src_dir.is_dir()
    assert src_dir.joinpath("files").is_dir()
    assert src_dir.joinpath("databases").is_dir()

    examples_dir = project_dir.joinpath("examples")
    assert examples_dir.is_dir()
    assert len(list(examples_dir.joinpath("src").joinpath("files").iterdir())) == 2
    assert examples_dir.joinpath("src").joinpath("databases").joinpath("example_postgres.yaml").is_file()

    config_file = project_dir.joinpath("nemory.ini")
    assert config_file.is_file()
    assert isinstance(ProjectConfig.from_file(config_file).project_id, uuid.UUID)


def test_init_project_dir_fails_when_dir_doesnt_exist(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")

    assert not project_dir.is_dir()

    with pytest.raises(ValueError):
        init_project_dir(project_dir=str(project_dir))


def test_init_project_dir_fails_when_dir_already_has_a_config(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")
    project_dir.mkdir()

    config_file = project_dir.joinpath("nemory.ini")
    config_file.touch()

    assert project_dir.is_dir()
    assert config_file.is_file()

    with pytest.raises(ValueError):
        init_project_dir(project_dir=str(project_dir))


def test_init_project_dir_fails_when_dir_already_has_a_src_dir(tmp_path: Path):
    project_dir = tmp_path.joinpath("project")
    project_dir.mkdir()

    src_dir = project_dir.joinpath("src")
    src_dir.mkdir()

    assert project_dir.is_dir()
    assert src_dir.is_dir()

    with pytest.raises(ValueError):
        init_project_dir(project_dir=str(project_dir))
