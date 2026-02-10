import uuid
from pathlib import Path

from databao_context_engine.project.project_config import ProjectConfig


def test_project_config(tmp_path: Path) -> None:
    new_project_config = ProjectConfig()

    project_config_path = tmp_path.joinpath("project_config.ini")

    assert not project_config_path.is_file()
    new_project_config.save(project_config_path)
    assert project_config_path.is_file()

    read_project_config = ProjectConfig.from_file(project_config_path)

    assert read_project_config.project_id == new_project_config.project_id


def test_project_config_with_provided_id(tmp_path: Path) -> None:
    new_project_id = uuid.uuid4()
    new_project_config = ProjectConfig(project_id=new_project_id)

    project_config_path = tmp_path.joinpath("project_config.ini")

    assert not project_config_path.is_file()
    new_project_config.save(project_config_path)
    assert project_config_path.is_file()

    read_project_config = ProjectConfig.from_file(project_config_path)

    assert read_project_config.project_id == new_project_id


def test_project_config_with_ollama_params(tmp_path: Path) -> None:
    project_config = ProjectConfig(ollama_model_id="EmbeddingGemma:300m", ollama_model_dim=768)
    project_config_path = tmp_path.joinpath("project_config.ini")
    project_config.save(project_config_path)
    assert project_config_path.is_file()

    read_project_config = ProjectConfig.from_file(project_config_path)

    assert read_project_config == project_config
