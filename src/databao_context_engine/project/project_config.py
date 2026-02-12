import configparser
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ProjectConfig:
    # Since we're supporting Python 3.10, we have to add a section
    # configparser.UNNAMED_SECTION was only introduced in Python 3.13
    _DEFAULT_SECTION = "DEFAULT"
    _PROJECT_ID_PROPERTY_NAME = "project-id"
    _OLLAMA_MODEL_ID_PROPERTY_NAME = "ollama-model-id"
    _OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME = "ollama-model-dim"

    project_id: uuid.UUID
    ollama_model_id: str | None
    ollama_model_dim: int | None

    @staticmethod
    def from_file(project_config_file: Path) -> "ProjectConfig":
        with open(project_config_file, "r") as file_stream:
            config = configparser.ConfigParser()
            config.read_file(file_stream)

            section = config[ProjectConfig._DEFAULT_SECTION]
            dim = section.get(ProjectConfig._OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME, None)
            return ProjectConfig(
                project_id=uuid.UUID(section[ProjectConfig._PROJECT_ID_PROPERTY_NAME]),
                ollama_model_id=section.get(ProjectConfig._OLLAMA_MODEL_ID_PROPERTY_NAME, None),
                ollama_model_dim=int(dim) if dim else None,
            )

    def __init__(
        self,
        project_id: uuid.UUID | None = None,
        ollama_model_id: str | None = None,
        ollama_model_dim: int | None = None,
    ):
        self.project_id = project_id or uuid.uuid4()
        self.ollama_model_id = ollama_model_id
        self.ollama_model_dim = ollama_model_dim

    def save(self, project_config_file: Path) -> None:
        config = configparser.ConfigParser()
        section = config[self._DEFAULT_SECTION]
        section[self._PROJECT_ID_PROPERTY_NAME] = str(self.project_id)

        if self.ollama_model_id is not None:
            section[self._OLLAMA_MODEL_ID_PROPERTY_NAME] = str(self.ollama_model_id)

        if self.ollama_model_dim is not None:
            section[self._OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME] = str(self.ollama_model_dim)

        with open(project_config_file, "w") as file_stream:
            config.write(file_stream)
