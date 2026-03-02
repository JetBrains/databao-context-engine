import configparser
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from databao_context_engine.llm.config import EmbeddingModelDetails


@dataclass(kw_only=True, frozen=True)
class ProjectConfig:
    # Since we're supporting Python 3.10, we have to add a section
    # configparser.UNNAMED_SECTION was only introduced in Python 3.13
    _DEFAULT_SECTION: ClassVar[str] = "DEFAULT"
    _PROJECT_ID_PROPERTY_NAME: ClassVar[str] = "project-id"
    _OLLAMA_MODEL_ID_PROPERTY_NAME: ClassVar[str] = "ollama-model-id"
    _OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME: ClassVar[str] = "ollama-model-dim"

    project_id: uuid.UUID
    ollama_embedding_model_details: EmbeddingModelDetails

    @staticmethod
    def from_file(project_config_file: Path) -> "ProjectConfig":
        with open(project_config_file, "r") as file_stream:
            config = configparser.ConfigParser()
            config.read_file(file_stream)

            section = config[ProjectConfig._DEFAULT_SECTION]

            model_id = section.get(ProjectConfig._OLLAMA_MODEL_ID_PROPERTY_NAME, None)
            dim = section.get(ProjectConfig._OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME, None)
            if model_id is None and dim is None:
                embedding_model_details = EmbeddingModelDetails.default()
            elif model_id is not None and dim is not None:
                embedding_model_details = EmbeddingModelDetails(model_id=model_id, model_dim=int(dim))
            else:
                raise ValueError(
                    f"Both {ProjectConfig._OLLAMA_MODEL_ID_PROPERTY_NAME} and {ProjectConfig._OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME} must be declared together"
                )

            return ProjectConfig(
                project_id=uuid.UUID(section[ProjectConfig._PROJECT_ID_PROPERTY_NAME]),
                ollama_embedding_model_details=embedding_model_details,
            )

    @staticmethod
    def save_config_file(
        project_config_file: Path,
        *,
        project_id: uuid.UUID | None = None,
        ollama_model_id: str | None = None,
        ollama_model_dim: int | None = None,
    ) -> None:
        config = configparser.ConfigParser()
        section = config[ProjectConfig._DEFAULT_SECTION]
        section[ProjectConfig._PROJECT_ID_PROPERTY_NAME] = str(project_id or uuid.uuid4())

        if ollama_model_id is not None:
            section[ProjectConfig._OLLAMA_MODEL_ID_PROPERTY_NAME] = str(ollama_model_id)

        if ollama_model_dim is not None:
            section[ProjectConfig._OLLAMA_MODEL_DIMENSIONS_PROPERTY_NAME] = str(ollama_model_dim)

        with open(project_config_file, "w") as file_stream:
            config.write(file_stream)
