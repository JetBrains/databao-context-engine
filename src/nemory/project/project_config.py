import configparser
import uuid
from pathlib import Path


class ProjectConfig:
    # Since we're supporting Python 3.10, we have to add a section
    # configparser.UNNAMED_SECTION was only introduced in Python 3.13
    _DEFAULT_SECTION = "DEFAULT"
    _PROJECT_ID_PROPERTY_NAME = "project-id"

    project_id: uuid.UUID

    @staticmethod
    def from_file(project_config_file: Path) -> "ProjectConfig":
        with open(project_config_file, "r") as file_stream:
            config = configparser.ConfigParser()
            config.read_file(file_stream)

            return ProjectConfig(
                project_id=uuid.UUID(config[ProjectConfig._DEFAULT_SECTION][ProjectConfig._PROJECT_ID_PROPERTY_NAME])
            )

    def __init__(self, project_id: uuid.UUID | None = None):
        self.project_id = project_id or uuid.uuid4()

    def save(self, project_config_file: Path) -> None:
        config = configparser.ConfigParser()
        config[self._DEFAULT_SECTION][self._PROJECT_ID_PROPERTY_NAME] = str(self.project_id)

        with open(project_config_file, "w") as file_stream:
            config.write(file_stream)
