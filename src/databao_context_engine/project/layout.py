import logging
from dataclasses import dataclass
from pathlib import Path

from databao_context_engine.project.project_config import ProjectConfig
from databao_context_engine.project.types import DatasourceId

SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"
EXAMPLES_FOLDER_NAME = "examples"
LOGS_FOLDER_NAME = "logs"
DEPRECATED_CONFIG_FILE_NAME = "nemory.ini"
CONFIG_FILE_NAME = "dce.ini"
ALL_RESULTS_FILE_NAME = "all_results.yaml"


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProjectLayout:
    project_dir: Path
    config_file: Path

    def read_config_file(self) -> ProjectConfig:
        return ProjectConfig.from_file(self.config_file)


def ensure_project_dir(project_dir: Path) -> ProjectLayout:
    return _ProjectValidator(project_dir).ensure_project_dir_valid()


def create_project_dir(project_dir: Path) -> Path:
    project_dir.mkdir(parents=True, exist_ok=False)

    return project_dir


def is_project_dir_valid(project_dir: Path) -> bool:
    return validate_project_dir(project_dir) is not None


def validate_project_dir(project_dir: Path) -> ProjectLayout | None:
    return _ProjectValidator(project_dir).validate()


def get_source_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(SOURCE_FOLDER_NAME)


def get_output_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(OUTPUT_FOLDER_NAME)


def get_examples_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(EXAMPLES_FOLDER_NAME)


def get_deprecated_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(DEPRECATED_CONFIG_FILE_NAME)


def get_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(CONFIG_FILE_NAME)


def get_logs_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(LOGS_FOLDER_NAME)


def ensure_datasource_config_file_doesnt_exist(project_dir: Path, datasource_id: DatasourceId) -> Path:
    config_file = get_source_dir(project_dir).joinpath(datasource_id.relative_path_to_config_file())

    if config_file.is_file():
        raise ValueError(f"A config file already exists for {str(datasource_id)}")

    return config_file


def create_datasource_config_file(
    project_dir: Path, datasource_id: DatasourceId, config_content: str, overwrite_existing: bool
) -> Path:
    if not overwrite_existing:
        ensure_datasource_config_file_doesnt_exist(project_dir, datasource_id)

    config_file = get_source_dir(project_dir).joinpath(datasource_id.relative_path_to_config_file())
    config_file.parent.mkdir(parents=True, exist_ok=True)

    config_file.write_text(config_content)

    return config_file


class _ProjectValidator:
    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.config_file = self.get_config_file()

    def ensure_project_dir_valid(self) -> ProjectLayout:
        if not self.project_dir.is_dir():
            raise ValueError(f"The current project directory is not valid: {self.project_dir.resolve()}")

        if self.config_file is None:
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a config file. [project_dir: {self.project_dir.resolve()}]"
            )

        if not self.is_src_valid():
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a src directory. [project_dir: {self.project_dir.resolve()}]"
            )

        return ProjectLayout(project_dir=self.project_dir, config_file=self.config_file)

    def validate(self) -> ProjectLayout | None:
        if self.config_file is not None and self.is_src_valid():
            return ProjectLayout(project_dir=self.project_dir, config_file=self.config_file)
        return None

    def is_src_valid(self) -> bool:
        return get_source_dir(self.project_dir).is_dir()

    def get_config_file(self) -> Path | None:
        deprecated_config_file = get_deprecated_config_file(self.project_dir)
        if deprecated_config_file.is_file():
            logger.warning(
                f"{DEPRECATED_CONFIG_FILE_NAME} project config file is deprecated, please rename this file to {CONFIG_FILE_NAME}"
            )
            return deprecated_config_file.resolve()
        config_file = get_config_file(self.project_dir)
        if config_file.is_file():
            return config_file.resolve()
        return None
