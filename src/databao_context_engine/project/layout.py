from pathlib import Path

from databao_context_engine.project.project_config import ProjectConfig
from databao_context_engine.project.types import DatasourceId

SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"
EXAMPLES_FOLDER_NAME = "examples"
LOGS_FOLDER_NAME = "logs"
CONFIG_FILE_NAME = "nemory.ini"
ALL_RESULTS_FILE_NAME = "all_results.yaml"


def ensure_project_dir(project_dir: Path) -> Path:
    if not project_dir.is_dir():
        raise ValueError(f"The current project directory is not valid: {project_dir.resolve()}")

    if not get_config_file(project_dir).is_file():
        raise ValueError(
            f"The current project directory has not been initialised. It should contain a config file. [project_dir: {project_dir.resolve()}]"
        )

    if not get_source_dir(project_dir).is_dir():
        raise ValueError(
            f"The current project directory has not been initialised. It should contain a src directory. [project_dir: {project_dir.resolve()}]"
        )

    return project_dir


def create_project_dir(project_dir: Path) -> Path:
    project_dir.mkdir(parents=True, exist_ok=False)

    return project_dir


def is_project_dir_valid(project_dir: Path) -> bool:
    return get_config_file(project_dir).is_file() and get_source_dir(project_dir).is_dir()


def get_source_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(SOURCE_FOLDER_NAME)


def get_output_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(OUTPUT_FOLDER_NAME)


def get_examples_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(EXAMPLES_FOLDER_NAME)


def get_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(CONFIG_FILE_NAME)


def get_logs_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(LOGS_FOLDER_NAME)


def read_config_file(project_dir: Path) -> ProjectConfig:
    return ProjectConfig.from_file(get_config_file(project_dir))


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
