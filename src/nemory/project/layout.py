from pathlib import Path

from nemory.project.project_config import ProjectConfig

SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"
EXAMPLES_FOLDER_NAME = "examples"
CONFIG_FILE_NAME = "nemory.ini"


def ensure_project_dir(project_dir: str, should_be_initialised: bool = True) -> Path:
    project_path = Path(project_dir)

    if not project_path.is_dir():
        raise ValueError(f"The current project directory is not valid: {project_path.resolve()}")

    if should_be_initialised:
        if not get_config_file(project_path).is_file():
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a config file. [project_dir: {project_path.resolve()}]"
            )

        if not get_source_dir(project_path).is_dir():
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a src directory. [project_dir: {project_path.resolve()}]"
            )

    return project_path


def ensure_can_init_project(project_dir: str) -> bool:
    project_path = Path(project_dir)

    if not project_path.is_dir():
        raise ValueError(f"{project_path.resolve()} does not exist or is not a directory")

    if get_source_dir(project_path).is_dir():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a src directory. [project_dir: {project_path.resolve()}"
        )

    if get_config_file(project_path).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a Nemory config file. [project_dir: {project_path.resolve()}"
        )

    if get_examples_dir(project_path).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains an examples dir. [project_dir: {project_path.resolve()}"
        )

    return True


def get_source_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(SOURCE_FOLDER_NAME)


def get_output_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(OUTPUT_FOLDER_NAME)


def get_examples_dir(project_path: Path) -> Path:
    return project_path.joinpath(EXAMPLES_FOLDER_NAME)


def get_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(CONFIG_FILE_NAME)


def read_config_file(project_dir: Path) -> ProjectConfig:
    return ProjectConfig.from_file(get_config_file(project_dir))


def get_db_path() -> Path:
    return Path("~/.nemory/nemory.duckdb")
