from datetime import datetime
from pathlib import Path

from nemory.project.project_config import ProjectConfig

SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"
EXAMPLES_FOLDER_NAME = "examples"
LOGS_FOLDER_NAME = "logs"
CONFIG_FILE_NAME = "nemory.ini"
RUN_DIR_PREFIX = "run-"
ALL_RESULTS_FILE_NAME = "all_results.yaml"


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


def is_project_dir_valid(project_dir: Path) -> bool:
    return get_config_file(project_dir).is_file() and get_source_dir(project_dir).is_dir()


def ensure_can_init_project(project_dir: str) -> bool:
    project_path = Path(project_dir)

    if not project_path.is_dir():
        raise ValueError(f"{project_path.resolve()} does not exist or is not a directory")

    if get_source_dir(project_path).is_dir():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a src directory. [project_dir: {project_path.resolve()}]"
        )

    if get_config_file(project_path).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a Nemory config file. [project_dir: {project_path.resolve()}]"
        )

    if get_examples_dir(project_path).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains an examples dir. [project_dir: {project_path.resolve()}]"
        )

    return True


def get_source_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(SOURCE_FOLDER_NAME)


def get_output_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(OUTPUT_FOLDER_NAME)


def get_run_dir_name(build_time: datetime) -> str:
    return f"{RUN_DIR_PREFIX}{build_time.isoformat(timespec='seconds')}"


def get_run_dir(project_dir: Path, run_name: str) -> Path:
    run_dir = get_output_dir(project_dir).joinpath(run_name)
    if not run_dir.is_dir():
        raise ValueError(
            f"The run with name {run_name} doesn't exist in the project. [project_dir: {project_dir.resolve()}]"
        )

    return run_dir


def get_latest_run_name(project_path: Path) -> str:
    output_dir = get_output_dir(project_path)

    if not output_dir.is_dir():
        raise ValueError(f"No build run exist in the project. [project_dir: {project_path.resolve()}]")

    sorted_output_dirs = sorted(
        (
            child_path.name
            for child_path in output_dir.iterdir()
            if child_path.is_dir() and child_path.name.startswith(RUN_DIR_PREFIX)
        ),
        reverse=True,
    )

    if len(sorted_output_dirs) == 0:
        raise ValueError(f"No build run exist in the project. [project_dir: {project_path.resolve()}]")

    return sorted_output_dirs[0]


def get_examples_dir(project_path: Path) -> Path:
    return project_path.joinpath(EXAMPLES_FOLDER_NAME)


def get_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(CONFIG_FILE_NAME)


def get_logs_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(LOGS_FOLDER_NAME)


def read_config_file(project_dir: Path) -> ProjectConfig:
    return ProjectConfig.from_file(get_config_file(project_dir))
