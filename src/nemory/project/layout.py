from pathlib import Path

from nemory.project.project_config import ProjectConfig

SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"
EXAMPLES_FOLDER_NAME = "examples"
LOGS_FOLDER_NAME = "logs"
CONFIG_FILE_NAME = "nemory.ini"
ALL_RESULTS_FILE_NAME = "all_results.yaml"


def ensure_project_dir(project_dir: Path, should_be_initialised: bool = True) -> Path:
    if not project_dir.is_dir():
        raise ValueError(f"The current project directory is not valid: {project_dir.resolve()}")

    if should_be_initialised:
        if not get_config_file(project_dir).is_file():
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a config file. [project_dir: {project_dir.resolve()}]"
            )

        if not get_source_dir(project_dir).is_dir():
            raise ValueError(
                f"The current project directory has not been initialised. It should contain a src directory. [project_dir: {project_dir.resolve()}]"
            )

    return project_dir


def is_project_dir_valid(project_dir: Path) -> bool:
    return get_config_file(project_dir).is_file() and get_source_dir(project_dir).is_dir()


def ensure_can_init_project(project_dir: Path) -> bool:
    if get_source_dir(project_dir).is_dir():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a src directory. [project_dir: {project_dir.resolve()}]"
        )

    if get_config_file(project_dir).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains a Nemory config file. [project_dir: {project_dir.resolve()}]"
        )

    if get_examples_dir(project_dir).is_file():
        raise ValueError(
            f"Can't initialise a Nemory project in a folder that already contains an examples dir. [project_dir: {project_dir.resolve()}]"
        )

    return True


def get_source_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(SOURCE_FOLDER_NAME)


def get_output_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(OUTPUT_FOLDER_NAME)


def get_run_dir(project_dir: Path, run_name: str) -> Path:
    run_dir = get_output_dir(project_dir).joinpath(run_name)
    if not run_dir.is_dir():
        raise ValueError(
            f"The run with name {run_name} doesn't exist in the project. [project_dir: {project_dir.resolve()}]"
        )

    return run_dir


def get_examples_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(EXAMPLES_FOLDER_NAME)


def get_config_file(project_dir: Path) -> Path:
    return project_dir.joinpath(CONFIG_FILE_NAME)


def get_logs_dir(project_dir: Path) -> Path:
    return project_dir.joinpath(LOGS_FOLDER_NAME)


def read_config_file(project_dir: Path) -> ProjectConfig:
    return ProjectConfig.from_file(get_config_file(project_dir))


def _get_datasource_config_file(project_dir: Path, config_folder_name: str, datasource_name: str):
    src_dir = get_source_dir(project_dir)

    return src_dir.joinpath(config_folder_name).joinpath(f"{datasource_name}.yaml")


def ensure_datasource_config_file_doesnt_exist(
    project_dir: Path, config_folder_name: str, datasource_name: str
) -> Path:
    config_file = _get_datasource_config_file(project_dir, config_folder_name, datasource_name)

    if config_file.is_file():
        raise ValueError(f"A config file already exists for {datasource_name} in {config_folder_name}")

    return config_file


def create_datasource_config_file(
    project_dir: Path, config_folder_name: str, datasource_name: str, config_content: str
) -> Path:
    config_file = ensure_datasource_config_file_doesnt_exist(project_dir, config_folder_name, datasource_name)
    config_file.parent.mkdir(parents=True, exist_ok=True)

    config_file.touch()
    config_file.write_text(config_content)

    return config_file
