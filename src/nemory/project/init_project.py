import shutil
from enum import Enum
from pathlib import Path

from nemory.project.layout import get_config_file, get_examples_dir, get_logs_dir, get_source_dir
from nemory.project.project_config import ProjectConfig


class InitErrorReason(Enum):
    PROJECT_DIR_DOESNT_EXIST = "PROJECT_DIR_DOESNT_EXIST"
    PROJECT_DIR_NOT_DIRECTORY = "PROJECT_DIR_NOT_DIRECTORY"
    PROJECT_DIR_ALREADY_INITIALISED = "PROJECT_DIR_ALREADY_INITIALISED"


class InitProjectError(Exception):
    reason: InitErrorReason

    def __init__(self, reason: InitErrorReason, message: str | None):
        super().__init__(message or "")

        self.reason = reason


def init_project_dir(project_dir: Path) -> Path:
    _ensure_can_init_project(project_dir=project_dir)

    _create_default_src_dir(project_dir=project_dir)
    _create_logs_dir(project_dir=project_dir)
    _create_examples_dir(project_dir=project_dir)
    _create_nemory_config_file(project_dir=project_dir)

    return project_dir


def _ensure_can_init_project(project_dir: Path) -> bool:
    if not project_dir.exists():
        raise InitProjectError(
            message=f"{project_dir.resolve()} does not exist", reason=InitErrorReason.PROJECT_DIR_DOESNT_EXIST
        )

    if not project_dir.is_dir():
        raise InitProjectError(
            message=f"{project_dir.resolve()} is not a directory", reason=InitErrorReason.PROJECT_DIR_NOT_DIRECTORY
        )

    if get_config_file(project_dir).is_file():
        raise InitProjectError(
            message=f"Can't initialise a Nemory project in a folder that already contains a Nemory config file. [project_dir: {project_dir.resolve()}]",
            reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
        )

    if get_source_dir(project_dir).is_dir():
        raise InitProjectError(
            message=f"Can't initialise a Nemory project in a folder that already contains a src directory. [project_dir: {project_dir.resolve()}]",
            reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
        )

    if get_examples_dir(project_dir).is_file():
        raise InitProjectError(
            message=f"Can't initialise a Nemory project in a folder that already contains an examples dir. [project_dir: {project_dir.resolve()}]",
            reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
        )

    return True


def _create_default_src_dir(project_dir: Path) -> None:
    src_dir = get_source_dir(project_dir=project_dir)
    src_dir.mkdir(parents=False, exist_ok=False)

    src_dir.joinpath("databases").mkdir(parents=False, exist_ok=False)
    src_dir.joinpath("files").mkdir(parents=False, exist_ok=False)


def _create_logs_dir(project_dir: Path) -> None:
    get_logs_dir(project_dir).mkdir(exist_ok=True)


def _create_examples_dir(project_dir: Path) -> None:
    examples_dir = get_examples_dir(project_dir)
    examples_to_copy = Path(__file__).parent.joinpath("resources").joinpath("examples")

    shutil.copytree(str(examples_to_copy), str(examples_dir))


def _create_nemory_config_file(project_dir: Path) -> None:
    config_file = get_config_file(project_dir=project_dir)
    config_file.touch()

    ProjectConfig().save(config_file)
