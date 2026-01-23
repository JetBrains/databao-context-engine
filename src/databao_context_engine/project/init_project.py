import shutil
from enum import Enum
from pathlib import Path

from databao_context_engine.project.layout import (
    get_config_file,
    get_deprecated_config_file,
    get_examples_dir,
    get_logs_dir,
    get_source_dir,
)
from databao_context_engine.project.project_config import ProjectConfig


class InitErrorReason(Enum):
    """Reasons for which project initialisation can fail."""

    PROJECT_DIR_DOESNT_EXIST = "PROJECT_DIR_DOESNT_EXIST"
    PROJECT_DIR_NOT_DIRECTORY = "PROJECT_DIR_NOT_DIRECTORY"
    PROJECT_DIR_ALREADY_INITIALISED = "PROJECT_DIR_ALREADY_INITIALISED"


class InitProjectError(Exception):
    """Raised when a project can't be initialised.

    Attributes:
        message: The error message.
        reason: The reason for the initialisation failure.
    """

    reason: InitErrorReason

    def __init__(self, reason: InitErrorReason, message: str | None):
        super().__init__(message or "")

        self.reason = reason


def init_project_dir(project_dir: Path) -> Path:
    project_creator = _ProjectCreator(project_dir=project_dir)
    project_creator.create()

    return project_dir


class _ProjectCreator:
    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.deprecated_config_file = get_deprecated_config_file(project_dir)
        self.config_file = get_config_file(project_dir)
        self.src_dir = get_source_dir(project_dir)
        self.examples_dir = get_examples_dir(project_dir)
        self.logs_dir = get_logs_dir(project_dir)

    def create(self):
        self.ensure_can_init_project()

        self.create_default_src_dir()
        self.create_logs_dir()
        self.create_examples_dir()
        self.create_dce_config_file()

    def ensure_can_init_project(self) -> bool:
        if not self.project_dir.exists():
            raise InitProjectError(
                message=f"{self.project_dir.resolve()} does not exist", reason=InitErrorReason.PROJECT_DIR_DOESNT_EXIST
            )

        if not self.project_dir.is_dir():
            raise InitProjectError(
                message=f"{self.project_dir.resolve()} is not a directory",
                reason=InitErrorReason.PROJECT_DIR_NOT_DIRECTORY,
            )

        if self.config_file.is_file() or self.deprecated_config_file.is_file():
            raise InitProjectError(
                message=f"Can't initialise a Databao Context Engine project in a folder that already contains a config file. [project_dir: {self.project_dir.resolve()}]",
                reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
            )

        if self.src_dir.is_dir():
            raise InitProjectError(
                message=f"Can't initialise a Databao Context Engine project in a folder that already contains a src directory. [project_dir: {self.project_dir.resolve()}]",
                reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
            )

        if self.examples_dir.is_file():
            raise InitProjectError(
                message=f"Can't initialise a Databao Context Engine project in a folder that already contains an examples dir. [project_dir: {self.project_dir.resolve()}]",
                reason=InitErrorReason.PROJECT_DIR_ALREADY_INITIALISED,
            )

        return True

    def create_default_src_dir(self) -> None:
        self.src_dir.mkdir(parents=False, exist_ok=False)

        self.src_dir.joinpath("databases").mkdir(parents=False, exist_ok=False)
        self.src_dir.joinpath("files").mkdir(parents=False, exist_ok=False)

    def create_logs_dir(self) -> None:
        self.logs_dir.mkdir(exist_ok=True)

    def create_examples_dir(self) -> None:
        examples_to_copy = Path(__file__).parent.joinpath("resources").joinpath("examples")

        shutil.copytree(str(examples_to_copy), str(self.examples_dir))

    def create_dce_config_file(self) -> None:
        self.config_file.touch()
        ProjectConfig().save(self.config_file)
