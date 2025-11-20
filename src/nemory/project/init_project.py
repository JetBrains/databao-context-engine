import shutil
from pathlib import Path

from nemory.project.layout import ensure_can_init_project, get_source_dir, get_config_file, get_logs_dir
from nemory.project.project_config import ProjectConfig


def init_project_dir(project_dir: str) -> None:
    ensure_can_init_project(project_dir=project_dir)

    project_path = Path(project_dir)
    _create_default_src_dir(project_dir=project_path)
    _create_logs_dir(project_dir=project_path)
    _create_examples_dir(project_dir=project_path)
    _create_nemory_config_file(project_dir=project_path)


def _create_default_src_dir(project_dir: Path) -> None:
    src_dir = get_source_dir(project_dir=project_dir)
    src_dir.mkdir(parents=False, exist_ok=False)

    src_dir.joinpath("databases").mkdir(parents=False, exist_ok=False)
    src_dir.joinpath("files").mkdir(parents=False, exist_ok=False)


def _create_logs_dir(project_dir: Path) -> None:
    get_logs_dir(project_dir).mkdir(exist_ok=True)


def _create_examples_dir(project_dir: Path) -> None:
    examples_dir = project_dir.joinpath("examples")
    examples_to_copy = Path(__file__).parent.joinpath("resources").joinpath("examples")

    shutil.copytree(str(examples_to_copy), str(examples_dir))


def _create_nemory_config_file(project_dir: Path) -> None:
    config_file = get_config_file(project_dir=project_dir)
    config_file.touch()

    ProjectConfig().save(config_file)
