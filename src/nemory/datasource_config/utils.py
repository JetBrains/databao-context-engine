from pathlib import Path

from nemory.project.layout import get_source_dir


def get_datasource_config_relative_path(project_dir: Path, datasource_config_file_path: Path) -> str:
    return str(datasource_config_file_path.relative_to(get_source_dir(project_dir=project_dir)))
