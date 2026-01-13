from pathlib import Path

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.project.layout import get_source_dir


def get_datasource_id_from_config_file_path(project_dir: Path, datasource_config_file_path: Path) -> str:
    return str(datasource_config_file_path.relative_to(get_source_dir(project_dir=project_dir)))


def get_datasource_id_from_type_and_name(datasource_type: DatasourceType, datasource_name: str) -> str:
    return f"{datasource_type.config_folder}/{datasource_name}"
