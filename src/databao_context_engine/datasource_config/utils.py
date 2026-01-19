from pathlib import Path

from databao_context_engine.project.layout import get_source_dir


def get_datasource_id_from_config_file_path(project_dir: Path, datasource_config_file_path: Path) -> str:
    return str(datasource_config_file_path.relative_to(get_source_dir(project_dir=project_dir)))


def get_datasource_id_from_main_type_and_file_name(datasource_main_type: str, config_file_name: str) -> str:
    return f"{datasource_main_type}/{config_file_name}"
