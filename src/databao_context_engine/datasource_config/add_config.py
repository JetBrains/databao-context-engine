from pathlib import Path
from typing import Any

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import (
    create_datasource_config_file as create_datasource_config_file_internal,
)
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.serialization.yaml import to_yaml_string


def create_datasource_config_file(
    project_dir: Path,
    datasource_type: DatasourceType,
    datasource_name: str,
    config_content: dict[str, Any],
    overwrite_existing: bool,
) -> Path:
    basic_config = {"type": datasource_type.subtype, "name": datasource_name}

    return create_datasource_config_file_internal(
        project_dir,
        get_datasource_id_for_config_file(datasource_type, datasource_name),
        to_yaml_string(basic_config | config_content),
        overwrite_existing=overwrite_existing,
    )


def get_datasource_id_for_config_file(datasource_type: DatasourceType, datasource_name: str) -> DatasourceId:
    return DatasourceId(
        datasource_config_folder=datasource_type.config_folder,
        datasource_name=datasource_name,
        config_file_suffix=".yaml",
    )
