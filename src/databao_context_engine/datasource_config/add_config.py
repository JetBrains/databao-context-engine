from pathlib import Path
from typing import Any

from databao_context_engine.introspection.property_extract import get_property_list_from_type
from databao_context_engine.pluginlib.build_plugin import BuildDatasourcePlugin, DatasourceType
from databao_context_engine.pluginlib.config import ConfigPropertyDefinition, CustomiseConfigProperties
from databao_context_engine.plugins.plugin_loader import get_plugin_for_type
from databao_context_engine.project.layout import (
    create_datasource_config_file as create_datasource_config_file_internal,
)
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.serialisation.yaml import to_yaml_string


def get_config_file_structure_for_datasource_type(datasource_type: DatasourceType) -> list[ConfigPropertyDefinition]:
    plugin = get_plugin_for_type(datasource_type)

    if isinstance(plugin, CustomiseConfigProperties):
        return plugin.get_config_file_properties()
    elif isinstance(plugin, BuildDatasourcePlugin):
        return get_property_list_from_type(plugin.config_file_type)
    else:
        raise ValueError(
            f"Impossible to create a config for type {datasource_type.full_type}. The plugin for this type is not a BuildDatasourcePlugin or CustomiseConfigProperties"
        )


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
