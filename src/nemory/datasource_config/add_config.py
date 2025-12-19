from pathlib import Path
from typing import Any

import yaml

from nemory.introspection.property_extract import get_property_list_from_type
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, DatasourceType
from nemory.pluginlib.config import ConfigPropertyDefinition, CustomiseConfigProperties
from nemory.plugins.plugin_loader import get_plugin_for_type
from nemory.project.layout import (
    create_datasource_config_file as create_datasource_config_file_internal,
)


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
    project_dir: Path, datasource_type: DatasourceType, datasource_name: str, config_content: dict[str, Any]
) -> Path:
    basic_config = {"type": datasource_type.subtype, "name": datasource_name}

    return create_datasource_config_file_internal(
        project_dir, datasource_type, datasource_name, config_content_to_yaml_string(basic_config | config_content)
    )


def config_content_to_yaml_string(config_content: dict[str, Any]) -> str:
    return yaml.safe_dump(config_content, sort_keys=False, default_flow_style=False)
