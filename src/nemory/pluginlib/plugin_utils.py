import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from pydantic import TypeAdapter

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, BuildExecutionResult, BuildFilePlugin

logger = logging.getLogger(__name__)


def execute_datasource_plugin(
    plugin: BuildDatasourcePlugin, full_type: str, config: Mapping[str, Any], datasource_name: str
) -> BuildExecutionResult:
    if not isinstance(plugin, BuildDatasourcePlugin):
        raise ValueError("This method can only execute a BuildDatasourcePlugin")

    validated_config = _validate_datasource_config_file(config, plugin)

    return plugin.execute(
        full_type=full_type,
        datasource_name=datasource_name,
        file_config=validated_config,
    )


def check_connection_for_datasource(
    plugin: BuildDatasourcePlugin, full_type: str, config: Mapping[str, Any], datasource_name: str
) -> None:
    if not isinstance(plugin, BuildDatasourcePlugin):
        raise ValueError("Connection checks can only be performed on BuildDatasourcePlugin")

    validated_config = _validate_datasource_config_file(config, plugin)

    plugin.check_connection(
        full_type=full_type,
        datasource_name=datasource_name,
        file_config=validated_config,
    )


def _validate_datasource_config_file(config: Mapping[str, Any], plugin: BuildDatasourcePlugin) -> Any:
    return TypeAdapter(plugin.config_file_type).validate_python(config)


def execute_file_plugin(plugin: BuildFilePlugin, full_type: str, file_path: Path) -> BuildExecutionResult:
    with file_path.open("rb") as fh:
        return plugin.execute(
            full_type=full_type,
            file_name=file_path.name,
            file_buffer=fh,
        )


def generate_json_schema(plugin: BuildDatasourcePlugin, pretty_print: bool = True) -> str | None:
    if plugin.config_file_type == dict[str, Any]:
        logger.debug(f"Skipping json schema generation for plugin {plugin.id}: no custom config_file_type provided")
        return None

    json_schema = TypeAdapter(plugin.config_file_type).json_schema(mode="serialization")

    return json.dumps(json_schema, indent=4 if pretty_print else None)


def format_json_schema_for_output(plugin: BuildDatasourcePlugin, json_schema: str) -> str:
    return os.linesep.join([f"JSON Schema for plugin {plugin.id}:", json_schema])
