import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from pydantic import TypeAdapter

from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    DatasourceType,
    SqlRunnablePlugin,
)
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult

logger = logging.getLogger(__name__)


def execute_datasource_plugin(
    plugin: BuildDatasourcePlugin, datasource_type: DatasourceType, config: Mapping[str, Any], datasource_name: str
) -> Any:
    if not isinstance(plugin, BuildDatasourcePlugin):
        raise ValueError("This method can only execute a BuildDatasourcePlugin")

    validated_config = _validate_datasource_config_file(config, plugin)

    return plugin.build_context(
        full_type=datasource_type.full_type,
        datasource_name=datasource_name,
        file_config=validated_config,
    )


def check_connection_for_datasource(
    plugin: BuildDatasourcePlugin, datasource_type: DatasourceType, config: Mapping[str, Any], datasource_name: str
) -> None:
    if not isinstance(plugin, BuildDatasourcePlugin):
        raise ValueError("Connection checks can only be performed on BuildDatasourcePlugin")

    validated_config = _validate_datasource_config_file(config, plugin)

    plugin.check_connection(
        full_type=datasource_type.full_type,
        datasource_name=datasource_name,
        file_config=validated_config,
    )


def _validate_datasource_config_file(config: Mapping[str, Any], plugin: BuildDatasourcePlugin) -> Any:
    return TypeAdapter(plugin.config_file_type).validate_python(config)


def execute_file_plugin(plugin: BuildFilePlugin, datasource_type: DatasourceType, file_path: Path) -> Any:
    with file_path.open("rb") as fh:
        return plugin.build_file_context(
            full_type=datasource_type.full_type,
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


def execute_sql_for_datasource(
    plugin: BuildDatasourcePlugin,
    datasource_type: DatasourceType,
    config: Mapping[str, Any],
    sql: str,
    params: list[Any] | None = None,
    read_only: bool = True,
) -> SqlExecutionResult:
    if not isinstance(plugin, SqlRunnablePlugin):
        raise ValueError("Sql query execution can only be performed on SqlRunnablePlugin")

    validated_config = _validate_datasource_config_file(config, plugin)

    return plugin.run_sql(
        file_config=validated_config,
        sql=sql,
        params=params,
        read_only=read_only,
    )
