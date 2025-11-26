from pathlib import Path
from typing import Any, Mapping

from pydantic import TypeAdapter

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, BuildFilePlugin, BuildExecutionResult


def execute_datasource_plugin(
    plugin: BuildDatasourcePlugin, full_type: str, config: Mapping[str, Any], datasource_name: str
) -> BuildExecutionResult:
    file_config = TypeAdapter(plugin.config_file_type).validate_python(config)

    return plugin.execute(
        full_type=full_type,
        datasource_name=datasource_name,
        file_config=file_config,
    )


def execute_file_plugin(plugin: BuildFilePlugin, full_type: str, file_path: Path) -> BuildExecutionResult:
    with file_path.open("rb") as fh:
        return plugin.execute(
            full_type=full_type,
            file_name=file_path.name,
            file_buffer=fh,
        )
