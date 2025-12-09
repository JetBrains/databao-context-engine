from typing import cast

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    BuildFilePlugin,
    BuildPlugin,
)
from nemory.pluginlib.plugin_utils import execute_datasource_plugin, execute_file_plugin
from nemory.project.types import PreparedConfig, PreparedDatasource


def execute(prepared_datasource: PreparedDatasource, plugin: BuildPlugin) -> BuildExecutionResult:
    """
    Run a prepared source through the plugin
    """
    if isinstance(prepared_datasource, PreparedConfig):
        ds_plugin = cast(BuildDatasourcePlugin, plugin)

        return execute_datasource_plugin(
            plugin=ds_plugin,
            full_type=prepared_datasource.full_type,
            config=prepared_datasource.config,
            datasource_name=prepared_datasource.datasource_name,
        )
    else:
        file_plugin = cast(BuildFilePlugin, plugin)
        return execute_file_plugin(
            plugin=file_plugin,
            full_type=prepared_datasource.full_type,
            file_path=prepared_datasource.path,
        )
