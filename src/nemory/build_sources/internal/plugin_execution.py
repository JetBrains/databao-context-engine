from typing import cast

from nemory.build_sources.internal.types import PreparedConfig, PreparedDatasource
from nemory.pluginlib.build_plugin import (
    BuildExecutionResult,
    BuildPlugin,
    BuildDatasourcePlugin,
    BuildFilePlugin,
)


def execute(prepared_datasource: PreparedDatasource, plugin: BuildPlugin) -> BuildExecutionResult:
    """
    Run a prepared source through the plugin
    """
    if isinstance(prepared_datasource, PreparedConfig):
        ds_plugin = cast(BuildDatasourcePlugin, plugin)
        return ds_plugin.execute(
            full_type=prepared_datasource.full_type,
            datasource_name=prepared_datasource.datasource_name,
            file_config=prepared_datasource.config,
        )
    else:
        file_plugin = cast(BuildFilePlugin, plugin)
        with prepared_datasource.path.open("rb") as fh:
            return file_plugin.execute(
                full_type=prepared_datasource.full_type,
                file_name=prepared_datasource.path.name,
                file_buffer=fh,
            )
