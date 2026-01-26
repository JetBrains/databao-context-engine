from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    BuildPlugin,
)
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin, execute_file_plugin
from databao_context_engine.datasources.types import PreparedConfig, PreparedDatasource, DatasourceId


@dataclass()
class BuiltDatasourceContext:
    """Dataclass defining the result of building a datasource's context."""

    datasource_id: str
    """
    The ID of the built data source
    """

    datasource_type: str
    """
    The type of the built data source
    """

    context_built_at: datetime
    """
    The time at which the context was built
    """

    context: Any
    """
    A dictionary containing the actual context generated for the data source.
    This dictionary should be serializable in YAML format.
    """


def execute(prepared_datasource: PreparedDatasource, plugin: BuildPlugin) -> BuiltDatasourceContext:
    built_context = _execute(prepared_datasource, plugin)

    datasource_id = DatasourceId.from_datasource_config_file_path(prepared_datasource.path)

    return BuiltDatasourceContext(
        datasource_id=str(datasource_id),
        datasource_type=prepared_datasource.datasource_type.full_type,
        context_built_at=datetime.now(),
        context=built_context,
    )


def _execute(prepared_datasource: PreparedDatasource, plugin: BuildPlugin) -> Any:
    """Run a prepared source through the plugin."""
    if isinstance(prepared_datasource, PreparedConfig):
        ds_plugin = cast(BuildDatasourcePlugin, plugin)

        return execute_datasource_plugin(
            plugin=ds_plugin,
            datasource_type=prepared_datasource.datasource_type,
            config=prepared_datasource.config,
            datasource_name=prepared_datasource.datasource_name,
        )
    else:
        file_plugin = cast(BuildFilePlugin, plugin)
        return execute_file_plugin(
            plugin=file_plugin,
            datasource_type=prepared_datasource.datasource_type,
            file_path=prepared_datasource.path,
        )
