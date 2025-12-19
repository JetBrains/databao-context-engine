import logging

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, BuildFilePlugin, BuildPlugin, DatasourceType

logger = logging.getLogger(__name__)


class DuplicatePluginTypeError(RuntimeError):
    """Raised when two plugins register the same <main>/<sub> plugin key."""


PluginList = dict[DatasourceType, BuildPlugin]


def load_plugins(exclude_file_plugins: bool = False) -> PluginList:
    """
    Loads both builtin and external plugins and merges them into one list
    """
    builtin_plugins = _load_builtin_plugins(exclude_file_plugins)
    external_plugins = _load_external_plugins(exclude_file_plugins)
    plugins = merge_plugins(builtin_plugins, external_plugins)

    return plugins


def _load_builtin_plugins(exclude_file_plugins: bool = False) -> list[BuildPlugin]:
    all_builtin_plugins: list[BuildPlugin] = []

    all_builtin_plugins += _load_builtin_datasource_plugins()

    if not exclude_file_plugins:
        all_builtin_plugins += _load_builtin_file_plugins()

    return all_builtin_plugins


def _load_builtin_file_plugins() -> list[BuildFilePlugin]:
    from nemory.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    return [
        InternalUnstructuredFilesPlugin(),
    ]


def _load_builtin_datasource_plugins() -> list[BuildDatasourcePlugin]:
    """
    Statically register built-in plugins
    """
    from nemory.plugins.athena_db_plugin import AthenaDbPlugin
    from nemory.plugins.clickhouse_db_plugin import ClickhouseDbPlugin
    from nemory.plugins.duckdb_db_plugin import DuckDbPlugin
    from nemory.plugins.mssql_db_plugin import MSSQLDbPlugin
    from nemory.plugins.mysql_db_plugin import MySQLDbPlugin
    from nemory.plugins.parquet_plugin import ParquetPlugin
    from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin
    from nemory.plugins.snowflake_db_plugin import SnowflakeDbPlugin

    return [
        AthenaDbPlugin(),
        ClickhouseDbPlugin(),
        DuckDbPlugin(),
        MSSQLDbPlugin(),
        MySQLDbPlugin(),
        PostgresqlDbPlugin(),
        SnowflakeDbPlugin(),
        ParquetPlugin(),
    ]


def _load_external_plugins(exclude_file_plugins: bool = False) -> list[BuildPlugin]:
    """
    Discover external plugins via entry points
    """
    # TODO: implement external plugin loading
    return []


def merge_plugins(*plugin_lists: list[BuildPlugin]) -> PluginList:
    """
    Merge multiple plugin maps
    """
    registry: PluginList = {}
    for plugins in plugin_lists:
        for plugin in plugins:
            for full_type in plugin.supported_types():
                datasource_type = DatasourceType(full_type=full_type)
                if datasource_type in registry:
                    raise DuplicatePluginTypeError(
                        f"Plugin type '{datasource_type.full_type}' is provided by both {type(registry[datasource_type]).__name__} and {type(plugin).__name__}"
                    )
                registry[datasource_type] = plugin
    return registry
