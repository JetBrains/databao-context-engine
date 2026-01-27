import logging

from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    BuildPlugin,
    DatasourceType,
)

logger = logging.getLogger(__name__)


class DuplicatePluginTypeError(RuntimeError):
    """Raised when two plugins register the same <main>/<sub> plugin key."""


def load_plugins(exclude_file_plugins: bool = False) -> dict[DatasourceType, BuildPlugin]:
    """Load both builtin and external plugins and merges them into one list."""
    builtin_plugins = _load_builtin_plugins(exclude_file_plugins)
    external_plugins = _load_external_plugins(exclude_file_plugins)
    plugins = _merge_plugins(builtin_plugins, external_plugins)

    return plugins


def _load_builtin_plugins(exclude_file_plugins: bool = False) -> list[BuildPlugin]:
    all_builtin_plugins: list[BuildPlugin] = []

    all_builtin_plugins += _load_builtin_datasource_plugins()

    if not exclude_file_plugins:
        all_builtin_plugins += _load_builtin_file_plugins()

    return all_builtin_plugins


def _load_builtin_file_plugins() -> list[BuildFilePlugin]:
    from databao_context_engine.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    return [
        InternalUnstructuredFilesPlugin(),
    ]


def _load_builtin_datasource_plugins() -> list[BuildDatasourcePlugin]:
    """Statically register built-in plugins."""
    from databao_context_engine.plugins.duckdb_db_plugin import DuckDbPlugin
    from databao_context_engine.plugins.parquet_plugin import ParquetPlugin
    from databao_context_engine.plugins.sqlite_db_plugin import SQLiteDbPlugin

    # optional plugins are added to the python environment via extras
    optional_plugins: list[BuildDatasourcePlugin] = []
    try:
        from databao_context_engine.plugins.mssql_db_plugin import MSSQLDbPlugin

        optional_plugins = [MSSQLDbPlugin()]
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.clickhouse_db_plugin import ClickhouseDbPlugin

        optional_plugins.append(ClickhouseDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.athena_db_plugin import AthenaDbPlugin

        optional_plugins.append(AthenaDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.snowflake_db_plugin import SnowflakeDbPlugin

        optional_plugins.append(SnowflakeDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.mysql_db_plugin import MySQLDbPlugin

        optional_plugins.append(MySQLDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.postgresql_db_plugin import PostgresqlDbPlugin

        optional_plugins.append(PostgresqlDbPlugin())
    except ImportError:
        pass

    required_plugins: list[BuildDatasourcePlugin] = [
        DuckDbPlugin(),
        ParquetPlugin(),
        SQLiteDbPlugin()
    ]
    return required_plugins + optional_plugins


def _load_external_plugins(exclude_file_plugins: bool = False) -> list[BuildPlugin]:
    """Discover external plugins via entry points."""
    # TODO: implement external plugin loading
    return []


def _merge_plugins(*plugin_lists: list[BuildPlugin]) -> dict[DatasourceType, BuildPlugin]:
    """Merge multiple plugin maps."""
    registry: dict[DatasourceType, BuildPlugin] = {}
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
