import logging

from databao_context_engine.introspection.property_extract import get_property_list_from_type
from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    BuildPlugin,
    DatasourceType,
)
from databao_context_engine.pluginlib.config import ConfigPropertyDefinition, CustomiseConfigProperties

logger = logging.getLogger(__name__)


class DatabaoContextPluginLoader:
    """Loader for plugins installed in the current environment."""

    def __init__(self, plugins_by_type: dict[DatasourceType, BuildPlugin] | None = None):
        """Initialize the DatabaoContextEngine.

        Args:
            plugins_by_type: Override the list of plugins loaded from the environment.
                Typical usage should not provide this argument and leave it as None.
        """
        self._all_plugins_by_type = _load_plugins() if plugins_by_type is None else plugins_by_type

    def get_loaded_plugin_ids(self) -> set[str]:
        return {plugin.id for plugin in self._all_plugins_by_type.values()}

    def get_all_supported_datasource_types(self, exclude_file_plugins: bool = False) -> set[DatasourceType]:
        """Return the list of all supported datasource types.

        Args:
            exclude_file_plugins: If True, do not return datasource types from plugins that deal with raw files.

        Returns:
            A set of all DatasourceType supported in the current installation environment.
        """
        if exclude_file_plugins:
            return {
                datasource_type
                for (datasource_type, plugin) in self._all_plugins_by_type.items()
                if not isinstance(plugin, BuildFilePlugin)
            }

        return set(self._all_plugins_by_type.keys())

    def get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> BuildPlugin | None:
        """Return the plugin able to build a context for the given datasource type.

        Args:
            datasource_type: The type of datasource for which to retrieve the plugin.

        Returns:
            The plugin able to build a context for the given datasource type.
        """
        return self._all_plugins_by_type.get(datasource_type, None)

    def get_config_file_type_for_datasource_type(self, datasource_type: DatasourceType) -> type:
        """Return the type of the config file for the given datasource type.

        Args:
            datasource_type: The type of datasource for which to retrieve the config file type.

        Returns:
            The type of the config file for the given datasource type.

        Raises:
            ValueError: If no plugin is found for the given datasource type.
            ValueError: If the plugin does not support config files.
        """
        plugin = self.get_plugin_for_datasource_type(datasource_type)

        if isinstance(plugin, BuildDatasourcePlugin):
            return plugin.config_file_type

        raise ValueError(
            f'Impossible to get a config file type for datasource type "{datasource_type.full_type}". The corresponding plugin is a {type(plugin).__name__} but should be a BuildDatasourcePlugin'
        )

    def get_config_file_structure_for_datasource_type(
        self, datasource_type: DatasourceType
    ) -> list[ConfigPropertyDefinition]:
        """Return the property structure of the config file for the given datasource type.

        This can be used to generate a form for the user to fill in the config file.

        Args:
            datasource_type: The type of datasource for which to retrieve the config file structure.

        Returns:
            The structure of the config file for the given datasource type.
                This structure is a list of ConfigPropertyDefinition objects.
                Each object in the list describes a property of the config file and its potential nested properties.

        Raises:
            ValueError: If no plugin is found for the given datasource type.
            ValueError: If the plugin does not support config files.
        """
        plugin = self.get_plugin_for_datasource_type(datasource_type)

        if isinstance(plugin, CustomiseConfigProperties):
            return plugin.get_config_file_properties()
        if isinstance(plugin, BuildDatasourcePlugin):
            return get_property_list_from_type(plugin.config_file_type)
        raise ValueError(
            f'Impossible to create a config for datasource type "{datasource_type.full_type}". The corresponding plugin is a {type(plugin).__name__} but should be a BuildDatasourcePlugin or CustomiseConfigProperties'
        )


class DuplicatePluginTypeError(RuntimeError):
    """Raised when two plugins register the same <main>/<sub> plugin key."""


def _load_plugins(exclude_file_plugins: bool = False) -> dict[DatasourceType, BuildPlugin]:
    """Load both builtin and external plugins and merges them into one list."""
    builtin_plugins = _load_builtin_plugins(exclude_file_plugins)
    external_plugins = _load_external_plugins(exclude_file_plugins)

    return _merge_plugins(builtin_plugins, external_plugins)


def _load_builtin_plugins(exclude_file_plugins: bool = False) -> list[BuildPlugin]:
    all_builtin_plugins: list[BuildPlugin] = []

    all_builtin_plugins += _load_builtin_datasource_plugins()

    if not exclude_file_plugins:
        all_builtin_plugins += _load_builtin_file_plugins()

    return all_builtin_plugins


def _load_builtin_file_plugins() -> list[BuildFilePlugin]:
    from databao_context_engine.plugins.files.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    plugins: list[BuildFilePlugin] = [InternalUnstructuredFilesPlugin()]

    try:
        from databao_context_engine.plugins.files.pdf_plugin import PDFPlugin

        plugins.append(PDFPlugin())
    except ImportError:
        pass

    return plugins


def _load_builtin_datasource_plugins() -> list[BuildDatasourcePlugin]:
    """Statically register built-in plugins."""
    from databao_context_engine.plugins.databases.duckdb.duckdb_db_plugin import DuckDbPlugin
    from databao_context_engine.plugins.databases.sqlite.sqlite_db_plugin import SQLiteDbPlugin
    from databao_context_engine.plugins.dbt.dbt_plugin import DbtPlugin
    from databao_context_engine.plugins.resources.parquet_plugin import ParquetPlugin

    # optional plugins are added to the python environment via extras
    optional_plugins: list[BuildDatasourcePlugin] = []
    try:
        from databao_context_engine.plugins.databases.mssql.mssql_db_plugin import MSSQLDbPlugin

        optional_plugins = [MSSQLDbPlugin()]
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.clickhouse.clickhouse_db_plugin import ClickhouseDbPlugin

        optional_plugins.append(ClickhouseDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.athena.athena_db_plugin import AthenaDbPlugin

        optional_plugins.append(AthenaDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.snowflake.snowflake_db_plugin import SnowflakeDbPlugin

        optional_plugins.append(SnowflakeDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.bigquery.bigquery_db_plugin import BigQueryDbPlugin

        optional_plugins.append(BigQueryDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.mysql.mysql_db_plugin import MySQLDbPlugin

        optional_plugins.append(MySQLDbPlugin())
    except ImportError:
        pass

    try:
        from databao_context_engine.plugins.databases.postgresql.postgresql_db_plugin import PostgresqlDbPlugin

        optional_plugins.append(PostgresqlDbPlugin())
    except ImportError:
        pass

    required_plugins: list[BuildDatasourcePlugin] = [DuckDbPlugin(), ParquetPlugin(), SQLiteDbPlugin(), DbtPlugin()]
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
