from databao_context_engine.introspection.property_extract import get_property_list_from_type
from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    BuildPlugin,
    DatasourceType,
)
from databao_context_engine.pluginlib.config import ConfigPropertyDefinition, CustomiseConfigProperties
from databao_context_engine.plugins.plugin_loader import (
    load_plugins,
)


class DatabaoContextPluginLoader:
    """Loader for plugins installed in the current environment."""

    def __init__(self, plugins_by_type: dict[DatasourceType, BuildPlugin] | None = None):
        """Initialise the DatabaoContextEngine.

        Args:
            plugins_by_type: Override the list of plugins loaded from the environment.
                Typical usage should not provide this argument and leave it as None.
        """
        self._all_plugins_by_type = load_plugins() if plugins_by_type is None else plugins_by_type

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
        else:
            return set(self._all_plugins_by_type.keys())

    def get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> BuildPlugin:
        """Return the plugin able to build a context for the given datasource type.

        Args:
            datasource_type: The type of datasource for which to retrieve the plugin.

        Returns:
            The plugin able to build a context for the given datasource type.

        Raises:
            ValueError: If no plugin is found for the given datasource type.
        """
        if datasource_type not in self._all_plugins_by_type:
            raise ValueError(f"No plugin found for type '{datasource_type.full_type}'")

        return self._all_plugins_by_type[datasource_type]

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
        elif isinstance(plugin, BuildDatasourcePlugin):
            return get_property_list_from_type(plugin.config_file_type)
        else:
            raise ValueError(
                f'Impossible to create a config for datasource type "{datasource_type.full_type}". The corresponding plugin is a {type(plugin).__name__} but should be a BuildDatasourcePlugin or CustomiseConfigProperties'
            )
