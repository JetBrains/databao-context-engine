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
    def __init__(self, plugins_by_type: dict[DatasourceType, BuildPlugin] | None = None):
        self._all_plugins_by_type = load_plugins() if plugins_by_type is None else plugins_by_type

    def get_all_supported_datasource_types(self, exclude_file_plugins: bool = False) -> set[DatasourceType]:
        if exclude_file_plugins:
            return {
                datasource_type
                for (datasource_type, plugin) in self._all_plugins_by_type.items()
                if not isinstance(plugin, BuildFilePlugin)
            }
        else:
            return set(self._all_plugins_by_type.keys())

    def get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> BuildPlugin:
        if datasource_type not in self._all_plugins_by_type:
            raise ValueError(f"No plugin found for type '{datasource_type.full_type}'")

        return self._all_plugins_by_type[datasource_type]

    def get_config_file_type_for_datasource_type(self, datasource_type: DatasourceType) -> type:
        plugin = self.get_plugin_for_datasource_type(datasource_type)

        if isinstance(plugin, BuildDatasourcePlugin):
            return plugin.config_file_type

        raise ValueError(
            f'Impossible to get a config file type for datasource type "{datasource_type.full_type}". The corresponding plugin is a {type(plugin).__name__} but should be a BuildDatasourcePlugin'
        )

    def get_config_file_structure_for_datasource_type(
        self, datasource_type: DatasourceType
    ) -> list[ConfigPropertyDefinition]:
        plugin = self.get_plugin_for_datasource_type(datasource_type)

        if isinstance(plugin, CustomiseConfigProperties):
            return plugin.get_config_file_properties()
        elif isinstance(plugin, BuildDatasourcePlugin):
            return get_property_list_from_type(plugin.config_file_type)
        else:
            raise ValueError(
                f'Impossible to create a config for datasource type "{datasource_type.full_type}". The corresponding plugin is a {type(plugin).__name__} but should be a BuildDatasourcePlugin or CustomiseConfigProperties'
            )
