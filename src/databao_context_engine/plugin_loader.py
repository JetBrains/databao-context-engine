from databao_context_engine.introspection.property_extract import get_property_list_from_type
from databao_context_engine.pluginlib.build_plugin import BuildDatasourcePlugin, BuildPlugin, DatasourceType
from databao_context_engine.pluginlib.config import ConfigPropertyDefinition, CustomiseConfigProperties
from databao_context_engine.plugins.plugin_loader import get_all_available_plugin_types, get_plugin_for_type


class DatabaoContextPluginLoader:
    def get_all_supported_datasource_types(self, exclude_file_plugins: bool = False) -> set[DatasourceType]:
        return get_all_available_plugin_types(exclude_file_plugins=exclude_file_plugins)

    def get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> BuildPlugin:
        return get_plugin_for_type(datasource_type)

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
