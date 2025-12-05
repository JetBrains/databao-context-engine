from nemory.pluginlib.config_properties import ConfigPropertyDefinition
from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.postgresql_introspector import PostgresConfigFile, PostgresqlIntrospector


class PostgresqlDbPlugin(BaseDatabasePlugin[PostgresConfigFile]):
    id = "jetbrains/postgres"
    name = "PostgreSQL DB Plugin"
    supported = {"databases/postgres"}
    config_file_type = PostgresConfigFile

    def __init__(self):
        super().__init__(PostgresqlIntrospector())

    def get_mandatory_config_file_structure(self) -> list[ConfigPropertyDefinition]:
        return [
            ConfigPropertyDefinition(
                property_key="connection",
                required=True,
                property_type=None,
                nested_properties=[
                    ConfigPropertyDefinition(property_key="host", required=True, default_value="localhost"),
                    ConfigPropertyDefinition(property_key="port", required=False, property_type=int),
                    ConfigPropertyDefinition(property_key="database", required=False),
                    ConfigPropertyDefinition(property_key="user", required=False),
                    ConfigPropertyDefinition(property_key="password", required=False),
                ],
            )
        ]
