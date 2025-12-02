from nemory.pluginlib.build_plugin import ConfigPropertyDefinition
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
                property_key="host", required=True, nested_in="connection", default_value="localhost"
            ),
            ConfigPropertyDefinition(property_key="port", required=False, nested_in="connection"),
            ConfigPropertyDefinition(property_key="database", required=False, nested_in="connection"),
            ConfigPropertyDefinition(property_key="user", required=False, nested_in="connection"),
            ConfigPropertyDefinition(property_key="password", required=False, nested_in="connection"),
        ]
