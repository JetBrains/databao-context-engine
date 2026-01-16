from databao_context_engine.plugins.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.snowflake_introspector import SnowflakeConfigFile, SnowflakeIntrospector


class SnowflakeDbPlugin(BaseDatabasePlugin[SnowflakeConfigFile]):
    id = "jetbrains/snowflake"
    name = "Snowflake DB Plugin"
    supported = {"databases/snowflake"}
    config_file_type = SnowflakeConfigFile

    def __init__(self):
        super().__init__(SnowflakeIntrospector())
