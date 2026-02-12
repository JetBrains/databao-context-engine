from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.snowflake.config_file import SnowflakeConfigFile
from databao_context_engine.plugins.databases.snowflake.snowflake_introspector import (
    SnowflakeIntrospector,
)


class SnowflakeDbPlugin(BaseDatabasePlugin[SnowflakeConfigFile]):
    id = "jetbrains/snowflake"
    name = "Snowflake DB Plugin"
    supported = {"snowflake"}
    config_file_type = SnowflakeConfigFile

    def __init__(self):
        super().__init__(SnowflakeIntrospector())
