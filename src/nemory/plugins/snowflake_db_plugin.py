from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.snowflake_introspector import SnowflakeIntrospector


class SnowflakeDbPlugin(BaseDatabasePlugin):
    id = "jetbrains/snowflake"
    name = "Snowflake DB Plugin"
    supported = {"databases/snowflake"}

    def __init__(self):
        super().__init__(SnowflakeIntrospector())
