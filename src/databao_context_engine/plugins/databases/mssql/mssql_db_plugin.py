from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.mssql.config_file import MSSQLConfigFile
from databao_context_engine.plugins.databases.mssql.mssql_introspector import MSSQLIntrospector


class MSSQLDbPlugin(BaseDatabasePlugin[MSSQLConfigFile]):
    id = "jetbrains/mssql"
    name = "MSSQL DB Plugin"
    supported = {"mssql"}
    config_file_type = MSSQLConfigFile

    def __init__(self):
        super().__init__(MSSQLIntrospector())
