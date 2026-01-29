from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.mssql.mssql_introspector import MSSQLConfigFile, MSSQLIntrospector


class MSSQLDbPlugin(BaseDatabasePlugin[MSSQLConfigFile]):
    id = "jetbrains/mssql"
    name = "MSSQL DB Plugin"
    supported = {"databases/mssql"}
    config_file_type = MSSQLConfigFile

    def __init__(self):
        super().__init__(MSSQLIntrospector())
