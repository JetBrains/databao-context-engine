from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.sqlite.config_file import SQLiteConfigFile
from databao_context_engine.plugins.databases.sqlite.sqlite_connector import SQLiteConnector
from databao_context_engine.plugins.databases.sqlite.sqlite_introspector import SQLiteIntrospector


class SQLiteDbPlugin(BaseDatabasePlugin[SQLiteConfigFile]):
    id = "jetbrains/sqlite"
    name = "SQLite Plugin"
    supported = {"sqlite"}
    config_file_type = SQLiteConfigFile

    def __init__(self):
        connector = SQLiteConnector()
        super().__init__(connector=connector, introspector=SQLiteIntrospector(connector))
