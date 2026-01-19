from databao_context_engine.plugins.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.duckdb_introspector import DuckDBConfigFile, DuckDBIntrospector


class DuckDbPlugin(BaseDatabasePlugin[DuckDBConfigFile]):
    id = "jetbrains/duckdb"
    name = "DuckDB Plugin"
    supported = {"databases/duckdb"}
    config_file_type = DuckDBConfigFile

    def __init__(self):
        super().__init__(DuckDBIntrospector())
