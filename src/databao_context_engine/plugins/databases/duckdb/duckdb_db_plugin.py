from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.duckdb.duckdb_introspector import DuckDBConfigFile, DuckDBIntrospector


class DuckDbPlugin(BaseDatabasePlugin[DuckDBConfigFile]):
    id = "jetbrains/duckdb"
    name = "DuckDB Plugin"
    supported = {"duckdb"}
    config_file_type = DuckDBConfigFile

    def __init__(self):
        super().__init__(DuckDBIntrospector())
