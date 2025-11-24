from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.duckdb_introspector import DuckDBIntrospector


class DuckDbPlugin(BaseDatabasePlugin):
    name = "DuckDB Plugin"
    supported = {"databases/duckdb"}

    def __init__(self):
        super().__init__(DuckDBIntrospector())
