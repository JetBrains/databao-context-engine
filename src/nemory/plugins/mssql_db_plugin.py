from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.mssql_introspector import MSSQLIntrospector


class MSSQLDbPlugin(BaseDatabasePlugin):
    name = "MSSQL DB Plugin"
    supported = {"databases/mssql"}

    def __init__(self):
        super().__init__(MSSQLIntrospector())
