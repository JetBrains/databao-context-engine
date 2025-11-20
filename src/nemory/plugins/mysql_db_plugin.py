from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.mysql_introspector import MySQLIntrospector


class MySQLDbPlugin(BaseDatabasePlugin):
    name = "MySQL DB Plugin"
    supported = {"databases/mysql"}

    def __init__(self):
        super().__init__(MySQLIntrospector())
