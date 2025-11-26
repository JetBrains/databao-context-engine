from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.mysql_introspector import MySQLConfigFile, MySQLIntrospector


class MySQLDbPlugin(BaseDatabasePlugin[MySQLConfigFile]):
    id = "jetbrains/mysql"
    name = "MySQL DB Plugin"
    supported = {"databases/mysql"}
    config_file_type = MySQLConfigFile

    def __init__(self):
        super().__init__(MySQLIntrospector())
