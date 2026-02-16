from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.mysql.config_file import MySQLConfigFile
from databao_context_engine.plugins.databases.mysql.mysql_introspector import MySQLIntrospector


class MySQLDbPlugin(BaseDatabasePlugin[MySQLConfigFile]):
    id = "jetbrains/mysql"
    name = "MySQL DB Plugin"
    supported = {"mysql"}
    config_file_type = MySQLConfigFile

    def __init__(self):
        super().__init__(MySQLIntrospector())
