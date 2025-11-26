from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.clickhouse_introspector import ClickhouseIntrospector


class ClickhouseDbPlugin(BaseDatabasePlugin):
    id = "jetbrains/clickhouse"
    name = "Clickhouse DB Plugin"
    supported = {"databases/clickhouse"}

    def __init__(self):
        super().__init__(ClickhouseIntrospector())
