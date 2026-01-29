from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.clickhouse.clickhouse_introspector import (
    ClickhouseConfigFile,
    ClickhouseIntrospector,
)


class ClickhouseDbPlugin(BaseDatabasePlugin[ClickhouseConfigFile]):
    id = "jetbrains/clickhouse"
    name = "Clickhouse DB Plugin"
    supported = {"databases/clickhouse"}
    config_file_type = ClickhouseConfigFile

    def __init__(self):
        super().__init__(ClickhouseIntrospector())
