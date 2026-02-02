from databao_context_engine.plugins.databases.athena.athena_introspector import AthenaConfigFile, AthenaIntrospector
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin


class AthenaDbPlugin(BaseDatabasePlugin[AthenaConfigFile]):
    id = "jetbrains/athena"
    name = "Athena DB Plugin"
    supported = {"athena"}
    config_file_type = AthenaConfigFile

    def __init__(self):
        super().__init__(AthenaIntrospector())
