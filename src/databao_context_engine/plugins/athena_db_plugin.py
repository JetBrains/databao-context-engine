from databao_context_engine.plugins.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.athena_introspector import AthenaConfigFile, AthenaIntrospector


class AthenaDbPlugin(BaseDatabasePlugin[AthenaConfigFile]):
    id = "jetbrains/athena"
    name = "Athena DB Plugin"
    supported = {"databases/athena"}
    config_file_type = AthenaConfigFile

    def __init__(self):
        super().__init__(AthenaIntrospector())
