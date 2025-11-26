from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.athena_introspector import AthenaIntrospector


class AthenaDbPlugin(BaseDatabasePlugin):
    id = "jetbrains/athena"
    name = "Athena DB Plugin"
    supported = {"databases/athena"}

    def __init__(self):
        super().__init__(AthenaIntrospector())
