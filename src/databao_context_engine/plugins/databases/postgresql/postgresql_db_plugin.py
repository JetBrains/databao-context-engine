from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.postgresql.postgresql_introspector import (
    PostgresConfigFile,
    PostgresqlIntrospector,
)


class PostgresqlDbPlugin(BaseDatabasePlugin[PostgresConfigFile]):
    id = "jetbrains/postgres"
    name = "PostgreSQL DB Plugin"
    supported = {"postgres"}
    config_file_type = PostgresConfigFile

    def __init__(self):
        super().__init__(PostgresqlIntrospector())
