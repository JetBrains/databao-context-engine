from nemory.plugins.base_db_plugin import BaseDatabasePlugin
from nemory.plugins.databases.postgresql_introspector import PostgresqlIntrospector


class PostgresqlDbPlugin(BaseDatabasePlugin):
    name = "PostgreSQL DB Plugin"
    supported = {"databases/postgres"}

    def __init__(self):
        super().__init__(PostgresqlIntrospector())
