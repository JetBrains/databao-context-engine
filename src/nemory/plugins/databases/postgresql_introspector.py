from typing import Any, Mapping, TypedDict

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class PostgresqlConnectionConfig(TypedDict, total=False):
    """
    Connection parameters for the PostgreSQL database.

    This can contain any of the keys that can be used in a Postgres connection string
    """

    host: str | None
    port: int | None
    database: str | None
    user: str | None
    password: str | None


class PostgresConfigFile(TypedDict):
    connection: PostgresqlConnectionConfig
    type: str


class PostgresqlIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config.get("connection")
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        connection_string = self._create_connection_string_for_config(connection)
        return psycopg.connect(connection_string)

    def _fetchall_dicts(self, connection: Connection, sql: str, params) -> list[dict]:
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return [r for r in cur.fetchall()]

    def _get_catalogs(self, connection: Connection, file_config: Mapping[str, Any]) -> list[str]:
        database = file_config["connection"].get("database")
        if database is not None:
            return [database]

        catalog_results = connection.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false"
        ).fetchall()

        return [row[0] for row in catalog_results]

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, tuple | list]:
        sql = """
        SELECT table_name, column_name, is_nullable, udt_name, data_type
        FROM information_schema.columns 
        WHERE table_catalog = %s AND table_schema = %s
        """
        return sql, (catalog, schema)

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["udt_name"],
            nullable=row["is_nullable"].upper() == "YES",
        )

    def _create_connection_string_for_config(self, connection_config: Mapping[str, Any]) -> str:
        # TODO: For all fields, surround with single quotes and escape backslashes and quotes in the values
        host = connection_config.get("host")
        if host is None:
            raise ValueError("A host must be provided to connect to the PostgreSQL database.")

        port = connection_config.get("port", 5432)

        connection_string = f"host={host} port={port}"

        database = connection_config.get("database")
        if database is not None:
            connection_string += f" dbname={database}"

        user = connection_config.get("user")
        if user is not None:
            connection_string += f" user={user}"

        password = connection_config.get("password")
        if password is not None:
            connection_string += f" password='{password}'"

        return connection_string
