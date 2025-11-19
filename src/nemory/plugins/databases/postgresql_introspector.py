from typing import Any, Mapping

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from nemory.plugins.databases.databases_types import DatabaseColumn
from nemory.plugins.databases.base_introspector import BaseIntrospector


class PostgresqlIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection_string = self._create_connection_string_for_config(file_config)
        return psycopg.connect(connection_string)

    def _fetchall_dicts(self, connection: Connection, sql: str, params) -> list[dict]:
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return [r for r in cur.fetchall()]

    def _get_catalogs(self, connection: Connection, file_config: Mapping[str, Any]) -> list[str]:
        database = file_config.get("database")
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

    def _create_connection_string_for_config(self, file_config: Mapping[str, Any]) -> str:
        # TODO: For all fields, surround with single quotes and escape backslashes and quotes in the values
        host = file_config.get("host")
        if host is None:
            raise ValueError("A host must be provided to connect to the PostgreSQL database.")

        port = file_config.get("port", 5432)

        connection_string = f"host={host} port={port}"

        database = file_config.get("database")
        if database is not None:
            connection_string += f" dbname={database}"

        user = file_config.get("user")
        if user is not None:
            connection_string += f" user={user}"

        password = file_config.get("password")
        if password is not None:
            connection_string += f" password='{password}'"

        return connection_string
