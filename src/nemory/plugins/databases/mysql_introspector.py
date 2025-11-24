from io import UnsupportedOperation
from typing import Any, Mapping

import pymysql

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class MySQLIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}

    supports_catalogs = False

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config["connection"]
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return pymysql.connect(
            **connection,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): v for k, v in row.items()} for row in rows]

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        raise UnsupportedOperation("MySQL doesn't support catalogs")

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, dict | tuple | list | None]:
        sql = """
        SELECT table_name, column_name, is_nullable, data_type
        FROM information_schema.columns WHERE table_schema = %s
        """

        return sql, (schema,)

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row.get("data_type", "unknown"),
            nullable=row.get("is_nullable", "NO").upper() == "YES",
        )
