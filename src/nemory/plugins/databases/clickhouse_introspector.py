from __future__ import annotations

from io import UnsupportedOperation
from typing import Any, Mapping

import clickhouse_connect

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class ClickhouseIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {"information_schema", "system"}

    supports_catalogs = False

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config.get("connection")
        return clickhouse_connect.get_client(**connection)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        result = connection.query(sql, parameters=params) if params else connection.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        raise UnsupportedOperation("Clickhouse doesnt support catalogs")

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, tuple]:
        sql = """
        SELECT 
            table AS table_name,
            name AS column_name, 
            type AS data_type,
            if(startsWith(type, 'Nullable('), 'YES', 'NO') AS is_nullable
        FROM system.columns
        WHERE database = %s
        ORDER BY table, position
        """
        return sql, (schema,)

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        raw_type = row["data_type"]
        if isinstance(raw_type, str) and raw_type.startswith("Nullable(") and raw_type.endswith(")"):
            clean_type = raw_type[len("Nullable(") : -1]
        else:
            clean_type = raw_type

        return DatabaseColumn(
            name=row["column_name"],
            type=clean_type,
            nullable=(row.get("is_nullable") == "YES"),
        )
