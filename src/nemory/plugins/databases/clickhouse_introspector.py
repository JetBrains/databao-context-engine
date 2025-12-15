from __future__ import annotations

from io import UnsupportedOperation
from typing import Any, Mapping

import clickhouse_connect
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseColumn


class ClickhouseConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/clickhouse")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Clickhouse database. It can contain any of the keys supported by the Clickhouse connection library (see https://clickhouse.com/docs/integrations/language-clients/python/driver-api#connection-arguments)"
    )


class ClickhouseIntrospector(BaseIntrospector[ClickhouseConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "system"}

    supports_catalogs = False

    def _connect(self, file_config: ClickhouseConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return clickhouse_connect.get_client(**connection)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        result = connection.query(sql, parameters=params) if params else connection.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def _get_catalogs(self, connection, file_config: ClickhouseConfigFile) -> list[str]:
        raise UnsupportedOperation("Clickhouse doesnt support catalogs")

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
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
        return SQLQuery(sql, (schema,))

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

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %s'
        return SQLQuery(sql, (limit,))
