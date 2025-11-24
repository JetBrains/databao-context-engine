from __future__ import annotations

from typing import Any, Mapping
import snowflake.connector

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class SnowflakeIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config["connection"]
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")
        snowflake.connector.paramstyle = "qmark"
        return snowflake.connector.connect(
            **connection,
        )

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): v for k, v in row.items()} for row in rows]

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        database = file_config["connection"].get("database")
        if database:
            return [database]

        rows = self._fetchall_dicts(connection, "SHOW DATABASES", None)
        return [r["name"] for r in rows if r["name"]]

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, dict | tuple | list | None]:
        sql = f"""
        SELECT table_name, column_name, is_nullable, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = ?
        """
        return sql, (schema,)

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=row["is_nullable"].upper() == "YES",
        )

    def _sql_list_schemas(self, catalogs: list[str] | None) -> tuple[str, dict | tuple | list | None]:
        # This way if one of the select clauses fails (which happens now for a few test databases), the whole query fails.
        # In the future, we can change the whole logic and call _sql_list_schemas() for each catalog separately.
        # Or maybe we can use 'show schemas' query.
        if not catalogs:
            return "SELECT schema_name, catalog_name FROM information_schema.schemata", None
        parts = []
        for catalog in catalogs:
            parts.append(f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata")
        return " UNION ALL ".join(parts), None
