from __future__ import annotations

from typing import Any, Mapping
import duckdb

from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseColumn


class DuckDBIntrospector(BaseIntrospector):
    _IGNORED_CATALOGS = {"system", "temp"}
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog"}
    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config["connection"]
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        database_path = str(connection.get("database"))
        return duckdb.connect(database=database_path)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        cur = connection.cursor()
        cur.execute(sql, params or [])
        columns = [desc[0].lower() for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        rows = self._fetchall_dicts(connection, "SELECT database_name FROM duckdb_databases();", None)
        catalogs = [r["database_name"] for r in rows if r.get("database_name")]
        catalogs_filtered = [c for c in catalogs if c.lower() not in self._IGNORED_CATALOGS]
        return catalogs_filtered or [self._resolve_pseudo_catalog_name(file_config)]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(?)"
        return SQLQuery(sql, (catalogs,))

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        sql = """
        SELECT table_name, column_name, is_nullable, data_type
        FROM information_schema.columns
        WHERE table_schema = ?
        """
        return SQLQuery(sql, (schema,))

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=row["is_nullable"].upper() == "YES",
        )
