from __future__ import annotations

from typing import Any, Mapping

from pyathena import connect
from pyathena.cursor import DictCursor

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class AthenaIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config["connection"]
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return connect(**connection, cursor_class=DictCursor)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        catalog = file_config["connection"].get("catalog", self._resolve_pseudo_catalog_name(file_config))
        return [catalog]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> tuple[str, dict | tuple | list | None]:
        if not catalogs:
            return "SELECT schema_name, catalog_name FROM information_schema.schemata", None
        catalog = catalogs[0]
        sql = "SELECT schema_name, catalog_name FROM information_schema.schemata WHERE catalog_name = %(catalog)s"
        return sql, {"catalog": catalog}

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, dict | tuple | list | None]:
        sql = f"""
        SELECT table_name, column_name, is_nullable, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = %(schema)s
        """
        return sql, {"schema": schema}

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=row["is_nullable"] == "YES",
        )

    def _resolve_pseudo_catalog_name(self, file_config: Mapping[str, Any]) -> str:
        return "awsdatacatalog"
