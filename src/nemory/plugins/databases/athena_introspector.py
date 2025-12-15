from __future__ import annotations

from typing import Any, Mapping

from pyathena import connect
from pyathena.cursor import DictCursor
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseColumn


class AthenaConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/athena")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Athena database. It can contain any of the keys supported by the Athena connection library"
    )


class AthenaIntrospector(BaseIntrospector[AthenaConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    supports_catalogs = True

    def _connect(self, file_config: AthenaConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return connect(**connection, cursor_class=DictCursor)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def _get_catalogs(self, connection, file_config: AthenaConfigFile) -> list[str]:
        catalog = file_config.connection.get("catalog", self._resolve_pseudo_catalog_name(file_config))
        return [catalog]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        catalog = catalogs[0]
        sql = "SELECT schema_name, catalog_name FROM information_schema.schemata WHERE catalog_name = %(catalog)s"
        return SQLQuery(sql, {"catalog": catalog})

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        sql = f"""
        SELECT table_name, column_name, is_nullable, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = %(schema)s
        """
        return SQLQuery(sql, {"schema": schema})

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=row["is_nullable"] == "YES",
        )

    def _resolve_pseudo_catalog_name(self, file_config: AthenaConfigFile) -> str:
        return "awsdatacatalog"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        return SQLQuery(sql, {"limit": limit})
