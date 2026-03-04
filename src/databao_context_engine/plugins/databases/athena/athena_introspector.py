from __future__ import annotations

from typing import Any

from pyathena import connect
from pyathena.cursor import DictCursor
from typing_extensions import override

from databao_context_engine.plugins.databases.athena.config_file import AthenaConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery


class AthenaIntrospector(BaseIntrospector[AthenaConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    supports_catalogs = True

    def _connect(self, file_config: AthenaConfigFile, *, catalog: str | None = None) -> Any:
        return connect(**file_config.connection.to_athena_kwargs(), cursor_class=DictCursor)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def _get_catalogs(self, connection, file_config: AthenaConfigFile) -> list[str]:
        catalog = file_config.connection.catalog or self._resolve_pseudo_catalog_name(file_config)
        return [catalog]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        catalog = catalogs[0]
        sql = f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata"
        return SQLQuery(sql, None)

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)

        return SQLQuery(
            sql=f"""
            SELECT
                table_schema AS schema_name,
                table_name,
                CASE table_type
                    WHEN 'BASE TABLE' THEN 'table'
                    WHEN 'VIEW' THEN 'view'
                    ELSE LOWER(table_type)
                END AS kind,
                NULL AS description
            FROM 
                {catalog}.information_schema.tables
            WHERE 
                table_schema IN ({schemas_in})
        """
        )

    @override
    def get_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)

        return SQLQuery(
            sql=f"""
        SELECT 
            table_schema AS schema_name,
            table_name, 
            column_name, 
            ordinal_position, 
            data_type,
            is_nullable
        FROM 
            {catalog}.information_schema.columns
        WHERE 
            table_schema IN ({schemas_in})
        ORDER BY
            table_schema,
            table_name,
            ordinal_position
        """
        )

    def _resolve_pseudo_catalog_name(self, file_config: AthenaConfigFile) -> str:
        return "awsdatacatalog"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        return SQLQuery(sql, {"limit": limit})

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"
