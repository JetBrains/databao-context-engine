from __future__ import annotations

from typing import Any, Mapping

from pyathena import connect
from pyathena.cursor import DictCursor
from pydantic import Field

from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseTable
from databao_context_engine.plugins.databases.table_builder import TableBuilder


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

    def _connect_to_catalog(self, file_config: AthenaConfigFile, catalog: str):
        self._connect(file_config)

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        catalog = catalogs[0]
        sql = "SELECT schema_name, catalog_name FROM information_schema.schemata WHERE catalog_name = %(catalog)s"
        return SQLQuery(sql, {"catalog": catalog})

    # TODO: Incomplete plugin
    def collect_schema_model(self, connection, catalog: str, schema: str) -> list[DatabaseTable] | None:
        q = self._sql_columns(catalog, schema)
        rows = self._fetchall_dicts(connection, q.sql, q.params)
        if not rows:
            return []

        rels: list[dict] = []
        cols: list[dict] = []
        seen_tables: set[str] = set()

        for r in rows:
            table_name = r["table_name"]
            if table_name not in seen_tables:
                seen_tables.add(table_name)
                rels.append(
                    {
                        "table_name": table_name,
                        "kind": "table",
                        "description": None,
                    }
                )

            cols.append(
                {
                    "table_name": table_name,
                    "column_name": r["column_name"],
                    "ordinal_position": r["ordinal_position"],
                    "data_type": r["data_type"],
                    "is_nullable": True,
                    "default_expression": None,
                    "generated": None,
                    "description": None,
                }
            )

        return TableBuilder.build_from_components(
            rels=rels,
            cols=cols,
            pk_cols=[],
            uq_cols=[],
            checks=[],
            fk_cols=[],
            idx_cols=[],
        )

    def _sql_columns(self, catalog: str, schema: str) -> SQLQuery:
        sql = f"""
        SELECT 
            table_name, 
            column_name, 
            ordinal_position, 
            data_type,
            is_nullable
        FROM 
            {catalog}.information_schema.columns
        WHERE 
            table_schema = %(schema)s
        ORDER BY
            table_name,
            ordinal_position
        """
        return SQLQuery(sql, {"schema": schema})

    def _resolve_pseudo_catalog_name(self, file_config: AthenaConfigFile) -> str:
        return "awsdatacatalog"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        return SQLQuery(sql, {"limit": limit})
