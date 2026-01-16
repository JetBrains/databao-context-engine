from __future__ import annotations

from typing import Any, Mapping

from pyathena import connect
from pyathena.cursor import DictCursor
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseSchema
from nemory.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


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

    # TODO: Incomplete plugin. Awaiting permission access to AWS to properly develop
    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = {"columns": self._sql_columns(catalog, schemas)}
        results: dict[str, list[dict]] = {}

        for name, q in comps.items():
            results[name] = self._fetchall_dicts(connection, q.sql, q.params)

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=[],
            uq_cols=[],
            checks=[],
            fk_cols=[],
            idx_cols=[],
        )

    def _sql_columns(self, catalog: str, schemas: list[str]) -> SQLQuery:
        sql = f"""
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
            table_schema IN ({schemas})
        ORDER BY
            table_schema,
            table_name,
            ordinal_position
        """
        return SQLQuery(sql, {"schema": schemas})

    def _resolve_pseudo_catalog_name(self, file_config: AthenaConfigFile) -> str:
        return "awsdatacatalog"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        return SQLQuery(sql, {"limit": limit})
