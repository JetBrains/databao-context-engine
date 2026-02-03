from __future__ import annotations

from typing import Annotated, Any, Dict, List

import snowflake.connector
from pydantic import BaseModel, Field
from snowflake.connector import DictCursor

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class SnowflakePasswordAuth(BaseModel):
    password: Annotated[str, ConfigPropertyAnnotation(secret=True)]


class SnowflakeKeyPairAuth(BaseModel):
    private_key_file: str | None = None
    private_key_file_pwd: str | None = None
    private_key: Annotated[str, ConfigPropertyAnnotation(secret=True)] = None


class SnowflakeSSOAuth(BaseModel):
    authenticator: str = Field(description='e.g. "externalbrowser"')


class SnowflakeConnectionProperties(BaseModel):
    account: Annotated[str, ConfigPropertyAnnotation(required=True)]
    warehouse: str | None = None
    database: str | None = None
    user: str | None = None
    role: str | None = None
    auth: SnowflakePasswordAuth | SnowflakeKeyPairAuth | SnowflakeSSOAuth
    additional_properties: dict[str, Any] = {}

    def to_snowflake_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(
            exclude={
                "additional_properties": True,
            },
            exclude_none=True,
        )
        auth_fields = kwargs.pop("auth", {})
        kwargs.update(auth_fields)
        kwargs.update(self.additional_properties)
        return kwargs


class SnowflakeConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="snowflake")
    connection: SnowflakeConnectionProperties


class SnowflakeIntrospector(BaseIntrospector[SnowflakeConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    _IGNORED_CATALOGS = {"STREAMLIT_APPS"}
    supports_catalogs = True

    def _connect(self, file_config: SnowflakeConfigFile, *, catalog: str | None = None):
        connection = file_config.connection
        snowflake.connector.paramstyle = "qmark"
        connection_kwargs = connection.to_snowflake_kwargs()
        if catalog:
            connection_kwargs["database"] = catalog

        return snowflake.connector.connect(
            **connection_kwargs,
        )

    def _get_catalogs(self, connection, file_config: SnowflakeConfigFile) -> list[str]:
        database = file_config.connection.database
        if database:
            return [database]

        rows = self._fetchall_dicts(connection, "SHOW DATABASES", None)
        return [r["name"] for r in rows if r["name"] and r["name"].upper() not in self._IGNORED_CATALOGS]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        parts = []
        for catalog in catalogs:
            parts.append(f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata")
        return SQLQuery(" UNION ALL ".join(parts), None)

    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries(catalog, schemas)

        statements = [c["sql"].rstrip().rstrip(";") for c in comps]
        batch_sql = ";\n".join(statements)

        results: dict[str, list[dict]] = {
            "relations": [],
            "columns": [],
            "pk": [],
            "fks": [],
            "uq": [],
        }

        with connection.cursor(DictCursor) as cur:
            cur.execute(batch_sql, num_statements=len(statements))

            for ix, comp in enumerate(comps, start=1):
                name = comp["name"]

                rows = self._lower_keys(cur.fetchall()) if cur.description else []

                if name:
                    results[name].extend(rows)

                if ix < len(comps):
                    ok = cur.nextset()
                    if not ok:
                        raise RuntimeError(
                            f"Snowflake multi-statement batch ended early after component #{ix} '{name}'"
                        )

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results["relations"],
            cols=results["columns"],
            pk_cols=results["pk"],
            uq_cols=results["uq"],
            checks=[],
            fk_cols=results["fks"],
            idx_cols=[],
        )

    def _component_queries(self, catalog: str, schemas: list[str]) -> list[dict]:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)

        return [
            {"name": "relations", "sql": self._sql_relations(catalog, schemas_in)},
            {"name": "columns", "sql": self._sql_columns(catalog, schemas_in)},
            {"name": None, "sql": self._sql_pk_show(catalog)},
            {"name": "pk", "sql": self._sql_pk_select(schemas_in)},
            {"name": None, "sql": self._sql_fk_show(catalog)},
            {"name": "fks", "sql": self._sql_fk_select(schemas_in)},
            {"name": None, "sql": self._sql_uq_show(catalog)},
            {"name": "uq", "sql": self._sql_uq_select(schemas_in)},
        ]

    def _sql_relations(self, catalog: str, schemas_in: str) -> str:
        isq = self._qual_is(catalog)

        return f"""
            SELECT
                t.TABLE_SCHEMA AS "schema_name",
                t.TABLE_NAME AS "table_name",
                CASE t.TABLE_TYPE
                    WHEN 'BASE TABLE'        THEN 'table'
                    WHEN 'VIEW'              THEN 'view'
                    WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
                    WHEN 'EXTERNAL TABLE'    THEN 'external_table'
                    ELSE LOWER(t.TABLE_TYPE)
                END AS "kind",
                t.COMMENT AS "description"
            FROM 
                {isq}.TABLES AS t
            WHERE 
                t.TABLE_SCHEMA IN ({schemas_in})
            ORDER BY 
                t.TABLE_SCHEMA,
                t.TABLE_NAME
        """

    def _sql_columns(self, catalog: str, schemas_in: str) -> str:
        isq = self._qual_is(catalog)

        return f"""
            SELECT
            c.TABLE_SCHEMA AS "schema_name",
                c.TABLE_NAME       AS "table_name",
                c.COLUMN_NAME      AS "column_name",
                c.ORDINAL_POSITION AS "ordinal_position",
                c.DATA_TYPE        AS "data_type",
                IFF(c.IS_NULLABLE = 'YES', TRUE, FALSE) AS "is_nullable",
                c.COLUMN_DEFAULT   AS "default_expression",
                IFF(c.IS_IDENTITY = 'YES', 'identity', NULL) AS "generated",
                c.COMMENT          AS "description"
            FROM 
                {isq}.COLUMNS AS c
            WHERE 
                c.TABLE_SCHEMA IN ({schemas_in})
            ORDER BY 
                c.TABLE_SCHEMA,
                c.TABLE_NAME, 
                c.ORDINAL_POSITION
        """

    def _sql_pk_show(self, catalog: str) -> str:
        return f"""
            SHOW PRIMARY KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    def _sql_pk_select(self, schemas_in: str) -> str:
        return f"""
               SELECT
                   "schema_name" AS schema_name,
                   "table_name" AS table_name,
                   "constraint_name"   AS constraint_name,
                   "column_name"       AS column_name,
                   "key_sequence"::INT AS position
               FROM 
                   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               WHERE
                    "schema_name" IN ({schemas_in})
               ORDER BY 
                   table_name, 
                   constraint_name, 
                   position
               """

    def _sql_fk_show(self, catalog: str) -> str:
        return f"""
            SHOW IMPORTED KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    def _sql_fk_select(self, schemas_in: str) -> str:
        return f"""
               SELECT
                   "fk_schema_name"     AS "schema_name",
                   "fk_table_name"      AS "table_name",
                   "fk_name"            AS "constraint_name",
                   "key_sequence"::INT  AS "position",
                   "fk_column_name"     AS "from_column",
                   "pk_schema_name"     AS "ref_schema",
                   "pk_table_name"      AS "ref_table",
                   "pk_column_name"     AS "to_column",
                   LOWER("update_rule") AS "on_update",
                   LOWER("delete_rule") AS "on_delete",
                   NULL::BOOLEAN        AS "enforced",
                   NULL::BOOLEAN        AS "validated"
               FROM 
                   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               WHERE
                   "fk_schema_name" IN ({schemas_in})
               ORDER BY 
                   "schema_name",
                   "table_name", 
                   "constraint_name", 
                   "position"
               """

    def _sql_uq_show(self, catalog: str) -> str:
        return f"""
            SHOW UNIQUE KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    def _sql_uq_select(self, schemas_in: str) -> str:
        return f"""
               SELECT
                   "schema_name"       AS "schema_name",
                   "table_name"        AS "table_name",
                   "constraint_name"   AS "constraint_name",
                   "column_name"       AS "column_name",
                   "key_sequence"::INT AS "position"
               FROM 
                   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               WHERE
                   "schema_name" IN ({schemas_in})
               ORDER BY 
                   "schema_name",
                   "table_name", 
                   "constraint_name", 
                   "position"
               """

    def _sql_checks(self, catalog: str, schemas_in: str) -> str:
        isq = self._qual_is(catalog)

        return f"""
            SELECT
                tc.TABLE_SCHEMA    AS "schema_name",
                tc.TABLE_NAME      AS "table_name",
                tc.CONSTRAINT_NAME AS "constraint_name",
                NULL::VARCHAR      AS "expression",
                TRUE               AS "validated"
            FROM 
                {isq}.TABLE_CONSTRAINTS AS tc
            WHERE 
                tc.TABLE_SCHEMA IN ({schemas_in})
                AND tc.CONSTRAINT_TYPE = 'CHECK'
            ORDER BY 
                tc.TABLE_SCHEMA, 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME
        """

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT ?'
        return SQLQuery(sql, (limit,))

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): v for k, v in row.items()} for row in rows]

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _quote_ident(self, ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    def _qual_is(self, catalog: str) -> str:
        return f"{self._quote_ident(catalog)}.INFORMATION_SCHEMA"

    @staticmethod
    def _lower_keys(rows: List[Dict]) -> List[Dict]:
        return [{k.lower(): v for k, v in row.items()} for row in rows]
