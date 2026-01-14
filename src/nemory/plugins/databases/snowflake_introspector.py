from __future__ import annotations

from typing import Any, Dict, List, Mapping

import snowflake.connector
from pydantic import Field
from snowflake.connector import DictCursor

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseTable
from nemory.plugins.databases.table_builder import TableBuilder


class SnowflakeConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/snowflake")
    connection: dict[str, Any] = Field(
        description="Connection parameters for Snowflake. It can contain any of the keys supported by the Snowflake connection library"
    )


class SnowflakeIntrospector(BaseIntrospector[SnowflakeConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    _IGNORED_CATALOGS = {"STREAMLIT_APPS"}
    supports_catalogs = True

    def _connect(self, file_config: SnowflakeConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")
        snowflake.connector.paramstyle = "qmark"
        return snowflake.connector.connect(
            **connection,
        )

    def _connect_to_catalog(self, file_config: SnowflakeConfigFile, catalog: str):
        cfg = dict(file_config.connection or {})
        cfg["database"] = catalog
        return snowflake.connector.connect(**cfg)

    def _get_catalogs(self, connection, file_config: SnowflakeConfigFile) -> list[str]:
        database = file_config.connection.get("database")
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

    def collect_schema_model(self, connection, catalog: str, schema: str) -> list[DatabaseTable] | None:
        comps = self._component_queries(catalog, schema)

        statements = [c["sql"].rstrip().rstrip(";") for c in comps]
        batch_sql = ";\n".join(statements)

        results: dict[str, list[dict]] = {c["name"]: [] for c in comps if c["name"]}

        with connection.cursor(DictCursor) as cur:
            cur.execute(batch_sql, num_statements=len(statements))

            for ix, comp in enumerate(comps, start=1):
                name = comp["name"]

                rows = self._lower_keys(cur.fetchall()) if cur.description else []

                if name:
                    results[name] = rows

                if ix < len(comps):
                    ok = cur.nextset()
                    if not ok:
                        raise RuntimeError(
                            f"Snowflake multi-statement batch ended early after component #{ix} '{name}'"
                        )

        return TableBuilder.build_from_components(
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=[],
            fk_cols=results.get("fks", []),
            idx_cols=[],
        )

    def _component_queries(self, catalog: str, schema: str) -> list[dict]:
        return [
            {"name": "relations", "sql": self._sql_relations(catalog, schema)},
            {"name": "columns", "sql": self._sql_columns(catalog, schema)},
            {"name": None, "sql": self._sql_pk_show(catalog, schema)},
            {"name": "pk", "sql": self._sql_pk_select()},
            {"name": None, "sql": self._sql_fk_show(catalog, schema)},
            {"name": "fks", "sql": self._sql_fk_select()},
            {"name": None, "sql": self._sql_uq_show(catalog, schema)},
            {"name": "uq", "sql": self._sql_uq_select()},
        ]

    def _sql_relations(self, catalog: str, schema: str) -> str:
        isq = self._qual_is(catalog)
        schema_lit = self._quote_literal(schema)

        return f"""
            SELECT
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
                t.TABLE_SCHEMA = {schema_lit}
            ORDER BY 
                t.TABLE_NAME
        """

    def _sql_columns(self, catalog: str, schema: str) -> str:
        isq = self._qual_is(catalog)
        schema_lit = self._quote_literal(schema)

        return f"""
            SELECT
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
                c.TABLE_SCHEMA = {schema_lit}
            ORDER BY 
                c.TABLE_NAME, 
                c.ORDINAL_POSITION
        """

    def _sql_pk_show(self, catalog: str, schema: str) -> str:
        return f"""
            SHOW PRIMARY KEYS IN SCHEMA {self._quote_ident(catalog)}.{self._quote_ident(schema)}
        """

    def _sql_pk_select(self) -> str:
        return """
               SELECT
                   "table_name" AS table_name,
                   "constraint_name"   AS constraint_name,
                   "column_name"       AS column_name,
                   "key_sequence"::INT AS position
               FROM 
                   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               ORDER BY 
                   table_name, 
                   constraint_name, 
                   position
               """

    def _sql_fk_show(self, catalog: str, schema: str) -> str:
        return f"""
            SHOW IMPORTED KEYS IN SCHEMA {self._quote_ident(catalog)}.{self._quote_ident(schema)}
        """

    def _sql_fk_select(self) -> str:
        return """
               SELECT
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
               ORDER BY 
                   "table_name", 
                   "constraint_name", 
                   "position"
               """

    def _sql_uq_show(self, catalog: str, schema: str) -> str:
        return f"""
            SHOW UNIQUE KEYS IN SCHEMA {self._quote_ident(catalog)}.{self._quote_ident(schema)}
        """

    def _sql_uq_select(self) -> str:
        return """
               SELECT
                   "table_name"        AS "table_name",
                   "constraint_name"   AS "constraint_name",
                   "column_name"       AS "column_name",
                   "key_sequence"::INT AS "position"
               FROM 
                   TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               ORDER BY 
                   "table_name", 
                   "constraint_name", 
                   "position"
               """

    def _sql_checks(self, catalog: str, schema: str) -> str:
        isq = self._qual_is(catalog)
        schema_lit = self._quote_literal(schema)

        return f"""
            SELECT
                tc.TABLE_NAME      AS "table_name",
                tc.CONSTRAINT_NAME AS "constraint_name",
                NULL::VARCHAR      AS "expression",
                TRUE               AS "validated"
            FROM 
                {isq}.TABLE_CONSTRAINTS AS tc
            WHERE 
                tc.TABLE_SCHEMA = {schema_lit}
                AND tc.CONSTRAINT_TYPE = 'CHECK'
            ORDER BY 
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
