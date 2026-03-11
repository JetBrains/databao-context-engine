from __future__ import annotations

from typing import Any, ClassVar, Dict, List

import snowflake.connector
from snowflake.connector import DictCursor
from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.snowflake.config_file import SnowflakeConfigFile


class SnowflakeIntrospector(BaseIntrospector[SnowflakeConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    _IGNORED_CATALOGS = {"STREAMLIT_APPS"}
    supports_catalogs = True
    _USE_BATCH: ClassVar[bool] = False

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

    def collect_catalog_model(self, connection: Any, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if self._USE_BATCH:
            return self.collect_catalog_model_batched(connection, catalog, schemas)
        return super().collect_catalog_model(connection, catalog, schemas)

    def collect_catalog_model_batched(
        self, connection, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._get_catalog_introspection_queries_for_batched_mode(catalog, schemas)

        statements = [c["sql"].sql.rstrip().rstrip(";") for c in comps]
        batch_sql = ";\n".join(statements)

        results: dict[str, list[dict]] = {
            "relations": [],
            "table_columns": [],
            "view_columns": [],
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
            cols=results["table_columns"] + results["view_columns"],
            pk_cols=results["pk"],
            uq_cols=results["uq"],
            checks=[],
            fk_cols=results["fks"],
            idx_cols=[],
        )

    def _get_catalog_introspection_queries_for_batched_mode(self, catalog: str, schemas: list[str]) -> list[dict]:
        return [
            {"name": "relations", "sql": self.get_relations_sql_query(catalog, schemas)},
            {"name": "table_columns", "sql": self.get_table_columns_sql_query(catalog, schemas)},
            {"name": None, "sql": SQLQuery(self._sql_pk_show(catalog), None)},
            {"name": "pk", "sql": self.get_primary_keys_sql_query(catalog, schemas)},
            {"name": None, "sql": SQLQuery(self._sql_fk_show(catalog), None)},
            {"name": "fks", "sql": self.get_foreign_keys_sql_query(catalog, schemas)},
            {"name": None, "sql": SQLQuery(self._sql_uq_show(catalog), None)},
            {"name": "uq", "sql": self.get_unique_constraints_sql_query(catalog, schemas)},
            # view_columns should stay at the end, in case it breaks, so that everything before is still executed
            {"name": "view_columns", "sql": self.get_view_columns_sql_query(catalog, schemas)},
        ]

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        isq = self._qual_is(catalog)
        return SQLQuery(
            f"""
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
        """,
            None,
        )

    @override
    def get_table_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(catalog, schemas, "t.TABLE_TYPE = 'BASE TABLE'")

    @override
    def get_view_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(catalog, schemas, "t.TABLE_TYPE <> 'BASE TABLE'")

    def _columns_sql_query(self, catalog: str, schemas: list[str], table_type_filter: str) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        isq = self._qual_is(catalog)
        return SQLQuery(
            f"""
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
                JOIN {isq}.TABLES AS t
                    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND t.TABLE_NAME = c.TABLE_NAME
            WHERE 
                c.TABLE_SCHEMA IN ({schemas_in})
                AND {table_type_filter}
            ORDER BY 
                c.TABLE_SCHEMA,
                c.TABLE_NAME, 
                c.ORDINAL_POSITION
        """,
            None,
        )

    @override
    def collect_primary_keys(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        with connection.cursor(DictCursor) as cur:
            cur.execute(self._sql_pk_show(catalog))
        return super().collect_primary_keys(connection, catalog, schemas)

    def _sql_pk_show(self, catalog: str) -> str:
        return f"""
            SHOW PRIMARY KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    @override
    def get_primary_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            f"""
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
               """,
            None,
        )

    def collect_foreign_keys(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        with connection.cursor(DictCursor) as cur:
            cur.execute(self._sql_fk_show(catalog))
        return super().collect_foreign_keys(connection, catalog, schemas)

    def _sql_fk_show(self, catalog: str) -> str:
        return f"""
            SHOW IMPORTED KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    @override
    def get_foreign_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            f"""
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
               """,
            None,
        )

    def collect_unique_constraints(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        with connection.cursor(DictCursor) as cur:
            cur.execute(self._sql_uq_show(catalog))
        return super().collect_unique_constraints(connection, catalog, schemas)

    def _sql_uq_show(self, catalog: str) -> str:
        return f"""
            SHOW UNIQUE KEYS IN DATABASE {self._quote_ident(catalog)}
        """

    @override
    def get_unique_constraints_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            f"""
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
               """,
            None,
        )

    @override
    def get_checks_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        isq = self._qual_is(catalog)

        return SQLQuery(
            f"""
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
        """,
            None,
        )

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
