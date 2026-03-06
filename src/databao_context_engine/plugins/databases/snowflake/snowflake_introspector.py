from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import snowflake.connector
from snowflake.connector import DictCursor

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.snowflake.config_file import SnowflakeConfigFile

logger = logging.getLogger(__name__)


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

        table_stats, column_stats = self._collect_stats(
            connection,
            catalog=catalog,
            schemas=schemas,
            relations=results.get("relations", []),
            columns=results.get("columns", []),
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
            table_stats=table_stats,
            column_stats=column_stats,
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
        def normalize_value(v):
            if isinstance(v, datetime):
                return v.isoformat()
            return v

        with connection.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): normalize_value(v) for k, v in row.items()} for row in rows]

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _quote_ident(self, ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    def _qual_is(self, catalog: str) -> str:
        return f"{self._quote_ident(catalog)}.INFORMATION_SCHEMA"

    @staticmethod
    def _lower_keys(rows: List[Dict]) -> List[Dict]:
        return [{k.lower(): v for k, v in row.items()} for row in rows]

    def _collect_stats(
        self,
        connection,
        catalog: str,
        schemas: list[str],
        relations: list[dict],
        columns: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Collect table and column statistics using approximate queries with adaptive sampling.

        Strategy:
        1. Get approximate row counts from INFORMATION_SCHEMA.TABLES (fast, metadata-only)
        2. For each table, collect all column stats in a single query with adaptive sampling
        3. Use APPROX_COUNT_DISTINCT for cardinality (HyperLogLog algorithm, ~2% error, 100x faster)
        4. Apply Bernoulli sampling with multiplier extrapolation for large tables

        Returns:
            Tuple of (table_stats, column_stats) - both as lists of dicts
        """
        table_stats = self._get_table_stats(connection, catalog, schemas)
        table_row_counts = {(ts["schema_name"], ts["table_name"]): ts["row_count"] for ts in table_stats}

        table_columns: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        base_tables = {(r["schema_name"], r["table_name"]) for r in relations if r.get("kind") == "table"}

        for column in columns:
            key = (column["schema_name"], column["table_name"])
            if key in base_tables:
                table_columns[key].append((column["column_name"], column["data_type"]))

        column_stats = []
        for (schema, table), columns_list in table_columns.items():
            try:
                estimated_row_count = table_row_counts.get((schema, table))
                column_names = [col_name for col_name, _ in columns_list]
                table_col_stats = self._collect_column_stats_for_table(
                    connection, schema, table, column_names, estimated_row_count
                )
                column_stats.extend(table_col_stats)
            except Exception as e:
                logger.debug(str(e), exc_info=True, stack_info=True)
                logger.warning(f"Failed to collect column stats for {schema}.{table}: {e}")

        return table_stats, column_stats

    def _get_table_stats(self, connection, catalog: str, schemas: list[str]) -> list[dict]:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        information_schema = self._qual_is(catalog)

        sql = f"""
            SELECT
                TABLE_SCHEMA AS "schema_name",
                TABLE_NAME AS "table_name",
                ROW_COUNT AS "row_count",
                TRUE AS "approximate"
            FROM
                {information_schema}.TABLES
            WHERE
                TABLE_SCHEMA IN ({schemas_in})
                AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY
                TABLE_SCHEMA,
                TABLE_NAME
        """

        return self._fetchall_dicts(connection, sql, None)

    def _collect_column_stats_for_table(
        self,
        connection,
        schema: str,
        table: str,
        columns: list[str],
        estimated_row_count: int | None,
    ) -> list[dict]:
        if not columns:
            return []

        sample_rate = self._determine_sample_rate(estimated_row_count)
        sample_clause = f"TABLESAMPLE BERNOULLI ({sample_rate * 100})" if sample_rate < 1.0 else ""
        is_sampled = sample_rate < 1.0

        column_expressions = []
        for column in columns:
            column_quoted = self._quote_ident(column)
            safe_column = self._sanitize_column_name(column)
            # TODO potentially it's better to use MIN/MAX only for specific colummn types
            column_expressions.append(
                f"""
                COUNT({column_quoted}) AS {self._quote_ident(f"nonnull_{safe_column}")},
                APPROX_COUNT_DISTINCT({column_quoted}) AS {self._quote_ident(f"distinct_{safe_column}")},
                MIN({column_quoted})::VARCHAR AS {self._quote_ident(f"min_{safe_column}")},
                MAX({column_quoted})::VARCHAR AS {self._quote_ident(f"max_{safe_column}")}
            """.strip()
            )

        stats_sql = f"""
            SELECT
                COUNT(*) AS total_count,
                {", ".join(column_expressions)}
            FROM {self._quote_ident(schema)}.{self._quote_ident(table)} {sample_clause}
        """

        try:
            stats_rows = self._fetchall_dicts(connection, stats_sql, None)
            if not stats_rows:
                return []

            stats_row = stats_rows[0]
            sampled_count = stats_row["total_count"]

            if sampled_count == 0:
                return []

            column_stats = []
            for column in columns:
                safe_column = self._sanitize_column_name(column)
                safe_col_lower = safe_column.lower()

                sampled_nonnull = stats_row.get(f"nonnull_{safe_col_lower}", 0)

                # Extrapolate counts if sampled
                if is_sampled:
                    non_null_count = round(sampled_nonnull / sample_rate)
                    total_count = round(sampled_count / sample_rate)
                else:
                    non_null_count = sampled_nonnull
                    total_count = sampled_count

                null_count = max(0, total_count - non_null_count)
                # distinct_count is approximate from APPROX_COUNT_DISTINCT (not extrapolated)
                distinct_count = stats_row.get(f"distinct_{safe_col_lower}")

                column_stats.append(
                    {
                        "schema_name": schema,
                        "table_name": table,
                        "column_name": column,
                        "null_count": null_count,
                        "non_null_count": non_null_count,
                        "distinct_count": distinct_count,
                        "min_value": stats_row.get(f"min_{safe_col_lower}"),
                        "max_value": stats_row.get(f"max_{safe_col_lower}"),
                        "total_row_count": total_count,
                    }
                )

            return column_stats

        except Exception as e:
            logger.warning(f"Failed to collect column stats for {schema}.{table}: {e}")
            return []

    def _sanitize_column_name(self, column: str) -> str:
        return column.replace('"', "").replace("'", "")

    def _determine_sample_rate(self, estimated_row_count: int | None) -> float:
        """Determine sample rate based on table size for cost-effective statistics collection.

        Sampling strategy balances accuracy vs cost:
        - Small tables (<10k): Full scan - negligible cost, exact stats
        - Medium tables (<1M): 10% sample - good accuracy, 10x speedup
        - Large tables (<100M): 1% sample - acceptable accuracy, 100x speedup
        - Very large tables (100M+): 0.1% sample - rough stats, 1000x speedup

        Args:
            estimated_row_count: Approximate row count from table metadata

        Returns:
            Sample rate as a float between 0.001 and 1.0
        """
        if not estimated_row_count:
            return 0.1

        if estimated_row_count < 10_000:
            return 1.0
        if estimated_row_count < 1_000_000:
            return 0.1
        if estimated_row_count < 100_000_000:
            return 0.01
        return 0.001
