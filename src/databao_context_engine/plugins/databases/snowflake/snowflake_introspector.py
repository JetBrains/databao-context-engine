from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, ClassVar, Dict, List

import snowflake.connector
from snowflake.connector import DictCursor
from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.snowflake.config_file import SnowflakeConfigFile

logger = logging.getLogger(__name__)

_SEMISTRUCTURED_TYPES = {
    "OBJECT",
    "VARIANT",
    "ARRAY",
}

_GEOSPATIAL_TYPES = {
    "GEOGRAPHY",
    "GEOMETRY",
}

_UNSUPPORTED_PROFILE_TYPES = _SEMISTRUCTURED_TYPES | _GEOSPATIAL_TYPES


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

        table_stats, column_stats = self.collect_stats(
            connection,
            catalog=catalog,
            schemas=schemas,
            relations=results.get("relations", []),
            columns=results.get("table_columns", []),
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
            table_stats=table_stats,
            column_stats=column_stats,
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

    @override
    def collect_stats(
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
        4. Use APPROX_TOP_K for top values (returns up to 5 most frequent values with counts)
        5. Apply Bernoulli sampling with multiplier extrapolation for large tables
        6. Compute cardinality buckets to categorize columns by distinct value count

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
                table_col_stats = self._collect_column_stats_for_table(
                    connection, schema, table, columns_list, estimated_row_count
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
        columns_list: list[tuple[str, str]],
        estimated_row_count: int | None,
    ) -> list[dict]:
        if not columns_list:
            return []

        sample_rate = self._determine_sample_rate(estimated_row_count)
        sample_clause = f"TABLESAMPLE BERNOULLI ({sample_rate * 100})" if sample_rate < 1.0 else ""
        is_sampled = bool(sample_clause)

        table_ref = f"{self._quote_ident(schema)}.{self._quote_ident(table)}"

        column_expressions = []
        for column, column_type in columns_list:
            column_quoted = self._quote_ident(column)
            safe_column = self._sanitize_column_name(column)
            normalized_type = self._normalize_snowflake_type(column_type)

            distinct_st = self._get_distinct_expression(column_quoted, normalized_type, safe_column)
            min_st, max_st = self._get_min_max_expressions(column_quoted, normalized_type, safe_column)
            topk_st = self._get_topk_expression(column_quoted, normalized_type, safe_column)

            column_expressions.append(
                f"""
                COUNT({column_quoted}) AS {self._quote_ident(f"nonnull_{safe_column}")},
                {distinct_st},
                {min_st},
                {max_st},
                {topk_st}
            """.strip()
            )

        stats_sql = f"""
            SELECT
                COUNT(*) AS sampled_count,
                {", ".join(column_expressions)}
            FROM {table_ref} {sample_clause}
        """

        try:
            stats_rows = self._fetchall_dicts(connection, stats_sql, None)
            if not stats_rows or stats_rows[0]["sampled_count"] == 0:
                return []

            stats_row = stats_rows[0]
            sampled_count = stats_row["sampled_count"]

            column_stats = []
            for column, _ in columns_list:
                safe_column = self._sanitize_column_name(column)
                safe_col_lower = safe_column.lower()

                sampled_nonnull = stats_row.get(f"nonnull_{safe_col_lower}", 0)
                min_value = stats_row.get(f"min_{safe_col_lower}")
                max_value = stats_row.get(f"max_{safe_col_lower}")
                sampled_distinct = stats_row.get(f"distinct_{safe_col_lower}")
                top_values = self._parse_top_k_result(stats_row.get(f"topk_{safe_col_lower}"))

                # Extrapolate counts if sampled
                if is_sampled:
                    non_null_count = round(sampled_nonnull / sample_rate)
                    total_count = estimated_row_count
                else:
                    non_null_count = sampled_nonnull
                    total_count = sampled_count

                null_count = max(0, total_count - non_null_count)

                # Determine cardinality bucket and whether to include the exact count
                cardinality_kind, low_cardinality_distinct_count = self._compute_cardinality_stats(sampled_distinct)

                column_stats.append(
                    {
                        "schema_name": schema,
                        "table_name": table,
                        "column_name": column,
                        "null_count": null_count,
                        "non_null_count": non_null_count,
                        "cardinality_kind": cardinality_kind,
                        "distinct_count": low_cardinality_distinct_count,
                        "min_value": min_value,
                        "max_value": max_value,
                        "top_values": top_values,
                        "total_row_count": total_count,
                    }
                )

            return column_stats

        except Exception as e:
            logger.warning(f"Failed to collect column stats for {schema}.{table}: {e}")
            return []

    def _normalize_snowflake_type(self, column_type: str) -> str:
        return column_type.split("(", 1)[0].strip().upper()

    def _supports_min_max(self, normalized_type: str) -> bool:
        return normalized_type not in _UNSUPPORTED_PROFILE_TYPES

    def _supports_approx_distinct(self, normalized_type: str) -> bool:
        return normalized_type not in _UNSUPPORTED_PROFILE_TYPES

    def _supports_top_k(self, normalized_type: str) -> bool:
        return normalized_type not in _UNSUPPORTED_PROFILE_TYPES and normalized_type != "BINARY"

    def _get_min_max_expressions(self, column_quoted: str, normalized_type: str, safe_column: str) -> tuple[str, str]:
        min_alias = self._quote_ident(f"min_{safe_column}")
        max_alias = self._quote_ident(f"max_{safe_column}")

        if self._supports_min_max(normalized_type):
            return (
                f"MIN({column_quoted})::VARCHAR AS {min_alias}",
                f"MAX({column_quoted})::VARCHAR AS {max_alias}",
            )

        return (
            f"NULL::VARCHAR AS {min_alias}",
            f"NULL::VARCHAR AS {max_alias}",
        )

    def _get_distinct_expression(self, column_quoted: str, normalized_type: str, safe_column: str) -> str:
        alias = self._quote_ident(f"distinct_{safe_column}")

        if self._supports_approx_distinct(normalized_type):
            return f"APPROX_COUNT_DISTINCT({column_quoted}) AS {alias}"

        return f"NULL AS {alias}"

    def _get_topk_expression(self, column_quoted: str, normalized_type: str, safe_column: str) -> str:
        alias = self._quote_ident(f"topk_{safe_column}")

        if self._supports_top_k(normalized_type):
            return f"APPROX_TOP_K({column_quoted}, 5) AS {alias}"

        return f"NULL AS {alias}"

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

    def _parse_top_k_result(self, top_k_json: str | None) -> list[tuple[Any, int]] | None:
        if top_k_json is None:
            return None

        try:
            data_top_k = json.loads(top_k_json) if isinstance(top_k_json, str) else top_k_json
        except (json.JSONDecodeError, TypeError) as e:
            logger.debug("Failed to parse APPROX_TOP_K result: %s", e)
            return None

        if not isinstance(data_top_k, list):
            return None

        result: list[tuple[Any, int]] = []
        for item in data_top_k:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue

            value, count = item
            try:
                result.append((value, int(count)))
            except (TypeError, ValueError):
                continue

        return result or None
