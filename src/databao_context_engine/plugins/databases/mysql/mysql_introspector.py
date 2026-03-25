import base64
import json
import logging
from typing import ClassVar

import pymysql
from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import (
    CatalogScope,
    ColumnStats,
    ColumnStatsEntry,
    DatabaseSchema,
    TableStats,
    TableStatsEntry,
)
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.mysql.config_file import MySQLConfigFile

logger = logging.getLogger(__name__)


class MySQLIntrospector(BaseIntrospector[MySQLConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}

    supports_catalogs = True
    _USE_BATCH: ClassVar[bool] = False

    def _get_catalogs(self, connection, file_config: MySQLConfigFile) -> list[str]:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                ORDER BY schema_name
                """
            )
            dbs = [row["SCHEMA_NAME"] for row in cur.fetchall()]
        return [d for d in dbs if d.lower() not in self._IGNORED_SCHEMAS]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        return SQLQuery(
            "SELECT DATABASE() AS schema_name, DATABASE() AS catalog_name",
            None,
        )

    @override
    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if self._USE_BATCH:
            return self.collect_catalog_model_batched(connection, catalog, schemas)

        return super().collect_catalog_model(connection, catalog, schemas)

    def collect_catalog_model_batched(
        self, connection, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        introspection_queries = self._get_catalog_introspection_queries_for_batched_mode(catalog, schemas)
        results: dict[str, list[dict]] = {name: [] for name in introspection_queries}
        sql_queries = {name: query for name, query in introspection_queries.items() if query is not None}
        batch = ";\n".join(query.sql.rstrip().rstrip(";") for query in sql_queries.values())

        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(batch)

            for ix, name in enumerate(sql_queries.keys(), start=1):
                raw_rows = cur.fetchall() if cur.description else ()

                if raw_rows and isinstance(raw_rows[0], dict):
                    rows_list = [{k.lower(): v for k, v in row.items()} for row in raw_rows]
                else:
                    if cur.description:
                        cols = [d[0].lower() for d in cur.description]
                        rows_list = [dict(zip(cols, r)) for r in raw_rows]
                    else:
                        rows_list = []

                results[name] = rows_list

                if ix < len(sql_queries):
                    ok = cur.nextset()
                    if not ok:
                        raise RuntimeError(f"MySQL batch ended early after component #{ix} '{name}'")

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("table_columns", []) + results.get("view_columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=results.get("checks", []),
            fk_cols=results.get("fks", []),
            idx_cols=results.get("idx", []),
            partitions=results.get("partitions", []),
        )

    def _get_catalog_introspection_queries_for_batched_mode(
        self, catalog: str, schemas: list[str]
    ) -> dict[str, SQLQuery | None]:
        return {
            "relations": self.get_relations_sql_query(catalog, schemas),
            "table_columns": self.get_table_columns_sql_query(catalog, schemas),
            "pk": self.get_primary_keys_sql_query(catalog, schemas),
            "uq": self.get_unique_constraints_sql_query(catalog, schemas),
            "checks": self.get_checks_sql_query(catalog, schemas),
            "fks": self.get_foreign_keys_sql_query(catalog, schemas),
            "idx": self.get_indexes_sql_query(catalog, schemas),
            "partitions": self.get_partitions_sql_query(catalog, schemas),
            # view_columns should stay at the end, in case it breaks, so that everything before is still executed
            "view_columns": self.get_view_columns_sql_query(catalog, schemas),
        }

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                t.TABLE_SCHEMA AS schema_name,
                t.TABLE_NAME        AS table_name,
                CASE t.TABLE_TYPE
                    WHEN 'BASE TABLE'  THEN 'table'
                    WHEN 'VIEW'        THEN 'view'
                    ELSE LOWER(t.TABLE_TYPE)
                END                 AS kind,
                CASE t.TABLE_TYPE
                    WHEN 'VIEW' THEN NULL
                    ELSE NULLIF(t.TABLE_COMMENT, '')
                END                 AS description
            FROM 
                INFORMATION_SCHEMA.TABLES t
            WHERE 
                t.TABLE_SCHEMA IN ({schemas_sql})
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
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                c.TABLE_SCHEMA AS schema_name,
                c.TABLE_NAME                         AS table_name,
                c.COLUMN_NAME                        AS column_name,
                c.ORDINAL_POSITION                   AS ordinal_position,
                c.COLUMN_TYPE                        AS data_type,
                CASE 
                    WHEN c.IS_NULLABLE = 'YES' THEN TRUE 
                    ELSE FALSE 
                END AS is_nullable,
                CASE
                    WHEN c.EXTRA RLIKE '\\b(VIRTUAL|STORED) GENERATED\\b' THEN NULLIF(c.GENERATION_EXPRESSION, '')
                    ELSE c.COLUMN_DEFAULT
                END AS default_expression,
                CASE
                    WHEN c.EXTRA LIKE '%auto_increment%' THEN 'identity'
                    WHEN c.EXTRA RLIKE '\\b(VIRTUAL|STORED) GENERATED\\b' THEN 'computed'
                    ELSE NULL
                END AS "generated",
                NULLIF(c.COLUMN_COMMENT, '')         AS description
            FROM 
                INFORMATION_SCHEMA.COLUMNS c
                JOIN INFORMATION_SCHEMA.TABLES t
                    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND t.TABLE_NAME = c.TABLE_NAME
            WHERE 
                c.TABLE_SCHEMA IN ({schemas_sql})
                AND {table_type_filter}
            ORDER BY 
                c.TABLE_SCHEMA,
                c.TABLE_NAME, 
                c.ORDINAL_POSITION
            """,
            None,
        )

    @override
    def get_primary_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                tc.TABLE_SCHEMA AS schema_name,
                tc.TABLE_NAME         AS table_name,
                tc.CONSTRAINT_NAME    AS constraint_name,
                kcu.COLUMN_NAME       AS column_name,
                kcu.ORDINAL_POSITION  AS position
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                     ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE 
                tc.TABLE_SCHEMA IN ({schemas_sql})
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
            """,
            None,
        )

    @override
    def get_unique_constraints_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                tc.TABLE_SCHEMA AS schema_name,
                tc.TABLE_NAME         AS table_name,
                tc.CONSTRAINT_NAME    AS constraint_name,
                kcu.COLUMN_NAME       AS column_name,
                kcu.ORDINAL_POSITION  AS position
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE 
                tc.TABLE_SCHEMA IN ({schemas_sql})
                AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            ORDER BY 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
            """,
            None,
        )

    @override
    def get_checks_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                tc.TABLE_SCHEMA AS schema_name,
                tc.TABLE_NAME        AS table_name,
                tc.CONSTRAINT_NAME   AS constraint_name,
                cc.CHECK_CLAUSE      AS expression,
                TRUE                 AS validated
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA = tc.TABLE_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE 
                tc.TABLE_SCHEMA IN ({schemas_sql})
                AND tc.CONSTRAINT_TYPE = 'CHECK'
            ORDER BY 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME
            """,
            None,
        )

    @override
    def get_foreign_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                kcu.TABLE_SCHEMA AS schema_name,
                kcu.TABLE_NAME                 AS table_name,
                kcu.CONSTRAINT_NAME            AS constraint_name,
                kcu.ORDINAL_POSITION           AS position,
                kcu.COLUMN_NAME                AS from_column,
                kcu.REFERENCED_TABLE_SCHEMA    AS ref_schema,
                kcu.REFERENCED_TABLE_NAME      AS ref_table,
                kcu.REFERENCED_COLUMN_NAME     AS to_column,
                LOWER(rc.UPDATE_RULE)          AS on_update,
                LOWER(rc.DELETE_RULE)          AS on_delete,
                TRUE                           AS enforced,
                TRUE                           AS validated
            FROM 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME
                JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE 
                kcu.TABLE_SCHEMA IN ({schemas_sql})
                AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            ORDER BY 
                kcu.TABLE_SCHEMA,
                kcu.TABLE_NAME, 
                kcu.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
            """,
            None,
        )

    @override
    def get_indexes_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)
        return SQLQuery(
            rf"""
            SELECT
                s.TABLE_SCHEMA AS schema_name,
                s.TABLE_NAME                                    AS table_name,
                s.INDEX_NAME                                    AS index_name,
                s.SEQ_IN_INDEX                                  AS position,
                COALESCE(s.EXPRESSION, s.COLUMN_NAME)           AS expr,
                (s.NON_UNIQUE = 0)                              AS is_unique,
                s.INDEX_TYPE                                    AS method,
                NULL                                            AS predicate
            FROM 
                INFORMATION_SCHEMA.STATISTICS s
            WHERE 
                s.TABLE_SCHEMA IN ({schemas_sql})
                AND s.INDEX_NAME <> 'PRIMARY'
            ORDER BY 
                s.TABLE_SCHEMA,
                s.TABLE_NAME, 
                s.INDEX_NAME, 
                s.SEQ_IN_INDEX
            """,
            None,
        )

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f"SELECT * FROM `{schema}`.`{table}` LIMIT %s"
        return SQLQuery(sql, (limit,))

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _quote_ident(self, ident: str) -> str:
        return "`" + ident.replace("`", "``") + "`"

    @override
    def collect_stats(
        self,
        connection,
        catalog: str,
        scope: CatalogScope,
    ) -> tuple[list[TableStatsEntry], list[ColumnStatsEntry]]:
        schema_names = [schema_scope.schema_name for schema_scope in scope.schemas]
        self._run_analyze(connection, scope)
        table_stats = self._get_table_stats(connection, schema_names)
        column_stats = self._get_column_stats(connection, schema_names, table_stats)
        return table_stats, column_stats

    def _run_analyze(self, connection, scope: CatalogScope) -> None:
        table_columns: dict[tuple[str, str], list[str]] = {}
        for schema_scope in scope.schemas:
            for table_ref in schema_scope.tables:
                if table_ref.kind.value == "table" and table_ref.columns:
                    table_columns[(schema_scope.schema_name, table_ref.table_name)] = [
                        col.name for col in table_ref.columns
                    ]

        n_buckets = 100  # postgres uses the same default
        with connection.cursor() as cur:
            for (schema, table), cols_list in table_columns.items():
                try:
                    cur.execute(f"ANALYZE TABLE {self._quote_ident(schema)}.{self._quote_ident(table)}")
                    cur.fetchall()
                    if cols_list:
                        col_names = ", ".join(self._quote_ident(c) for c in cols_list)
                        cur.execute(
                            f"ANALYZE TABLE {self._quote_ident(schema)}.{self._quote_ident(table)} "
                            f"UPDATE HISTOGRAM ON {col_names} WITH {n_buckets} BUCKETS"
                        )
                        cur.fetchall()
                except Exception as e:
                    logger.warning(f"Failed to analyze table {schema}.{table}: {e}")

    def _get_table_stats(self, connection, schemas: list[str]) -> list[TableStatsEntry]:
        rows = self._connector.execute(
            connection,
            """
            SELECT
                t.TABLE_SCHEMA AS schema_name,
                t.TABLE_NAME AS table_name,
                t.TABLE_ROWS AS row_count,
                TRUE AS approximate
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_SCHEMA IN ({})
              AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """.format(", ".join(self._quote_literal(s) for s in schemas)),
            None,
        )
        return [
            TableStatsEntry(
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                stats=TableStats(row_count=r.get("row_count"), approximate=bool(r.get("approximate", True))),
            )
            for r in rows
        ]

    def _get_column_stats(
        self, connection, schemas: list[str], table_stats: list[TableStatsEntry]
    ) -> list[ColumnStatsEntry]:
        raw_stats = self._connector.execute(
            connection,
            """
            SELECT
                s.SCHEMA_NAME AS schema_name,
                s.TABLE_NAME AS table_name,
                s.COLUMN_NAME AS column_name,
                s.HISTOGRAM AS histogram_json
            FROM INFORMATION_SCHEMA.COLUMN_STATISTICS s
            WHERE s.SCHEMA_NAME IN ({})
            ORDER BY s.SCHEMA_NAME, s.TABLE_NAME, s.COLUMN_NAME
            """.format(", ".join(self._quote_literal(s) for s in schemas)),
            None,
        )

        table_row_counts = {(ts.schema_name, ts.table_name): ts.stats.row_count for ts in table_stats}

        processed_stats: list[ColumnStatsEntry] = []
        for stat in raw_stats:
            if not stat.get("histogram_json"):
                continue
            result = self._process_histogram(stat, table_row_counts)
            if result:
                processed_stats.append(result)

        return processed_stats

    def _process_histogram(
        self, stat: dict, table_row_counts: dict[tuple[str, str], int | None]
    ) -> ColumnStatsEntry | None:
        try:
            histogram_json = stat["histogram_json"]
            histogram = json.loads(histogram_json) if isinstance(histogram_json, str) else histogram_json
            histogram_type = histogram.get("histogram-type")

            table_key = (stat["schema_name"], stat["table_name"])
            table_row_count = table_row_counts.get(table_key) or 0

            col_stats = ColumnStats(total_row_count=table_row_count)

            if histogram_type == "singleton":
                self._process_singleton_histogram(histogram, col_stats, table_row_count)
            elif histogram_type == "equi-height":
                self._process_equiheight_histogram(histogram, col_stats, table_row_count)

            return ColumnStatsEntry(
                schema_name=stat["schema_name"],
                table_name=stat["table_name"],
                column_name=stat["column_name"],
                stats=col_stats,
            )

        except Exception as e:
            logger.warning(
                f"Failed to process histogram for {stat['schema_name']}.{stat['table_name']}.{stat['column_name']}: {e}"
            )
            return None

    def _process_singleton_histogram(self, histogram: dict, stats: ColumnStats, table_row_count: int) -> None:
        buckets = histogram.get("buckets", [])
        distinct_count = len(buckets)

        null_frac = float(histogram.get("null-values") or 0.0)
        null_count = max(0, min(table_row_count, round(null_frac * table_row_count)))

        cardinality_kind, low_cardinality_distinct_count = self._compute_cardinality_stats(distinct_count)

        stats.cardinality_kind = cardinality_kind
        stats.distinct_count = low_cardinality_distinct_count
        stats.non_null_count = table_row_count - null_count
        stats.null_count = null_count

        if buckets:
            stats.min_value = self._decode_histogram_value(buckets[0][0])
            stats.max_value = self._decode_histogram_value(buckets[-1][0])

            top_values = []
            prev_cumulative = 0.0
            for raw_val, cumulative_freq in buckets:
                value = self._decode_histogram_value(raw_val)
                frequency_fraction = cumulative_freq - prev_cumulative
                frequency_count = round(frequency_fraction * table_row_count)
                top_values.append((value, frequency_count))
                prev_cumulative = cumulative_freq

            top_values.sort(key=lambda x: x[1], reverse=True)
            stats.top_values = top_values[:5]

    def _process_equiheight_histogram(self, histogram: dict, stats: ColumnStats, table_row_count: int) -> None:
        buckets = histogram.get("buckets", [])
        null_frac = float(histogram.get("null-values") or 0.0)
        null_count = max(0, min(table_row_count, round(null_frac * table_row_count)))
        stats.null_count = null_count
        stats.non_null_count = table_row_count - null_count

        # distinct estimate: sum the 4th element of each bucket if present
        distinct_est = 0
        for b in buckets:
            if isinstance(b, list) and len(b) >= 4:
                distinct_est += int(b[3] or 0)

        cardinality_kind, low_cardinality_distinct_count = self._compute_cardinality_stats(distinct_est)
        stats.cardinality_kind = cardinality_kind
        stats.distinct_count = low_cardinality_distinct_count

        if buckets:
            first_bucket = buckets[0]
            last_bucket = buckets[-1]
            stats.min_value = self._decode_histogram_value(first_bucket[0])
            stats.max_value = self._decode_histogram_value(last_bucket[1])

    @staticmethod
    def _decode_histogram_value(value):
        if not (isinstance(value, str) and value.startswith("base64:")):
            return value
        try:
            _, _, payload = value.split(":", 2)
            raw = base64.b64decode(payload, validate=True)
            return raw.decode("utf-8", errors="replace")
        except Exception as e:
            logger.debug(f"Failed to decode histogram value: {e}")
            return None
