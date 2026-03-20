import logging

from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import (
    CatalogScope,
    ColumnStats,
    ColumnStatsEntry,
    TableStats,
    TableStatsEntry,
)
from databao_context_engine.plugins.databases.postgresql.config_file import (
    PostgresConfigFile,
)
from databao_context_engine.plugins.databases.postgresql.postgresql_connector import _SyncAsyncpgConnection

logger = logging.getLogger(__name__)


class PostgresqlIntrospector(BaseIntrospector[PostgresConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY($1)"
            return SQLQuery(sql, (catalogs,))

        sql = "SELECT schema_name FROM information_schema.schemata"
        return SQLQuery(sql, None)

    def _get_catalogs(self, connection: _SyncAsyncpgConnection, file_config: PostgresConfigFile) -> list[str]:
        database = file_config.connection.database
        if database is not None:
            return [database]

        return connection.fetch_scalar_values("SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false")

    @override
    def collect_stats(
        self,
        connection,
        catalog: str,
        scope: CatalogScope,
    ) -> tuple[list[TableStatsEntry], list[ColumnStatsEntry]]:
        schema_names = [schema_scope.schema_name for schema_scope in scope.schemas]
        table_stats_query = SQLQuery(self._sql_table_stats(), (schema_names,))
        table_stat_rows = self._connector.execute(connection, table_stats_query.sql, table_stats_query.params)
        table_stats = [
            TableStatsEntry(
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                stats=TableStats(row_count=r.get("row_count"), approximate=bool(r.get("approximate", True))),
            )
            for r in table_stat_rows
        ]

        column_stats_query = SQLQuery(self._sql_column_stats(), (schema_names,))
        column_stat_rows = self._connector.execute(connection, column_stats_query.sql, column_stats_query.params)
        column_stats = self._enrich_column_stats(column_stat_rows, table_stats)

        return table_stats, column_stats

    def _enrich_column_stats(
        self, column_stat_rows: list[dict], table_stats: list[TableStatsEntry]
    ) -> list[ColumnStatsEntry]:
        def parse_pg_array(arr_str: str | None) -> list[str] | None:
            """Simple parser that doesn't handle quoted strings with commas or escapes."""
            if not arr_str or not isinstance(arr_str, str):
                return None
            if not arr_str.startswith("{") or not arr_str.endswith("}"):
                return None
            content = arr_str[1:-1]
            if not content:
                return []
            return [v.strip() for v in content.split(",") if v.strip()]

        table_row_counts = {(ts.schema_name, ts.table_name): ts.stats.row_count for ts in table_stats}

        result: list[ColumnStatsEntry] = []
        for row in column_stat_rows:
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            row_count = table_row_counts.get((schema_name, table_name))

            min_value = None
            max_value = None
            bounds = parse_pg_array(row.get("histogram_bounds"))
            if bounds:
                min_value = bounds[0]
                max_value = bounds[-1]

            null_count = None
            non_null_count = None
            null_frac = row.get("null_frac")
            if null_frac is not None and row_count is not None:
                null_count = round(row_count * null_frac)
                non_null_count = row_count - null_count

            n_distinct = row.get("n_distinct")
            distinct_count = None
            if n_distinct is not None and row_count is not None:
                if n_distinct < 0:
                    distinct_count = round(abs(n_distinct) * row_count)
                elif n_distinct > 0:
                    distinct_count = round(n_distinct)

            cardinality_kind, low_cardinality_distinct_count = self._compute_cardinality_stats(distinct_count)

            top_values = None
            vals_str = row.get("most_common_vals")
            freqs_str = row.get("most_common_freqs")
            top_n = 5
            if vals_str and freqs_str and row_count is not None:
                vals = parse_pg_array(vals_str)
                freqs = parse_pg_array(freqs_str)
                if vals and freqs and len(vals) == len(freqs):
                    try:
                        top_values = [(str(v), round(float(f) * row_count)) for v, f in zip(vals, freqs)][:top_n]
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Failed to parse column stats for {schema_name}.{table_name}.{row['column_name']}"
                        )

            result.append(
                ColumnStatsEntry(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=row["column_name"],
                    stats=ColumnStats(
                        null_count=null_count,
                        non_null_count=non_null_count,
                        distinct_count=low_cardinality_distinct_count,
                        cardinality_kind=cardinality_kind,
                        min_value=min_value,
                        max_value=max_value,
                        top_values=top_values,
                        total_row_count=row_count,
                    ),
                )
            )

        return result

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT 
                n.nspname AS schema_name,
                c.relname AS table_name,
                CASE c.relkind
                    WHEN 'v' THEN 'view'
                    WHEN 'm' THEN 'materialized_view'
                    WHEN 'f' THEN 'external_table'
                    ELSE 'table'   
                END AS kind,
                obj_description(c.oid, 'pg_class') AS description
            FROM 
                pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE 
                n.nspname = ANY($1)
                AND c.relkind IN ('r','p','v','m','f')
                AND NOT c.relispartition
            ORDER BY 
                schema_name,
                c.relname
        """,
            (schemas,),
        )

    @override
    def get_table_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(schemas, "c.relkind IN ('r','p')")

    @override
    def get_view_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(schemas, "c.relkind IN ('v','m','f')")

    def _columns_sql_query(self, schemas: list[str], relation_kind_filter: str) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                a.attnum  AS ordinal_position,
                format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull AS is_nullable,
                pg_get_expr(ad.adbin, ad.adrelid) AS default_expression,
                CASE
                    WHEN a.attidentity IN ('a','d') THEN 'identity'
                    WHEN a.attgenerated = 's'       THEN 'computed'
                END AS generated,
                col_description(a.attrelid, a.attnum) AS description
            FROM 
                pg_attribute a
                JOIN pg_class c ON c.oid  = a.attrelid
                JOIN pg_namespace n ON n.oid  = c.relnamespace
                LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE 
                n.nspname = ANY($1)
                AND a.attnum > 0
                AND """
            + relation_kind_filter
            + """
                AND NOT a.attisdropped
                AND NOT c.relispartition
            ORDER BY 
                schema_name,
                c.relname, 
                a.attnum
        """,
            (schemas,),
        )

    @override
    def get_primary_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname        AS table_name,
                con.conname      AS constraint_name,
                att.attname      AS column_name,
                k.pos            AS position
            FROM 
                pg_constraint con
                JOIN pg_class      c   ON c.oid = con.conrelid
                JOIN pg_namespace  n   ON n.oid = c.relnamespace
                JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, pos) ON TRUE
                JOIN pg_attribute  att ON att.attrelid = c.oid AND att.attnum = k.attnum
            WHERE 
                n.nspname = ANY($1)
                AND con.contype = 'p'
                AND NOT c.relispartition
            ORDER BY 
                schema_name,
                c.relname, 
                con.conname, 
                k.pos
        """,
            (schemas,),
        )

    @override
    def get_unique_constraints_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname        AS table_name,
                con.conname      AS constraint_name,
                att.attname      AS column_name,
                k.pos            AS position
            FROM 
                pg_constraint con
                JOIN pg_class      c   ON c.oid = con.conrelid
                JOIN pg_namespace  n   ON n.oid = c.relnamespace
                JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, pos) ON TRUE
                JOIN pg_attribute  att ON att.attrelid = c.oid AND att.attnum = k.attnum
            WHERE 
                n.nspname = ANY($1)
                AND con.contype = 'u'
                AND NOT c.relispartition
            ORDER BY 
                schema_name,
                c.relname, 
                con.conname, 
                k.pos
        """,
            (schemas,),
        )

    @override
    def get_checks_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                pg_get_expr(con.conbin, con.conrelid) AS expression,
                con.convalidated AS validated
            FROM 
                pg_constraint con
                JOIN pg_class c     ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE 
                n.nspname = ANY($1)
                AND con.contype = 'c'
                AND NOT c.relispartition
            ORDER BY 
                schema_name, 
                c.relname, 
                con.conname
        """,
            (schemas,),
        )

    @override
    def get_foreign_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname           AS table_name,
                con.conname         AS constraint_name,
                src.ord             AS position,
                attc.attname        AS from_column,
                nref.nspname        AS ref_schema,
                cref.relname        AS ref_table,
                attref.attname      AS to_column,
                con.convalidated    AS validated,
                CASE con.confupdtype
                    WHEN 'a' THEN 'no action' WHEN 'r' THEN 'restrict' WHEN 'c' THEN 'cascade'
                    WHEN 'n' THEN 'set null'  WHEN 'd' THEN 'set default' 
                END AS on_update,
                CASE con.confdeltype
                    WHEN 'a' THEN 'no action' WHEN 'r' THEN 'restrict' WHEN 'c' THEN 'cascade'
                    WHEN 'n' THEN 'set null'  WHEN 'd' THEN 'set default' 
                END AS on_delete
            FROM 
                pg_constraint con
                JOIN pg_class      c    ON c.oid  = con.conrelid
                JOIN pg_namespace  n    ON n.oid  = c.relnamespace
                JOIN pg_class      cref ON cref.oid = con.confrelid
                JOIN pg_namespace  nref ON nref.oid = cref.relnamespace
                JOIN LATERAL unnest(con.conkey)  WITH ORDINALITY AS src(src_attnum, ord)  ON TRUE
                JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS ref(ref_attnum, ord2) ON ref.ord2 = src.ord
                JOIN pg_attribute attc   ON attc.attrelid = c.oid     AND attc.attnum   = src.src_attnum
                JOIN pg_attribute attref ON attref.attrelid = cref.oid AND attref.attnum = ref.ref_attnum
            WHERE 
                n.nspname = ANY($1)
                AND con.contype = 'f'
                AND NOT c.relispartition
            ORDER BY 
                schema_name, 
                c.relname, 
                con.conname, 
                src.ord
        """,
            (schemas,),
        )

    @override
    def get_indexes_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname                                   AS table_name,
                idx.relname                                 AS index_name,
                k.pos                                       AS position,
                pg_get_indexdef(i.indexrelid, k.pos, true)  AS expr,
                i.indisunique                               AS is_unique,
                am.amname                                   AS method,
                pg_get_expr(i.indpred, i.indrelid)          AS predicate
            FROM 
                pg_index i
                JOIN pg_class     idx ON idx.oid = i.indexrelid
                JOIN pg_class     c   ON c.oid  = i.indrelid
                JOIN pg_namespace n   ON n.oid  = c.relnamespace
                JOIN pg_am        am  ON am.oid = idx.relam
                CROSS JOIN LATERAL generate_series(1, i.indnkeyatts::int) AS k(pos)
            WHERE 
                n.nspname = ANY($1)
                AND i.indisprimary = false
                AND NOT EXISTS (
                    SELECT 
                        1
                    FROM 
                        pg_constraint cc
                    WHERE 
                        cc.conindid = i.indexrelid
                        AND cc.contype IN ('p','u')
                )
                AND NOT c.relispartition
            ORDER BY 
                n.nspname, 
                c.relname, 
                idx.relname, 
                k.pos
        """,
            (schemas,),
        )

    @override
    def get_partitions_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            """
            WITH partitions AS (
                SELECT
                    parentrel.oid,
                    array_agg(childrel.relname) as partition_tables
                FROM
                    pg_catalog.pg_class parentrel
                    JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parentrel.oid
                    JOIN pg_catalog.pg_class childrel ON inh.inhrelid = childrel.oid
                GROUP BY
                    parentrel.oid
            )
            SELECT
                nsp.nspname AS schema_name,
                rel.relname            AS table_name,
                CASE part.partstrat
                    WHEN 'h' THEN 'hash partitioned'
                    WHEN 'l' THEN 'list partitioned'
                    WHEN 'r' THEN 'range partitioned'
                END                    AS partitioning_strategy,
                array_agg(att.attname) AS columns_in_partition_key,
                partitions.partition_tables
            FROM
                pg_catalog.pg_partitioned_table part
                JOIN pg_catalog.pg_class rel ON part.partrelid = rel.oid
                JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
                JOIN pg_catalog.pg_attribute att ON att.attrelid = rel.oid AND att.attnum = ANY (part.partattrs)
                JOIN partitions ON partitions.oid = rel.oid
            WHERE
                nsp.nspname = ANY($1)
            GROUP BY
                schema_name,
                rel.relname, 
                part.partstrat, 
                partitions.partition_tables
               """,
            (schemas,),
        )

    def _sql_table_stats(self) -> str:
        return """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                CASE
                    WHEN c.relkind = 'p' THEN (
                        SELECT
                            CASE
                                -- If any partition is unanalyzed (< 0), we can't trust the sum
                                WHEN MIN(child.reltuples) < 0 THEN NULL
                                ELSE COALESCE(SUM(child.reltuples), 0)::bigint
                            END
                        FROM pg_inherits i
                        JOIN pg_class child ON child.oid = i.inhrelid
                        WHERE i.inhparent = c.oid
                    )
                    WHEN c.reltuples < 0 THEN NULL
                    ELSE c.reltuples::bigint
                END AS row_count,
                TRUE AS approximate
            FROM
                pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE
                n.nspname = ANY($1)
                AND c.relkind IN ('r','p')
                AND NOT c.relispartition
        """

    def _sql_column_stats(self) -> str:
        return """
            SELECT
                s.schemaname AS schema_name,
                s.tablename AS table_name,
                s.attname AS column_name,
                s.null_frac,
                s.n_distinct,
                s.most_common_vals::text AS most_common_vals,
                s.most_common_freqs::text AS most_common_freqs,
                s.histogram_bounds::text AS histogram_bounds
            FROM
                pg_stats s
            WHERE
                s.schemaname = ANY($1)
            ORDER BY
                s.schemaname,
                s.tablename,
                s.attname
        """

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT $1'
        return SQLQuery(sql, (limit,))
