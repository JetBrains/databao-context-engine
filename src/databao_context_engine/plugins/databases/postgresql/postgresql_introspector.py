import asyncio
import concurrent.futures
import queue
import threading
from collections.abc import Coroutine
from typing import Any, Sequence

import asyncpg

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.postgresql.config_file import (
    PostgresConfigFile,
    PostgresConnectionProperties,
)


class _SyncAsyncpgConnection:
    """A synchronous wrapper around asyncpg that works correctly in both sync and async contexts.

    When called from an async context (e.g., MCP server), operations run in a separate thread
    with its own event loop to avoid blocking the calling event loop.

    Note: Uses class-level thread-local storage for event loops in sync contexts. Multiple
    connection instances in the same thread will share the same event loop.
    """

    # Thread-local storage for event loops in sync contexts
    # Shared across all instances to avoid creating multiple loops per thread
    _thread_local = threading.local()

    # Worker loop initialization timeout in seconds
    _WORKER_LOOP_INIT_TIMEOUT = 1.0

    def __init__(self, connect_kwargs: dict[str, Any]):
        self._connect_kwargs = connect_kwargs
        self._conn: asyncpg.Connection | None = None
        self._in_async_context = False
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._worker_loop: asyncio.AbstractEventLoop | None = None

    async def _async_connect(self) -> None:
        """Establish the async database connection."""
        self._conn = await asyncpg.connect(**self._connect_kwargs)

    async def _async_close(self) -> None:
        """Close the async database connection if it exists."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _async_fetch_rows(self, sql: str, params: Sequence[Any] | None) -> list[dict]:
        """Fetch rows from the database and return as list of dicts."""
        if self._conn is None:
            raise RuntimeError("Connection is not open")
        query_params = [] if params is None else list(params)
        records = await self._conn.fetch(sql, *query_params)
        return [dict(r) for r in records]

    async def _async_fetch_scalar_values(self, sql: str) -> list[Any]:
        """Fetch scalar values (first column) from the database."""
        if self._conn is None:
            raise RuntimeError("Connection is not open")
        records = await self._conn.fetch(sql)
        return [r[0] for r in records]

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create a persistent event loop for this thread."""
        if not hasattr(self._thread_local, "loop") or self._thread_local.loop is None:
            self._thread_local.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._thread_local.loop)
        return self._thread_local.loop

    def _setup_worker_loop(self) -> None:
        """Initialize a persistent worker loop for async context.

        Creates a dedicated thread with its own event loop that runs for the
        lifetime of this connection. This ensures all asyncpg operations run
        in the same loop, which is required by asyncpg.
        """
        result_queue: queue.Queue[asyncio.AbstractEventLoop] = queue.Queue()

        def init_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_queue.put(loop)
            loop.run_forever()
            loop.close()

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._executor.submit(init_loop)
        self._worker_loop = result_queue.get(timeout=self._WORKER_LOOP_INIT_TIMEOUT)

    def _run_sync(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run a coroutine synchronously, handling both sync and async contexts.

        - In sync context: uses a persistent thread-local event loop
        - In async context: uses a persistent worker thread with its own event loop

        Args:
            coro: The coroutine to execute synchronously

        Returns:
            The result of the coroutine execution

        Raises:
            RuntimeError: If worker loop is not initialized in async context
        """
        if self._in_async_context:
            # We're in an async context - use the persistent worker loop
            if self._worker_loop is None:
                raise RuntimeError("Worker loop not initialized")
            future = asyncio.run_coroutine_threadsafe(coro, self._worker_loop)
            return future.result()
        # No async context - use our thread-local loop
        loop = self._get_or_create_loop()
        return loop.run_until_complete(coro)

    def __enter__(self):
        # Check if we're in an async context and remember it
        try:
            # This raises RuntimeError if no event loop is running
            asyncio.get_running_loop()
            self._in_async_context = True
            self._setup_worker_loop()
        except RuntimeError:
            # No event loop running - we're in sync context
            self._in_async_context = False

        self._run_sync(self._async_connect())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._run_sync(self._async_close())
        finally:
            # Always cleanup resources, even if close fails
            if self._in_async_context:
                # Stop the worker loop and shutdown executor
                if self._worker_loop is not None:
                    self._worker_loop.call_soon_threadsafe(self._worker_loop.stop)
                    self._worker_loop = None
                if self._executor is not None:
                    self._executor.shutdown(wait=True)
                    self._executor = None
            else:
                # Only close the loop if we're in sync context
                loop = getattr(self._thread_local, "loop", None)
                if loop and not loop.is_closed():
                    loop.close()
                self._thread_local.loop = None
                asyncio.set_event_loop(None)

    def fetch_rows(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        return self._run_sync(self._async_fetch_rows(sql, params))

    def fetch_scalar_values(self, sql: str) -> list[Any]:
        return self._run_sync(self._async_fetch_scalar_values(sql))


class PostgresqlIntrospector(BaseIntrospector[PostgresConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY($1)"
            return SQLQuery(sql, (catalogs,))

        sql = "SELECT schema_name FROM information_schema.schemata"
        return SQLQuery(sql, None)

    def _connect(self, file_config: PostgresConfigFile, *, catalog: str | None = None):
        kwargs = self._create_connection_kwargs(file_config.connection)
        if catalog:
            kwargs["database"] = catalog
        return _SyncAsyncpgConnection(kwargs)

    def _fetchall_dicts(self, connection: _SyncAsyncpgConnection, sql: str, params) -> list[dict]:
        return connection.fetch_rows(sql, params)

    def _get_catalogs(self, connection: _SyncAsyncpgConnection, file_config: PostgresConfigFile) -> list[str]:
        database = file_config.connection.database
        if database is not None:
            return [database]

        return connection.fetch_scalar_values("SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false")

    def collect_catalog_model(
        self, connection: _SyncAsyncpgConnection, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries()
        results: dict[str, list[dict]] = {name: [] for name in comps}

        for cq, sql in comps.items():
            results[cq] = self._fetchall_dicts(connection, sql, (schemas,)) or []

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=results.get("checks", []),
            fk_cols=results.get("fks", []),
            idx_cols=results.get("idx", []),
            partitions=results.get("partitions", []),
            table_stats=results.get("table_stats", []),
            column_stats=results.get("column_stats", []),
        )

    def _component_queries(self) -> dict[str, str]:
        return {
            "relations": self._sql_relations(),
            "columns": self._sql_columns(),
            "pk": self._sql_primary_keys(),
            "uq": self._sql_uniques(),
            "checks": self._sql_checks(),
            "fks": self._sql_foreign_keys(),
            "idx": self._sql_indexes(),
            "partitions": self._sql_partitions(),
            "table_stats": self._sql_table_stats(),
            "column_stats": self._sql_column_stats(),
        }

    def _sql_relations(self) -> str:
        return """
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
        """

    def _sql_columns(self) -> str:
        return """
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
                AND c.relkind IN ('r','p','v','m','f') 
                AND NOT a.attisdropped
                AND NOT c.relispartition
            ORDER BY 
                schema_name,
                c.relname, 
                a.attnum
        """

    def _sql_primary_keys(self) -> str:
        return """
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
        """

    def _sql_uniques(self) -> str:
        return """
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
        """

    def _sql_checks(self) -> str:
        return """
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
        """

    def _sql_foreign_keys(self) -> str:
        return """
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
        """

    def _sql_indexes(self) -> str:
        return """
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
        """

    def _sql_partitions(self) -> str:
        return """
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
               """

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
                END AS row_count
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
                s.most_common_freqs::text AS most_common_freqs
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

    def _create_connection_string_for_config(self, connection_config: PostgresConnectionProperties) -> str:
        def _escape_pg_value(value: str) -> str:
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"

        host = connection_config.host
        if host is None:
            raise ValueError("A host must be provided to connect to the PostgreSQL database.")

        connection_parts = {
            "host": host,
            "port": connection_config.port or 5432,
            "dbname": connection_config.database,
            "user": connection_config.user,
            "password": connection_config.password,
        }
        connection_parts.update(connection_config.additional_properties)

        return " ".join(f"{k}={_escape_pg_value(str(v))}" for k, v in connection_parts.items() if v is not None)

    def _create_connection_kwargs(self, connection_config: PostgresConnectionProperties) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": connection_config.host,
            "port": connection_config.port or 5432,
            "database": connection_config.database or "postgres",
        }

        if connection_config.user:
            kwargs["user"] = connection_config.user
        if connection_config.password:
            kwargs["password"] = connection_config.password
        kwargs.update(connection_config.additional_properties or {})
        return kwargs
