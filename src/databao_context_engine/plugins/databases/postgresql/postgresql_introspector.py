import asyncio
from typing import Annotated, Any, Sequence

import asyncpg
from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class PostgresConnectionProperties(BaseModel):
    host: Annotated[str, ConfigPropertyAnnotation(default_value="localhost", required=True)]
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    additional_properties: dict[str, Any] = {}


class PostgresConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="postgres")
    connection: PostgresConnectionProperties


class _SyncAsyncpgConnection:
    def __init__(self, connect_kwargs: dict[str, Any]):
        self._connect_kwargs = connect_kwargs
        self._conn: asyncpg.Connection | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None

    def __enter__(self):
        self._event_loop = asyncio.new_event_loop()
        try:
            self._conn = self._event_loop.run_until_complete(asyncpg.connect(**self._connect_kwargs))
        except Exception:
            self._event_loop.close()
            self._event_loop = None
            raise
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        try:
            if self._conn is not None and self._event_loop is not None and not self._event_loop.is_closed():
                self._event_loop.run_until_complete(self._conn.close())
        finally:
            self._conn = None
            if self._event_loop is not None and not self._event_loop.is_closed():
                self._event_loop.close()
            self._event_loop = None

    @property
    def conn(self) -> asyncpg.Connection:
        if self._conn is None:
            raise RuntimeError("Connection is not open")
        return self._conn

    def _run_blocking(self, awaitable) -> Any:
        if self._event_loop is None:
            raise RuntimeError("Event loop is not initialized")
        return self._event_loop.run_until_complete(awaitable)

    def fetch_rows(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        query_params = [] if params is None else list(params)
        records = self._run_blocking(self.conn.fetch(sql, *query_params))
        return [dict(record) for record in records]

    def fetch_scalar_values(self, sql: str) -> list[Any]:
        records = self._run_blocking(self.conn.fetch(sql))
        return [record[0] for record in records]


class PostgresqlIntrospector(BaseIntrospector[PostgresConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY($1)"
            return SQLQuery(sql, (catalogs,))

        sql = "SELECT schema_name FROM information_schema.schemata"
        return SQLQuery(sql, None)

    def _connect(self, file_config: PostgresConfigFile):
        kwargs = self._create_connection_kwargs(file_config.connection)
        return _SyncAsyncpgConnection(kwargs)

    def _fetchall_dicts(self, connection: _SyncAsyncpgConnection, sql: str, params) -> list[dict]:
        return connection.fetch_rows(sql, params)

    def _get_catalogs(self, connection: _SyncAsyncpgConnection, file_config: PostgresConfigFile) -> list[str]:
        database = file_config.connection.database
        if database is not None:
            return [database]

        return connection.fetch_scalar_values("SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false")

    def _connect_to_catalog(self, file_config: PostgresConfigFile, catalog: str):
        cfg = file_config.model_copy(deep=True)
        cfg.connection.database = catalog
        return self._connect(cfg)

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
