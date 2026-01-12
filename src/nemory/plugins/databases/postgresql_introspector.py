import asyncio
from typing import Annotated, Any, Sequence

import asyncpg
from pydantic import BaseModel, Field

from nemory.pluginlib.config_properties import ConfigPropertyAnnotation
from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseColumn, DatabasePartitionInfo


class PostgresConnectionProperties(BaseModel):
    host: Annotated[str, ConfigPropertyAnnotation(default_value="localhost", required=True)]
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    additional_properties: dict[str, Any] = {}


class PostgresConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/postgres")
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
        else:
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

        rows = connection.fetch_scalar_values("SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false")
        return rows

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        sql = """
        SELECT rel.relname    as table_name,
               att.attname    as column_name,
               not att.attnotnull as is_nullable,
               typ.typname    as type_name
        FROM pg_catalog.pg_attribute att
                 JOIN pg_catalog.pg_class rel ON att.attrelid = rel.oid
                 JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
                 JOIN pg_catalog.pg_type typ ON att.atttypid = typ.oid
        WHERE
            -- filter out system columns
            att.attnum >= 1 AND
            -- filter out partitions
            not rel.relispartition AND
            -- filter out indexes and views
            rel.relkind IN ('r', 'p')
          AND
            nsp.nspname = $1
        """
        return SQLQuery(sql, [schema])

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["type_name"],
            nullable=row["is_nullable"],
        )

    def _sql_partitions_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        sql = """
        WITH partitions AS
                 (SELECT parentrel.oid, array_agg(childrel.relname) as partition_tables
                  FROM pg_catalog.pg_class parentrel
                           JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parentrel.oid
                           JOIN pg_catalog.pg_class childrel ON inh.inhrelid = childrel.oid
                  GROUP BY parentrel.oid)
        SELECT rel.relname            as table_name,
               CASE part.partstrat
                   WHEN 'h' THEN 'hash partitioned'
                   WHEN 'l' THEN 'list partitioned'
                   WHEN 'r' THEN 'range partitioned'
                   END                AS partitioning_strategy,
               array_agg(att.attname) as columns_in_partition_key,
               partitions.partition_tables
        FROM pg_catalog.pg_partitioned_table part
                 JOIN pg_catalog.pg_class rel ON part.partrelid = rel.oid
                 JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
                 JOIN pg_catalog.pg_attribute att ON att.attrelid = rel.oid AND att.attnum = ANY (part.partattrs)
                 JOIN partitions ON partitions.oid = rel.oid
        WHERE nsp.nspname = $1
        GROUP BY rel.relname, part.partstrat, partitions.partition_tables
        """
        return SQLQuery(sql, [schema])

    def _construct_partition_info(self, row: dict[str, Any]) -> DatabasePartitionInfo:
        return DatabasePartitionInfo(
            meta={
                "partitioning_strategy": row["partitioning_strategy"],
                "columns_in_partition_key": row["columns_in_partition_key"],
            },
            partition_tables=row["partition_tables"],
        )

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

        connection_string = " ".join(
            f"{k}={_escape_pg_value(str(v))}" for k, v in connection_parts.items() if v is not None
        )
        return connection_string

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
