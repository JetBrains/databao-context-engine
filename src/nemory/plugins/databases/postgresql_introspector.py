from typing import Annotated, Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
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


class PostgresqlIntrospector(BaseIntrospector[PostgresConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _connect(self, file_config: PostgresConfigFile):
        connection_string = self._create_connection_string_for_config(file_config.connection)

        return psycopg.connect(connection_string)

    def _fetchall_dicts(self, connection: Connection, sql: str, params) -> list[dict]:
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return [r for r in cur.fetchall()]

    def _get_catalogs(self, connection: Connection, file_config: PostgresConfigFile) -> list[str]:
        database = file_config.connection.database
        if database is not None:
            return [database]

        catalog_results = connection.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false"
        ).fetchall()

        return [row[0] for row in catalog_results]

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
            not rel.relispartition 
          AND
            nsp.nspname = %s
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
        WHERE nsp.nspname = %s
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
