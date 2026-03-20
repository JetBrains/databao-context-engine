from __future__ import annotations

from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.clickhouse.config_file import ClickhouseConfigFile


class ClickhouseIntrospector(BaseIntrospector[ClickhouseConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "system", "INFORMATION_SCHEMA"}

    supports_catalogs = True

    def _get_catalogs(self, connection, file_config: ClickhouseConfigFile) -> list[str]:
        return ["clickhouse"]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        return SQLQuery(
            """
                SELECT 
                    name AS schema_name, 
                    'clickhouse' AS catalog_name 
                FROM 
                    system.databases
            """,
            None,
        )

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            r"""
            SELECT
                t.database AS schema_name,
                t.name AS table_name,
                multiIf(
                    t.engine = 'View', 'view',
                    t.engine = 'MaterializedView', 'materialized_view',
                    t.engine IN (
                    'File', 'URL',
                    'S3', 'S3Queue',
                    'AzureBlobStorage', 'AzureQueue',
                    'HDFS',
                    'MySQL', 'PostgreSQL', 'MongoDB', 'Redis',
                    'JDBC', 'ODBC', 'SQLite',
                    'Kafka', 'RabbitMQ', 'NATS',
                    'ExternalDistributed',
                    'DeltaLake', 'Iceberg', 'Hudi', 'Hive',
                    'MaterializedPostgreSQL',
                    'YTsaurus', 'ArrowFlight'
                    ), 'external_table',
                    'table'
                ) AS kind,
                t.comment AS description
            FROM 
                system.tables t
            WHERE 
                has({schemas:Array(String)}, t.database)
            ORDER BY 
                t.name
        """,
            {"schemas": schemas},
        )

    @override
    def get_table_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(
            schemas,
            "t.engine = 'table'",
        )

    @override
    def get_view_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query(
            schemas,
            "t.engine <> 'table'",
        )

    def _columns_sql_query(self, schemas: list[str], engine_filter: str) -> SQLQuery:
        return SQLQuery(
            r"""
            SELECT
                c.database AS schema_name,
                c.table AS table_name,
                c.name AS column_name,
                c.position AS ordinal_position,
                c.type AS data_type,
                (c.type LIKE 'Nullable(%') AS is_nullable,
                c.default_expression AS default_expression,
                CASE 
                    WHEN c.default_kind IN ('MATERIALIZED','ALIAS') THEN 'computed' 
                END AS generated,
                c.comment AS description
            FROM 
                system.columns c
                JOIN system.tables t ON t.database = c.database AND t.name = c.table
            WHERE 
                has({schemas:Array(String)}, c.database)
                AND """
            + engine_filter
            + r"""
            ORDER BY 
                c.table, 
                c.position
        """,
            {"schemas": schemas},
        )

    @override
    def get_indexes_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            r"""
            SELECT
                i.database AS schema_name,
                i.table AS table_name,
                i.name AS index_name,
                1 AS position,
                i.expr AS expr,
                0 AS is_unique,
                i.type AS method,
                NULL AS predicate
            FROM 
                system.data_skipping_indices i
            WHERE 
                has({schemas:Array(String)}, i.database)
            ORDER BY 
                i.table, 
                i.name
        """,
            {"schemas": schemas},
        )

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %s'
        return SQLQuery(sql, (limit,))
