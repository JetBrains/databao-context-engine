from __future__ import annotations

import clickhouse_connect
from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.clickhouse.config_file import ClickhouseConfigFile
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class ClickhouseIntrospector(BaseIntrospector[ClickhouseConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "system", "INFORMATION_SCHEMA"}

    supports_catalogs = True

    def _connect(self, file_config: ClickhouseConfigFile, *, catalog: str | None = None):
        return clickhouse_connect.get_client(
            **file_config.connection.to_clickhouse_kwargs(),
        )

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

    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries(catalog, schemas)
        results: dict[str, list[dict]] = {cq: [] for cq in comps}
        for name, sql_query in comps.items():
            if sql_query is None:
                continue
            results[name] = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=[],
            uq_cols=[],
            checks=[],
            fk_cols=[],
            idx_cols=results.get("idx", []),
        )

    def _component_queries(self, catalog: str, schemas: list[str]) -> dict[str, SQLQuery | None]:
        return {
            "relations": self.get_relations_sql_query(catalog, schemas),
            "columns": self.get_columns_sql_query(catalog, schemas),
            "idx": self.get_indexes_sql_query(catalog, schemas),
        }

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
    def get_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
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
            WHERE 
                has({schemas:Array(String)}, c.database)
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

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        res = connection.query(sql, parameters=params) if params else connection.query(sql)
        cols = [c.lower() for c in res.column_names]
        return [dict(zip(cols, row)) for row in res.result_rows]
