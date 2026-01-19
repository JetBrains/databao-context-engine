from __future__ import annotations

from typing import Any, Mapping

import clickhouse_connect
from pydantic import Field

from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class ClickhouseConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/clickhouse")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Clickhouse database. It can contain any of the keys supported by the Clickhouse connection library (see https://clickhouse.com/docs/integrations/language-clients/python/driver-api#connection-arguments)"
    )


class ClickhouseIntrospector(BaseIntrospector[ClickhouseConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "system", "INFORMATION_SCHEMA"}

    supports_catalogs = True

    def _connect(self, file_config: ClickhouseConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return clickhouse_connect.get_client(**connection)

    def _connect_to_catalog(self, file_config: ClickhouseConfigFile, catalog: str):
        return self._connect(file_config)

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

        schemas_sql = ", ".join(self._quote_literal(s) for s in schemas)

        comps = self._component_queries()
        results: dict[str, list[dict]] = {cq: [] for cq in comps}
        for cq, template_sql in comps.items():
            sql = template_sql.replace("{SCHEMAS}", schemas_sql)
            results[cq] = self._fetchall_dicts(connection, sql, None)

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

    def _component_queries(self) -> dict[str, str]:
        return {"relations": self._sql_relations(), "columns": self._sql_columns(), "idx": self._sql_indexes()}

    def _sql_relations(self) -> str:
        return r"""
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
                t.database IN ({SCHEMAS})
            ORDER BY 
                t.name
        """

    def _sql_columns(self) -> str:
        return r"""
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
                c.database IN ({SCHEMAS})
            ORDER BY 
                c.table, 
                c.position
        """

    def _sql_indexes(self) -> str:
        return r"""
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
                i.database IN ({SCHEMAS})
            ORDER BY 
                i.table, 
                i.name
        """

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %s'
        return SQLQuery(sql, (limit,))

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        res = connection.query(sql, parameters=params) if params else connection.query(sql)
        cols = [c.lower() for c in res.column_names]
        return [dict(zip(cols, row)) for row in res.result_rows]

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "\\'") + "'"
