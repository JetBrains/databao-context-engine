from __future__ import annotations

import json
from typing import Any

from google.cloud import bigquery

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.bigquery.config_file import (
    BigQueryConfigFile,
    BigQueryConnectionProperties,
    BigQueryServiceAccountJsonAuth,
    BigQueryServiceAccountKeyFileAuth,
)
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class BigQueryIntrospector(BaseIntrospector[BigQueryConfigFile]):
    _IGNORED_SCHEMAS: set[str] = set()
    supports_catalogs = True

    def _connect(self, file_config: BigQueryConfigFile, *, catalog: str | None = None) -> Any:
        conn = file_config.connection
        credentials = self._build_credentials(conn)
        project = catalog or conn.project

        default_query_job_config = None
        if conn.dataset:
            default_query_job_config = bigquery.QueryJobConfig(
                default_dataset=bigquery.DatasetReference(project, conn.dataset),
            )

        return bigquery.Client(
            project=project,
            credentials=credentials,
            location=conn.location,
            default_query_job_config=default_query_job_config,
        )

    @staticmethod
    def _build_credentials(conn: BigQueryConnectionProperties) -> Any:
        auth = conn.auth
        if isinstance(auth, BigQueryServiceAccountKeyFileAuth):
            from google.oauth2 import service_account

            try:
                return service_account.Credentials.from_service_account_file(auth.credentials_file)
            except FileNotFoundError:
                raise FileNotFoundError(f"BigQuery credentials file not found: {auth.credentials_file}")
        if isinstance(auth, BigQueryServiceAccountJsonAuth):
            from google.oauth2 import service_account

            try:
                info = json.loads(auth.credentials_json)
            except json.JSONDecodeError as e:
                raise ValueError(f"BigQuery credentials_json is not valid JSON: {e}") from e
            return service_account.Credentials.from_service_account_info(info)
        return None

    def _fetchall_dicts(self, connection: bigquery.Client, sql: str, params: Any) -> list[dict]:
        job_config = None
        if params is not None:
            job_config = bigquery.QueryJobConfig(query_parameters=[self._to_query_param(v) for v in params])
        query_job = connection.query(sql, job_config=job_config)
        return [dict(row) for row in query_job.result()]

    @staticmethod
    def _infer_bq_type(v: Any) -> str:
        if isinstance(v, bool):
            return "BOOL"
        if isinstance(v, int):
            return "INT64"
        if isinstance(v, float):
            return "FLOAT64"
        return "STRING"

    @staticmethod
    def _to_query_param(value: Any) -> bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter:
        if isinstance(value, (list, tuple)):
            element_type = "STRING"
            for elem in value:
                if elem is not None:
                    element_type = BigQueryIntrospector._infer_bq_type(elem)
                    break
            return bigquery.ArrayQueryParameter(None, element_type, list(value))
        return bigquery.ScalarQueryParameter(None, BigQueryIntrospector._infer_bq_type(value), value)

    def _get_catalogs(self, connection: bigquery.Client, file_config: BigQueryConfigFile) -> list[str]:
        return [file_config.connection.project]

    def _list_schemas_for_catalog(self, connection: bigquery.Client, catalog: str) -> list[str]:
        default_job_config = connection.default_query_job_config
        if default_job_config and default_job_config.default_dataset:
            return [default_job_config.default_dataset.dataset_id]
        return [ds.dataset_id for ds in connection.list_datasets()]

    def collect_catalog_model(
        self, connection: bigquery.Client, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries(catalog, schemas)
        results: dict[str, list[dict]] = {}

        for name, sql in comps.items():
            results[name] = self._fetchall_dicts(connection, sql, None)

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=[],
            fk_cols=results.get("fks", []),
            idx_cols=[],
        )

    def _component_queries(self, catalog: str, schemas: list[str]) -> dict[str, str]:
        return {
            "relations": self._sql_relations(catalog, schemas),
            "columns": self._sql_columns(catalog, schemas),
            "pk": self._sql_primary_keys(catalog, schemas),
            "uq": self._sql_unique_constraints(catalog, schemas),
            "fks": self._sql_foreign_keys(catalog, schemas),
        }

    def _sql_relations(self, catalog: str, schemas: list[str]) -> str:
        cat = self._quote_ident(catalog)
        parts = []
        for schema in schemas:
            sch = self._quote_ident(schema)
            parts.append(f"""
                SELECT
                    t.table_schema AS schema_name,
                    t.table_name,
                    CASE t.table_type
                        WHEN 'BASE TABLE' THEN 'table'
                        WHEN 'VIEW' THEN 'view'
                        WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
                        WHEN 'EXTERNAL' THEN 'external_table'
                        WHEN 'SNAPSHOT' THEN 'snapshot'
                        WHEN 'CLONE' THEN 'clone'
                        ELSE LOWER(t.table_type)
                    END AS kind,
                    TRIM(opt.option_value, '"') AS description
                FROM {cat}.{sch}.INFORMATION_SCHEMA.TABLES t
                LEFT JOIN {cat}.{sch}.INFORMATION_SCHEMA.TABLE_OPTIONS opt
                    ON t.table_name = opt.table_name
                    AND opt.option_name = 'description'
            """)
        return " UNION ALL ".join(parts)

    def _sql_columns(self, catalog: str, schemas: list[str]) -> str:
        cat = self._quote_ident(catalog)
        parts = []
        for schema in schemas:
            sch = self._quote_ident(schema)
            parts.append(f"""
                SELECT
                    c.table_schema AS schema_name,
                    c.table_name,
                    c.column_name,
                    c.ordinal_position,
                    c.data_type,
                    c.is_nullable,
                    c.column_default AS default_expression,
                    CASE c.is_generated
                        WHEN 'ALWAYS' THEN 'generated'
                        ELSE NULL
                    END AS generated,
                    cfp.description
                FROM {cat}.{sch}.INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN {cat}.{sch}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS cfp
                    ON c.table_catalog = cfp.table_catalog
                    AND c.table_schema = cfp.table_schema
                    AND c.table_name = cfp.table_name
                    AND c.column_name = cfp.column_name
                    AND cfp.column_name = cfp.field_path
            """)
        return " UNION ALL ".join(parts)

    def _sql_primary_keys(self, catalog: str, schemas: list[str]) -> str:
        cat = self._quote_ident(catalog)
        parts = []
        for schema in schemas:
            sch = self._quote_ident(schema)
            parts.append(f"""
                SELECT
                    kcu.table_schema AS schema_name,
                    kcu.table_name,
                    tc.constraint_name,
                    kcu.column_name,
                    kcu.ordinal_position AS position
                FROM {cat}.{sch}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {cat}.{sch}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.constraint_catalog = kcu.constraint_catalog
                    AND tc.constraint_schema = kcu.constraint_schema
                    AND tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            """)
        return " UNION ALL ".join(parts)

    def _sql_unique_constraints(self, catalog: str, schemas: list[str]) -> str:
        cat = self._quote_ident(catalog)
        parts = []
        for schema in schemas:
            sch = self._quote_ident(schema)
            parts.append(f"""
                SELECT
                    kcu.table_schema AS schema_name,
                    kcu.table_name,
                    tc.constraint_name,
                    kcu.column_name,
                    kcu.ordinal_position AS position
                FROM {cat}.{sch}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {cat}.{sch}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.constraint_catalog = kcu.constraint_catalog
                    AND tc.constraint_schema = kcu.constraint_schema
                    AND tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'UNIQUE'
            """)
        return " UNION ALL ".join(parts)

    def _sql_foreign_keys(self, catalog: str, schemas: list[str]) -> str:
        cat = self._quote_ident(catalog)
        parts = []
        for schema in schemas:
            sch = self._quote_ident(schema)
            parts.append(f"""
                SELECT
                    fk_kcu.table_schema AS schema_name,
                    fk_kcu.table_name,
                    fk_tc.constraint_name,
                    fk_kcu.ordinal_position AS position,
                    fk_kcu.column_name AS from_column,
                    ref_ccu.table_schema AS ref_schema,
                    ref_ccu.table_name AS ref_table,
                    ref_pk_kcu.column_name AS to_column,
                    fk_tc.enforced
                FROM {cat}.{sch}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk_tc
                JOIN {cat}.{sch}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_kcu
                    ON fk_tc.constraint_catalog = fk_kcu.constraint_catalog
                    AND fk_tc.constraint_schema = fk_kcu.constraint_schema
                    AND fk_tc.constraint_name = fk_kcu.constraint_name
                JOIN (
                    SELECT DISTINCT
                        constraint_catalog, constraint_schema, constraint_name,
                        table_catalog, table_schema, table_name
                    FROM {cat}.{sch}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
                ) ref_ccu
                    ON fk_tc.constraint_catalog = ref_ccu.constraint_catalog
                    AND fk_tc.constraint_schema = ref_ccu.constraint_schema
                    AND fk_tc.constraint_name = ref_ccu.constraint_name
                JOIN {cat}.{sch}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS ref_pk_tc
                    ON ref_ccu.table_catalog = ref_pk_tc.table_catalog
                    AND ref_ccu.table_schema = ref_pk_tc.table_schema
                    AND ref_ccu.table_name = ref_pk_tc.table_name
                    AND ref_pk_tc.constraint_type = 'PRIMARY KEY'
                JOIN {cat}.{sch}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE ref_pk_kcu
                    ON ref_pk_tc.constraint_catalog = ref_pk_kcu.constraint_catalog
                    AND ref_pk_tc.constraint_schema = ref_pk_kcu.constraint_schema
                    AND ref_pk_tc.constraint_name = ref_pk_kcu.constraint_name
                    AND fk_kcu.position_in_unique_constraint = ref_pk_kcu.ordinal_position
                WHERE fk_tc.constraint_type = 'FOREIGN KEY'
            """)
        return " UNION ALL ".join(parts)

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sch = self._quote_ident(schema)
        tbl = self._quote_ident(table)
        return SQLQuery(f"SELECT * FROM {sch}.{tbl} LIMIT {limit}", None)

    def _resolve_pseudo_catalog_name(self, file_config: BigQueryConfigFile) -> str:
        return file_config.connection.project

    @staticmethod
    def _quote_ident(ident: str) -> str:
        return "`" + ident.replace("`", "``") + "`"
