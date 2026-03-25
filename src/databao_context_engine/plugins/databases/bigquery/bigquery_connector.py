from __future__ import annotations

import json
from contextlib import AbstractContextManager, closing
from typing import Any

from google.cloud import bigquery

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.bigquery.config_file import (
    BigQueryConfigFile,
    BigQueryConnectionProperties,
    BigQueryServiceAccountJsonAuth,
    BigQueryServiceAccountKeyFileAuth,
)


class BigQueryConnector(BaseConnector[BigQueryConfigFile]):
    def connect(self, file_config: BigQueryConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        conn = file_config.connection
        credentials = self._build_credentials(conn)
        project = catalog or conn.project

        default_query_job_config = None
        if conn.dataset:
            default_query_job_config = bigquery.QueryJobConfig(
                default_dataset=bigquery.DatasetReference(project, conn.dataset),
            )

        return closing(
            bigquery.Client(
                project=project,
                credentials=credentials,
                location=conn.location,
                default_query_job_config=default_query_job_config,
            )
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

    def execute(self, connection: bigquery.Client, sql: str, params: Any) -> list[dict]:
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
                    element_type = BigQueryConnector._infer_bq_type(elem)
                    break
            return bigquery.ArrayQueryParameter(None, element_type, list(value))
        return bigquery.ScalarQueryParameter(None, BigQueryConnector._infer_bq_type(value), value)
