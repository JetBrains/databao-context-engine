from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class BigQueryDefaultAuth(BaseModel):
    # Uses Google Application Default Credentials (ADC):
    # locally via `gcloud auth application-default login`,
    # in CI/CD via GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service-account key file,
    # or automatically on GCP (Compute Engine, Cloud Run, GKE, etc.)
    pass


class BigQueryServiceAccountKeyFileAuth(BaseModel):
    credentials_file: str


class BigQueryServiceAccountJsonAuth(BaseModel):
    credentials_json: Annotated[str, ConfigPropertyAnnotation(secret=True)]


class BigQueryConnectionProperties(BaseModel):
    project: Annotated[str, ConfigPropertyAnnotation(required=True)]
    dataset: str | None = None
    location: str | None = None
    auth: BigQueryDefaultAuth | BigQueryServiceAccountKeyFileAuth | BigQueryServiceAccountJsonAuth = Field(
        default_factory=BigQueryDefaultAuth,
    )
    additional_properties: dict[str, Any] = {}


class BigQueryConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="bigquery")
    connection: BigQueryConnectionProperties
