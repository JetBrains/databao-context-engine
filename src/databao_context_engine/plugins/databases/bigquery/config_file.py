from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class BigQueryDefaultAuth(BaseModel):
    type: Literal["default"] = "default"


class BigQueryServiceAccountKeyFileAuth(BaseModel):
    type: Literal["service_account_key_file"]
    credentials_file: str


class BigQueryServiceAccountJsonAuth(BaseModel):
    type: Literal["service_account_json"]
    credentials_json: Annotated[str, ConfigPropertyAnnotation(secret=True)]


class BigQueryConnectionProperties(BaseModel):
    project: Annotated[str, ConfigPropertyAnnotation(required=True)]
    location: str | None = None
    auth: BigQueryDefaultAuth | BigQueryServiceAccountKeyFileAuth | BigQueryServiceAccountJsonAuth = Field(
        default_factory=BigQueryDefaultAuth,
        discriminator="type",
    )
    additional_properties: dict[str, Any] = {}

    @model_validator(mode="before")
    @classmethod
    def _default_empty_auth(cls, data: Any) -> Any:
        if isinstance(data, dict):
            auth = data.get("auth")
            if isinstance(auth, dict) and "type" not in auth:
                data["auth"] = {**auth, "type": "default"}
        return data


class BigQueryConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="bigquery")
    connection: BigQueryConnectionProperties
