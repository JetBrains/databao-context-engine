from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class SnowflakePasswordAuth(BaseModel):
    password: Annotated[str, ConfigPropertyAnnotation(secret=True)]


class SnowflakeKeyPairAuth(BaseModel):
    private_key_file: str | None = None
    private_key_file_pwd: str | None = None
    private_key: Annotated[str | None, ConfigPropertyAnnotation(secret=True)] = None


class SnowflakeSSOAuth(BaseModel):
    authenticator: str = Field(description='e.g. "externalbrowser"')


class SnowflakeConnectionProperties(BaseModel):
    account: Annotated[str, ConfigPropertyAnnotation(required=True)]
    warehouse: str | None = None
    database: str | None = None
    user: str | None = None
    role: str | None = None
    auth: SnowflakePasswordAuth | SnowflakeKeyPairAuth | SnowflakeSSOAuth
    additional_properties: dict[str, Any] = {}

    def to_snowflake_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(
            exclude={
                "additional_properties": True,
            },
            exclude_none=True,
        )
        auth_fields = kwargs.pop("auth", {})
        kwargs.update(auth_fields)
        kwargs.update(self.additional_properties)
        return kwargs


class SnowflakeConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="snowflake")
    connection: SnowflakeConnectionProperties
