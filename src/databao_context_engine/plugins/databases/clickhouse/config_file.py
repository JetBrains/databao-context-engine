from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class ClickhouseConnectionProperties(BaseModel):
    host: Annotated[str, ConfigPropertyAnnotation(default_value="localhost", required=True)]
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    additional_properties: dict[str, Any] = {}

    def to_clickhouse_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(exclude={"additional_properties"}, exclude_none=True)
        kwargs.update(self.additional_properties)
        return kwargs


class ClickhouseConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="clickhouse")
    connection: ClickhouseConnectionProperties
