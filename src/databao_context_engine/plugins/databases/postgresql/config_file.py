from typing import Annotated, Any

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class PostgresConnectionProperties(BaseModel):
    host: Annotated[str, ConfigPropertyAnnotation(default_value="localhost", required=True)]
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    additional_properties: dict[str, Any] = {}


class PostgresConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="postgres")
    connection: PostgresConnectionProperties
