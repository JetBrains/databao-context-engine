from __future__ import annotations

from typing import Annotated, Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    EmbeddableChunk,
)
from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector
from databao_context_engine.plugins.databases.database_chunker import build_database_chunks
from databao_context_engine.plugins.databases.introspection_scope import IntrospectionScope


class BaseDatabaseConfigFile(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str | None = Field(default=None)
    type: str
    introspection_scope: Annotated[
        IntrospectionScope | None, ConfigPropertyAnnotation(ignored_for_config_wizard=True)
    ] = Field(default=None, alias="introspection-scope")


T = TypeVar("T", bound=BaseDatabaseConfigFile)


class BaseDatabasePlugin(BuildDatasourcePlugin[T]):
    name: str
    supported: set[str]

    def __init__(self, introspector: BaseIntrospector):
        self._introspector = introspector

    def supported_types(self) -> set[str]:
        return self.supported

    def build_context(self, full_type: str, datasource_name: str, file_config: T) -> Any:
        return self._introspector.introspect_database(file_config)

    def check_connection(self, full_type: str, datasource_name: str, file_config: T) -> None:
        self._introspector.check_connection(file_config)

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return build_database_chunks(context)

    def run_sql(
        self, file_config: T, sql: str, params: list[Any] | None = None, read_only: bool = True
    ) -> SqlExecutionResult:
        return self._introspector.run_sql(
            file_config=file_config,
            sql=sql,
            params=params,
            read_only=read_only,
        )
