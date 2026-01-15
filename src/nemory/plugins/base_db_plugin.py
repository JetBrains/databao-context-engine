from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    EmbeddableChunk,
)
from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.database_chunker import build_database_chunks
from nemory.plugins.databases.introspection_scope import IntrospectionScope


class BaseDatabaseConfigFile(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str | None = Field(default=None)
    type: str
    introspection_scope: IntrospectionScope | None = Field(default=None, alias="introspection-scope")


T = TypeVar("T", bound=BaseDatabaseConfigFile)


class BaseDatabasePlugin(BuildDatasourcePlugin[T]):
    name: str
    supported: set[str]

    def __init__(self, introspector: BaseIntrospector):
        self._introspector = introspector

    def supported_types(self) -> set[str]:
        return self.supported

    def build_context(self, full_type: str, datasource_name: str, file_config: T) -> Any:
        introspection_result = self._introspector.introspect_database(file_config)

        return introspection_result

    def check_connection(self, full_type: str, datasource_name: str, file_config: T) -> None:
        self._introspector.check_connection(file_config)

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return build_database_chunks(context)
