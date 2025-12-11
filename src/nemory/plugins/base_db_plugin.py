from __future__ import annotations

import uuid
from datetime import datetime
from typing import TypeVar

from pydantic import BaseModel, Field

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    EmbeddableChunk,
)
from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.database_chunker import build_database_chunks


class BaseDatabaseConfigFile(BaseModel):
    id: str | None = Field(default=None)
    name: str | None = Field(default=None)
    type: str


T = TypeVar("T", bound=BaseDatabaseConfigFile)


class BaseDatabasePlugin(BuildDatasourcePlugin[T]):
    name: str
    supported: set[str]

    def __init__(self, introspector: BaseIntrospector):
        self._introspector = introspector

    def supported_types(self) -> set[str]:
        return self.supported

    def execute(self, full_type: str, datasource_name: str, file_config: T) -> BuildExecutionResult:
        introspection_result = self._introspector.introspect_database(file_config)

        return BuildExecutionResult(
            id=file_config.id or str(uuid.uuid4()),
            name=file_config.name or datasource_name,
            type=full_type,
            description=None,
            version=None,
            executed_at=datetime.now(),
            result=introspection_result,
        )

    def check_connection(self, full_type: str, datasource_name: str, file_config: T) -> None:
        self._introspector.check_connection(file_config)

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return build_database_chunks(build_result.result)
