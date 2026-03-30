from __future__ import annotations

from abc import ABC
from typing import Annotated, Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.pluginlib.build_plugin import (
    AbstractConfigFile,
    BuildDatasourcePlugin,
    EmbeddableChunk,
)
from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector
from databao_context_engine.plugins.databases.context_enricher import enrich_database_context
from databao_context_engine.plugins.databases.database_chunker import build_database_chunks
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from databao_context_engine.plugins.databases.introspection_scope import IntrospectionScope
from databao_context_engine.plugins.databases.profiling_config import ProfilingConfig
from databao_context_engine.plugins.databases.sampling_scope import SamplingConfig


class BaseDatabaseConfigFile(BaseModel, AbstractConfigFile):
    model_config = ConfigDict(populate_by_name=True)
    name: str
    type: str
    introspection_scope: Annotated[
        IntrospectionScope | None, ConfigPropertyAnnotation(ignored_for_config_wizard=True)
    ] = Field(default=None, alias="introspection-scope")
    sampling: Annotated[SamplingConfig | None, ConfigPropertyAnnotation(ignored_for_config_wizard=True)] = Field(
        default=None
    )
    profiling: Annotated[ProfilingConfig | None, ConfigPropertyAnnotation(ignored_for_config_wizard=True)] = Field(
        default=None
    )


T = TypeVar("T", bound="BaseDatabaseConfigFile")


class BaseDatabasePlugin(BuildDatasourcePlugin[T], ABC):
    name: str
    supported: set[str]
    context_type = DatabaseIntrospectionResult

    def __init__(self, connector: BaseConnector[T], introspector: BaseIntrospector[T]):
        self._connector = connector
        self._introspector = introspector

    def supported_types(self) -> set[str]:
        return self.supported

    def build_context(self, full_type: str, datasource_name: str, file_config: T) -> Any:
        return self._introspector.introspect_database(file_config)

    def enrich_context(self, context: Any, description_provider: DescriptionProvider) -> Any:
        return enrich_database_context(context, description_provider)

    def check_connection(self, full_type: str, file_config: T) -> None:
        self._connector.check_connection(file_config)

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return build_database_chunks(context)

    def run_sql(
        self, file_config: T, sql: str, params: list[Any] | None = None, read_only: bool = True
    ) -> SqlExecutionResult:
        # for now, we don't have any read-only related logic implemented on the database side
        with self._connector.connect(file_config) as connection:
            rows_dicts: list[dict] = self._connector.execute(connection, sql, params)

        if not rows_dicts:
            return SqlExecutionResult(columns=[], rows=[])

        columns: list[str] = list(rows_dicts[0].keys())
        rows: list[tuple[Any, ...]] = [tuple(row.get(col) for col in columns) for row in rows_dicts]
        return SqlExecutionResult(columns=columns, rows=rows)
