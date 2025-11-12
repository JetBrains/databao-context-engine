import uuid
from datetime import datetime
from typing import Any, Mapping

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    EmbeddableChunk,
)
from nemory.plugins.databases.database_chunker import build_database_chunks
from nemory.plugins.databases.postgresql_introspector import PostgresqlIntrospector


class PostgresqlDbPlugin(BuildDatasourcePlugin):
    name = "PostgreSQL DB Plugin"

    def __init__(self):
        self._introspector = PostgresqlIntrospector()

    def supported_types(self) -> set[str]:
        return {"databases/postgres"}

    def execute(self, full_type: str, datasource_name: str, file_config: Mapping[str, Any]) -> BuildExecutionResult:
        introspection_result = self._introspector.introspect_database(file_config)

        return BuildExecutionResult(
            id=file_config.get("id", str(uuid.uuid4())),
            name=file_config.get("name", datasource_name),
            type=full_type,
            description=None,
            version=None,
            executed_at=datetime.now(),
            result=introspection_result,
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return build_database_chunks(build_result.result)
