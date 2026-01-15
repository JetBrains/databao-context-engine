from datetime import datetime

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, BuildExecutionResult, EmbeddableChunk
from nemory.plugins.resources.parquet_chunker import build_parquet_chunks
from nemory.plugins.resources.parquet_introspector import (
    ParquetConfigFile,
    ParquetIntrospector,
    parquet_type,
)


class ParquetPlugin(BuildDatasourcePlugin[ParquetConfigFile]):
    id = "jetbrains/parquet"
    name = "Parquet Plugin"
    config_file_type = ParquetConfigFile

    def __init__(self):
        self._introspector = ParquetIntrospector()

    def supported_types(self) -> set[str]:
        return {parquet_type}

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return build_parquet_chunks(build_result.result)

    def execute(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> BuildExecutionResult:
        introspection_result = self._introspector.introspect(file_config)

        return BuildExecutionResult(
            name=file_config.name or datasource_name,
            type=full_type,
            description=None,
            version=None,
            executed_at=datetime.now(),
            result=introspection_result,
        )

    def check_connection(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> None:
        self._introspector.check_connection(file_config)
