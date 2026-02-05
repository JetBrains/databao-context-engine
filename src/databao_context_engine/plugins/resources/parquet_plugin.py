from typing import Any

from databao_context_engine.pluginlib.build_plugin import BuildDatasourcePlugin, EmbeddableChunk
from databao_context_engine.plugins.resources.parquet_chunker import build_parquet_chunks
from databao_context_engine.plugins.resources.parquet_introspector import (
    ParquetConfigFile,
    ParquetIntrospectionResult,
    ParquetIntrospector,
    parquet_type,
)


class ParquetPlugin(BuildDatasourcePlugin[ParquetConfigFile]):
    id = "jetbrains/parquet"
    name = "Parquet Plugin"
    config_file_type = ParquetConfigFile
    context_type = ParquetIntrospectionResult

    def __init__(self):
        self._introspector = ParquetIntrospector()

    def supported_types(self) -> set[str]:
        return {parquet_type}

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return build_parquet_chunks(context)

    def build_context(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> Any:
        return self._introspector.introspect(file_config)

    def check_connection(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> None:
        self._introspector.check_connection(file_config)
