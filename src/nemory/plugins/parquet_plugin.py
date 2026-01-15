from typing import Any

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, EmbeddableChunk
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

    def divide_result_into_chunks(self, build_result: Any) -> list[EmbeddableChunk]:
        return build_parquet_chunks(build_result)

    def build_context(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> Any:
        introspection_result = self._introspector.introspect(file_config)

        return introspection_result

    def check_connection(self, full_type: str, datasource_name: str, file_config: ParquetConfigFile) -> None:
        self._introspector.check_connection(file_config)
