from typing import Any

from databao_context_engine import BuildDatasourcePlugin
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.plugins.dbt.dbt_chunker import build_dbt_chunks
from databao_context_engine.plugins.dbt.dbt_context_extractor import check_connection, extract_context
from databao_context_engine.plugins.dbt.types import DbtConfigFile, DbtContext


class DbtPlugin(BuildDatasourcePlugin[DbtConfigFile]):
    id = "jetbrains/dbt"
    name = "Dbt Plugin"
    config_file_type = DbtConfigFile
    context_type = DbtContext

    def supported_types(self) -> set[str]:
        return {"dbt"}

    def build_context(self, full_type: str, datasource_name: str, file_config: DbtConfigFile) -> Any:
        return extract_context(file_config)

    def check_connection(self, full_type: str, datasource_name: str, file_config: DbtConfigFile) -> None:
        check_connection(file_config)

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return build_dbt_chunks(context)
