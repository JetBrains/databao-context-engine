from dataclasses import dataclass

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.plugins.resources.parquet_introspector import ParquetColumn, ParquetIntrospectionResult


@dataclass
class ParquetColumnChunkContent:
    file_name: str
    column: ParquetColumn


def build_parquet_chunks(result: ParquetIntrospectionResult) -> list[EmbeddableChunk]:
    chunks = []
    for file in result.files:
        for column in file.columns:
            chunks.append(
                EmbeddableChunk(
                    embeddable_text=f"Column [name = {column.name}, type = {column.type}, number of values = {column.num_values}] in parquet file {file.name}",
                    content=ParquetColumnChunkContent(file_name=file.name, column=column),
                )
            )
    return chunks
