from dataclasses import dataclass

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseTable,
)


@dataclass
class DatabaseTableChunkContent:
    catalog_name: str
    schema_name: str
    table: DatabaseTable


@dataclass
class DatabaseColumnChunkContent:
    catalog_name: str
    schema_name: str
    table_name: str
    column: DatabaseColumn


def build_database_chunks(result: DatabaseIntrospectionResult) -> list[EmbeddableChunk]:
    chunks = []
    for catalog in result.catalogs:
        for schema in catalog.schemas:
            for table in schema.tables:
                chunks.append(_create_table_chunk(catalog.name, schema.name, table))

                for column in table.columns:
                    chunks.append(_create_column_chunk(catalog.name, schema.name, table.name, column))

    return chunks


def _create_table_chunk(catalog_name: str, schema_name: str, table: DatabaseTable) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_table_chunk_text(table),
        content=DatabaseTableChunkContent(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table=table,
        ),
    )


def _create_column_chunk(
    catalog_name: str, schema_name: str, table_name: str, column: DatabaseColumn
) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_column_chunk_text(table_name, column),
        content=DatabaseColumnChunkContent(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column=column,
        ),
    )


def _build_table_chunk_text(database_table: DatabaseTable) -> str:
    return f"Table {database_table.name} with columns {','.join([column.name for column in database_table.columns])}"


def _build_column_chunk_text(table_name: str, database_object: DatabaseColumn) -> str:
    return f"Column {database_object.name} in table {table_name}"
