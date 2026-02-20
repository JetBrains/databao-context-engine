from dataclasses import dataclass
from os import environ

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
                    chunks.append(_create_column_chunk(catalog.name, schema.name, table, column))

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
    catalog_name: str, schema_name: str, table: DatabaseTable, column: DatabaseColumn
) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_column_chunk_text(table, column),
        content=DatabaseColumnChunkContent(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table.name,
            column=column,
        ),
    )


def _should_use_old_table_chunk_text():
    return environ.get("DATABAO_OLD_TABLE_CHUNK_TEXT")


def _should_use_old_column_chunk_text():
    return environ.get("DATABAO_OLD_COLUMN_CHUNK_TEXT")


def _build_table_chunk_text(table: DatabaseTable) -> str:
    if _should_use_old_table_chunk_text():
        return f"Table {table.name} with columns {','.join([column.name for column in table.columns])}"

    sections = [
        f"{table.name} is a database {table.kind.value} with {len(table.columns)} columns",
        _build_table_primary_key_text(table),
        _build_table_foreign_keys_section(table),
        _build_table_all_columns_section(table),
        table.description if table.description else "",
    ]

    return ". ".join([section for section in sections if section])


def _build_table_all_columns_section(table: DatabaseTable) -> str:
    all_columns = ", ".join([column.name for column in table.columns])
    return f"Here is the full list of columns for the {table.kind.value}: {all_columns}"


def _build_table_primary_key_text(table: DatabaseTable) -> str:
    if table.primary_key is None:
        return ""

    if len(table.primary_key.columns) == 1:
        primary_key_column_name = table.primary_key.columns[0]
        column_details = next((column for column in table.columns if column.name == primary_key_column_name), None)
        if column_details is None:
            return ""

        return f"Its primary key is the column {primary_key_column_name} of type {column_details.type}"

    return f"Its primary key is composed of the columns ({', '.join(table.primary_key.columns)})"


def _build_table_foreign_keys_section(table: DatabaseTable) -> str:
    if not table.foreign_keys:
        return ""

    all_foreign_keys_destinations = _join_with_different_last_separator(
        ", ", " and ", [foreign_key.referenced_table for foreign_key in table.foreign_keys]
    )
    if len(table.foreign_keys) == 1:
        return f"The column has a foreign key to {all_foreign_keys_destinations}"

    return f"The {table.kind.value} has foreign keys to {all_foreign_keys_destinations}"


def _build_column_chunk_text(table: DatabaseTable, column: DatabaseColumn) -> str:
    if _should_use_old_column_chunk_text():
        return f"Column {column.name} in table {table.name}"

    sections = [
        f"{column.name} is a column with type {column.type} in the {table.kind.value} {table.name}",
        f"It can{'' if column.nullable else ' not'} contain null values",
        _build_column_is_primary_key_section(table, column),
        _build_column_is_foreign_key_section(table, column),
        _build_column_generated_section(column),
        column.description if column.description else "",
    ]

    return ". ".join([section for section in sections if section])


def _build_column_generated_section(column: DatabaseColumn) -> str:
    if not column.generated:
        return ""

    match column.generated:
        case "identity":
            return "This column is an identity column"
        case _:
            return "This column is a generated column"


def _build_column_is_primary_key_section(table: DatabaseTable, column: DatabaseColumn) -> str:
    if table.primary_key is None:
        return ""

    if len(table.primary_key.columns) == 1 and table.primary_key.columns[0] == column.name:
        return f"It is the primary key of the {table.kind.value}"

    if column.name in table.primary_key.columns:
        return f"It is part of the primary key of the {table.kind.value}"

    return ""


def _build_column_is_foreign_key_section(table: DatabaseTable, column: DatabaseColumn) -> str:
    if not table.foreign_keys:
        return ""

    foreign_keys_column_is_part_of = [
        foreign_key
        for foreign_key in table.foreign_keys
        if any(mapping.from_column == column.name for mapping in foreign_key.mapping)
    ]

    if not foreign_keys_column_is_part_of:
        return ""

    single_foreign_keys_column_is_part_of = [
        foreign_key for foreign_key in foreign_keys_column_is_part_of if len(foreign_key.mapping) == 1
    ]

    all_single_foreign_key_joined = _join_with_different_last_separator(
        ", ",
        " and ",
        [
            f"{foreign_key.referenced_table}.{foreign_key.mapping[0].to_column}"
            for foreign_key in single_foreign_keys_column_is_part_of
        ],
    )
    all_single_foreign_key_str = (
        f"This column is a foreign key to {all_single_foreign_key_joined}"
        if single_foreign_keys_column_is_part_of
        else ""
    )

    complex_foreign_keys_column_is_part_of = [
        foreign_key for foreign_key in foreign_keys_column_is_part_of if len(foreign_key.mapping) > 1
    ]

    all_complex_foreign_key_joined = _join_with_different_last_separator(
        ", ",
        " and ",
        [f"{foreign_key.referenced_table}" for foreign_key in complex_foreign_keys_column_is_part_of],
    )
    all_complex_foreign_key_str = (
        f"This column is part of a foreign key to {all_complex_foreign_key_joined}"
        if complex_foreign_keys_column_is_part_of
        else ""
    )

    return ". ".join([str for str in [all_single_foreign_key_str, all_complex_foreign_key_str] if str])


def _join_with_different_last_separator(separator: str, last_separator: str, iterable: list[str]) -> str:
    if len(iterable) > 1:
        return separator.join(iterable[:-1]) + last_separator + iterable[-1]

    if len(iterable) == 1:
        return iterable[0]

    return ""
