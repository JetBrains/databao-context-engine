import dataclasses

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from databao_context_engine.serialization.yaml import to_yaml_string


def enrich_database_context(
    context: DatabaseIntrospectionResult, description_provider: DescriptionProvider
) -> DatabaseIntrospectionResult:
    enriched_catalogs = []
    for catalog in context.catalogs:
        enriched_catalogs.append(_get_enriched_catalog(description_provider, catalog))

    return dataclasses.replace(context, catalogs=enriched_catalogs)


def _get_enriched_catalog(description_provider: DescriptionProvider, catalog: DatabaseCatalog) -> DatabaseCatalog:
    enriched_schemas = []
    for schema in catalog.schemas:
        enriched_schemas.append(_get_enriched_schema(description_provider, catalog.name, schema))

    catalog_description = (
        _describe_catalog(description_provider, catalog) if not catalog.description else catalog.description
    )

    return dataclasses.replace(catalog, schemas=enriched_schemas, description=catalog_description)


def _get_enriched_schema(
    description_provider: DescriptionProvider, catalog_name: str, schema: DatabaseSchema
) -> DatabaseSchema:
    enriched_tables = []
    for table in schema.tables:
        enriched_tables.append(_get_enriched_table(description_provider, catalog_name, schema.name, table))

    schema_description = (
        _describe_schema(description_provider, catalog_name, schema) if not schema.description else schema.description
    )

    return dataclasses.replace(schema, tables=enriched_tables, description=schema_description)


def _get_enriched_table(
    description_provider: DescriptionProvider, catalog_name: str, schema_name: str, table: DatabaseTable
) -> DatabaseTable:
    enriched_columns = []
    for column in table.columns:
        enriched_columns.append(_get_enriched_column(description_provider, catalog_name, schema_name, table, column))

    table_description = (
        _describe_table(description_provider, catalog_name, schema_name, table)
        if not table.description
        else table.description
    )

    return dataclasses.replace(table, columns=enriched_columns, description=table_description)


def _get_enriched_column(
    description_provider: DescriptionProvider,
    catalog_name: str,
    schema_name: str,
    table: DatabaseTable,
    column: DatabaseColumn,
) -> DatabaseColumn:
    if not column.description:
        column_description = _describe_column(description_provider, catalog_name, schema_name, table, column)
        return dataclasses.replace(column, description=column_description)

    return column


def _describe_catalog(description_provider: DescriptionProvider, catalog: DatabaseCatalog) -> str:
    return description_provider.describe(
        text=catalog.name,
        context=to_yaml_string(
            {
                "catalog": catalog,
                "schemas": [
                    {"name": schema.name, "tables": [table.name for table in schema.tables]}
                    for schema in catalog.schemas
                ],
            }
        ),
    )


def _describe_schema(description_provider: DescriptionProvider, catalog_name: str, schema: DatabaseSchema) -> str:
    return description_provider.describe(
        text=schema.name,
        context=to_yaml_string(
            {
                "catalog_name": catalog_name,
                "schema": {"name": schema.name, "tables": [table.name for table in schema.tables]},
            }
        ),
    )


def _describe_table(
    description_provider: DescriptionProvider, catalog_name: str, schema_name: str, table: DatabaseTable
) -> str:
    return description_provider.describe(
        text=table.name,
        context=to_yaml_string({"catalog_name": catalog_name, "schema_name": schema_name, "table": table}),
    )


def _describe_column(
    description_provider: DescriptionProvider,
    catalog_name: str,
    schema_name: str,
    table: DatabaseTable,
    column: DatabaseColumn,
) -> str:
    return description_provider.describe(
        text=to_yaml_string(column),
        context=to_yaml_string({"catalog_name": catalog_name, "schema_name": schema_name, "table": table}),
    )
