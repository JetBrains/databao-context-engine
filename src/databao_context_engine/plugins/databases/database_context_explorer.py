from dataclasses import dataclass

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult, DatabaseTable


@dataclass(kw_only=True, frozen=True)
class DatabaseTableLite:
    table_name: str
    description: str | None


@dataclass(kw_only=True, frozen=True)
class DatabaseSchemaLite:
    datasource_id: str
    catalog_name: str
    schema_name: str
    description: str | None
    tables: list[DatabaseTableLite] | None


@dataclass(kw_only=True, frozen=True)
class DatabaseTableDetails:
    datasource_id: str
    catalog_name: str
    schema_name: str
    table: DatabaseTable


def list_database_schemas_and_tables(context: BuiltDatasourceContext) -> list[DatabaseSchemaLite]:
    if not isinstance(context.context, DatabaseIntrospectionResult):
        raise ValueError(
            f"Impossible to list database's schemas for a context that is not a `DatabaseIntrospectionResult` (received: {type(context.context)})"
        )

    return [
        DatabaseSchemaLite(
            datasource_id=context.datasource_id,
            catalog_name=catalog.name,
            schema_name=schema.name,
            description=schema.description,
            tables=[
                DatabaseTableLite(
                    table_name=table.name,
                    description=table.description,
                )
                for table in schema.tables
            ],
        )
        for catalog in context.context.catalogs
        for schema in catalog.schemas
    ]


def get_database_table_details(
    context: BuiltDatasourceContext, catalog_name: str, schema_name: str, table_name: str
) -> DatabaseTableDetails:
    if not isinstance(context.context, DatabaseIntrospectionResult):
        raise ValueError(
            f"Impossible to get database's table details for a context that is not a `DatabaseIntrospectionResult` (received: {type(context.context)})"
        )

    datasource_id = context.datasource_id
    catalog = next((catalog for catalog in context.context.catalogs if catalog.name == catalog_name), None)
    if catalog is None:
        available_catalogs = [catalog.name for catalog in context.context.catalogs]
        raise ValueError(
            f"Unknown catalog {catalog_name!r} for datasource {datasource_id}. "
            f"Available catalogs: {', '.join(available_catalogs) or '(none)'}"
        )

    schema = next((schema for schema in catalog.schemas if schema.name == schema_name), None)
    if schema is None:
        available_schemas = [schema.name for schema in catalog.schemas]
        raise ValueError(
            f"Unknown schema {schema_name!r} in catalog {catalog_name!r} for datasource {datasource_id}. "
            f"Available schemas: {', '.join(available_schemas) or '(none)'}"
        )

    table = next((table for table in schema.tables if table.name == table_name), None)
    if table is None:
        available_tables = [table.name for table in schema.tables]
        raise ValueError(
            f"Unknown table {table_name!r} in {catalog_name}.{schema_name} for datasource {datasource_id}. "
            f"Available tables: {', '.join(available_tables) or '(none)'}"
        )

    return DatabaseTableDetails(
        datasource_id=str(datasource_id),
        catalog_name=catalog_name,
        schema_name=schema_name,
        table=table,
    )
