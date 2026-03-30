import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Literal

from mcp.server import FastMCP
from mcp.types import ToolAnnotations

from databao_context_engine import DatabaoContextEngine, DatasourceId
from databao_context_engine.serialization.yaml import to_plain_python

logger = logging.getLogger(__name__)

McpTransport = Literal["stdio", "streamable-http"]


@asynccontextmanager
async def mcp_server_lifespan(server: FastMCP):
    logger.info(f"Starting MCP server on {server.settings.host}:{server.settings.port}...")
    yield
    logger.info("Stopping MCP server")


class McpServer:
    def __init__(
        self,
        project_dir: Path,
        host: str | None = None,
        port: int | None = None,
    ):
        self._databao_context_engine = DatabaoContextEngine(project_dir)

        self._mcp_server = self._create_mcp_server(host, port)

    def _create_mcp_server(self, host: str | None = None, port: int | None = None) -> FastMCP:
        mcp = FastMCP(
            host=host or "127.0.0.1",
            port=port or 8000,
            lifespan=mcp_server_lifespan,
            name="Databao Context Engine",
            instructions="""Use this server to understand and query datasources configured in this project.

Prefer the built metadata tools for schema discovery:
- use "list_all_datasources" to discover available datasources
- use "list_database_datasources" to restrict to datasources that support database metadata and SQL
- use "list_database_schemas" to browse catalogs, schemas, and table summaries for one datasource
- use "get_database_table_details" to inspect the full metadata of one specific table

Use "search_context" for fuzzy or semantic lookup across all datasource types, including databases, dbt, and files, when you do not know the exact datasource or table yet.

Use "run_sql_on_database" only when you need live query results or need to validate a query against the actual datasource, rather than browsing built metadata.
            """,
        )

        @mcp.tool(
            name="search_context",
            description="Search built context across all datasources in this project using free-text or semantic matching. Use this when you do not know the exact datasource, schema, or table yet, or when you want to find relevant information across databases, dbt models, and files. Prefer the database metadata tools instead when you want structured schema browsing or full details for a known table.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def search_context_tool(text: str, limit: int | None):
            retrieve_results = self._databao_context_engine.search_context(search_text=text, limit=limit)

            display_results = [context_search_result.context_result for context_search_result in retrieve_results]

            display_results.append(f"\nToday's date is {date.today()}")

            return "\n".join(display_results)

        @mcp.tool(
            name="list_all_datasources",
            description="List all datasources configured in this project, including their IDs, names, and types. Use this first when you need to discover what datasources are available or when another tool requires a datasource_id.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def list_datasources_tool():
            datasources = self._databao_context_engine.get_introspected_datasource_list()
            return {
                "datasources": [
                    {
                        "id": str(ds.id),
                        "name": ds.id.name,
                        "type": ds.type.full_type,
                    }
                    for ds in datasources
                ]
            }

        @mcp.tool(
            name="list_database_datasources",
            description="List all configured datasources that support database metadata tools and SQL execution. Use this to narrow datasource selection before browsing schemas, inspecting table metadata, or running SQL.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def list_database_datasources():
            datasources = self._databao_context_engine.list_database_datasources()

            return {
                "datasources": [
                    {
                        "id": str(ds.id),
                        "name": ds.id.name,
                        "type": ds.type.full_type,
                    }
                    for ds in datasources
                ]
            }

        @mcp.tool(
            name="list_database_schemas",
            description='List all catalogs, schemas and tables for a database-capable datasource. The returned list will only contain the name and description of the schemas and tables. This allows to find tables related to your query and then query the full details using the "get_database_table_details" tool',
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def list_database_schema_tree(datasource_id: str):
            ds = DatasourceId.from_string_repr(datasource_id)
            return {
                "schemas": to_plain_python(self._databao_context_engine.list_database_schemas_and_tables(ds)),
            }

        @mcp.tool(
            name="get_database_table_details",
            description="Get the full built metadata for one specific table in a database-capable datasource. Requires an exact datasource_id, catalog, schema, and table name. Use this when you already know which table you want and need detailed schema information such as columns, types, keys, indexes, samples, or profiling data to help write or validate SQL.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def get_database_table_details(datasource_id: str, catalog: str, schema: str, table: str):
            ds = DatasourceId.from_string_repr(datasource_id)
            return to_plain_python(
                self._databao_context_engine.get_database_table_details(
                    datasource_id=ds,
                    catalog_name=catalog,
                    schema_name=schema,
                    table_name=table,
                )
            )

        @mcp.tool(
            name="run_sql_on_database",
            description="Execute SQL against a configured database-capable datasource. Use this when you need live rows, aggregates, or query validation against the actual datasource. Prefer the metadata tools for schema discovery and table inspection. Defaults to read-only queries; set read_only=false only when mutations are intentionally required. If datasource_id is omitted, it will only work when exactly one datasource is configured in the project.",
            annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=False, openWorldHint=True),
        )
        async def run_sql_tool(
            sql: str,
            datasource_id: str | None = None,
            read_only: bool = True,
        ):
            # If no datasource_id provided, try to use the only one available
            if datasource_id is None:
                datasources = self._databao_context_engine.get_introspected_datasource_list()
                if len(datasources) == 0:
                    raise ValueError("No datasources configured in the project")
                if len(datasources) > 1:
                    available_ids = [str(ds.id) for ds in datasources]
                    raise ValueError(
                        f"Multiple datasources configured. Please specify datasource_id. "
                        f"Available datasources: {', '.join(available_ids)}"
                    )
                ds = datasources[0].id
            else:
                ds = DatasourceId.from_string_repr(datasource_id)

            res = self._databao_context_engine.run_sql(ds, sql, read_only=read_only)
            return {"columns": res.columns, "rows": res.rows}

        return mcp

    def run(self, transport: McpTransport):
        self._mcp_server.run(transport=transport)
