import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Literal

from mcp.server import FastMCP
from mcp.types import ToolAnnotations

from databao_context_engine import DatabaoContextEngine

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
        mcp = FastMCP(host=host or "127.0.0.1", port=port or 8000, lifespan=mcp_server_lifespan)

        @mcp.tool(
            description="Retrieve the contents of the all_results file",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def all_results_tool():
            return self._databao_context_engine.get_all_contexts_formatted()

        @mcp.tool(
            description="Retrieve the context built from various resources, including databases, dbt tools, plain and structured files, to retrieve relevant information",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def retrieve_tool(text: str, limit: int | None):
            retrieve_results = self._databao_context_engine.search_context(retrieve_text=text, limit=limit)

            display_results = [context_search_result.context_result for context_search_result in retrieve_results]

            display_results.append(f"\nToday's date is {date.today()}")

            return "\n".join(display_results)

        return mcp

    def run(self, transport: McpTransport):
        self._mcp_server.run(transport=transport)
