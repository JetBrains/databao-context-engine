import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from mcp.server import FastMCP
from mcp.types import ToolAnnotations

from databao_context_engine.databao_engine import DatabaoContextEngine
from databao_context_engine.mcp.all_results_tool import run_all_results_tool
from databao_context_engine.mcp.retrieve_tool import run_retrieve_tool

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
        run_name: str,
        host: str | None = None,
        port: int | None = None,
    ):
        self._databao_context_engine = DatabaoContextEngine(project_dir)
        self._run_name = run_name

        self._mcp_server = self._create_mcp_server(host, port)

    def _create_mcp_server(self, host: str | None = None, port: int | None = None) -> FastMCP:
        mcp = FastMCP(host=host or "127.0.0.1", port=port or 8000, lifespan=mcp_server_lifespan)

        @mcp.tool(
            description="Retrieve the contents of the all_results file",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def all_results_tool():
            return run_all_results_tool(self._databao_context_engine, self._run_name)

        @mcp.tool(
            description="Retrieve the context built from various resources, including databases, dbt tools, plain and structured files, to retrieve relevant information",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def retrieve_tool(text: str, limit: int | None):
            return run_retrieve_tool(
                databao_context_engine=self._databao_context_engine,
                run_name=self._run_name,
                text=text,
                limit=limit or 50,
            )

        return mcp

    def run(self, transport: McpTransport):
        self._mcp_server.run(transport=transport)
