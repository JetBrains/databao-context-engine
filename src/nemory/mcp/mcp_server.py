import logging
from contextlib import asynccontextmanager
from typing import Literal

from mcp.server import FastMCP
from mcp.types import ToolAnnotations

from nemory.mcp.all_results_tool import run_all_results_tool
from nemory.mcp.query_tool import run_query_tool
from nemory.project.layout import ensure_project_dir, get_latest_run_name

logger = logging.getLogger(__name__)

McpTransport = Literal["stdio", "streamable-http"]


@asynccontextmanager
async def mcp_server_lifespan(server: FastMCP):
    logger.info(f"Starting MCP server on {server.settings.host}:{server.settings.port}...")
    yield
    logger.info("Stopping MCP server")


def _create_mcp_server(
    project_dir: str, run_name: str | None, host: str | None = None, port: int | None = None
) -> FastMCP:
    project_path = ensure_project_dir(project_dir=project_dir)
    if run_name is None:
        run_name = get_latest_run_name(project_path)

    logger.info(f"Using {run_name} from project {project_path}")

    mcp = FastMCP(host=host or "127.0.0.1", port=port or 8000, lifespan=mcp_server_lifespan)

    @mcp.tool(
        description="Retrieve the contents of the all_results file",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def all_results_tool():
        return run_all_results_tool(project_path, run_name)

    @mcp.tool(
        description="Query the context built from various resources, including databases, dbt tools, plain and structured files, to retrieve relevant information",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_tool(text: str, limit: int | None):
        return run_query_tool(project_path, run_name=run_name, text=text, limit=limit or 50)

    return mcp


def run_mcp_server(
    project_dir: str, run_name: str | None, transport: McpTransport, host: str | None = None, port: int | None = None
) -> None:
    server = _create_mcp_server(project_dir=project_dir, run_name=run_name, host=host, port=port)

    server.run(transport=transport)
