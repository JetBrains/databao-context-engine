import logging
from pathlib import Path

from databao_context_engine.mcp.mcp_server import McpServer, McpTransport

logger = logging.getLogger(__name__)


def run_mcp_server(
    project_dir: Path,
    run_name: str | None,
    transport: McpTransport,
    host: str | None = None,
    port: int | None = None,
) -> None:
    McpServer(project_dir, run_name, host, port).run(transport)
