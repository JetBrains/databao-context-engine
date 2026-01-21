import logging
from pathlib import Path

from databao_context_engine.mcp.mcp_server import McpServer, McpTransport
from databao_context_engine.project.layout import ensure_project_dir

logger = logging.getLogger(__name__)


def run_mcp_server(
    project_dir: Path,
    transport: McpTransport,
    host: str | None = None,
    port: int | None = None,
) -> None:
    ensure_project_dir(project_dir=project_dir)

    McpServer(project_dir, host, port).run(transport)
