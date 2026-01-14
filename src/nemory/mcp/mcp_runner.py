import logging
from pathlib import Path

from nemory.mcp.mcp_server import McpServer, McpTransport
from nemory.project.layout import ensure_project_dir
from nemory.project.runs import resolve_run_name

logger = logging.getLogger(__name__)


def run_mcp_server(
    project_dir: Path,
    run_name: str | None,
    transport: McpTransport,
    host: str | None = None,
    port: int | None = None,
) -> None:
    ensure_project_dir(project_dir=project_dir)
    resolved_run_name = resolve_run_name(project_dir=project_dir, run_name=run_name)

    logger.info(f"Using {resolved_run_name} from project {project_dir.resolve()}")

    McpServer(project_dir, resolved_run_name, host, port).run(transport)
