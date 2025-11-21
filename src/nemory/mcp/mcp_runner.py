import logging
from pathlib import Path

from nemory.mcp.mcp_server import McpServer, McpTransport
from nemory.project.layout import ensure_project_dir, get_latest_run_name

logger = logging.getLogger(__name__)


def run_mcp_server(
    project_dir: str, run_name: str | None, transport: McpTransport, host: str | None = None, port: int | None = None
) -> None:
    project_path = ensure_project_dir(project_dir=project_dir)
    if run_name is None:
        run_name = _get_latest_run_name(project_path)

    logger.info(f"Using {run_name} from project {project_path.resolve()}")

    McpServer(project_path, run_name, host, port).run(transport)


def _get_latest_run_name(
    project_dir: Path,
) -> str:
    # TODO: Read the latest run name from the DB instead
    return get_latest_run_name(project_dir)
