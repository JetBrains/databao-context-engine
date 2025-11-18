import logging
from pathlib import Path

from mcp.server import FastMCP
from mcp.types import ToolAnnotations

from nemory.project.layout import ALL_RESULTS_FILE_NAME, ensure_project_dir, get_latest_run_dir, get_run_dir

logger = logging.getLogger(__name__)


def _read_all_results_file(run_directory: Path) -> str:
    with open(run_directory.joinpath(ALL_RESULTS_FILE_NAME), "r") as file:
        return file.read()


def _create_mcp_server(project_dir: str, run_name: str | None) -> FastMCP:
    project_path = ensure_project_dir(project_dir=project_dir)
    if run_name is None:
        run_directory = get_latest_run_dir(project_path)
    else:
        run_directory = get_run_dir(project_path, run_name)

    mcp = FastMCP()

    @mcp.tool(
        description="Retrieve the contents of the all_results file",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def all_results_tool():
        return _read_all_results_file(run_directory)

    return mcp


def run_mcp_server(project_dir: str, run_name: str | None) -> None:
    server = _create_mcp_server(project_dir=project_dir, run_name=run_name)

    logger.info(f"Starting MCP server on {server.settings.host}:{server.settings.port}...")

    server.run(transport="streamable-http")
