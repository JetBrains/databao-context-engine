import logging
from pathlib import Path

from nemory.mcp.mcp_server import McpServer, McpTransport
from nemory.project.layout import ensure_project_dir, read_config_file
from nemory.services.factories import create_run_repository
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path

logger = logging.getLogger(__name__)


def run_mcp_server(
    project_dir: str,
    run_name: str | None,
    transport: McpTransport,
    host: str | None = None,
    port: int | None = None,
    db_path: Path | None = None,
) -> None:
    project_path = ensure_project_dir(project_dir=project_dir)
    if run_name is None:
        run_name = _get_latest_run_name(project_path, db_path)

    logger.info(f"Using {run_name} from project {project_path.resolve()}")

    McpServer(project_path, run_name, host, port).run(transport)


def _get_latest_run_name(project_dir: Path, db_path: Path | None = None) -> str:
    project_id = read_config_file(project_dir).project_id
    real_db_path = db_path or get_db_path()
    with open_duckdb_connection(real_db_path) as conn:
        run_repository = create_run_repository(conn)
        run = run_repository.get_latest_run_for_project(str(project_id))

        if run is None:
            raise ValueError(
                f"No runs found for project {project_id} using db path {real_db_path} and project {project_dir}"
            )

        return run.run_name
