from pathlib import Path

from nemory.project.layout import read_config_file, get_output_dir
from nemory.storage.repositories.factories import create_run_repository
from nemory.storage.connection import open_duckdb_connection
from nemory.storage.repositories.run_repository import RunRepository
from nemory.system.properties import get_db_path


def resolve_run_name(*, project_dir: Path, run_name: str | None) -> str:
    project_id = str(read_config_file(project_dir).project_id)

    with open_duckdb_connection(get_db_path()) as conn:
        run_repository = create_run_repository(conn)

        return resolve_run_name_from_repo(run_repository=run_repository, project_id=project_id, run_name=run_name)


def resolve_run_name_from_repo(*, run_repository: RunRepository, project_id: str, run_name: str | None) -> str:
    if run_name is None:
        latest = run_repository.get_latest_run_for_project(project_id=project_id)
        if latest is None:
            raise LookupError(f"No runs found for project '{project_id}'. Run a build first.")
        return latest.run_name
    else:
        run = run_repository.get_by_run_name(project_id=project_id, run_name=run_name)
        if run is None:
            raise LookupError(f"Run '{run_name}' not found for project '{project_id}'.")
        return run.run_name


def get_run_dir(project_dir: Path, run_name: str) -> Path:
    run_dir = get_output_dir(project_dir).joinpath(run_name)
    if not run_dir.is_dir():
        raise ValueError(
            f"The run with name {run_name} doesn't exist in the project. [project_dir: {project_dir.resolve()}]"
        )

    return run_dir
