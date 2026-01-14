from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from nemory.project.layout import ALL_RESULTS_FILE_NAME, get_output_dir, read_config_file
from nemory.storage.connection import open_duckdb_connection
from nemory.storage.repositories.factories import create_run_repository
from nemory.storage.repositories.run_repository import RunRepository


@dataclass(kw_only=True, frozen=True)
class Run:
    run_dir: Path
    run_name: str
    all_results_file_content: str


@dataclass(kw_only=True)
class ProjectWithRuns:
    project_dir: Path
    runs: list[Run]


def _create_run_dir(run_repo: RunRepository, project_id: str, output_path: Path, started_at: datetime) -> Run:
    run = run_repo.create(project_id=str(project_id), nemory_version="1.0", started_at=started_at)

    run_dir = output_path.joinpath(run.run_name)
    run_dir.mkdir()

    all_results_content = _create_all_results_file(run_dir)

    return Run(run_dir=run_dir, run_name=run.run_name, all_results_file_content=all_results_content)


def _create_all_results_file(run_dir: Path) -> str:
    all_results_file_path = run_dir.joinpath(ALL_RESULTS_FILE_NAME)
    file_content = (
        "# ===== Dummy - Dummy =====\n\n"
        "# This file contains all the results of the build process.\n"
        f"# Run name: {run_dir.name}"
    )

    with open(all_results_file_path, "w") as file:
        file.write(file_content)

    return file_content


@pytest.fixture
def project_with_runs(create_db, project_path: Path, db_path: Path) -> ProjectWithRuns:
    output_dir = get_output_dir(project_path)
    output_dir.mkdir()

    with open_duckdb_connection(db_path) as conn:
        run_repo = create_run_repository(conn)
        project_id = str(read_config_file(project_path).project_id)

        run_1 = _create_run_dir(run_repo, project_id, output_dir, datetime.now() - timedelta(days=10))
        run_2 = _create_run_dir(run_repo, project_id, output_dir, datetime.now())
        run_3 = _create_run_dir(run_repo, project_id, output_dir, datetime.now() - timedelta(days=3))

    return ProjectWithRuns(project_dir=project_path, runs=[run_1, run_2, run_3])
