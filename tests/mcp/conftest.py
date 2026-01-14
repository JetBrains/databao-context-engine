from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from nemory.datasource_config.datasource_context import DatasourceContext
from nemory.project.datasource_discovery import DatasourceId
from nemory.project.layout import get_output_dir, read_config_file
from nemory.storage.connection import open_duckdb_connection
from nemory.storage.repositories.factories import create_run_repository
from nemory.storage.repositories.run_repository import RunRepository


@dataclass(kw_only=True, frozen=True)
class Run:
    run_dir: Path
    run_name: str
    datasource_contexts: list[DatasourceContext]


@dataclass(kw_only=True)
class ProjectWithRuns:
    project_dir: Path
    runs: list[Run]


def _create_output_context(run_dir: Path, datasource_id: DatasourceId, output: str):
    output_file = run_dir.joinpath(datasource_id).with_suffix(".yaml")
    output_file.parent.mkdir(exist_ok=True)
    output_file.touch()
    output_file.write_text(output)


def _create_run_dir(run_repo: RunRepository, project_id: str, output_path: Path, started_at: datetime) -> Run:
    run = run_repo.create(project_id=str(project_id), nemory_version="1.0", started_at=started_at)

    run_dir = output_path.joinpath(run.run_name)
    run_dir.mkdir()

    datasource_contexts = [
        DatasourceContext(datasource_id="main_type/datasource_name", context="Context for datasource name"),
        DatasourceContext(datasource_id="dummy/my_datasource", context="Context for dummy/my_datasource"),
    ]
    for context in datasource_contexts:
        _create_output_context(run_dir, context.datasource_id, context.context)

    return Run(run_dir=run_dir, run_name=run.run_name, datasource_contexts=datasource_contexts)


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
