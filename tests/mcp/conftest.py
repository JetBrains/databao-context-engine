from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from databao_context_engine.datasource_config.datasource_context import DatasourceContext
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.project.layout import get_output_dir
from tests.utils.project_creation import with_run_dir


@dataclass(kw_only=True, frozen=True)
class Run:
    run_dir: Path
    run_name: str
    datasource_contexts: list[DatasourceContext]


@dataclass(kw_only=True)
class ProjectWithRuns:
    project_dir: Path
    runs: list[Run]


@pytest.fixture
def project_with_runs(create_db, project_path: Path, db_path: Path) -> ProjectWithRuns:
    output_dir = get_output_dir(project_path)
    output_dir.mkdir()

    datasource_contexts = [
        DatasourceContext(
            datasource_id=DatasourceId.from_string_repr("main_type/datasource_name.yaml"),
            context="Context for datasource name",
        ),
        DatasourceContext(
            datasource_id=DatasourceId.from_string_repr("dummy/my_datasource.yaml"),
            context="Context for dummy/my_datasource",
        ),
    ]

    run_1_contexts = datasource_contexts[0:1]
    run_1_dir = with_run_dir(db_path, project_path, run_1_contexts, datetime.now() - timedelta(days=10))
    run_1 = Run(run_dir=run_1_dir, run_name=run_1_dir.name, datasource_contexts=run_1_contexts)

    run_2_contexts = datasource_contexts
    run_2_dir = with_run_dir(db_path, project_path, run_2_contexts, datetime.now())
    run_2 = Run(run_dir=run_2_dir, run_name=run_2_dir.name, datasource_contexts=run_2_contexts)

    run_3_contexts = datasource_contexts[1:2]
    run_3_dir = with_run_dir(db_path, project_path, run_3_contexts, datetime.now() - timedelta(days=3))
    run_3 = Run(run_dir=run_3_dir, run_name=run_3_dir.name, datasource_contexts=run_3_contexts)

    return ProjectWithRuns(project_dir=project_path, runs=[run_1, run_2, run_3])
