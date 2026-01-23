from dataclasses import dataclass
from pathlib import Path

import pytest

from databao_context_engine import DatasourceContext, DatasourceId
from databao_context_engine.project.layout import ProjectLayout, get_output_dir
from tests.utils.project_creation import with_output


@dataclass(kw_only=True, frozen=True)
class Output:
    output_dir: Path
    datasource_contexts: list[DatasourceContext]


@dataclass(kw_only=True)
class Project:
    project_dir: Path
    output: Output


@pytest.fixture
def project(create_db, project_layout: ProjectLayout, db_path: Path) -> Project:
    output_dir = get_output_dir(project_layout.project_dir)
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

    output_dir = with_output(project_layout, datasource_contexts)
    output = Output(output_dir=output_dir, datasource_contexts=datasource_contexts)

    return Project(project_dir=project_layout.project_dir, output=output)
