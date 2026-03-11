from dataclasses import dataclass
from pathlib import Path

import pytest

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.project.layout import ProjectLayout, get_output_dir
from tests.utils.project_creation import given_output_dir_with_built_contexts


@dataclass(kw_only=True, frozen=True)
class Output:
    output_dir: Path
    datasource_contexts: list[tuple[DatasourceId, str]]


@dataclass(kw_only=True)
class Project:
    project_dir: Path
    output: Output


@pytest.fixture
def project(create_db, project_layout: ProjectLayout, db_path: Path) -> Project:
    output_dir = get_output_dir(project_layout.project_dir)
    output_dir.mkdir(exist_ok=True)

    datasource_contexts = [
        (
            DatasourceId.from_string_repr("main_type/datasource_name.yaml"),
            "Context for datasource name",
        ),
        (
            DatasourceId.from_string_repr("dummy/my_datasource.yaml"),
            "Context for dummy/my_datasource",
        ),
    ]

    output_dir = given_output_dir_with_built_contexts(project_layout, datasource_contexts)
    output = Output(output_dir=output_dir, datasource_contexts=datasource_contexts)

    return Project(project_dir=project_layout.project_dir, output=output)
