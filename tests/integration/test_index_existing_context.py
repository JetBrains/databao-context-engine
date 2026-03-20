from pathlib import Path

from databao_context_engine import (
    DatabaoContextDomainManager,
    DatasourceStatus,
    SQLiteConfigFile,
    SQLiteConnectionConfig,
)
from databao_context_engine.project.layout import ProjectLayout
from tests.integration.sqlite_integration_test_utils import create_sqlite_with_base_schema
from tests.utils.project_creation import given_datasource_config_file


def test_index_existing_contexts(tmp_path: Path, project_layout: ProjectLayout):
    sqlite1_path = tmp_path / "sqlite1.db"
    create_sqlite_with_base_schema(sqlite1_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite1",
        SQLiteConfigFile(
            name="my_sqlite1", connection=SQLiteConnectionConfig(database_path=str(sqlite1_path))
        ).model_dump(),
    )

    sqlite2_path = tmp_path / "sqlite2.db"
    create_sqlite_with_base_schema(sqlite2_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite2",
        SQLiteConfigFile(
            name="my_sqlite2", connection=SQLiteConnectionConfig(database_path=str(sqlite2_path))
        ).model_dump(),
    )

    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)

    assert not project_layout.output_dir.is_dir()

    domain_manager.build_context(should_enrich_context=False, should_index=False)

    results = domain_manager.index_built_contexts()

    assert all(result.status == DatasourceStatus.OK for result in results)
