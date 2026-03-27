import pytest

from databao_context_engine.build_sources.context_loader import load_database_built_context
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseIntrospectionResult,
    DatabaseSchema,
)
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.serialization.yaml import to_yaml_string
from tests.utils.project_creation import given_output_dir_with_built_contexts


def test_load_database_built_context(project_layout):
    datasource_id = DatasourceId.from_string_repr("databases/warehouse.yaml")
    expected_context = BuiltDatasourceContext(
        datasource_id=str(datasource_id),
        datasource_type="sqlite",
        context=DatabaseIntrospectionResult(
            catalogs=[DatabaseCatalog(name="analytics", schemas=[DatabaseSchema(name="public", tables=[])])]
        ),
    )
    given_output_dir_with_built_contexts(
        project_layout,
        [
            (
                datasource_id,
                to_yaml_string(expected_context),
            )
        ],
    )

    built = load_database_built_context(
        project_layout=project_layout,
        plugin_loader=DatabaoContextPluginLoader(),
        datasource_id=datasource_id,
    )

    assert built == expected_context


def test_load_database_built_context_rejects_non_database_context(project_layout):
    datasource_id = DatasourceId.from_string_repr("analytics/dbt_project.yaml")
    given_output_dir_with_built_contexts(
        project_layout,
        [
            (
                datasource_id,
                to_yaml_string(
                    BuiltDatasourceContext(
                        datasource_id=str(datasource_id),
                        datasource_type="dbt",
                        context={"nodes": []},
                    )
                ),
            )
        ],
    )

    with pytest.raises(ValueError, match="not database-capable"):
        load_database_built_context(
            project_layout=project_layout,
            plugin_loader=DatabaoContextPluginLoader(),
            datasource_id=datasource_id,
        )
