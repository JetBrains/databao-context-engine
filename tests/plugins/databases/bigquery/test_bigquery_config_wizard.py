from pathlib import Path

import pytest

from databao_context_engine import DatabaoContextPluginLoader, DatabaoContextProjectManager, DatasourceType
from databao_context_engine.plugins.databases.bigquery.bigquery_db_plugin import BigQueryDbPlugin
from tests.utils.config_wizard import MockUserInputCallback


@pytest.fixture
def project_manager(project_path: Path) -> DatabaoContextProjectManager:
    plugin_loader = DatabaoContextPluginLoader(
        plugins_by_type={
            DatasourceType(full_type="bigquery"): BigQueryDbPlugin(),
        }
    )
    return DatabaoContextProjectManager(project_dir=project_path, plugin_loader=plugin_loader)


def test_add_bigquery_datasource_config_with_default_auth(project_manager):
    inputs = [
        "my-gcp-project",
        "",
        "",
        "BigQueryDefaultAuth",
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="bigquery"),
        datasource_name="databases/my_bq",
        user_input_callback=user_input_callback,
        validate_config_content=False,
    )

    assert configured_datasource.config == {
        "type": "bigquery",
        "name": "my_bq",
        "connection": {
            "project": "my-gcp-project",
            "auth": {},
        },
    }


def test_add_bigquery_datasource_config_with_service_account_key(project_manager):
    inputs = [
        "my-gcp-project",
        "my_dataset",
        "",
        "BigQueryServiceAccountKeyFileAuth",
        "/path/to/credentials.json",
    ]

    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="bigquery"),
        datasource_name="databases/my_bq_sa",
        user_input_callback=user_input_callback,
        validate_config_content=False,
    )

    assert configured_datasource.config == {
        "type": "bigquery",
        "name": "my_bq_sa",
        "connection": {
            "project": "my-gcp-project",
            "dataset": "my_dataset",
            "auth": {
                "credentials_file": "/path/to/credentials.json",
            },
        },
    }
