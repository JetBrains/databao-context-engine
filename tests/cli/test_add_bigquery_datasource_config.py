from pathlib import Path

import pytest
from click.testing import CliRunner

from databao_context_engine import DatabaoContextPluginLoader, DatasourceType
from databao_context_engine.cli.add_datasource_config import add_datasource_config_interactive
from databao_context_engine.project.layout import get_source_dir
from databao_context_engine.serialization.yaml import to_yaml_string

bigquery = pytest.importorskip("google.cloud.bigquery")


def _make_plugin_loader():
    from databao_context_engine.plugins.databases.bigquery.bigquery_db_plugin import BigQueryDbPlugin

    return DatabaoContextPluginLoader(
        plugins_by_type={
            DatasourceType(full_type="bigquery"): BigQueryDbPlugin(),
        }
    )


def test_add_bigquery_datasource_config_with_default_auth(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "bigquery",
        "databases/my_bq",
        "my-gcp-project",
        "",
        "",
        "BigQueryDefaultAuth",
        "\n",
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path, plugin_loader=_make_plugin_loader())

    result_config_file = get_source_dir(project_path) / "databases" / "my_bq.yaml"
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "bigquery",
            "name": "my_bq",
            "connection": {
                "project": "my-gcp-project",
                "auth": {},
            },
        }
    )


def test_add_bigquery_datasource_config_with_service_account_key(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "bigquery",
        "databases/my_bq_sa",
        "my-gcp-project",
        "my_dataset",
        "",
        "BigQueryServiceAccountKeyFileAuth",
        "/path/to/credentials.json",
        "\n",
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path, plugin_loader=_make_plugin_loader())

    result_config_file = get_source_dir(project_path) / "databases" / "my_bq_sa.yaml"
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
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
    )
