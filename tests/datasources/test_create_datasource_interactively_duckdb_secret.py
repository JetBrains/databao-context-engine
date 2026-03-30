from pathlib import Path

import pytest
from pydantic import BaseModel

from databao_context_engine import (
    ConfiguredDatasource,
    DatabaoContextDomainManager,
    DatabaoContextPluginLoader,
    DatasourceType,
)
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile, BuildDatasourcePlugin, EmbeddableChunk
from databao_context_engine.pluginlib.config import DuckDBSecret
from tests.utils.config_wizard import MockUserInputCallback


class DummyConfigWithDuckDBSecret(BaseModel, AbstractConfigFile):
    type: str = "dummy_with_duckdb_secret"
    name: str
    duckdb_secret: DuckDBSecret | None = None


class DummyPluginWithDuckDBSecretConfig(BuildDatasourcePlugin[dict, DummyConfigWithDuckDBSecret]):
    id = "dummy/with_duckdb_secret"
    name = "Dummy Plugin with DuckDBSecret Config"
    config_file_type = DummyConfigWithDuckDBSecret
    context_type = dict

    def supported_types(self) -> set[str]:
        return {"dummy_with_duckdb_secret"}

    def build_context(self, full_type: str, datasource_name: str, file_config: DummyConfigWithDuckDBSecret) -> dict:
        return {}

    def divide_context_into_chunks(self, context: dict) -> list[EmbeddableChunk]:
        return []


@pytest.fixture
def project_manager(project_path: Path) -> DatabaoContextDomainManager:
    plugin_loader = DatabaoContextPluginLoader(
        plugins_by_type={DatasourceType(full_type="dummy_with_duckdb_secret"): DummyPluginWithDuckDBSecretConfig()}
    )
    return DatabaoContextDomainManager(domain_dir=project_path, plugin_loader=plugin_loader)


def test_add_datasource_config__with_duckdb_secret_skipped(project_manager):
    inputs = [
        False,  # skip duckdb_secret
    ]
    configured_datasource = create_datasource_config(project_manager, inputs=inputs)

    assert configured_datasource.config == {
        "type": "dummy_with_duckdb_secret",
        "name": "my datasource name",
    }


def test_add_datasource_config__with_duckdb_secret_filled(project_manager):
    inputs = [
        True,  # confirm duckdb_secret
        "s3",  # duckdb_secret.type
    ]
    configured_datasource = create_datasource_config(project_manager, inputs=inputs)

    assert configured_datasource.config == {
        "type": "dummy_with_duckdb_secret",
        "name": "my datasource name",
        "duckdb_secret": {"type": "s3"},
    }


def create_datasource_config(
    project_manager: DatabaoContextDomainManager, inputs: list[bool | str]
) -> ConfiguredDatasource:
    user_input_callback = MockUserInputCallback(inputs=inputs)

    return project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="dummy_with_duckdb_secret"),
        datasource_name="resources/my datasource name",
        user_input_callback=user_input_callback,
    )
