from dataclasses import dataclass

import pytest

from databao_context_engine.datasources.check_config import (
    CheckDatasourceConnectionResult,
    DatasourceConnectionStatus,
    check_datasource_connection,
)
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import BuildDatasourcePlugin, BuildPlugin, DatasourceType
from databao_context_engine.project.layout import ProjectLayout
from tests.utils.dummy_build_plugin import (
    DummyDefaultDatasourcePlugin,
)
from tests.utils.project_creation import given_datasource_config_file


@dataclass
class ConfigToValidate:
    host: str
    port: int


class DummyPluginWithSimpleConfig(BuildDatasourcePlugin[ConfigToValidate]):
    config_file_type = ConfigToValidate

    def supported_types(self) -> set[str]:
        return {"simple_config"}

    def check_connection(self, full_type: str, datasource_name: str, file_config: ConfigToValidate) -> None:
        if file_config.host != "localhost":
            raise ValueError("Host must be localhost")

        if file_config.port not in (1234, 5678):
            raise ValueError("Port must be 1234 or 5678")


@pytest.fixture(autouse=True)
def patch_load_plugins(mocker):
    mocker.patch(
        "databao_context_engine.datasources.check_config.load_plugins",
        return_value=load_dummy_plugins(),
    )


def load_dummy_plugins() -> dict[DatasourceType, BuildPlugin]:
    return {
        DatasourceType(full_type="simple_config"): DummyPluginWithSimpleConfig(),  # type: ignore[abstract]
        DatasourceType(full_type="dummy_default"): DummyDefaultDatasourcePlugin(),
    }


def test_check_datasource_connection_with_failing_config_validation(project_layout: ProjectLayout):
    given_datasource_config_file(project_layout, "databases/unknown", {"type": "unknown", "name": "my datasource name"})
    given_datasource_config_file(
        project_layout, "dummy/not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid2",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "invalid"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = check_datasource_connection(project_layout)

    assert {str(key): value.format(show_summary_only=True) for key, value in result.items()} == {
        "databases/unknown.yaml": "Invalid - No compatible plugin found",
        "dummy/not_implemented.yaml": "Unknown - Plugin doesn't support validating its config",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/invalid2.yaml": "Invalid - Config file is invalid",
        "dummy/invalid3.yaml": "Invalid - Connection with the datasource can not be established",
    }


def test_check_datasource_connection_with_valid_connections(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )

    result = check_datasource_connection(project_layout)

    assert result == {
        DatasourceId.from_string_repr("dummy/valid.yaml"): CheckDatasourceConnectionResult(
            datasource_id=DatasourceId.from_string_repr("dummy/valid.yaml"),
            connection_status=DatasourceConnectionStatus.VALID,
            summary=None,
        ),
    }


def test_check_datasource_connection_with_filter(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    given_datasource_config_file(
        project_layout, "dummy/not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = check_datasource_connection(
        project_layout,
        datasource_ids=[
            DatasourceId.from_string_repr("dummy/not_implemented.yaml"),
            DatasourceId.from_string_repr("dummy/invalid3.yaml"),
        ],
    )

    assert {str(key): value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/not_implemented.yaml": "Unknown - Plugin doesn't support validating its config",
        "dummy/invalid3.yaml": "Invalid - Connection with the datasource can not be established",
    }


def test_check_datasource_connection_with_single_filter(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    given_datasource_config_file(
        project_layout, "dummy/not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = check_datasource_connection(
        project_layout, datasource_ids=[DatasourceId.from_string_repr("dummy/valid.yaml")]
    )

    assert result == {
        DatasourceId.from_string_repr("dummy/valid.yaml"): CheckDatasourceConnectionResult(
            datasource_id=DatasourceId.from_string_repr("dummy/valid.yaml"),
            connection_status=DatasourceConnectionStatus.VALID,
            summary=None,
        ),
    }


def test_check_datasource_connection_with_invalid_filter(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    given_datasource_config_file(
        project_layout, "dummy/not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    with pytest.raises(ValueError):
        check_datasource_connection(
            project_layout, datasource_ids=[DatasourceId.from_string_repr("dummy/not_a_file.yaml")]
        )


def test_check_datasource_connection_with_no_type(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(project_layout, "dummy/no_type", {"name": "no_type"})

    result = check_datasource_connection(project_layout)

    assert {str(key): value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/valid.yaml": "Valid",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/no_type.yaml": "Invalid - Failed to prepare source",
    }


def test_check_datasource_connection_with_invalid_template(project_layout: ProjectLayout):
    given_datasource_config_file(
        project_layout,
        "dummy/valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    given_datasource_config_file(
        project_layout,
        "dummy/template_error",
        {"name": "{{ unexisting_function() }}", "type": "dummy_default"},
    )

    result = check_datasource_connection(project_layout)

    assert {str(key): value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/valid.yaml": "Valid",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/template_error.yaml": "Invalid - Failed to prepare source",
    }
