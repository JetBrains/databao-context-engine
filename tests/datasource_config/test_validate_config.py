from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from nemory.datasource_config.validate_config import ValidationResult, ValidationStatus, _validate_datasource_config
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, BuildPlugin, DatasourceType
from nemory.project.layout import create_datasource_config_file
from nemory.serialisation.yaml import to_yaml_string
from tests.utils.dummy_build_plugin import (
    DummyDefaultDatasourcePlugin,
)


@dataclass
class ConfigToValidate:
    host: str
    port: int


class DummyPluginWithSimpleConfig(BuildDatasourcePlugin[ConfigToValidate]):
    config_file_type = ConfigToValidate

    def supported_types(self) -> set[str]:
        return {"dummy/simple_config"}

    def check_connection(self, full_type: str, datasource_name: str, file_config: ConfigToValidate) -> None:
        if file_config.host != "localhost":
            raise ValueError("Host must be localhost")

        if file_config.port not in (1234, 5678):
            raise ValueError("Port must be 1234 or 5678")


@pytest.fixture(autouse=True)
def patch_load_plugins(mocker):
    mocker.patch(
        "nemory.datasource_config.validate_config.load_plugins",
        return_value=load_dummy_plugins(),
    )


def load_dummy_plugins() -> dict[DatasourceType, BuildPlugin]:
    return {
        DatasourceType(full_type="dummy/simple_config"): DummyPluginWithSimpleConfig(),  # type: ignore[abstract]
        DatasourceType(full_type="dummy/dummy_default"): DummyDefaultDatasourcePlugin(),
    }


def with_config_file(project_dir: Path, full_type: str, datasource_name: str, config_content: dict[str, Any]):
    create_datasource_config_file(
        project_dir=project_dir,
        config_folder_name=full_type.split("/")[0],
        datasource_name=datasource_name,
        config_content=to_yaml_string(config_content),
    )


def test_validate_datasource_config_with_failing_config_validation(project_path: Path):
    with_config_file(project_path, "databases/unknown", "unknown", {"type": "unknown", "name": "my datasource name"})
    with_config_file(
        project_path, "dummy/dummy_default", "not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid2",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "invalid"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = _validate_datasource_config(project_path)

    assert {key: value.format(show_summary_only=True) for key, value in result.items()} == {
        "databases/unknown.yaml": "Invalid - No compatible plugin found",
        "dummy/not_implemented.yaml": "Unknown - Plugin doesn't support validating its config",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/invalid2.yaml": "Invalid - Config file is invalid",
        "dummy/invalid3.yaml": "Invalid - Connection with the datasource can not be established",
    }


def test_validate_datasource_config_with_valid_connections(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )

    result = _validate_datasource_config(project_path)

    assert result == {
        "dummy/valid.yaml": ValidationResult(validation_status=ValidationStatus.VALID, summary=None),
    }


def test_validate_datasource_config_with_filter(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    with_config_file(
        project_path, "dummy/dummy_default", "not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = _validate_datasource_config(
        project_path, datasource_config_files=["dummy/not_implemented.yaml", "dummy/invalid3.yaml"]
    )

    assert {key: value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/not_implemented.yaml": "Unknown - Plugin doesn't support validating its config",
        "dummy/invalid3.yaml": "Invalid - Connection with the datasource can not be established",
    }


def test_validate_datasource_config_with_single_filter(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    with_config_file(
        project_path, "dummy/dummy_default", "not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    result = _validate_datasource_config(project_path, datasource_config_files=["dummy/valid.yaml"])

    assert result == {
        "dummy/valid.yaml": ValidationResult(validation_status=ValidationStatus.VALID, summary=None),
    }


def test_validate_datasource_config_with_invalid_filter(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    with_config_file(
        project_path, "dummy/dummy_default", "not_implemented", {"type": "dummy_default", "name": "my datasource name"}
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid3",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "9876"},
    )

    with pytest.raises(ValueError):
        _validate_datasource_config(project_path, datasource_config_files=["dummy/not_a_file.yaml"])


def test_validate_datasource_config_with_no_type(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(project_path, "dummy/dummy_default", "no_type", {"name": "no_type"})

    result = _validate_datasource_config(project_path)

    assert {key: value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/valid.yaml": "Valid",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/no_type.yaml": "Invalid - Failed to prepare source",
    }


def test_validate_datasource_config_with_invalid_template(project_path: Path):
    with_config_file(
        project_path,
        "dummy/simple_config",
        "valid",
        {"type": "simple_config", "name": "my datasource name", "host": "localhost", "port": "1234"},
    )
    with_config_file(
        project_path,
        "dummy/simple_config",
        "invalid",
        {"type": "simple_config", "name": "my datasource name"},
    )
    with_config_file(
        project_path,
        "dummy/dummy_default",
        "template_error",
        {"name": "{{ unexisting_function() }}", "type": "dummy_default"},
    )

    result = _validate_datasource_config(project_path)

    assert {key: value.format(show_summary_only=True) for key, value in result.items()} == {
        "dummy/valid.yaml": "Valid",
        "dummy/invalid.yaml": "Invalid - Config file is invalid",
        "dummy/template_error.yaml": "Invalid - Failed to prepare source",
    }
