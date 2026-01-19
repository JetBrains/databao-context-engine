import os
from pathlib import Path

import pytest
from click import Abort
from click.testing import CliRunner

from databao_context_engine.cli.add_datasource_config import add_datasource_config_interactive
from databao_context_engine.project.layout import get_source_dir
from databao_context_engine.serialisation.yaml import to_yaml_string
from tests.utils.dummy_build_plugin import load_dummy_plugins
from tests.utils.project_creation import with_config_file


@pytest.fixture(autouse=True)
def patch_load_plugins(mocker):
    mocker.patch("databao_context_engine.plugins.plugin_loader.load_plugins", new=load_dummy_plugins)


def test_add_datasource_config__with_no_custom_properties(project_path: Path):
    cli_runner = CliRunner()

    inputs = ["dummy", "dummy_default", "my datasource name"]

    with cli_runner.isolation(input=os.linesep.join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("dummy").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string({"type": "dummy_default", "name": "my datasource name"})


def test_add_datasource_config__with_all_values_filled(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "databases",
        "dummy_db",
        "my datasource name",
        "15.356",
        "property_with_default",
        "nested_field",
        "other_nested_property",
        "87654",
    ]

    with cli_runner.isolation(input=os.linesep.join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("databases").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "dummy_db",
            "name": "my datasource name",
            "other_property": "15.356",
            "property_with_default": "property_with_default",
            "nested_dict": {
                "nested_field": "nested_field",
                "other_nested_property": "other_nested_property",
                "optional_with_default": "87654",
            },
        }
    )


def test_add_datasource_config__with_partial_values_filled(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "databases",
        "dummy_db",
        "my datasource name",
        "3.14",
        "",
        "nested_field",
        "5",
        "\n",  # TextIOWrapper hack: For some reason, having two \n at the end of the input is considered the end of the file. Adding a third one make sure that the last property will actually be read as an empty string
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("databases").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "dummy_db",
            "name": "my datasource name",
            "other_property": "3.14",
            "property_with_default": "default_value",
            "nested_dict": {
                "nested_field": "nested_field",
                "other_nested_property": "5",
                "optional_with_default": "1111",
            },
        }
    )


def test_add_datasource_config__with_custom_property_list(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "dummy",
        "no_config_type",
        "my datasource name",
        "3.14",
        "value",
        "nested_field",
        "other_nested_property",
        "\n",  # TextIOWrapper hack: For some reason, having two \n at the end of the input is considered the end of the file. Adding a third one make sure that the last property will actually be read as an empty string
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("dummy").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "no_config_type",
            "name": "my datasource name",
            "float_property": "3.14",
            "nested_with_only_optionals": {
                "optional_field": "value",
                "nested_field": "nested_field",
            },
            "nested_dict": {
                "other_nested_property": "other_nested_property",
                "optional_with_default": "1111",
            },
        }
    )


def test_add_datasource_config__with_custom_property_list_and_optionals(project_path: Path):
    cli_runner = CliRunner()

    inputs = [
        "dummy",
        "no_config_type",
        "my datasource name",
        "3.14",
        "",
        "",
        "",
        "\n",  # TextIOWrapper hack: For some reason, having two \n at the end of the input is considered the end of the file. Adding a third one make sure that the last property will actually be read as an empty string
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("dummy").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "no_config_type",
            "name": "my datasource name",
            "float_property": "3.14",
            "nested_dict": {
                "optional_with_default": "1111",
            },
        }
    )


def test_add_datasource_config__abort_if_existing_config_and_no_overwrite(project_path: Path):
    with_config_file(
        project_path,
        "dummy/no_config_type",
        "my datasource name",
        {"type": "no_config_type", "name": "my datasource name", "old_attribute": "old_value"},
    )

    cli_runner = CliRunner()

    inputs = [
        "dummy",
        "no_config_type",
        "my datasource name",
        "n",
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        with pytest.raises(Abort):
            add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("dummy").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {"type": "no_config_type", "name": "my datasource name", "old_attribute": "old_value"}
    )


def test_add_datasource_config__overwrite_existing_config(project_path: Path):
    with_config_file(
        project_path,
        "dummy/no_config_type",
        "my datasource name",
        {"type": "no_config_type", "name": "my datasource name", "old_attribute": "old_value"},
    )

    cli_runner = CliRunner()

    inputs = [
        "dummy",
        "no_config_type",
        "my datasource name",
        "y",
        "3.14",
        "",
        "",
        "",
        "\n",  # TextIOWrapper hack: For some reason, having two \n at the end of the input is considered the end of the file. Adding a third one make sure that the last property will actually be read as an empty string
    ]

    with cli_runner.isolation(input="\n".join(inputs)):
        add_datasource_config_interactive(project_path)

    result_config_file = get_source_dir(project_path).joinpath("dummy").joinpath("my datasource name.yaml")
    assert result_config_file.is_file()
    assert result_config_file.read_text() == to_yaml_string(
        {
            "type": "no_config_type",
            "name": "my datasource name",
            "float_property": "3.14",
            "nested_dict": {
                "optional_with_default": "1111",
            },
        }
    )
