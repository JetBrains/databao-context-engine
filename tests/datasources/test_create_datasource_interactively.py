from pathlib import Path

import pytest

from databao_context_engine import (
    DatabaoContextPluginLoader,
    DatabaoContextProjectManager,
    DatasourceType,
)
from databao_context_engine.project.layout import ProjectLayout
from tests.utils.config_wizard import MockUserInputCallback
from tests.utils.dummy_build_plugin import load_dummy_plugins
from tests.utils.project_creation import given_datasource_config_file


@pytest.fixture
def project_manager(project_path: Path) -> DatabaoContextProjectManager:
    plugin_loader = DatabaoContextPluginLoader(plugins_by_type=load_dummy_plugins())
    return DatabaoContextProjectManager(project_dir=project_path, plugin_loader=plugin_loader)


def test_add_datasource_config__with_no_custom_properties(project_manager):
    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="dummy_default"),
        datasource_name="my datasource name",
        user_input_callback=MockUserInputCallback(inputs=[]),
    )
    assert configured_datasource.config == {"type": "dummy_default", "name": "my datasource name"}


def test_add_datasource_config__with_all_values_filled(project_manager):
    inputs = [
        "15.356",  # other_property
        "property_with_default",  # property_with_default
        True,  # confirm nested_dict
        "nested_field",  # nested_field
        "other_nested_property",  # other_nested_property
        "87654",  # optional_with_default
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="dummy_db"),
        datasource_name="databases/my datasource name",
        user_input_callback=user_input_callback,
        validate_config_content=False,
    )

    assert configured_datasource.config == {
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


def test_add_datasource_config__with_partial_values_filled(project_manager):
    inputs = [
        "3.14",  # other_property
        "",  # property_with_default (will use default: "default_value")
        True,  # confirm nested_dict
        "nested_field",  # nested_field
        "5",  # other_nested_property
        "",  # optional_with_default (will use default: "1111")
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="dummy_db"),
        datasource_name="databases/my datasource name",
        user_input_callback=user_input_callback,
        validate_config_content=False,
    )

    assert configured_datasource.config == {
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


def test_add_datasource_config__with_custom_property_list(project_manager):
    inputs = [
        "3.14",  # float_property
        True,  # confirm nested_with_only_optionals
        "value",  # optional_field
        "nested_field",  # nested_field
        "other_nested_property",  # nested_dict.other_nested_property
        "",  # nested_dict.optional_with_default (use default)
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="no_config_type"),
        datasource_name="dummy/my datasource name",
        user_input_callback=user_input_callback,
    )

    assert configured_datasource.config == {
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


def test_add_datasource_config__with_custom_property_list_and_optionals(project_manager):
    inputs = [
        "3.14",  # float_property
        False,  # skip nested_with_only_optionals
        "",  # nested_dict.other_nested_property (skip)
        "",  # nested_dict.optional_with_default (use default)
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="no_config_type"),
        datasource_name="dummy/my datasource name",
        user_input_callback=user_input_callback,
    )

    assert configured_datasource.config == {
        "type": "no_config_type",
        "name": "my datasource name",
        "float_property": "3.14",
        "nested_dict": {
            "optional_with_default": "1111",
        },
    }


def test_add_datasource_config__overwrite_existing_config(project_layout: ProjectLayout, project_manager):
    given_datasource_config_file(
        project_layout,
        "dummy/my datasource name",
        {"type": "no_config_type", "name": "my datasource name", "old_attribute": "old_value"},
    )

    inputs = [
        "3.14",  # float_property
        False,  # skip nested_with_only_optionals
        "",  # nested_dict.other_nested_property (skip)
        "",  # nested_dict.optional_with_default (use default)
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="no_config_type"),
        datasource_name="dummy/my datasource name",
        user_input_callback=user_input_callback,
        overwrite_existing=True,
    )

    assert configured_datasource.config == {
        "type": "no_config_type",
        "name": "my datasource name",
        "float_property": "3.14",
        "nested_dict": {
            "optional_with_default": "1111",
        },
    }
