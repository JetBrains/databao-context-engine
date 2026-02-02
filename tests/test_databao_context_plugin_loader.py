from typing import Any

import pytest

from databao_context_engine import DatabaoContextPluginLoader, DatasourceType
from tests.utils.dummy_build_plugin import (
    DummyBuildDatasourcePlugin,
    DummyConfigFileType,
    DummyFilePlugin,
    load_dummy_plugins,
)


@pytest.fixture(autouse=True)
def plugin_loader_under_test():
    return DatabaoContextPluginLoader(plugins_by_type=load_dummy_plugins())


def test_databao_context_plugin_loader__get_all_supported_datasource_types(plugin_loader_under_test):
    result = plugin_loader_under_test.get_all_supported_datasource_types()

    assert result == {
        DatasourceType(full_type="dummy_txt"),
        DatasourceType(full_type="additional_dummy_type"),
        DatasourceType(full_type="no_config_type"),
        DatasourceType(full_type="dummy_db"),
        DatasourceType(full_type="dummy_default"),
    }


def test_databao_context_plugin_loader__get_all_supported_datasource_types_exclude_file_plugins(
    plugin_loader_under_test,
):
    result = plugin_loader_under_test.get_all_supported_datasource_types(exclude_file_plugins=True)

    assert result == {
        DatasourceType(full_type="no_config_type"),
        DatasourceType(full_type="dummy_default"),
        DatasourceType(full_type="dummy_db"),
        DatasourceType(full_type="additional_dummy_type"),
    }


@pytest.mark.parametrize(
    ["full_type", "plugin_type"], [("dummy_txt", DummyFilePlugin), ("dummy_db", DummyBuildDatasourcePlugin)]
)
def test_databao_context_plugin_loader__get_plugin_for_datasource_type(
    plugin_loader_under_test, full_type, plugin_type
):
    result = plugin_loader_under_test.get_plugin_for_datasource_type(
        datasource_type=DatasourceType(full_type=full_type)
    )

    assert isinstance(result, plugin_type)


def test_databao_context_plugin_loader__get_plugin_for_datasource_type_unknown(plugin_loader_under_test):
    with pytest.raises(ValueError):
        plugin_loader_under_test.get_plugin_for_datasource_type(
            datasource_type=DatasourceType(full_type="unknown/unknown")
        )


@pytest.mark.parametrize(
    ["full_type", "config_file_type"],
    [("dummy_default", dict[str, Any]), ("dummy_db", DummyConfigFileType)],
)
def test_databao_context_plugin_loader__get_config_file_type_for_datasource_type(
    plugin_loader_under_test, full_type, config_file_type
):
    result = plugin_loader_under_test.get_config_file_type_for_datasource_type(
        datasource_type=DatasourceType(full_type=full_type)
    )

    assert result == config_file_type


def test_databao_context_plugin_loader__get_config_file_type_for_datasource_type__fails_for_file_plugin(
    plugin_loader_under_test,
):
    with pytest.raises(ValueError):
        plugin_loader_under_test.get_config_file_type_for_datasource_type(
            datasource_type=DatasourceType(full_type="files/dummy")
        )
