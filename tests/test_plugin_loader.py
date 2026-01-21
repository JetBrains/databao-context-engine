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
def patch_load_plugins(mocker):
    mocker.patch("databao_context_engine.plugins.plugin_loader.load_plugins", new=load_dummy_plugins)


def test_databao_context_plugin_loader__get_all_supported_datasource_types():
    result = DatabaoContextPluginLoader().get_all_supported_datasource_types()

    assert result == {
        DatasourceType(full_type="files/dummy"),
        DatasourceType(full_type="dummy/no_config_type"),
        DatasourceType(full_type="dummy/dummy_default"),
        DatasourceType(full_type="databases/dummy_db"),
        DatasourceType(full_type="additional/dummy_type"),
    }


def test_databao_context_plugin_loader__get_all_supported_datasource_types_exclude_file_plugins():
    result = DatabaoContextPluginLoader().get_all_supported_datasource_types(exclude_file_plugins=True)

    assert result == {
        DatasourceType(full_type="dummy/no_config_type"),
        DatasourceType(full_type="dummy/dummy_default"),
        DatasourceType(full_type="databases/dummy_db"),
        DatasourceType(full_type="additional/dummy_type"),
    }


@pytest.mark.parametrize(
    ["full_type", "plugin_type"], [("files/dummy", DummyFilePlugin), ("databases/dummy_db", DummyBuildDatasourcePlugin)]
)
def test_databao_context_plugin_loader__get_plugin_for_datasource_type(full_type, plugin_type):
    result = DatabaoContextPluginLoader().get_plugin_for_datasource_type(
        datasource_type=DatasourceType(full_type=full_type)
    )

    assert isinstance(result, plugin_type)


def test_databao_context_plugin_loader__get_plugin_for_datasource_type_unknown():
    with pytest.raises(ValueError):
        DatabaoContextPluginLoader().get_plugin_for_datasource_type(
            datasource_type=DatasourceType(full_type="unknown/unknown")
        )


@pytest.mark.parametrize(
    ["full_type", "config_file_type"],
    [("dummy/dummy_default", dict[str, Any]), ("databases/dummy_db", DummyConfigFileType)],
)
def test_databao_context_plugin_loader__get_config_file_type_for_datasource_type(full_type, config_file_type):
    result = DatabaoContextPluginLoader().get_config_file_type_for_datasource_type(
        datasource_type=DatasourceType(full_type=full_type)
    )

    assert result == config_file_type


def test_databao_context_plugin_loader__get_config_file_type_for_datasource_type__fails_for_file_plugin():
    with pytest.raises(ValueError):
        DatabaoContextPluginLoader().get_config_file_type_for_datasource_type(
            datasource_type=DatasourceType(full_type="files/dummy")
        )
