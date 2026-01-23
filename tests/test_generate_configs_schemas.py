import pytest

from databao_context_engine import BuildPlugin, DatasourceType
from databao_context_engine.generate_configs_schemas import _generate_json_schema_output_for_plugins
from tests.utils.dummy_build_plugin import (
    AdditionalDummyPlugin,
    DummyBuildDatasourcePlugin,
    DummyDefaultDatasourcePlugin,
    DummyPluginWithNoConfigType,
    load_dummy_plugins,
)


def _patch_load_plugins(mocker, return_value: dict[DatasourceType, BuildPlugin] | None = None):
    if return_value is None:
        return_value = load_dummy_plugins(exclude_file_plugins=True)

    mocker.patch("databao_context_engine.generate_configs_schemas.load_plugins", return_value=return_value)


def test_generate_configs_schemas__all(mocker):
    _patch_load_plugins(mocker)

    results = _generate_json_schema_output_for_plugins(tuple(), None)

    assert len(results) == 2
    assert next((result for result in results if DummyBuildDatasourcePlugin.id in result), None) is not None
    assert next((result for result in results if AdditionalDummyPlugin.id in result), None) is not None


def test_generate_configs_schemas__with_both_include_and_exclude(mocker):
    _patch_load_plugins(mocker)

    with pytest.raises(ValueError) as e:
        _generate_json_schema_output_for_plugins(("id1",), ("id2",))

    assert str(e.value) == "Can't use --include-plugins and --exclude-plugins together"


def test_generate_configs_schemas__with_include(mocker):
    _patch_load_plugins(mocker)

    results = _generate_json_schema_output_for_plugins((DummyBuildDatasourcePlugin.id,), tuple())

    assert len(results) == 1
    assert next((result for result in results if DummyBuildDatasourcePlugin.id in result), None) is not None


def test_generate_configs_schemas__with_all_inluded(mocker):
    _patch_load_plugins(mocker)

    results = _generate_json_schema_output_for_plugins(
        (DummyBuildDatasourcePlugin.id, AdditionalDummyPlugin.id), tuple()
    )

    assert len(results) == 2
    assert next((result for result in results if DummyBuildDatasourcePlugin.id in result), None) is not None
    assert next((result for result in results if AdditionalDummyPlugin.id in result), None) is not None


def test_generate_configs_schemas__with_unknown_include(mocker):
    _patch_load_plugins(mocker)

    with pytest.raises(ValueError) as e:
        _generate_json_schema_output_for_plugins(("unknown_id",), tuple())

    assert "No plugin found with id in" in str(e.value)


def test_generate_configs_schemas__with_exclude(mocker):
    _patch_load_plugins(mocker)

    results = _generate_json_schema_output_for_plugins(tuple(), (DummyBuildDatasourcePlugin.id,))

    assert len(results) == 1
    assert next((result for result in results if AdditionalDummyPlugin.id in result), None) is not None


def test_generate_configs_schemas__with_all_excluded(mocker):
    _patch_load_plugins(mocker)

    with pytest.raises(ValueError) as e:
        _generate_json_schema_output_for_plugins(
            tuple(),
            (
                DummyBuildDatasourcePlugin.id,
                AdditionalDummyPlugin.id,
                DummyDefaultDatasourcePlugin.id,
                DummyPluginWithNoConfigType.id,
            ),
        )

    assert "No plugin found when excluding" in str(e.value)
