from pathlib import Path

from nemory.features.build_sources.internal.build_sources_from_plugins import (
    _execute_plugins_for_all_config_files,
)
from nemory.features.build_sources.plugin_lib.build_plugin import BuildPlugin


def with_dummy_plugin() -> dict[str, BuildPlugin]:
    from nemory.features.build_sources.internal.tests.data.dummy_build_plugin import (
        DummyBuildPlugin,
    )

    dummy_plugin = DummyBuildPlugin()
    return {
        # fmt: skip
        supported_type: dummy_plugin
        for supported_type in dummy_plugin.supported_types()
    }


def test_execute_plugins_for_all_config_files():
    test_plugins = with_dummy_plugin()

    results = _execute_plugins_for_all_config_files(
        project_dir=Path(__file__).parent.joinpath("data"),
        plugins_per_type=test_plugins,
    )

    assert len(results) == 1
    dummy_plugin_result = results[0]
    assert dummy_plugin_result.name == "my connection"
    assert dummy_plugin_result.type == "databases/dummy_db"
    assert len(dummy_plugin_result.result["catalogs"][0]["schemas"][0]["tables"]) == 2

    chunks = next(iter(test_plugins.values())).divide_result_into_chunks(dummy_plugin_result)
    assert len(chunks) == 2
