import pytest

from databao_context_engine.plugins.plugin_loader import load_plugins


@pytest.mark.recommended_extras
def test_default_plugins_loaded():
    """
    This test should be run with default set of extras.
    See Makefile
    """
    plugins = load_plugins()
    assert {
        "jetbrains/duckdb",
        "jetbrains/mysql",
        "jetbrains/postgres",
        "jetbrains/parquet",
        "jetbrains/unstructured_files",
    } == {plugin.id for plugin in plugins.values()}
