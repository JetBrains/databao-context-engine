from pathlib import Path

import pytest

from nemory.build_sources.internal.execute_plugins import execute_plugins_for_all_datasource_files
from nemory.pluginlib.build_plugin import BuildExecutionResult, BuildPlugin, EmbeddableChunk
from nemory.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin
from tests.utils.dummy_build_plugin import DummyBuildDatasourcePlugin
from tests.utils.project_creation import copy_resources_as_datasources


@pytest.fixture
def project_dir_for_execute_plugins_tests(project_path: Path) -> Path:
    copy_resources_as_datasources(
        project_path, [("databases", "dummy-db-connection.yml"), ("files", "unstructured.txt")]
    )

    return project_path


def with_dummy_plugin() -> dict[str, BuildPlugin]:
    dummy_plugin = DummyBuildDatasourcePlugin()
    return {
        # fmt: skip
        supported_type: dummy_plugin
        for supported_type in dummy_plugin.supported_types()
    }


def with_unstructured_files_plugin(
    max_tokens: int | None = None, tokens_overlap: int | None = None
) -> dict[str, BuildPlugin]:
    files_plugin = InternalUnstructuredFilesPlugin(max_tokens=max_tokens, tokens_overlap=tokens_overlap)
    return {
        # fmt: skip
        supported_type: files_plugin
        for supported_type in files_plugin.supported_types()
    }


def test_execute_plugins_for_dummy_config_file(project_dir_for_execute_plugins_tests: Path):
    test_plugins = with_dummy_plugin()

    results = execute_plugins_for_all_datasource_files(
        project_dir=project_dir_for_execute_plugins_tests,
        plugins_per_type=test_plugins,
    )

    assert len(results) == 1
    (dummy_plugin_result, plugin) = results[0]
    _assert_dummy_plugin_result(dummy_plugin_result)

    chunks = next(iter(test_plugins.values())).divide_result_into_chunks(dummy_plugin_result)
    assert len(chunks) == 2


def _assert_dummy_plugin_result(dummy_plugin_result: BuildExecutionResult):
    assert dummy_plugin_result.name == "my connection"
    assert dummy_plugin_result.type == "databases/dummy_db"
    assert len(dummy_plugin_result.result["catalogs"][0]["schemas"][0]["tables"]) == 2


def test_execute_plugins_with_unstructured_files_plugin(project_dir_for_execute_plugins_tests: Path):
    test_plugins = with_unstructured_files_plugin(3, 1)

    results = execute_plugins_for_all_datasource_files(
        project_dir=project_dir_for_execute_plugins_tests,
        plugins_per_type=test_plugins,
    )

    assert len(results) == 1
    (plugin_result, plugin) = results[0]
    _assert_unstructured_files_plugin_result(plugin_result)

    chunks = next(iter(test_plugins.values())).divide_result_into_chunks(plugin_result)
    assert len(chunks) == 3
    assert chunks == [
        EmbeddableChunk(embeddable_text="My text file", content={"chunk_content": "My text file", "chunk_index": 0}),
        EmbeddableChunk(embeddable_text="file with a", content={"chunk_content": "file with a", "chunk_index": 2}),
        EmbeddableChunk(embeddable_text="a few words", content={"chunk_content": "a few words", "chunk_index": 4}),
    ]


def _assert_unstructured_files_plugin_result(plugin_result: BuildExecutionResult):
    assert plugin_result.name == "unstructured.txt"
    assert plugin_result.type == "files/txt"
    assert len(plugin_result.result["chunks"]) == 3


def test_execute_plugins_with_unstructured_files_plugin_and_dummy_config_file_plugin(
    project_dir_for_execute_plugins_tests: Path,
):
    test_plugins = with_dummy_plugin()
    test_plugins.update(with_unstructured_files_plugin(3, 1))

    results = execute_plugins_for_all_datasource_files(
        project_dir=project_dir_for_execute_plugins_tests,
        plugins_per_type=test_plugins,
    )

    assert len(results) == 2

    dummy_plugin_result = next(
        ((result, plugin) for (result, plugin) in results if isinstance(plugin, DummyBuildDatasourcePlugin)), None
    )
    assert dummy_plugin_result is not None
    _assert_dummy_plugin_result(dummy_plugin_result[0])

    unstructured_files_plugin_result = next(
        ((result, plugin) for (result, plugin) in results if isinstance(plugin, InternalUnstructuredFilesPlugin)), None
    )
    assert unstructured_files_plugin_result is not None
    _assert_unstructured_files_plugin_result(unstructured_files_plugin_result[0])
