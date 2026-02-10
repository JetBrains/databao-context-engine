from datetime import datetime
from pathlib import Path

import pytest

from databao_context_engine import (
    BuildContextResult,
    ChunkEmbeddingMode,
    DatabaoContextProjectManager,
    Datasource,
    DatasourceContext,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.project.layout import get_output_dir
from tests.utils.dummy_build_plugin import load_dummy_plugins
from tests.utils.project_creation import (
    given_datasource_config_file,
    given_raw_source_file,
)


@pytest.fixture(autouse=True)
def patch_load_plugins(mocker):
    mocker.patch("databao_context_engine.build_sources.build_runner.load_plugins", new=load_dummy_plugins)


@pytest.fixture(autouse=True)
def use_test_db(create_db):
    pass


def test_databao_engine__get_datasource_list_with_no_datasources(project_path):
    datasource_list = DatabaoContextProjectManager(project_dir=project_path).get_configured_datasource_list()

    assert datasource_list == []


def test_databao_engine__get_datasource_list_with_multiple_datasources(project_path):
    project_manager = DatabaoContextProjectManager(project_dir=project_path)
    given_datasource_config_file(
        project_manager._project_layout,
        datasource_name="full/a",
        config_content={"type": "any", "name": "a"},
    )
    given_datasource_config_file(
        project_manager._project_layout,
        datasource_name="other/b",
        config_content={"type": "type", "name": "b"},
    )
    given_datasource_config_file(
        project_manager._project_layout,
        datasource_name="full/c",
        config_content={"type": "type2", "name": "c"},
    )

    datasource_list = project_manager.get_configured_datasource_list()

    assert datasource_list == [
        Datasource(id=DatasourceId.from_string_repr("full/a.yaml"), type=DatasourceType(full_type="any")),
        Datasource(id=DatasourceId.from_string_repr("full/c.yaml"), type=DatasourceType(full_type="type2")),
        Datasource(id=DatasourceId.from_string_repr("other/b.yaml"), type=DatasourceType(full_type="type")),
    ]


def test_databao_context_project_manager__build_with_no_datasource(project_path):
    project_manager = DatabaoContextProjectManager(project_dir=project_path)

    result = project_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert result == []


def test_databao_context_project_manager__build_with_multiple_datasource(project_path, create_db):
    project_manager = DatabaoContextProjectManager(project_dir=project_path)

    given_datasource_config_file(
        project_manager._project_layout,
        datasource_name="dummy/my_dummy_data",
        config_content={"type": "dummy_default", "name": "my_dummy_data"},
    )
    given_raw_source_file(
        project_dir=project_manager.project_dir,
        file_name="files/my_dummy_file.dummy_txt",
        file_content="Content of my dummy file",
    )

    result = project_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert len(result) == 2, str(result)
    assert_build_context_result(
        result[0],
        project_manager.project_dir,
        datasource_id=DatasourceId.from_string_repr("dummy/my_dummy_data.yaml"),
        datasource_type=DatasourceType(full_type="dummy_default"),
        context_file_relative_path="dummy/my_dummy_data.yaml",
    )

    assert_build_context_result(
        result[1],
        project_manager.project_dir,
        datasource_id=DatasourceId.from_string_repr("files/my_dummy_file.dummy_txt"),
        datasource_type=DatasourceType(full_type="dummy_txt"),
        context_file_relative_path="files/my_dummy_file.dummy_txt.yaml",
    )


def test_databao_context_project_manager__index_built_contexts_indexes_all_when_no_ids(project_path, mocker):
    pm = DatabaoContextProjectManager(project_dir=project_path)

    c1 = DatasourceContext(DatasourceId.from_string_repr("full/a.yaml"), context="A")
    c2 = DatasourceContext(DatasourceId.from_string_repr("other/b.yaml"), context="B")

    engine = mocker.Mock()
    engine.get_all_contexts.return_value = [c1, c2]
    mocker.patch.object(pm, "get_engine_for_project", return_value=engine)

    index_fn = mocker.patch(
        "databao_context_engine.databao_context_project_manager.index_built_contexts",
        autospec=True,
        return_value="OK",
    )

    result = pm.index_built_contexts(datasource_ids=None)

    assert result == "OK"
    index_fn.assert_called_once_with(
        project_layout=pm._project_layout,
        contexts=[c1, c2],
        chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
        progress=None,
    )


def test_databao_context_project_manager__index_built_contexts_filters_by_datasource_path(project_path, mocker):
    pm = DatabaoContextProjectManager(project_dir=project_path)

    c1 = DatasourceContext(DatasourceId.from_string_repr("full/a.yaml"), context="A")
    c2 = DatasourceContext(DatasourceId.from_string_repr("other/b.yaml"), context="B")
    c3 = DatasourceContext(DatasourceId.from_string_repr("full/c.yaml"), context="C")

    engine = mocker.Mock()
    engine.get_all_contexts.return_value = [c1, c2, c3]
    mocker.patch.object(pm, "get_engine_for_project", return_value=engine)

    index_fn = mocker.patch(
        "databao_context_engine.databao_context_project_manager.index_built_contexts",
        autospec=True,
        return_value="OK",
    )

    wanted = [
        DatasourceId.from_string_repr("full/a.yaml"),
        DatasourceId.from_string_repr("full/c.yaml"),
    ]

    result = pm.index_built_contexts(datasource_ids=wanted)

    assert result == "OK"
    index_fn.assert_called_once_with(
        project_layout=pm._project_layout,
        contexts=[c1, c3],
        chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
        progress=None,
    )


def assert_build_context_result(
    context_result: BuildContextResult,
    project_dir: Path,
    *,
    datasource_id: DatasourceId,
    datasource_type: DatasourceType,
    context_file_relative_path: str,
):
    assert context_result.datasource_id == datasource_id
    assert context_result.datasource_type == datasource_type
    assert context_result.context_built_at < datetime.now()
    assert str(context_result.context_file_path).endswith(context_file_relative_path)
    assert context_result.context_file_path.is_relative_to(get_output_dir(project_dir))
