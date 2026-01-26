from datetime import datetime
from pathlib import Path

import pytest

from databao_context_engine import (
    BuildContextResult,
    ChunkEmbeddingMode,
    DatabaoContextProjectManager,
    Datasource,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.project.layout import get_output_dir
from tests.utils.dummy_build_plugin import load_dummy_plugins
from tests.utils.project_creation import with_config_file, with_raw_source_file


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
    databao_context_engine = DatabaoContextProjectManager(project_dir=project_path)
    with_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="full/any",
        datasource_name="a",
        config_content={"type": "any", "name": "a"},
    )
    with_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="other/type",
        datasource_name="b",
        config_content={"type": "type", "name": "b"},
    )
    with_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="full/type2",
        datasource_name="c",
        config_content={"type": "type2", "name": "c"},
    )

    datasource_list = databao_context_engine.get_configured_datasource_list()

    assert datasource_list == [
        Datasource(id=DatasourceId.from_string_repr("full/a.yaml"), type=DatasourceType(full_type="full/any")),
        Datasource(id=DatasourceId.from_string_repr("full/c.yaml"), type=DatasourceType(full_type="full/type2")),
        Datasource(id=DatasourceId.from_string_repr("other/b.yaml"), type=DatasourceType(full_type="other/type")),
    ]


def test_databao_context_project_manager__build_with_no_datasource(project_path):
    project_manager = DatabaoContextProjectManager(project_dir=project_path)

    result = project_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert result == []


def test_databao_context_project_manager__build_with_multiple_datasource(project_path, create_db):
    project_manager = DatabaoContextProjectManager(project_dir=project_path)

    with_config_file(
        project_dir=project_manager.project_dir,
        full_type="dummy/dummy_default",
        datasource_name="my_dummy_data",
        config_content={"type": "dummy_default", "name": "my_dummy_data"},
    )
    with_raw_source_file(
        project_dir=project_manager.project_dir,
        file_name="my_dummy_file",
        datasource_type=DatasourceType(full_type="files/dummy"),
        file_content="Content of my dummy file",
    )

    result = project_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert len(result) == 2
    assert_build_context_result(
        result[0],
        project_manager.project_dir,
        datasource_id=DatasourceId.from_string_repr("dummy/my_dummy_data.yaml"),
        datasource_type=DatasourceType(full_type="dummy/dummy_default"),
        context_file_relative_path="dummy/my_dummy_data.yaml",
    )

    assert_build_context_result(
        result[1],
        project_manager.project_dir,
        datasource_id=DatasourceId.from_string_repr("files/my_dummy_file.dummy"),
        datasource_type=DatasourceType(full_type="files/dummy"),
        context_file_relative_path="files/my_dummy_file.dummy.yaml",
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
