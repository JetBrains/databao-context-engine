from datetime import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from databao_context_engine import (
    BuildDatasourceResult,
    ChunkEmbeddingMode,
    ConfiguredDatasource,
    DatabaoContextDomainManager,
    DatabaoContextPluginLoader,
    Datasource,
    DatasourceConnectionStatus,
    DatasourceContext,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.project.layout import get_output_dir
from tests.utils.dummy_build_plugin import SimplePydanticConfig, load_dummy_plugins
from tests.utils.project_creation import (
    given_datasource_config_file,
    given_raw_source_file,
)


@pytest.fixture
def domain_manager(project_path: Path) -> DatabaoContextDomainManager:
    return DatabaoContextDomainManager(
        domain_dir=project_path, plugin_loader=DatabaoContextPluginLoader(plugins_by_type=load_dummy_plugins())
    )


@pytest.fixture(autouse=True)
def use_test_db(create_db):
    pass


def test_databao_engine__get_datasource_list_with_no_datasources(domain_manager):
    datasource_list = domain_manager.get_configured_datasource_list()

    assert datasource_list == []


def test_databao_engine__get_datasource_list_with_multiple_datasources(domain_manager):
    given_datasource_config_file(
        domain_manager._project_layout,
        datasource_name="full/a",
        config_content={"type": "any", "name": "a"},
    )
    given_datasource_config_file(
        domain_manager._project_layout,
        datasource_name="other/b",
        config_content={"type": "type", "name": "b"},
    )
    given_datasource_config_file(
        domain_manager._project_layout,
        datasource_name="full/c",
        config_content={"type": "type2", "name": "c"},
    )

    datasource_list = domain_manager.get_configured_datasource_list()

    assert datasource_list == [
        ConfiguredDatasource(
            datasource=Datasource(
                id=DatasourceId.from_string_repr("full/a.yaml"), type=DatasourceType(full_type="any")
            ),
            config={"type": "any", "name": "a"},
        ),
        ConfiguredDatasource(
            datasource=Datasource(
                id=DatasourceId.from_string_repr("full/c.yaml"), type=DatasourceType(full_type="type2")
            ),
            config={"type": "type2", "name": "c"},
        ),
        ConfiguredDatasource(
            datasource=Datasource(
                id=DatasourceId.from_string_repr("other/b.yaml"), type=DatasourceType(full_type="type")
            ),
            config={"type": "type", "name": "b"},
        ),
    ]


def test_databao_context_domain_manager__build_with_no_datasource(domain_manager):
    result = domain_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert result == []


def test_databao_context_domain_manager__build_with_multiple_datasource(domain_manager, create_db):
    given_datasource_config_file(
        domain_manager._project_layout,
        datasource_name="dummy/my_dummy_data",
        config_content={"type": "dummy_default", "name": "my_dummy_data"},
    )
    given_raw_source_file(
        project_dir=domain_manager.domain_dir,
        file_name="files/my_dummy_file.dummy_txt",
        file_content="Content of my dummy file",
    )

    result = domain_manager.build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert len(result) == 2, str(result)
    assert_build_context_result(
        result[0],
        domain_manager.domain_dir,
        datasource_id=DatasourceId.from_string_repr("dummy/my_dummy_data.yaml"),
        datasource_type=DatasourceType(full_type="dummy_default"),
        context_file_relative_path="dummy/my_dummy_data.yaml",
    )

    assert_build_context_result(
        result[1],
        domain_manager.domain_dir,
        datasource_id=DatasourceId.from_string_repr("files/my_dummy_file.dummy_txt"),
        datasource_type=DatasourceType(full_type="dummy_txt"),
        context_file_relative_path="files/my_dummy_file.dummy_txt.yaml",
    )


def test_databao_context_domain_manager__index_built_contexts_indexes_all_when_no_ids(domain_manager, mocker):
    c1 = DatasourceContext(DatasourceId.from_string_repr("full/a.yaml"), context="A")
    c2 = DatasourceContext(DatasourceId.from_string_repr("other/b.yaml"), context="B")

    engine = mocker.Mock()
    engine.get_all_contexts.return_value = [c1, c2]
    mocker.patch.object(domain_manager, "get_engine_for_domain", return_value=engine)

    index_fn = mocker.patch(
        "databao_context_engine.databao_context_domain_manager.index_built_contexts",
        autospec=True,
        return_value="OK",
    )

    result = domain_manager.index_built_contexts(datasource_ids=None)

    assert result == "OK"
    index_fn.assert_called_once_with(
        project_layout=domain_manager._project_layout,
        plugin_loader=domain_manager._plugin_loader,
        contexts=[c1, c2],
        chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
        ollama_model_id=None,
        ollama_model_dim=None,
    )


def test_databao_context_domain_manager__index_built_contexts_filters_by_datasource_path(domain_manager, mocker):
    c1 = DatasourceContext(DatasourceId.from_string_repr("full/a.yaml"), context="A")
    c2 = DatasourceContext(DatasourceId.from_string_repr("other/b.yaml"), context="B")
    c3 = DatasourceContext(DatasourceId.from_string_repr("full/c.yaml"), context="C")

    engine = mocker.Mock()
    engine.get_all_contexts.return_value = [c1, c2, c3]
    mocker.patch.object(domain_manager, "get_engine_for_domain", return_value=engine)

    index_fn = mocker.patch(
        "databao_context_engine.databao_context_domain_manager.index_built_contexts",
        autospec=True,
        return_value="OK",
    )

    wanted = [
        DatasourceId.from_string_repr("full/a.yaml"),
        DatasourceId.from_string_repr("full/c.yaml"),
    ]

    result = domain_manager.index_built_contexts(datasource_ids=wanted)

    assert result == "OK"
    index_fn.assert_called_once_with(
        project_layout=domain_manager._project_layout,
        plugin_loader=domain_manager._plugin_loader,
        contexts=[c1, c3],
        chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
        ollama_model_id=None,
        ollama_model_dim=None,
    )


def test_databao_context_domain_manager__create_datasource_config__fails_invalid_config_content(domain_manager):
    with pytest.raises(ValidationError) as e:
        domain_manager.create_datasource_config(
            datasource_type=DatasourceType(full_type="dummy_simple_pydantic"),
            datasource_name="my_datasource",
            config_content={
                "a": "not_an_int",
            },
        )

    validation_errors = e.value.errors()
    assert len(validation_errors) == 2
    assert len([error for error in validation_errors if error["type"] == "missing"]) == 1
    assert len([error for error in validation_errors if error["type"] == "int_parsing"]) == 1


def test_databao_context_domain_manager__create_datasource_config__fails_invalid_config_content_non_validated(
    domain_manager,
):
    configured_datasource = domain_manager.create_datasource_config(
        datasource_type=DatasourceType(full_type="dummy_simple_pydantic"),
        datasource_name="my_datasource",
        config_content={
            "a": "not_an_int",
        },
        validate_config_content=False,
    )

    assert configured_datasource.datasource.id.absolute_path_to_config_file(domain_manager._project_layout).is_file()
    assert configured_datasource.config == {
        "name": "my_datasource",
        "type": "dummy_simple_pydantic",
        "a": "not_an_int",
    }


def test_databao_context_domain_manager__create_datasource_config__valid_config_content_dict(domain_manager):
    configured_datasource = domain_manager.create_datasource_config(
        datasource_type=DatasourceType(full_type="dummy_simple_pydantic"),
        datasource_name="my_datasource",
        config_content={
            "a": "12",
            "b": "some string",
        },
    )

    assert configured_datasource.datasource.id.absolute_path_to_config_file(domain_manager._project_layout).is_file()
    assert configured_datasource.config == {
        "name": "my_datasource",
        "type": "dummy_simple_pydantic",
        "a": "12",
        "b": "some string",
    }


def test_databao_context_domain_manager__create_datasource_config__valid_config_content_from_config_type(
    domain_manager,
):
    configured_datasource = domain_manager.create_datasource_config(
        datasource_type=DatasourceType(full_type="dummy_simple_pydantic"),
        datasource_name="my_datasource",
        config_content=SimplePydanticConfig(
            name="my_datasource",
            a=12,
            b="some string",
        ),
    )

    assert configured_datasource.datasource.id.absolute_path_to_config_file(domain_manager._project_layout).is_file()
    assert configured_datasource.config == {
        "name": "my_datasource",
        "type": "dummy_simple_pydantic",
        "a": 12,
        "b": "some string",
    }


def test_databao_context_domain_manager__create_datasource_config__wrong_config_content_type_for_plugin(
    domain_manager,
):
    with pytest.raises(ValidationError) as e:
        domain_manager.create_datasource_config(
            datasource_type=DatasourceType(full_type="dummy_other_pydantic"),
            datasource_name="my_datasource",
            config_content=SimplePydanticConfig(
                name="my_datasource",
                a=12,
                b="some string",
            ),
        )

    validation_errors = e.value.errors()
    assert len(validation_errors) == 1
    assert validation_errors[0]["type"] == "model_type"


def test_databao_context_domain_manager__check_datasource_config_connection(domain_manager):
    result = domain_manager.check_datasource_config_connection(
        datasource_type=DatasourceType(full_type="dummy_simple_pydantic"),
        datasource_name="my_datasource",
        config_content=SimplePydanticConfig(
            name="my_datasource",
            a=12,
            b="some string",
        ),
    )

    assert result.connection_status == DatasourceConnectionStatus.VALID


def assert_build_context_result(
    context_result: BuildDatasourceResult,
    domain_dir: Path,
    *,
    datasource_id: DatasourceId,
    datasource_type: DatasourceType,
    context_file_relative_path: str,
):
    assert context_result.datasource_id == datasource_id
    assert context_result.datasource_type == datasource_type
    assert context_result.context_built_at is not None
    assert context_result.context_built_at < datetime.now()
    assert context_result.context_file_path is not None
    assert str(context_result.context_file_path).endswith(context_file_relative_path)
    assert context_result.context_file_path.is_relative_to(get_output_dir(domain_dir))
