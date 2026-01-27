import pytest

from databao_context_engine import DatabaoContextEngine, Datasource, DatasourceContext, DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.serialization.yaml import to_yaml_string
from tests.utils.project_creation import given_datasource_config_file, given_output_dir_with_built_contexts


def test_databao_engine__can_not_be_created_on_non_existing_project(tmp_path):
    non_existing_project_dir = tmp_path / "non-existing-project"

    with pytest.raises(ValueError):
        DatabaoContextEngine(project_dir=non_existing_project_dir)


def test_databao_engine__get_datasource_list_with_no_datasources(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [],
    )

    datasource_list = databao_context_engine.get_introspected_datasource_list()

    assert datasource_list == []


def test_databao_engine__get_datasource_list_with_multiple_datasources(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)
    given_datasource_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="full/any",
        datasource_name="a",
        config_content={"type": "any", "name": "a"},
    )
    given_datasource_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="other/type",
        datasource_name="b",
        config_content={"type": "type", "name": "b"},
    )
    given_datasource_config_file(
        project_dir=databao_context_engine.project_dir,
        full_type="full/type2",
        datasource_name="c",
        config_content={"type": "type2", "name": "c"},
    )

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
                context=to_yaml_string(
                    {"datasource_id": "full/a.yaml", "datasource_type": "full/any", "context": "Context for a"}
                ),
            ),
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("other/b.yaml"),
                context=to_yaml_string(
                    {"datasource_id": "other/b.yaml", "datasource_type": "other/type", "context": "Context for b"}
                ),
            ),
        ],
    )

    datasource_list = databao_context_engine.get_introspected_datasource_list()

    assert datasource_list == [
        Datasource(id=DatasourceId.from_string_repr("full/a.yaml"), type=DatasourceType(full_type="full/any")),
        Datasource(id=DatasourceId.from_string_repr("other/b.yaml"), type=DatasourceType(full_type="other/type")),
    ]


def test_databao_engine__get_datasource_context(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/a.yaml"), context="Context for a"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("other/c.yaml"), context="Context for c"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"),
        ],
    )

    result = databao_context_engine.get_datasource_context(DatasourceId.from_string_repr("full/b.yaml"))

    assert result == DatasourceContext(
        datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"
    )


def test_databao_engine__get_datasource_context_for_unbuilt_datasource(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/a.yaml"), context="Context for a"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("other/c.yaml"), context="Context for c"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"),
        ],
    )

    with pytest.raises(ValueError):
        databao_context_engine.get_datasource_context(DatasourceId.from_string_repr("full/d.yaml"))


def test_databao_engine__get_all_contexts(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/a.yaml"), context="Context for a"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("other/c.yaml"), context="Context for c"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("files/d.txt.yaml"), context="Context for d"),
        ],
    )

    result = databao_context_engine.get_all_contexts()

    assert result == [
        DatasourceContext(datasource_id=DatasourceId.from_string_repr("files/d.txt"), context="Context for d"),
        DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/a.yaml"), context="Context for a"),
        DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"),
        DatasourceContext(datasource_id=DatasourceId.from_string_repr("other/c.yaml"), context="Context for c"),
    ]


def test_databao_engine__get_all_contexts_formatted(project_path, db_path, create_db):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/a.yaml"), context="Context for a"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("other/c.yaml"), context="Context for c"),
            DatasourceContext(datasource_id=DatasourceId.from_string_repr("full/b.yaml"), context="Context for b"),
        ],
    )

    result = databao_context_engine.get_all_contexts_formatted()

    assert (
        result.strip()
        == """
# ===== full/a.yaml =====
Context for a
# ===== full/b.yaml =====
Context for b
# ===== other/c.yaml =====
Context for c
    """.strip()
    )
