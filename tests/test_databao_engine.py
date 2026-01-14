import pytest

from nemory.databao_engine import DatabaoContextEngine
from nemory.pluginlib.build_plugin import DatasourceType
from nemory.project.datasource_discovery import Datasource
from tests.utils.project_creation import with_config_file


def test_databao_engine__can_not_be_created_on_non_existing_project(tmp_path):
    non_existing_project_dir = tmp_path / "non-existing-project"

    with pytest.raises(ValueError):
        DatabaoContextEngine(project_dir=non_existing_project_dir)


def test_databao_engine__get_datasource_list_with_no_datasources(project_path):
    datasource_list = DatabaoContextEngine(project_dir=project_path).get_datasource_list()

    assert datasource_list == []


def test_databao_engine__get_datasource_list_with_multiple_datasources(project_path):
    databao_context_engine = DatabaoContextEngine(project_dir=project_path)
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

    datasource_list = databao_context_engine.get_datasource_list()

    assert datasource_list == [
        Datasource(id="full/a.yaml", type=DatasourceType(full_type="full/any")),
        Datasource(id="full/c.yaml", type=DatasourceType(full_type="full/type2")),
        Datasource(id="other/b.yaml", type=DatasourceType(full_type="other/type")),
    ]
