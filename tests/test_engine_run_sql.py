import pytest

from databao_context_engine import DatabaoContextEngine, DatasourceId
from databao_context_engine.pluginlib.build_plugin import (
    BuildPlugin,
    DatasourceType,
    DefaultBuildDatasourcePlugin,
    NotSupportedError,
)
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from tests.utils.project_creation import given_datasource_config_file


class DummySqlPlugin(DefaultBuildDatasourcePlugin):
    id = "dummy/dumme_sql"
    name = "Dummy SQL Plugin"

    def supported_types(self) -> set[str]:
        return {"dummy_sql"}

    def build_context(self, full_type: str, datasource_name: str, file_config: dict) -> dict:
        return {"ok": True}

    def run_sql(
        self, file_config: dict, sql: str, params: list[object] | None = None, read_only: bool = True
    ) -> SqlExecutionResult:
        cols = ["a", "b"]
        row = (sql, tuple(params) if params is not None else None)
        return SqlExecutionResult(columns=cols, rows=[row])


class DummyNonSqlPlugin(DefaultBuildDatasourcePlugin):
    id = "dummy/dummy_no_sql"
    name = "Dummy Non-SQL Plugin"

    def supported_types(self) -> set[str]:
        return {"dummy_no_sql"}

    def build_context(self, full_type: str, datasource_name: str, file_config: dict) -> dict:
        return {"ok": True}


def _plugins_map_with(*plugins):
    mapping: dict[DatasourceType, BuildPlugin] = {}
    for p in plugins:
        for t in p.supported_types():
            mapping[DatasourceType(full_type=t)] = p
    return mapping


def test_engine_run_sql_happy_path(project_path):
    plugins_map = _plugins_map_with(DummySqlPlugin())
    engine = DatabaoContextEngine(project_dir=project_path, plugin_loader=DatabaoContextPluginLoader(plugins_map))
    given_datasource_config_file(
        engine._project_layout,
        datasource_name="databases/my_ds",
        config_content={"type": "dummy_sql", "name": "my_ds"},
    )

    ds_id = DatasourceId.from_string_repr("databases/my_ds.yaml")

    res = engine.run_sql(ds_id, "SELECT 1", params=[123], read_only=True)

    assert res.columns == ["a", "b"]
    assert res.rows and isinstance(res.rows[0], tuple)


def test_engine_run_sql_params_passthrough(project_path):
    received = {}

    class CapturingSqlPlugin(DummySqlPlugin):
        def run_sql(
            self, file_config: dict, sql: str, params: list[object] | None = None, read_only: bool = True
        ) -> SqlExecutionResult:
            received["sql"] = sql
            received["params"] = params
            return super().run_sql(file_config, sql, params, read_only)

    plugins_map = _plugins_map_with(CapturingSqlPlugin())
    engine = DatabaoContextEngine(project_dir=project_path, plugin_loader=DatabaoContextPluginLoader(plugins_map))
    given_datasource_config_file(
        engine._project_layout,
        datasource_name="databases/my_ds2",
        config_content={"type": "dummy_sql", "name": "my_ds2"},
    )
    ds_id = DatasourceId.from_string_repr("databases/my_ds2.yaml")

    params = ["x", 42]
    engine.run_sql(ds_id, "SELECT $1, $2", params=params)

    assert received["sql"].startswith("SELECT")
    assert received["params"] == params


def test_engine_run_sql_unsupported_plugin(project_path):
    plugins_map = _plugins_map_with(DummyNonSqlPlugin())
    engine = DatabaoContextEngine(project_dir=project_path, plugin_loader=DatabaoContextPluginLoader(plugins_map))
    given_datasource_config_file(
        engine._project_layout,
        datasource_name="databases/no_sql",
        config_content={"type": "dummy_no_sql", "name": "no_sql"},
    )
    ds_id = DatasourceId.from_string_repr("databases/no_sql.yaml")

    with pytest.raises(NotSupportedError):
        engine.run_sql(ds_id, "SELECT 1")
