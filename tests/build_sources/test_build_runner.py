from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from databao_context_engine import DatasourceContext, DatasourceId
from databao_context_engine.build_sources import build_runner
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.types import PreparedFile
from databao_context_engine.pluginlib.build_plugin import DatasourceType


def _result(name="files/demo.md", typ="files/md"):
    return BuiltDatasourceContext(
        datasource_id=name,
        datasource_type=typ,
        context_built_at=datetime.now(),
        context={"ok": True},
    )


@pytest.fixture
def mock_build_service(mocker):
    return mocker.Mock(name="BuildService")


@pytest.fixture
def stub_prepare(mocker):
    def _stub(prepared_list):
        items = list(prepared_list)

        def side_effect(_project_layout, _ds):
            return items.pop(0) if items else None

        return mocker.patch.object(build_runner, "prepare_source", side_effect=side_effect)

    return _stub


def _read_all_results(path: Path):
    p = path / "all_results.yaml"
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as fh:
        try:
            return list(yaml.safe_load_all(fh))
        except Exception:
            return [yaml.safe_load(fh)]


def test_build_returns_early_when_no_sources(stub_sources, stub_plugins, mock_build_service, project_layout):
    stub_sources([])
    build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
    )
    mock_build_service.start_run.assert_not_called()


def test_build_skips_source_without_plugin(
    stub_sources, stub_plugins, stub_prepare, mock_build_service, project_layout, fake_output_dir
):
    datasources = SimpleNamespace(path=project_layout.src_dir / "files" / "one.md")
    stub_sources([datasources])
    stub_plugins({})
    stub_prepare(
        [
            PreparedFile(
                DatasourceId.from_string_repr("files/one.md"),
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )

    build_runner.build(project_layout=project_layout, build_service=mock_build_service)
    mock_build_service.start_run.assert_not_called()
    mock_build_service.process_prepared_source.assert_not_called()
    mock_build_service.finalize_run.assert_not_called()

    exports = list(fake_output_dir.glob("*.yaml"))
    assert not any(p.name != "all_results.yaml" for p in exports)


def test_build_processes_file_source_and_exports(
    stub_sources, stub_plugins, stub_prepare, mock_build_service, project_layout
):
    src = SimpleNamespace(datasource_id=DatasourceId(datasource_path="files/one", config_file_suffix=".md"))
    stub_sources([src])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/one.md")),
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )
    stub_plugins({DatasourceType(full_type="files/md"): object()})

    mock_build_service.process_prepared_source.return_value = _result(name="files/one.md", typ="files/md")

    build_runner.build(project_layout=project_layout, build_service=mock_build_service)

    mock_build_service.process_prepared_source.assert_called_once()


def test_build_continues_on_service_exception(
    stub_sources, stub_plugins, stub_prepare, mock_build_service, project_layout
):
    stub_sources(
        [
            (DatasourceId(datasource_path="files/a", config_file_suffix=".md")),
            (DatasourceId(datasource_path="files/b", config_file_suffix=".md")),
        ]
    )
    stub_prepare(
        [
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/a.md")),
                datasource_type=DatasourceType(full_type="files/md"),
            ),
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/b.md")),
                datasource_type=DatasourceType(full_type="files/md"),
            ),
        ]
    )
    stub_plugins({DatasourceType(full_type="files/md"): object()})

    mock_build_service.process_prepared_source.side_effect = [RuntimeError("boom"), _result(name="files/b.md")]

    build_runner.build(project_layout=project_layout, build_service=mock_build_service)

    assert mock_build_service.process_prepared_source.call_count == 2


def test_run_indexing_indexes_when_plugin_exists(mocker, mock_build_service, project_layout):
    plugin = object()
    ds_type = DatasourceType(full_type="files/md")

    mocker.patch.object(build_runner, "load_plugins", return_value={ds_type: plugin})
    mocker.patch.object(build_runner, "read_datasource_type_from_context", return_value=ds_type)

    ctx = DatasourceContext(
        datasource_id=DatasourceId.from_string_repr("files/one.md"),
        context="irrelevant for this test",
    )

    build_runner.run_indexing(project_layout=project_layout, build_service=mock_build_service, contexts=[ctx])

    mock_build_service.index_built_context.assert_called_once_with(context=ctx, plugin=plugin)


def test_run_indexing_skips_when_plugin_missing(mocker, mock_build_service, project_layout, caplog):
    ds_type = DatasourceType(full_type="files/md")

    mocker.patch.object(build_runner, "load_plugins", return_value={})
    mocker.patch.object(build_runner, "read_datasource_type_from_context", return_value=ds_type)

    ctx = DatasourceContext(
        datasource_id=DatasourceId.from_string_repr("files/one.md"),
        context="irrelevant for this test",
    )

    build_runner.run_indexing(project_layout=project_layout, build_service=mock_build_service, contexts=[ctx])

    mock_build_service.index_built_context.assert_not_called()


def test_run_indexing_continues_on_exception(mocker, mock_build_service, project_layout):
    plugin = object()
    ds_type = DatasourceType(full_type="files/md")

    mocker.patch.object(build_runner, "load_plugins", return_value={ds_type: plugin})
    mocker.patch.object(build_runner, "read_datasource_type_from_context", return_value=ds_type)

    c1 = DatasourceContext(DatasourceId.from_string_repr("files/a.md"), context="a")
    c2 = DatasourceContext(DatasourceId.from_string_repr("files/b.md"), context="b")

    mock_build_service.index_built_context.side_effect = [RuntimeError("boom"), None]

    build_runner.run_indexing(project_layout=project_layout, build_service=mock_build_service, contexts=[c1, c2])

    assert mock_build_service.index_built_context.call_count == 2
    mock_build_service.index_built_context.assert_any_call(context=c1, plugin=plugin)
    mock_build_service.index_built_context.assert_any_call(context=c2, plugin=plugin)
