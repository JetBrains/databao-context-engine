from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from databao_context_engine import DatasourceId
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

        def side_effect(_pl, _ds):
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
                path=datasources.path,
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
    src = SimpleNamespace(path=project_layout.src_dir / "files" / "one.md")
    stub_sources([src])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/one.md")),
                datasource_type=DatasourceType(full_type="files/md"),
                path=src.path,
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
    s1 = SimpleNamespace(path=project_layout.src_dir / "files" / "a.md")
    s2 = SimpleNamespace(path=project_layout.src_dir / "files" / "b.md")
    stub_sources([s1, s2])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/a.md")),
                datasource_type=DatasourceType(full_type="files/md"),
                path=s1.path,
            ),
            PreparedFile(
                datasource_id=DatasourceId.from_datasource_context_file_path(Path("files/b.md")),
                datasource_type=DatasourceType(full_type="files/md"),
                path=s2.path,
            ),
        ]
    )
    stub_plugins({DatasourceType(full_type="files/md"): object()})

    mock_build_service.process_prepared_source.side_effect = [RuntimeError("boom"), _result(name="files/b.md")]

    build_runner.build(project_layout=project_layout, build_service=mock_build_service)

    assert mock_build_service.process_prepared_source.call_count == 2
