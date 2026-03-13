from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import time_machine

from databao_context_engine import DatasourceContext, DatasourceId, DatasourceStatus
from databao_context_engine.build_sources import build_runner
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.datasources.types import PreparedConfig, PreparedFile
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import NoPluginFoundForDatasource
from databao_context_engine.serialization.yaml import to_yaml_string


def _result(name="files/demo.md", typ="files/md"):
    return BuiltDatasourceContext(
        datasource_id=name,
        datasource_type=typ,
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


def test_build_returns_early_when_no_sources(stub_sources, mock_build_service, project_layout):
    stub_sources([])
    build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )
    mock_build_service.start_run.assert_not_called()


def test_build_skips_source_without_plugin(
    stub_sources, stub_prepare, mock_build_service, project_layout, fake_output_dir
):
    datasources = SimpleNamespace(path=project_layout.src_dir / "files" / "one.md")
    stub_sources([datasources])
    stub_prepare(
        [
            PreparedFile(
                DatasourceId.from_string_repr("files/one.md"),
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )
    mock_build_service.build_context.side_effect = [NoPluginFoundForDatasource("boom", DatasourceType(full_type="md"))]

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )
    mock_build_service.build_context.assert_called_once()

    assert len(results) == 1
    assert results[0].status == DatasourceStatus.SKIPPED


def test_build_processes_file_source_and_exports(
    stub_sources, stub_prepare, mock_build_service, project_layout, mocker
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

    with time_machine.travel("2025-03-11 10:30:00", tick=False):
        result = _result(name="files/one.md", typ="files/md")
        mock_build_service.build_context.return_value = result

        build_runner.build(
            project_layout=project_layout,
            build_service=mock_build_service,
            should_index=True,
            should_enrich_context=False,
        )

        mock_build_service.build_context.assert_called_once()
        mock_build_service.index_built_context.assert_called_once_with(
            built_context=result,
            context_hash=DatasourceContextHash(
                datasource_id=DatasourceId.from_string_repr(result.datasource_id),
                hash="a59b9fa8606781aaf033490eb635ed7e",
                hash_algorithm="XXH3_128",
                hashed_at=datetime.now(),
            ),
            progress=None,
        )


def test_build_continues_on_service_exception(stub_sources, stub_prepare, mock_build_service, project_layout):
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
    mock_build_service.build_context.side_effect = [RuntimeError("boom"), _result(name="files/b.md")]

    build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert mock_build_service.build_context.call_count == 2


def test_run_indexing_indexes_when_plugin_exists(mock_build_service, project_layout):
    ctx = DatasourceContext(
        datasource_id=DatasourceId.from_string_repr("files/one.md"),
        context=to_yaml_string({"type": "md"}),
        context_hash=DatasourceContextHash(
            datasource_id=DatasourceId.from_string_repr("files/one.md"),
            hash="irrelevant for this test",
            hash_algorithm="XXH3_128",
            hashed_at=datetime.now(),
        ),
    )

    build_runner.run_indexing(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[ctx],
    )

    mock_build_service.index_datasource_context.assert_called_once_with(context=ctx, progress=None)


def test_run_indexing_continues_on_exception(mock_build_service, project_layout):
    c1 = DatasourceContext(
        DatasourceId.from_string_repr("files/a.md"),
        context="a",
        context_hash=DatasourceContextHash(
            DatasourceId.from_string_repr("files/a.md"),
            hash="irrelevant for this test",
            hash_algorithm="irrelevant for this test",
            hashed_at=datetime.now(),
        ),
    )
    c2 = DatasourceContext(
        DatasourceId.from_string_repr("files/b.md"),
        context="b",
        context_hash=DatasourceContextHash(
            DatasourceId.from_string_repr("files/b.md"),
            hash="irrelevant for this test",
            hash_algorithm="irrelevant for this test",
            hashed_at=datetime.now(),
        ),
    )

    mock_build_service.index_datasource_context.side_effect = [RuntimeError("boom"), None]

    build_runner.run_indexing(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[c1, c2],
    )

    assert mock_build_service.index_datasource_context.call_count == 2
    mock_build_service.index_datasource_context.assert_any_call(context=c1, progress=None)
    mock_build_service.index_datasource_context.assert_any_call(context=c2, progress=None)


def test_build_skips_disabled_config_source(stub_sources, stub_prepare, mock_build_service, project_layout, mocker):
    datasource_disabled_id = DatasourceId.from_string_repr("configs/my_source.yaml")
    datasource_disabled_2_id = DatasourceId.from_string_repr("configs/my_source_2.yaml")
    datasource_enabled_id = DatasourceId.from_string_repr("configs/my_source_3.yaml")
    datasource_enabled_2_id = DatasourceId.from_string_repr("configs/my_source_4.yaml")
    datasource_file_id = DatasourceId.from_string_repr("my_file.md")
    stub_sources(
        [
            datasource_disabled_id,
            datasource_disabled_2_id,
            datasource_enabled_id,
            datasource_enabled_2_id,
            datasource_file_id,
        ]
    )

    stub_prepare(
        [
            PreparedConfig(
                datasource_id=datasource_disabled_id,
                datasource_type=DatasourceType(full_type="my/type"),
                config={"type": "my/type", "enabled": False},
                datasource_name="my_source",
            ),
            PreparedConfig(
                datasource_id=datasource_disabled_id,
                datasource_type=DatasourceType(full_type="my/type"),
                config={"type": "my/type", "enabled": "False"},
                datasource_name="my_source_2",
            ),
            PreparedConfig(
                datasource_id=datasource_enabled_id,
                datasource_type=DatasourceType(full_type="my/type"),
                config={"type": "my/type", "enabled": "True"},
                datasource_name="my_source_3",
            ),
            PreparedConfig(
                datasource_id=datasource_enabled_2_id,
                datasource_type=DatasourceType(full_type="my/type"),
                config={"type": "my/type", "enabled": True},
                datasource_name="my_source_4",
            ),
            PreparedFile(
                datasource_id=datasource_file_id,
                datasource_type=DatasourceType(full_type="my/type"),
            ),
        ]
    )

    mock_build_service.build_context.return_value = _result()

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert {result.datasource_id: result.status for result in results} == {
        datasource_disabled_id: DatasourceStatus.SKIPPED,
        datasource_disabled_2_id: DatasourceStatus.SKIPPED,
        datasource_enabled_id: DatasourceStatus.OK,
        datasource_enabled_2_id: DatasourceStatus.OK,
        datasource_file_id: DatasourceStatus.OK,
    }
