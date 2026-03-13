from datetime import datetime

import pytest

from databao_context_engine import DatasourceContext, DatasourceId, DatasourceStatus
from databao_context_engine.build_sources import build_runner
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.datasources.types import PreparedConfig, PreparedFile
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import NoPluginFoundForDatasource
from databao_context_engine.serialization.yaml import to_yaml_string


def _result(name: str = "files/demo.md", typ: str = "files/md", context: dict | None = None) -> BuiltDatasourceContext:
    return BuiltDatasourceContext(
        datasource_id=name,
        datasource_type=typ,
        context=context if context is not None else {"ok": True},
    )


def _datasource_id(path: str) -> DatasourceId:
    return DatasourceId.from_string_repr(path)


def _context(datasource_path: str, context: str = "a") -> DatasourceContext:
    datasource_id = _datasource_id(datasource_path)
    return DatasourceContext(
        datasource_id=datasource_id,
        context=context,
        context_hash=DatasourceContextHash(
            datasource_id=datasource_id,
            hash="irrelevant for this test",
            hash_algorithm="irrelevant for this test",
            hashed_at=datetime.now(),
        ),
    )


@pytest.fixture
def mock_build_service(mocker):
    return mocker.Mock(name="BuildService")


@pytest.fixture
def stub_prepare(mocker):
    def _stub(prepared_list):
        items = list(prepared_list)

        def side_effect(_project_layout, _datasource_id):
            return items.pop(0)

        return mocker.patch.object(build_runner, "prepare_source", side_effect=side_effect)

    return _stub


def test_build_returns_empty_when_no_sources(stub_sources, mock_build_service, project_layout):
    stub_sources([])

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert results == []
    mock_build_service.build_context.assert_not_called()


def test_build_skips_source_without_plugin(stub_sources, stub_prepare, mock_build_service, project_layout):
    datasource_id = _datasource_id("files/one.md")
    stub_sources([datasource_id])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=datasource_id,
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )
    mock_build_service.build_context.side_effect = NoPluginFoundForDatasource("boom", DatasourceType(full_type="md"))

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert len(results) == 1
    assert results[0].datasource_id == datasource_id
    assert results[0].status == DatasourceStatus.SKIPPED
    mock_build_service.index_built_context.assert_not_called()


def test_build_processes_file_source_and_exports_and_indexes(
    stub_sources, stub_prepare, mock_build_service, project_layout
):
    datasource_id = _datasource_id("files/one.md")
    stub_sources([datasource_id])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=datasource_id,
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )

    built_context = _result(name="files/one.md", typ="files/md")
    mock_build_service.build_context.return_value = built_context

    now_before_test = datetime.now()

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert len(results) == 1
    result = results[0]
    assert result.status == DatasourceStatus.OK
    assert result.datasource_id == datasource_id
    assert result.datasource_type == DatasourceType(full_type="files/md")
    assert result.context_file_path is not None
    assert result.context_file_path.is_file()
    assert result.context_file_path == datasource_id.absolute_path_to_context_file(project_layout)
    assert "ok: true" in result.context_file_path.read_text()

    mock_build_service.build_context.assert_called_once()

    mock_build_service.index_built_context.assert_called_once()
    index_call = mock_build_service.index_built_context.call_args
    assert index_call.kwargs["built_context"] == built_context
    assert index_call.kwargs["context_hash"].datasource_id == datasource_id
    assert index_call.kwargs["context_hash"].hash_algorithm == "XXH3_128"
    assert index_call.kwargs["context_hash"].hashed_at > now_before_test


def test_build_skips_indexing_when_disabled(stub_sources, stub_prepare, mock_build_service, project_layout):
    datasource_id = _datasource_id("files/no_index.md")
    stub_sources([datasource_id])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=datasource_id,
                datasource_type=DatasourceType(full_type="files/md"),
            )
        ]
    )
    mock_build_service.build_context.return_value = _result(name="files/no_index.md")

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=False,
        should_enrich_context=False,
    )

    assert len(results) == 1
    assert results[0].status == DatasourceStatus.OK
    mock_build_service.index_built_context.assert_not_called()


def test_build_enriches_before_export_and_index(stub_sources, stub_prepare, mock_build_service, project_layout):
    datasource_id = _datasource_id("files/enriched.md")
    prepared_source = PreparedFile(
        datasource_id=datasource_id,
        datasource_type=DatasourceType(full_type="files/md"),
    )
    built_context = _result(name="files/enriched.md", context={"raw": True})
    enriched_context = _result(name="files/enriched.md", context={"enriched": True})
    context_path = datasource_id.absolute_path_to_context_file(project_layout)

    stub_sources([datasource_id])
    stub_prepare([prepared_source])
    mock_build_service.build_context.return_value = built_context
    mock_build_service.enrich_built_context.return_value = enriched_context

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=True,
    )

    assert len(results) == 1
    assert results[0].status == DatasourceStatus.OK
    assert context_path.is_file()
    written_text = context_path.read_text()
    assert "enriched: true" in written_text
    assert "raw: true" not in written_text
    mock_build_service.enrich_built_context.assert_called_once_with(built_context=built_context, progress=None)
    mock_build_service.index_built_context.assert_called_once()
    index_call = mock_build_service.index_built_context.call_args
    assert index_call.kwargs["built_context"] == enriched_context
    assert index_call.kwargs["context_hash"].datasource_id == datasource_id


def test_build_returns_failed_result_and_continues_on_service_exception(
    stub_sources, stub_prepare, mock_build_service, project_layout
):
    datasource_a = _datasource_id("files/a.md")
    datasource_b = _datasource_id("files/b.md")
    stub_sources([datasource_a, datasource_b])
    stub_prepare(
        [
            PreparedFile(
                datasource_id=datasource_a,
                datasource_type=DatasourceType(full_type="files/md"),
            ),
            PreparedFile(
                datasource_id=datasource_b,
                datasource_type=DatasourceType(full_type="files/md"),
            ),
        ]
    )
    mock_build_service.build_context.side_effect = [RuntimeError("boom"), _result(name="files/b.md")]

    results = build_runner.build(
        project_layout=project_layout,
        build_service=mock_build_service,
        should_index=True,
        should_enrich_context=False,
    )

    assert [result.status for result in results] == [DatasourceStatus.FAILED, DatasourceStatus.OK]
    assert results[0].datasource_id == datasource_a
    assert results[0].error == "boom"
    assert results[1].datasource_id == datasource_b


def test_run_indexing_returns_ok_result(mock_build_service, project_layout):
    ctx = DatasourceContext(
        datasource_id=_datasource_id("files/one.md"),
        context=to_yaml_string({"type": "md"}),
        context_hash=DatasourceContextHash(
            datasource_id=_datasource_id("files/one.md"),
            hash="irrelevant for this test",
            hash_algorithm="XXH3_128",
            hashed_at=datetime.now(),
        ),
    )

    results = build_runner.run_indexing(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[ctx],
    )

    assert len(results) == 1
    assert results[0].status == DatasourceStatus.OK
    mock_build_service.index_datasource_context.assert_called_once_with(context=ctx, progress=None)


def test_run_indexing_returns_failed_result_and_continues_on_exception(mock_build_service, project_layout):
    c1 = _context("files/a.md", context="a")
    c2 = _context("files/b.md", context="b")

    mock_build_service.index_datasource_context.side_effect = [RuntimeError("boom"), None]

    results = build_runner.run_indexing(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[c1, c2],
    )

    assert len(results) == 2
    assert results[0].datasource_id == DatasourceId.from_string_repr("files/a.md")
    assert results[0].status == DatasourceStatus.FAILED
    assert results[1].datasource_id == DatasourceId.from_string_repr("files/b.md")
    assert results[1].status == DatasourceStatus.OK

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
                datasource_id=datasource_disabled_2_id,
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


def test_run_enrich_context_returns_ok_result(project_layout, mock_build_service):
    datasource_id = _datasource_id("files/enrich.md")
    context = _context("files/enrich.md", context=to_yaml_string({"type": "files/md"}))
    enriched_context = _result(name="files/enrich.md", context={"summary": "ok"})
    output_path = datasource_id.absolute_path_to_context_file(project_layout)

    mock_build_service.enrich_datasource_context.return_value = enriched_context

    results = build_runner.run_enrich_context(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[context],
        should_index=False,
    )

    assert len(results) == 1
    assert results[0].datasource_id == datasource_id
    assert results[0].status == DatasourceStatus.OK
    assert results[0].context_file_path == output_path
    assert output_path.is_file()
    assert "summary: ok" in output_path.read_text()
    mock_build_service.index_built_context.assert_not_called()


def test_run_enrich_context_skips_when_plugin_missing(project_layout, mock_build_service):
    context = _context("files/no_plugin.md", context=to_yaml_string({"type": "files/md"}))
    mock_build_service.enrich_datasource_context.side_effect = NoPluginFoundForDatasource(
        "boom", DatasourceType(full_type="files/md")
    )

    results = build_runner.run_enrich_context(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[context],
        should_index=False,
    )

    assert len(results) == 1
    assert results[0].datasource_id == context.datasource_id
    assert results[0].status == DatasourceStatus.SKIPPED


def test_run_enrich_context_returns_failed_result_and_continues_on_exception(project_layout, mock_build_service):
    c1 = _context("files/a.md", context="a")
    c2 = _context("files/b.md", context="b")
    mock_build_service.enrich_datasource_context.side_effect = [RuntimeError("boom"), _result(name="files/b.md")]

    results = build_runner.run_enrich_context(
        project_layout=project_layout,
        build_service=mock_build_service,
        contexts=[c1, c2],
        should_index=False,
    )

    assert [result.status for result in results] == [DatasourceStatus.FAILED, DatasourceStatus.OK]
    assert results[0].datasource_id == c1.datasource_id
    assert results[0].error == "boom"
    assert results[1].datasource_id == c2.datasource_id
