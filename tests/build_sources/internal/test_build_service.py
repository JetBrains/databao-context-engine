from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from nemory.build_sources.internal.build_service import BuildService
from nemory.build_sources.internal.types import PreparedDatasource, PreparedFile
from nemory.pluginlib.build_plugin import BuildExecutionResult, EmbeddableChunk


def mk_result(*, id="ds-1", name="foo", typ="files/md", result=None):
    return BuildExecutionResult(
        id=id,
        name=name,
        type=typ,
        description=None,
        version=None,
        executed_at=datetime.now(),
        result=result if result is not None else {"ok": True},
    )


def mk_prepared(path: Path, full_type: str) -> PreparedDatasource:
    return PreparedFile(path=path, full_type=full_type)


@pytest.fixture
def repos(mocker):
    run_repo = mocker.Mock(name="RunRepository")
    ds_repo = mocker.Mock(name="DatasourceRunRepository")
    return run_repo, ds_repo


@pytest.fixture
def chunk_embed_svc(mocker):
    return mocker.Mock(name="ChunkEmbeddingService")


@pytest.fixture
def svc(repos, chunk_embed_svc):
    run_repo, ds_repo = repos
    return BuildService(
        run_repo=run_repo,
        datasource_run_repo=ds_repo,
        chunk_embedding_service=chunk_embed_svc,
    )


def test_start_run_calls_repo_and_returns_dto(svc, repos):
    run_repo, _ = repos
    dto = SimpleNamespace(run_id=123)
    run_repo.create.return_value = dto

    out = svc.start_run(project_id="proj-1", nemory_version="1.2.3")

    run_repo.create.assert_called_once_with(project_id="proj-1", nemory_version="1.2.3")
    assert out is dto


def test_finalize_run_sets_ended_at(svc, repos, mocker):
    run_repo, _ = repos
    svc.finalize_run(run_id=42)

    assert run_repo.update.call_count == 1
    kwargs = run_repo.update.call_args.kwargs
    assert kwargs["run_id"] == 42
    assert isinstance(kwargs["ended_at"], datetime)
    assert datetime.now() - kwargs["ended_at"] < timedelta(seconds=2)


def test_process_prepared_source_no_chunks_skips_write_and_embed(svc, repos, chunk_embed_svc, mocker, tmp_path):
    run_repo, ds_repo = repos
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(tmp_path / "src" / "files" / "md" / "one.md", full_type="files/md")

    mocker.patch("nemory.build_sources.internal.build_service.execute", return_value=mk_result())
    plugin.divide_result_into_chunks.return_value = []

    out = svc.process_prepared_source(run_id=7, prepared_source=prepared, plugin=plugin)

    ds_repo.create.assert_not_called()
    chunk_embed_svc.embed_chunks.assert_not_called()
    assert isinstance(out, BuildExecutionResult)


def test_process_prepared_source_happy_path_creates_row_and_embeds(svc, repos, chunk_embed_svc, mocker, tmp_path):
    run_repo, ds_repo = repos
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(tmp_path / "src" / "files" / "md" / "two.md", full_type="files/md")

    result = mk_result(id="ext-id", name="two", typ="files/md")
    mocker.patch("nemory.build_sources.internal.build_service.execute", return_value=result)

    chunks = [EmbeddableChunk("a", "A"), EmbeddableChunk("b", "B")]
    plugin.divide_result_into_chunks.return_value = chunks

    ds_row = SimpleNamespace(datasource_run_id=555)
    ds_repo.create.return_value = ds_row

    out = svc.process_prepared_source(run_id=9, prepared_source=prepared, plugin=plugin)

    ds_repo.create.assert_called_once_with(
        run_id=9,
        plugin="pluggy",
        full_type="files/md",
        source_id="ext-id",
        storage_directory=str(prepared.path.parent),
    )
    chunk_embed_svc.embed_chunks.assert_called_once_with(
        datasource_run_id=555,
        chunks=chunks,
        result=repr(result),
    )
    assert out is result


def test_process_prepared_source_uses_path_stem_when_result_id_missing(svc, repos, chunk_embed_svc, mocker, tmp_path):
    run_repo, ds_repo = repos
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(tmp_path / "src" / "databases" / "pg.yaml", full_type="databases/postgres")

    res = mk_result(id=None, name="pg", typ="databases/postgres")
    mocker.patch("nemory.build_sources.internal.build_service.execute", return_value=res)

    plugin.divide_result_into_chunks.return_value = [EmbeddableChunk("e", "E")]

    ds_row = SimpleNamespace(datasource_run_id=777)
    ds_repo.create.return_value = ds_row

    svc.process_prepared_source(run_id=1, prepared_source=prepared, plugin=plugin)

    assert ds_repo.create.call_args.kwargs["source_id"] == "pg"
    assert ds_repo.create.call_args.kwargs["storage_directory"] == str(prepared.path.parent)


def test_process_prepared_source_execute_error_bubbles_and_no_writes(svc, repos, chunk_embed_svc, mocker, tmp_path):
    run_repo, ds_repo = repos
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(tmp_path / "src" / "files" / "md" / "boom.md", full_type="files/md")

    mocker.patch("nemory.build_sources.internal.build_service.execute", side_effect=RuntimeError("exec-fail"))

    with pytest.raises(RuntimeError):
        svc.process_prepared_source(run_id=1, prepared_source=prepared, plugin=plugin)

    ds_repo.create.assert_not_called()
    chunk_embed_svc.embed_chunks.assert_not_called()


def test_process_prepared_source_embed_error_bubbles_after_row_creation(svc, repos, chunk_embed_svc, mocker, tmp_path):
    run_repo, ds_repo = repos
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(tmp_path / "src" / "files" / "md" / "x.md", full_type="files/md")

    mocker.patch("nemory.build_sources.internal.build_service.execute", return_value=mk_result())
    plugin.divide_result_into_chunks.return_value = [EmbeddableChunk("x", "X")]

    ds_repo.create.return_value = SimpleNamespace(datasource_run_id=42)
    chunk_embed_svc.embed_chunks.side_effect = RuntimeError("embed-fail")

    with pytest.raises(RuntimeError):
        svc.process_prepared_source(run_id=99, prepared_source=prepared, plugin=plugin)

    ds_repo.create.assert_called_once()
