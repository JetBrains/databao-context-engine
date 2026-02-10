import os
from datetime import datetime
from pathlib import Path

import pytest

from databao_context_engine import DatasourceId
from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.types import PreparedDatasource, PreparedFile
from databao_context_engine.pluginlib.build_plugin import DatasourceType, EmbeddableChunk


def mk_result(*, name="files/foo.md", typ="files/md", result=None):
    return BuiltDatasourceContext(
        datasource_id=name,
        datasource_type=typ,
        context_built_at=datetime.now(),
        context=result if result is not None else {"ok": True},
    )


def mk_prepared(path: Path, full_type: str) -> PreparedDatasource:
    return PreparedFile(
        DatasourceId._from_relative_datasource_config_file_path(path),
        datasource_type=DatasourceType(full_type=full_type),
    )


@pytest.fixture
def chunk_embed_svc(mocker):
    return mocker.Mock(name="ChunkEmbeddingService")


@pytest.fixture
def svc(chunk_embed_svc, mocker):
    return BuildService(
        project_layout=mocker.Mock(name="ProjectLayout"),
        chunk_embedding_service=chunk_embed_svc,
    )


def test_process_prepared_source_no_chunks_skips_write_and_embed(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(Path("files") / "one.md", full_type="files/md")

    mocker.patch("databao_context_engine.build_sources.build_service.execute_plugin", return_value=mk_result())
    plugin.divide_context_into_chunks.return_value = []

    out = svc.process_prepared_source(prepared_source=prepared, plugin=plugin)

    chunk_embed_svc.embed_chunks.assert_not_called()
    assert isinstance(out, BuiltDatasourceContext)


def test_process_prepared_source_happy_path_creates_row_and_embeds(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(Path("files") / "two.md", full_type="files/md")

    result = mk_result(name="files/two.md", typ="files/md", result={"context": "ok"})
    mocker.patch("databao_context_engine.build_sources.build_service.execute_plugin", return_value=result)

    chunks = [EmbeddableChunk("a", "A"), EmbeddableChunk("b", "B")]
    plugin.divide_context_into_chunks.return_value = chunks

    out = svc.process_prepared_source(prepared_source=prepared, plugin=plugin)

    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=chunks,
        result=f"context: ok{os.linesep}",
        datasource_id="files/two.md",
        full_type="files/md",
        progress=None,
    )
    assert out is result


def test_process_prepared_source_execute_error_bubbles_and_no_writes(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(Path("files") / "boom.md", full_type="files/md")

    mocker.patch(
        "databao_context_engine.build_sources.build_service.execute_plugin", side_effect=RuntimeError("exec-fail")
    )

    with pytest.raises(RuntimeError):
        svc.process_prepared_source(prepared_source=prepared, plugin=plugin)

    chunk_embed_svc.embed_chunks.assert_not_called()


def test_process_prepared_source_embed_error_bubbles_after_row_creation(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(Path("files") / "x.md", full_type="files/md")

    mocker.patch("databao_context_engine.build_sources.build_service.execute_plugin", return_value=mk_result())
    plugin.divide_context_into_chunks.return_value = [EmbeddableChunk("x", "X")]

    chunk_embed_svc.embed_chunks.side_effect = RuntimeError("embed-fail")

    with pytest.raises(RuntimeError):
        svc.process_prepared_source(prepared_source=prepared, plugin=plugin)
