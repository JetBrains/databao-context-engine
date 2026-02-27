from datetime import datetime
from pathlib import Path

import pytest
import yaml

from databao_context_engine import DatasourceContext, DatasourceId
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

    chunks = [EmbeddableChunk(embeddable_text="a", content="A"), EmbeddableChunk(embeddable_text="b", content="B")]
    plugin.divide_context_into_chunks.return_value = chunks

    out = svc.process_prepared_source(prepared_source=prepared, plugin=plugin)

    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=chunks,
        result=result,
        datasource_id="files/two.md",
        full_type="files/md",
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
    plugin.divide_context_into_chunks.return_value = [EmbeddableChunk(embeddable_text="x", content="X")]

    chunk_embed_svc.embed_chunks.side_effect = RuntimeError("embed-fail")

    with pytest.raises(RuntimeError):
        svc.process_prepared_source(prepared_source=prepared, plugin=plugin)


def test_index_built_context_happy_path_embeds(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    plugin.context_type = dict

    built_at = datetime(2026, 2, 4, 12, 0, 0)
    raw = {
        "datasource_id": "files/two.md",
        "datasource_type": "files/md",
        "context_built_at": built_at,
        "context": {"hello": "world"},
    }
    yaml_text = yaml.safe_dump(raw)

    dsid = DatasourceId.from_string_repr("files/two.md")
    ctx = DatasourceContext(datasource_id=dsid, context=yaml_text)

    chunks = [EmbeddableChunk(embeddable_text="a", content="A"), EmbeddableChunk(embeddable_text="b", content="B")]
    plugin.divide_context_into_chunks.return_value = chunks

    svc.index_built_context(context=ctx, plugin=plugin)

    plugin.divide_context_into_chunks.assert_called_once_with({"hello": "world"})
    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=chunks,
        result=BuiltDatasourceContext(
            datasource_id="files/two.md",
            datasource_type="files/md",
            context_built_at=built_at,
            context={"hello": "world"},
        ),
        full_type="files/md",
        datasource_id="files/two.md",
        override=True,
    )


def test_index_built_context_no_chunks_skips_embed(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    plugin.context_type = dict

    raw = {
        "datasource_id": "files/empty.md",
        "datasource_type": "files/md",
        "context_built_at": datetime(2026, 2, 4, 12, 0, 0),
        "context": {"nothing": True},
    }
    yaml_text = yaml.safe_dump(raw)

    dsid = DatasourceId.from_string_repr("files/empty.md")
    ctx = DatasourceContext(datasource_id=dsid, context=yaml_text)

    plugin.divide_context_into_chunks.return_value = []

    svc.index_built_context(context=ctx, plugin=plugin)

    chunk_embed_svc.embed_chunks.assert_not_called()


def test_process_prepared_source_generate_embeddings_false_skips_chunking_and_embed(svc, chunk_embed_svc, mocker):
    plugin = mocker.Mock(name="Plugin")
    plugin.name = "pluggy"
    prepared = mk_prepared(Path("files") / "noembed.md", full_type="files/md")

    result = mk_result(name="files/noembed.md", typ="files/md", result={"context": "ok"})
    mocker.patch("databao_context_engine.build_sources.build_service.execute_plugin", return_value=result)

    out = svc.process_prepared_source(prepared_source=prepared, plugin=plugin, generate_embeddings=False)

    plugin.divide_context_into_chunks.assert_not_called()
    chunk_embed_svc.embed_chunks.assert_not_called()
    assert out is result
