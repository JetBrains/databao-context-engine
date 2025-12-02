from unittest.mock import Mock

import pytest

from nemory.llm.descriptions.provider import DescriptionProvider
from nemory.llm.embeddings.provider import EmbeddingProvider
from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.table_name_policy import TableNamePolicy
from tests.utils.factories import make_datasource_run


def _expected_table(provider) -> str:
    return TableNamePolicy().build(embedder=provider.embedder, model_id=provider.model_id, dim=provider.dim)


def _vec(fill: float, dim: int) -> list[float]:
    return [fill] * dim


def test_noop_on_empty_chunks(persistence, resolver, chunk_repo, embedding_repo, registry_repo):
    embedding_provider = Mock(spec=EmbeddingProvider)
    description_provider = Mock(spec=DescriptionProvider)
    service = ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        shard_resolver=resolver,
    )
    embedding_provider.embedder = "tests"
    embedding_provider.model_id = "model:v1"
    embedding_provider.dim = 768

    service.embed_chunks(datasource_run_id=123, chunks=[], result="")

    assert chunk_repo.list() == []
    assert registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id) is None

    embedding_provider.embed.assert_not_called()
    description_provider.describe.assert_not_called()


def test_embeds_resolves_and_persists(
    persistence, resolver, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo
):
    embedding_provider = Mock(spec=EmbeddingProvider)
    description_provider = Mock(spec=DescriptionProvider)

    embedding_provider.embedder = "ollama"
    embedding_provider.model_id = "nomic-embed-text:v1.5"
    embedding_provider.dim = 768

    embedding_provider.embed.side_effect = [
        _vec(0.0, embedding_provider.dim),
        _vec(1.0, embedding_provider.dim),
        _vec(2.0, embedding_provider.dim),
    ]

    description_provider.describe.side_effect = lambda *, text, context: f"desc-{text}"

    service = ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        shard_resolver=resolver,
    )

    datasource_run = make_datasource_run(
        run_repo,
        datasource_run_repo,
        plugin="p",
        source_id="s",
        storage_directory="/tmp",
    )

    chunks = [
        EmbeddableChunk("A", "a"),
        EmbeddableChunk("B", "b"),
        EmbeddableChunk("C", "c"),
    ]

    service.embed_chunks(
        datasource_run_id=datasource_run.datasource_run_id,
        chunks=chunks,
        result="",
    )

    expected_table = _expected_table(embedding_provider)
    reg = registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id)
    assert reg is not None
    assert reg.table_name == expected_table
    assert reg.dim == embedding_provider.dim

    saved = [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id]
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=expected_table)
    assert len(rows) == 3

    embedding_provider.embed.assert_called()
    assert embedding_provider.embed.call_count == 3


def test_provider_failure_writes_nothing(
    persistence, resolver, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo
):
    embedding_provider = Mock(spec=EmbeddingProvider)
    description_provider = Mock(spec=DescriptionProvider)

    embedding_provider.embedder = "tests"
    embedding_provider.model_id = "model:v1"
    embedding_provider.dim = 768


    embedding_provider.embed.side_effect = [
        _vec(2.0, embedding_provider.dim),
        RuntimeError("provider embed failed"),
    ]

    description_provider.describe.side_effect = lambda *, text, context: f"desc-{text}"

    service = ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        shard_resolver=resolver,
    )

    datasource_run = make_datasource_run(
        run_repo, datasource_run_repo, plugin="p", source_id="s", storage_directory="/tmp"
    )

    with pytest.raises(RuntimeError):
        service.embed_chunks(
            datasource_run_id=datasource_run.datasource_run_id,
            chunks=[EmbeddableChunk("X", "x"), EmbeddableChunk("Y", "y")],
            result="",
        )

    assert registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id) is None
    assert [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id] == []
