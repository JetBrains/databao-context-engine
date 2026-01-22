from unittest.mock import Mock

import pytest

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingService
from databao_context_engine.services.table_name_policy import TableNamePolicy


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

    service.embed_chunks(chunks=[], result="", full_type="databases/some", datasource_id="test.yml")

    assert chunk_repo.list() == []
    assert registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id) is None

    embedding_provider.embed.assert_not_called()
    description_provider.describe.assert_not_called()


def test_embeds_resolves_and_persists(persistence, resolver, chunk_repo, embedding_repo, registry_repo):
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

    chunks = [
        EmbeddableChunk("A", "a"),
        EmbeddableChunk("B", "b"),
        EmbeddableChunk("C", "c"),
    ]

    service.embed_chunks(
        chunks=chunks,
        result="",
        full_type="databases/some",
        datasource_id="test.yml",
    )

    expected_table = _expected_table(embedding_provider)
    reg = registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id)
    assert reg is not None
    assert reg.table_name == expected_table
    assert reg.dim == embedding_provider.dim

    saved = chunk_repo.list()
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=expected_table)
    assert len(rows) == 3

    embedding_provider.embed.assert_called()
    assert embedding_provider.embed.call_count == 3


def test_provider_failure_writes_nothing(persistence, resolver, chunk_repo, embedding_repo, registry_repo):
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

    with pytest.raises(RuntimeError):
        service.embed_chunks(
            chunks=[EmbeddableChunk("X", "x"), EmbeddableChunk("Y", "y")],
            result="",
            full_type="databases/some",
            datasource_id="test.yml",
        )

    assert registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id) is None
    assert chunk_repo.list() == []
