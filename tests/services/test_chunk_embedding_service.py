from typing import List, Sequence

import pytest

from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.table_name_policy import TableNamePolicy
from tests.utils.factories import make_datasource_run


class _FakeProvider:
    def __init__(self, *, embedder="tests", model_id="model:v1", dim=768, fail_at=None, bad_dim_at=None):
        self.embedder = embedder
        self.model_id = model_id
        self.dim = dim
        self._fail_at = set(fail_at or [])
        self._bad_dim_at = set(bad_dim_at or [])
        self.calls: List[str] = []

    def embed(self, text: str) -> Sequence[float]:
        i = len(self.calls)
        self.calls.append(text)
        if i in self._fail_at:
            raise RuntimeError("provider embed failed")
        if i in self._bad_dim_at:
            return [0.0]
        return [float(i)] * self.dim


class _FakeOllamaService:
    def __init__(self, *, fail_at=None):
        self._fail_at = set(fail_at or [])
        self.calls: list[tuple[str, str]] = []

    def describe(self, *, text: str, context: str) -> str:
        i = len(self.calls)
        self.calls.append((text, context))

        if i in self._fail_at:
            raise RuntimeError("fake describe failure")

        return f"desc-{i}-{text}"


def _expected_table(provider: _FakeProvider) -> str:
    return TableNamePolicy().build(embedder=provider.embedder, model_id=provider.model_id, dim=provider.dim)


def test_noop_on_empty_chunks(persistence, resolver, chunk_repo, embedding_repo, registry_repo):
    provider = _FakeProvider()
    ollama_service = _FakeOllamaService()
    service = ChunkEmbeddingService(
        persistence_service=persistence, provider=provider, shard_resolver=resolver, ollama_service=ollama_service
    )

    service.embed_chunks(datasource_run_id=123, chunks=[], result="")

    assert chunk_repo.list() == []
    assert registry_repo.get(embedder=provider.embedder, model_id=provider.model_id) is None


def test_embeds_resolves_and_persists(
    persistence, resolver, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo
):
    provider = _FakeProvider(embedder="ollama", model_id="nomic-embed-text:v1.5", dim=768)
    ollama_service = _FakeOllamaService()
    service = ChunkEmbeddingService(
        persistence_service=persistence, provider=provider, shard_resolver=resolver, ollama_service=ollama_service
    )

    datasource_run = make_datasource_run(
        run_repo, datasource_run_repo, plugin="p", source_id="s", storage_directory="/tmp"
    )

    chunks = [EmbeddableChunk("A", "a"), EmbeddableChunk("B", "b"), EmbeddableChunk("C", "c")]

    service.embed_chunks(datasource_run_id=datasource_run.datasource_run_id, chunks=chunks, result="")

    expected_table = _expected_table(provider)
    reg = registry_repo.get(embedder=provider.embedder, model_id=provider.model_id)
    assert reg is not None and reg.table_name == expected_table and reg.dim == provider.dim

    saved = [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id]
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=expected_table)
    assert len(rows) == 3


def test_provider_failure_writes_nothing(
    persistence, resolver, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo
):
    provider = _FakeProvider(fail_at={1})
    ollama_service = _FakeOllamaService()
    service = ChunkEmbeddingService(
        persistence_service=persistence, provider=provider, shard_resolver=resolver, ollama_service=ollama_service
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

    assert registry_repo.get(embedder=provider.embedder, model_id=provider.model_id) is None
    assert [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id] == []
