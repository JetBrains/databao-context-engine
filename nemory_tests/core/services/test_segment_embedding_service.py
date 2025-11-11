import pytest

from nemory.core.services.models import EmbeddingItem
from nemory.core.services.segment_embedding_service import SegmentEmbeddingService
from nemory.pluginlib.build_plugin import EmbeddableChunk


def test_noop_on_empty_chunks():
    persistence = _FakePersistence(seg_ids=[])
    provider = _FakeProvider()

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)
    svc.embed_chunks(entity_id=123, chunks=[])

    assert persistence.calls["write_segments"] == []
    assert persistence.calls["write_embeddings"] == []
    assert provider.calls == []


def test_writes_segments_then_embeddings():
    chunks = [EmbeddableChunk("a", "a"), EmbeddableChunk("b", "b"), EmbeddableChunk("c", "c")]
    seg_ids = [10, 11, 12]

    persistence = _FakePersistence(seg_ids=seg_ids)
    provider = _FakeProvider(dim=768, model_id="ollama:nomic-embed-text;dim=768")

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)
    svc.embed_chunks(entity_id=7, chunks=chunks)

    assert len(persistence.calls["write_segments"]) == 1
    entity_id_arg, segments_arg = persistence.calls["write_segments"][0]
    assert entity_id_arg == 7

    assert provider.calls == ["a", "b", "c"]

    assert len(persistence.calls["write_embeddings"]) == 1
    items, embedder, model_id = persistence.calls["write_embeddings"][0]
    assert embedder == "fake-provider"
    assert model_id == "ollama:nomic-embed-text;dim=768"
    assert [it.segment_id for it in items] == seg_ids
    assert all(isinstance(it, EmbeddingItem) for it in items)


def test_provider_error_aborts_before_embedding_write():
    chunks = [EmbeddableChunk("ok", "ok"), EmbeddableChunk("boom", "boom"), EmbeddableChunk("later", "later")]
    persistence = _FakePersistence(seg_ids=[1, 2, 3])
    provider = _FakeProvider(fail_at={1})

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    with pytest.raises(RuntimeError):
        svc.embed_chunks(entity_id=1, chunks=chunks)

    assert len(persistence.calls["write_segments"]) == 1
    assert persistence.calls["write_embeddings"] == []


def test_provider_bad_dim_bubbles_and_no_embeddings_written():
    chunks = [EmbeddableChunk("x", "x"), EmbeddableChunk("y", "y")]
    persistence = _FakePersistence(seg_ids=[5, 6])
    provider = _FakeProvider(bad_dim_at={0})

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    with pytest.raises(ValueError):
        svc.embed_chunks(entity_id=9, chunks=chunks)

    assert persistence.calls["write_embeddings"] == []


def test_embedding_persistence_failure_bubbles_after_provider_calls():
    chunks = [EmbeddableChunk("a", "a"), EmbeddableChunk("b", "b")]
    persistence = _FakePersistence(seg_ids=[100, 101], should_raise_on_embeddings=True)
    provider = _FakeProvider()

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    with pytest.raises(RuntimeError):
        svc.embed_chunks(entity_id=2, chunks=chunks)

    assert provider.calls == ["a", "b"]
    assert len(persistence.calls["write_embeddings"]) == 1


def test_segment_count_mismatch_raises():
    chunks = [EmbeddableChunk("only-one", "only-one")]
    persistence = _FakePersistence(seg_ids=[])
    provider = _FakeProvider()

    svc = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    with pytest.raises(RuntimeError):
        svc.embed_chunks(entity_id=3, chunks=chunks)

    assert persistence.calls["write_embeddings"] == []
    assert provider.calls == []


class _FakePersistence:
    def __init__(self, seg_ids=None, should_raise_on_embeddings=False):
        self.seg_ids = list(seg_ids or [])
        self.should_raise_on_embeddings = should_raise_on_embeddings
        self.calls = {
            "write_segments": [],
            "write_embeddings": [],
        }

    def write_segments(self, *, entity_id: int, chunks: list[EmbeddableChunk]) -> list[int]:
        self.calls["write_segments"].append((entity_id, list(chunks)))
        return list(self.seg_ids)

    def write_embeddings(self, *, items: list[EmbeddingItem], embedder: str, model_id: str) -> int:
        self.calls["write_embeddings"].append((list(items), embedder, model_id))
        if self.should_raise_on_embeddings:
            raise RuntimeError("DB fail")
        return len(items)


class _FakeProvider:
    def __init__(self, *, dim=768, model_id="model", fail_at=None, bad_dim_at=None):
        self.dim = dim
        self.model_id = model_id
        self.calls = []
        self._fail_at = set(fail_at or [])
        self._bad_dim_at = set(bad_dim_at or [])

    @property
    def embedder(self):
        return "fake-provider"

    @property
    def dim(self):
        return self._dim

    @dim.setter
    def dim(self, v):
        self._dim = v

    @property
    def model_id(self):
        return self._model_id

    @model_id.setter
    def model_id(self, v):
        self._model_id = v

    def embed(self, text: str):
        idx = len(self.calls)
        self.calls.append(text)
        if idx in self._fail_at:
            raise RuntimeError("provider permanent error")
        if idx in self._bad_dim_at:
            raise ValueError("wrong dim")
        return [float(idx)] * self._dim
