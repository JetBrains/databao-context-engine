import pytest


from nemory.core.db.dtos import RunStatus
from nemory.core.db.exceptions.exceptions import IntegrityError
from nemory.core.services.models import EmbeddingItem
from nemory.core.services.persistence_service import PersistenceService
from nemory.features.build_sources.plugin_lib.build_plugin import EmbeddableChunk


def _make_entity(entity_repo, run_repo):
    run = run_repo.create(
        status=RunStatus.RUNNING,
        nemory_version=None,
    )
    return entity_repo.create(run_id=run.run_id, plugin="p", source_id="s", document="d")


def _make_segments(conn, embedding_repo, segment_repo, entity_repo, run_repo, texts):
    ent = _make_entity(entity_repo, run_repo)
    persistence_service = PersistenceService(conn, segment_repo, embedding_repo)
    chunks = [EmbeddableChunk(t, f"disp:{t}") for t in texts]
    return persistence_service.write_segments(entity_id=ent.entity_id, chunks=chunks)


def test_write_embeddings(conn, embedding_repo, segment_repo, entity_repo, run_repo):
    seg_ids = _make_segments(conn, embedding_repo, segment_repo, entity_repo, run_repo, ["a", "b", "c"])
    items = [
        EmbeddingItem(segment_id=seg_ids[0], vec=_vec(1.0)),
        EmbeddingItem(segment_id=seg_ids[1], vec=_vec(2.0)),
        EmbeddingItem(segment_id=seg_ids[2], vec=_vec(3.0)),
    ]
    service = PersistenceService(conn, segment_repo, embedding_repo)

    count = service.write_embeddings(items=items, embedder="ollama", model_id="m1")

    assert count == 3
    rows = embedding_repo.list()
    assert len(rows) == 3

    assert {r.segment_id for r in rows} == set(seg_ids)
    assert all(r.model_id == "m1" for r in rows)
    assert all(r.embedder == "ollama" for r in rows)


def test_empty_items_raises_value_error(conn, segment_repo, embedding_repo):
    service = PersistenceService(conn, segment_repo, embedding_repo)
    with pytest.raises(ValueError):
        service.write_embeddings(items=[], embedder="ollama", model_id="m1")


def test_wrong_vector_length_raises_and_writes_nothing(conn, embedding_repo, segment_repo, entity_repo, run_repo):
    seg_ids = _make_segments(conn, embedding_repo, segment_repo, entity_repo, run_repo, ["x"])
    bad = EmbeddingItem(segment_id=seg_ids[0], vec=[0.0])
    service = PersistenceService(conn, segment_repo, embedding_repo)

    with pytest.raises(ValueError):
        service.write_embeddings(items=[bad], embedder="ollama", model_id="m1")

    assert embedding_repo.list() == []


def test_invalid_fk_raises_and_writes_nothing(conn, segment_repo, embedding_repo):
    items = [EmbeddingItem(segment_id=999_999, vec=_vec(0.0))]
    service = PersistenceService(conn, segment_repo, embedding_repo)

    with pytest.raises(IntegrityError):
        service.write_embeddings(items=items, embedder="ollama", model_id="m1")

    assert embedding_repo.list() == []


def test_mid_batch_failure_rolls_back_entire_batch(
    conn, embedding_repo, segment_repo, entity_repo, run_repo, monkeypatch
):
    seg_ids = _make_segments(conn, embedding_repo, segment_repo, entity_repo, run_repo, ["a", "b", "c"])
    items = [
        EmbeddingItem(segment_id=seg_ids[0], vec=_vec(1.0)),
        EmbeddingItem(segment_id=seg_ids[1], vec=_vec(2.0)),
        EmbeddingItem(segment_id=seg_ids[2], vec=_vec(3.0)),
    ]

    service = PersistenceService(conn, segment_repo, embedding_repo)

    calls = {"n": 0}

    def flaky_create(*, segment_id: int, embedder: str, model_id: str, vec):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return embedding_repo.create(segment_id=segment_id, embedder=embedder, model_id=model_id, vec=vec)

    monkeypatch.setattr(type(embedding_repo), "create", lambda self, **kw: flaky_create(**kw))

    with pytest.raises(RuntimeError):
        service.write_embeddings(items=items, embedder="ollama", model_id="m1")

    assert embedding_repo.list() == []


def test_write_segments(conn, segment_repo, entity_repo, run_repo, embedding_repo):
    entity = _make_entity(entity_repo, run_repo)
    service = PersistenceService(conn, segment_repo, embedding_repo)

    chunks = [EmbeddableChunk("e1", "d1"), EmbeddableChunk("e2", "d2"), EmbeddableChunk("e3", "d3")]
    ids = service.write_segments(entity_id=entity.entity_id, chunks=chunks)

    assert len(ids) == 3
    assert ids == sorted(ids)
    seg = segment_repo.get(ids[0])
    assert seg is not None
    assert seg.embeddable_text == "e1"
    assert seg.display_text == "d1"
    seg = segment_repo.get(ids[1])
    assert seg is not None
    assert seg.embeddable_text == "e2"
    assert seg.display_text == "d2"
    seg = segment_repo.get(ids[2])
    assert seg is not None
    assert seg.embeddable_text == "e3"
    assert seg.display_text == "d3"


def test_none_chunks_raises_value_error(conn, segment_repo, embedding_repo):
    service = PersistenceService(conn, segment_repo, embedding_repo)
    with pytest.raises(ValueError):
        service.write_segments(entity_id=123, chunks=None)


def test_invalid_fk_rolls_back(conn, segment_repo, embedding_repo):
    service = PersistenceService(conn, segment_repo, embedding_repo)
    with pytest.raises(IntegrityError):
        service.write_segments(entity_id=999_999, chunks=[EmbeddableChunk("e1", "d1")])
    assert segment_repo.list() == []


def test_mid_batch_failure_rolls_back(conn, segment_repo, entity_repo, run_repo, embedding_repo, monkeypatch):
    ent = _make_entity(entity_repo, run_repo)
    service = PersistenceService(conn, segment_repo, embedding_repo)

    calls = {"n": 0}

    def flaky_create(*, entity_id: int, embeddable_text: str, display_text: str | None):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return segment_repo.create(entity_id=entity_id, embeddable_text=embeddable_text, display_text=display_text)

    monkeypatch.setattr(segment_repo, "create", flaky_create)

    with pytest.raises(RuntimeError):
        service.write_segments(
            entity_id=ent.entity_id,
            chunks=[EmbeddableChunk("a", "b"), EmbeddableChunk("c", "d"), EmbeddableChunk("e", "f")],
        )

    assert segment_repo.list() == []


def _vec(fill: float) -> list[float]:
    return [fill] * 768
