import pytest

from nemory.core.db.dtos import RunStatus, EmbeddingDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


def _make_entity(entity_repo, run_repo, *, plugin="p", source_id="s", storage_directory="d"):
    run = run_repo.create(status=RunStatus.RUNNING, project_id="project-id", nemory_version=None)
    return entity_repo.create(run_id=run.run_id, plugin=plugin, source_id=source_id, storage_directory=storage_directory)


def _make_segment(segment_repo, entity_repo, run_repo, *, emb="e", disp="d"):
    ent = _make_entity(entity_repo, run_repo)
    return segment_repo.create(entity_id=ent.entity_id, embeddable_text=emb, display_text=disp)


def test_create_and_get(embedding_repo, segment_repo, entity_repo, run_repo):
    seg = _make_segment(segment_repo, entity_repo, run_repo)
    created = embedding_repo.create(segment_id=seg.segment_id, embedder="ollama", model_id="m1", vec=_vec(0.0))

    assert isinstance(created, EmbeddingDTO)
    assert created.segment_id == seg.segment_id
    assert created.embedder == "ollama"
    assert created.model_id == "m1"
    assert list(created.vec) == _vec(0.0)

    fetched = embedding_repo.get(seg.segment_id)
    assert fetched == created


def test_get_missing_returns_none(embedding_repo):
    assert embedding_repo.get(999_999) is None


def test_update_vec(embedding_repo, segment_repo, entity_repo, run_repo):
    seg = _make_segment(segment_repo, entity_repo, run_repo)
    embedder = "ollama"
    model_id = "m1"
    emb = embedding_repo.create(segment_id=seg.segment_id, embedder=embedder, model_id=model_id, vec=_vec(0.0))

    updated_vec = _vec(9.9)
    updated = embedding_repo.update(seg.segment_id, embedder=embedder, model_id=model_id, vec=updated_vec)
    assert updated is not None
    assert updated.segment_id == seg.segment_id
    assert updated.embedder == embedder
    assert updated.model_id == model_id
    assert updated.vec == pytest.approx(updated_vec, rel=1e-6, abs=1e-6)
    assert updated.created_at == emb.created_at


def test_update_missing_returns_none(embedding_repo):
    assert embedding_repo.update(424242, "ollama", "m1", vec=_vec(0.0)) is None


def test_delete(embedding_repo, segment_repo, entity_repo, run_repo):
    seg = _make_segment(segment_repo, entity_repo, run_repo)
    embedding_repo.create(segment_id=seg.segment_id, embedder="ollama", model_id="m1", vec=_vec(0.0))

    deleted = embedding_repo.delete(seg.segment_id, embedder="ollama", model_id="m1")
    assert deleted == 1
    assert embedding_repo.get(seg.segment_id) is None


def test_delete_missing_returns_zero(embedding_repo):
    assert embedding_repo.delete(424242, "a", "b") == 0


def test_list(embedding_repo, segment_repo, entity_repo, run_repo):
    s1 = _make_segment(segment_repo, entity_repo, run_repo, emb="e1", disp="d1")
    e1 = embedding_repo.create(segment_id=s1.segment_id, embedder="ollama", model_id="m1", vec=_vec(1.0))

    s2 = _make_segment(segment_repo, entity_repo, run_repo, emb="e2", disp="d2")
    e2 = embedding_repo.create(segment_id=s2.segment_id, embedder="ollama", model_id="m2", vec=_vec(2.0))

    rows = embedding_repo.list()
    assert [e.segment_id for e in rows] == [e2.segment_id, e1.segment_id]


def test_create_with_missing_fk_raises(embedding_repo):
    with pytest.raises(IntegrityError):
        embedding_repo.create(segment_id=999_999, embedder="ollama", model_id="m1", vec=_vec(0.0))


def _vec(fill: float | None = None, *, pattern_start: float | None = None) -> list[float]:
    dim = 768
    if fill is not None:
        return [float(fill)] * dim
    if pattern_start is not None:
        start = float(pattern_start)
        return [start + i for i in range(dim)]
    return [0.0] * dim
