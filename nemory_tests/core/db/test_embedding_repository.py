import pytest

from nemory.core.db.dtos import EmbeddingDTO
from nemory.core.db.exceptions.exceptions import IntegrityError
from nemory_tests._utils.factories import make_segment


def test_create_and_get(embedding_repo, segment_repo, entity_repo, run_repo, table_name):
    seg = make_segment(run_repo, entity_repo, segment_repo)
    created = embedding_repo.create(segment_id=seg.segment_id, table_name=table_name, vec=_vec(0.0))

    assert isinstance(created, EmbeddingDTO)
    assert created.segment_id == seg.segment_id
    assert list(created.vec) == _vec(0.0)

    fetched = embedding_repo.get(table_name=table_name, segment_id=seg.segment_id)
    assert fetched == created


def test_get_missing_returns_none(embedding_repo, table_name):
    assert embedding_repo.get(table_name=table_name, segment_id=999_999) is None


def test_update_vec(embedding_repo, segment_repo, entity_repo, run_repo, table_name):
    seg = make_segment(run_repo, entity_repo, segment_repo)
    emb = embedding_repo.create(segment_id=seg.segment_id, table_name=table_name, vec=_vec(0.0))

    updated_vec = _vec(9.9)
    updated = embedding_repo.update(segment_id=seg.segment_id, table_name=table_name, vec=updated_vec)
    assert updated is not None
    assert updated.segment_id == seg.segment_id
    assert updated.vec == pytest.approx(updated_vec, rel=1e-6, abs=1e-6)
    assert updated.created_at == emb.created_at


def test_update_missing_returns_none(embedding_repo, table_name):
    assert embedding_repo.update(table_name=table_name, segment_id=424242, vec=_vec(0.0)) is None


def test_delete(embedding_repo, segment_repo, entity_repo, run_repo, table_name):
    seg = make_segment(run_repo, entity_repo, segment_repo)
    embedding_repo.create(segment_id=seg.segment_id, table_name=table_name, vec=_vec(0.0))

    deleted = embedding_repo.delete(segment_id=seg.segment_id, table_name=table_name)
    assert deleted == 1
    assert embedding_repo.get(table_name=table_name, segment_id=seg.segment_id) is None


def test_delete_missing_returns_zero(embedding_repo, table_name):
    assert embedding_repo.delete(table_name=table_name, segment_id=424242) == 0


def test_list(embedding_repo, segment_repo, entity_repo, run_repo, table_name):
    s1 = make_segment(run_repo, entity_repo, segment_repo, embeddable_text="e1", display_text="d1")
    e1 = embedding_repo.create(table_name=table_name, segment_id=s1.segment_id, vec=_vec(1.0))

    s2 = make_segment(run_repo, entity_repo, segment_repo, embeddable_text="e2", display_text="d2")
    e2 = embedding_repo.create(table_name=table_name, segment_id=s2.segment_id, vec=_vec(2.0))

    rows = embedding_repo.list(table_name=table_name)
    assert [e.segment_id for e in rows] == [e2.segment_id, e1.segment_id]


def test_create_with_missing_fk_raises(embedding_repo, table_name):
    with pytest.raises(IntegrityError):
        embedding_repo.create(table_name=table_name, segment_id=999_999, vec=_vec(0.0))


def test_update_with_missing_table_raises(embedding_repo):
    with pytest.raises(ValueError, match="invalid table_name"):
        embedding_repo.update(table_name="123", segment_id=1, vec=_vec(0.0))


def _vec(fill: float | None = None, *, pattern_start: float | None = None) -> list[float]:
    dim = 768
    if fill is not None:
        return [float(fill)] * dim
    if pattern_start is not None:
        start = float(pattern_start)
        return [start + i for i in range(dim)]
    return [0.0] * dim
