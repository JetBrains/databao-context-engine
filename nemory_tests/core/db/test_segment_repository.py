import pytest

from nemory.core.db.dtos import RunStatus, SegmentDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


def _make_entity(entity_repo, run_repo, *, plugin="p", source_id="s", storage_directory="d"):
    run = run_repo.create(status=RunStatus.RUNNING, project_id="project-id")
    return entity_repo.create(run_id=run.run_id, plugin=plugin, source_id=source_id, storage_directory=storage_directory)


def test_create_and_get(segment_repo, entity_repo, run_repo):
    ent = _make_entity(entity_repo, run_repo)

    created = segment_repo.create(
        entity_id=ent.entity_id,
        embeddable_text="embed me",
        display_text="visible content",
    )
    assert isinstance(created, SegmentDTO)
    assert created.entity_id == ent.entity_id

    fetched = segment_repo.get(created.segment_id)
    assert fetched == created


def test_update_fields(segment_repo, entity_repo, run_repo):
    ent = _make_entity(entity_repo, run_repo)
    seg = segment_repo.create(entity_id=ent.entity_id, embeddable_text="a", display_text="b")

    updated = segment_repo.update(seg.segment_id, embeddable_text="A+", display_text="B+")
    assert updated is not None
    assert updated.entity_id == ent.entity_id
    assert updated.embeddable_text == "A+"
    assert updated.display_text == "B+"
    assert updated.created_at == seg.created_at


def test_delete(segment_repo, entity_repo, run_repo):
    ent = _make_entity(entity_repo, run_repo)
    seg = segment_repo.create(entity_id=ent.entity_id, embeddable_text="x", display_text=None)

    deleted = segment_repo.delete(seg.segment_id)
    assert deleted == 1
    assert segment_repo.get(seg.segment_id) is None


def test_list(segment_repo, entity_repo, run_repo):
    ent = _make_entity(entity_repo, run_repo)
    s1 = segment_repo.create(entity_id=ent.entity_id, embeddable_text="e1", display_text="d1")
    s2 = segment_repo.create(entity_id=ent.entity_id, embeddable_text="e2", display_text="d2")
    s3 = segment_repo.create(entity_id=ent.entity_id, embeddable_text="e3", display_text="d3")

    all_rows = segment_repo.list()
    assert [s.segment_id for s in all_rows] == [s3.segment_id, s2.segment_id, s1.segment_id]


def test_create_with_missing_fk_raises(segment_repo):
    with pytest.raises(IntegrityError):
        segment_repo.create(entity_id=999_999, embeddable_text="e1", display_text="d1")
