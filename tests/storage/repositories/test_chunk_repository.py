import pytest

from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import ChunkDTO
from tests.utils.factories import make_datasource_run


def test_create_and_get(chunk_repo, datasource_run_repo, run_repo):
    datasource_run = make_datasource_run(run_repo, datasource_run_repo)

    created = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="embed me",
        display_text="visible content",
        generated_description="generated description",
    )
    assert isinstance(created, ChunkDTO)
    assert created.datasource_run_id == datasource_run.datasource_run_id

    fetched = chunk_repo.get(created.chunk_id)
    assert fetched == created


def test_update_fields(chunk_repo, datasource_run_repo, run_repo):
    datasource_run = make_datasource_run(run_repo, datasource_run_repo)
    chunk = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="a",
        display_text="b",
        generated_description="c",
    )

    updated = chunk_repo.update(chunk.chunk_id, embeddable_text="A+", display_text="B+")
    assert updated is not None
    assert updated.datasource_run_id == datasource_run.datasource_run_id
    assert updated.embeddable_text == "A+"
    assert updated.display_text == "B+"
    assert updated.created_at == chunk.created_at
    assert updated.generated_description == chunk.generated_description


def test_delete(chunk_repo, datasource_run_repo, run_repo):
    datasource_run = make_datasource_run(run_repo, datasource_run_repo)
    chunk = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="x",
        display_text=None,
        generated_description="d",
    )

    deleted = chunk_repo.delete(chunk.chunk_id)
    assert deleted == 1
    assert chunk_repo.get(chunk.chunk_id) is None


def test_list(chunk_repo, datasource_run_repo, run_repo):
    datasource_run = make_datasource_run(run_repo, datasource_run_repo)
    s1 = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="e1",
        display_text="d1",
        generated_description="g1",
    )
    s2 = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="e2",
        display_text="d2",
        generated_description="g2",
    )
    s3 = chunk_repo.create(
        datasource_run_id=datasource_run.datasource_run_id,
        embeddable_text="e3",
        display_text="d3",
        generated_description="g3",
    )

    all_rows = chunk_repo.list()
    assert [s.chunk_id for s in all_rows] == [s3.chunk_id, s2.chunk_id, s1.chunk_id]


def test_create_with_missing_fk_raises(chunk_repo):
    with pytest.raises(IntegrityError):
        chunk_repo.create(
            datasource_run_id=999_999, embeddable_text="e1", display_text="d1", generated_description="g1"
        )
