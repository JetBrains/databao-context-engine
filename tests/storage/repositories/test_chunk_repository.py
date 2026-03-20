from datetime import datetime

import pytest

from databao_context_engine.storage.models import ChunkDTO
from databao_context_engine.storage.repositories.datasource_context_repository import DatasourceContextHashRepository
from tests.utils.factories import make_datasource_context_hash


@pytest.fixture
def datasource_context_hash_id(datasource_context_hash_repo: DatasourceContextHashRepository) -> int:
    return make_datasource_context_hash(datasource_context_hash_repo).datasource_context_hash_id


def test_create_and_get(chunk_repo, datasource_context_hash_id):
    created = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="embed me",
        display_text="visible content",
        keyword_index_text="keyword_index",
        datasource_context_hash_id=datasource_context_hash_id,
    )
    assert isinstance(created, ChunkDTO)

    fetched = chunk_repo.get(created.chunk_id)
    assert fetched == created
    assert fetched == ChunkDTO(
        chunk_id=created.chunk_id,
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="embed me",
        display_text="visible content",
        keyword_index_text="keyword_index",
        datasource_context_hash_id=datasource_context_hash_id,
        created_at=created.created_at,
    )


def test_update_fields(chunk_repo, datasource_context_hash_repo, datasource_context_hash_id):
    chunk = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="a",
        display_text="b",
        keyword_index_text="keyword_index",
        datasource_context_hash_id=datasource_context_hash_id,
    )

    new_datasource_context_id = datasource_context_hash_repo.insert(
        datasource_id="test.yaml", hash_algorithm="test-algorithm", hash_="hash-2", hashed_at=datetime.now()
    ).datasource_context_hash_id

    updated = chunk_repo.update(
        chunk.chunk_id,
        datasource_id="types/txt",
        embeddable_text="A+",
        display_text="B+",
        datasource_context_hash_id=new_datasource_context_id,
    )
    assert updated is not None
    assert updated.datasource_id == "types/txt"
    assert updated.embeddable_text == "A+"
    assert updated.display_text == "B+"
    assert updated.created_at == chunk.created_at
    assert updated.datasource_context_hash_id == new_datasource_context_id


def test_delete(chunk_repo, datasource_context_hash_id):
    chunk = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="x",
        display_text="b",
        keyword_index_text="k",
        datasource_context_hash_id=datasource_context_hash_id,
    )

    deleted = chunk_repo.delete(chunk.chunk_id)
    assert deleted == 1
    assert chunk_repo.get(chunk.chunk_id) is None


def test_list(chunk_repo, datasource_context_hash_id):
    s1 = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="e1",
        display_text="d1",
        keyword_index_text="k1",
        datasource_context_hash_id=datasource_context_hash_id,
    )
    s2 = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="e2",
        display_text="d2",
        keyword_index_text="k2",
        datasource_context_hash_id=datasource_context_hash_id,
    )
    s3 = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="e3",
        display_text="d3",
        keyword_index_text="k3",
        datasource_context_hash_id=datasource_context_hash_id,
    )

    all_rows = chunk_repo.list()
    assert [s.chunk_id for s in all_rows] == [s3.chunk_id, s2.chunk_id, s1.chunk_id]


def test_delete_by_datasource_id(chunk_repo, datasource_context_hash_id):
    d1_a = chunk_repo.create(
        full_type="type/md",
        datasource_id="ds1",
        embeddable_text="a",
        display_text="a",
        keyword_index_text="a",
        datasource_context_hash_id=datasource_context_hash_id,
    )
    d1_b = chunk_repo.create(
        full_type="type/md",
        datasource_id="ds1",
        embeddable_text="b",
        display_text="b",
        keyword_index_text="b",
        datasource_context_hash_id=datasource_context_hash_id,
    )
    d2_c = chunk_repo.create(
        full_type="type/md",
        datasource_id="ds2",
        embeddable_text="c",
        display_text="c",
        keyword_index_text="c",
        datasource_context_hash_id=datasource_context_hash_id,
    )

    chunk_repo.delete_by_datasource_id(datasource_id="ds1")

    remaining = chunk_repo.list()
    remaining_ids = {c.chunk_id for c in remaining}

    assert d1_a.chunk_id not in remaining_ids
    assert d1_b.chunk_id not in remaining_ids
    assert d2_c.chunk_id in remaining_ids

    assert {c.datasource_id for c in remaining} == {"ds2"}
