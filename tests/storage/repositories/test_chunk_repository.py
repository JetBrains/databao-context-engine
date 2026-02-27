from databao_context_engine.storage.models import ChunkDTO


def test_create_and_get(chunk_repo):
    created = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="embed me",
        display_text="visible content",
        keyword_index_text="keyword_index",
    )
    assert isinstance(created, ChunkDTO)

    fetched = chunk_repo.get(created.chunk_id)
    assert fetched == created


def test_update_fields(chunk_repo):
    chunk = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="a",
        display_text="b",
        keyword_index_text="keyword_index",
    )

    updated = chunk_repo.update(chunk.chunk_id, datasource_id="types/txt", embeddable_text="A+", display_text="B+")
    assert updated is not None
    assert updated.datasource_id == "types/txt"
    assert updated.embeddable_text == "A+"
    assert updated.display_text == "B+"
    assert updated.created_at == chunk.created_at


def test_delete(chunk_repo):
    chunk = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        embeddable_text="x",
        display_text="b",
        keyword_index_text="k",
    )

    deleted = chunk_repo.delete(chunk.chunk_id)
    assert deleted == 1
    assert chunk_repo.get(chunk.chunk_id) is None


def test_list(chunk_repo):
    s1 = chunk_repo.create(
        full_type="type/md", datasource_id="12345", embeddable_text="e1", display_text="d1", keyword_index_text="k1"
    )
    s2 = chunk_repo.create(
        full_type="type/md", datasource_id="12345", embeddable_text="e2", display_text="d2", keyword_index_text="k2"
    )
    s3 = chunk_repo.create(
        full_type="type/md", datasource_id="12345", embeddable_text="e3", display_text="d3", keyword_index_text="k3"
    )

    all_rows = chunk_repo.list()
    assert [s.chunk_id for s in all_rows] == [s3.chunk_id, s2.chunk_id, s1.chunk_id]


def test_delete_by_datasource_id(chunk_repo):
    d1_a = chunk_repo.create(
        full_type="type/md", datasource_id="ds1", embeddable_text="a", display_text="a", keyword_index_text="a"
    )
    d1_b = chunk_repo.create(
        full_type="type/md", datasource_id="ds1", embeddable_text="b", display_text="b", keyword_index_text="b"
    )
    d2_c = chunk_repo.create(
        full_type="type/md", datasource_id="ds2", embeddable_text="c", display_text="c", keyword_index_text="c"
    )

    chunk_repo.delete_by_datasource_id(datasource_id="ds1")

    remaining = chunk_repo.list()
    remaining_ids = {c.chunk_id for c in remaining}

    assert d1_a.chunk_id not in remaining_ids
    assert d1_b.chunk_id not in remaining_ids
    assert d2_c.chunk_id in remaining_ids

    assert {c.datasource_id for c in remaining} == {"ds2"}
