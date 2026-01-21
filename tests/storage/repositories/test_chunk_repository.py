from databao_context_engine.storage.models import ChunkDTO


def test_create_and_get(chunk_repo):
    created = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        display_text="visible content",
    )
    assert isinstance(created, ChunkDTO)

    fetched = chunk_repo.get(created.chunk_id)
    assert fetched == created


def test_update_fields(chunk_repo):
    chunk = chunk_repo.create(
        full_type="type/md",
        datasource_id="12345",
        display_text="b",
    )

    updated = chunk_repo.update(chunk.chunk_id, datasource_id="types/txt", display_text="B+")
    assert updated is not None
    assert updated.datasource_id == "types/txt"
    assert updated.display_text == "B+"
    assert updated.created_at == chunk.created_at


def test_delete(chunk_repo):
    chunk = chunk_repo.create(full_type="type/md", datasource_id="12345", display_text="b")

    deleted = chunk_repo.delete(chunk.chunk_id)
    assert deleted == 1
    assert chunk_repo.get(chunk.chunk_id) is None


def test_list(chunk_repo):
    s1 = chunk_repo.create(full_type="type/md", datasource_id="12345", display_text="d1")
    s2 = chunk_repo.create(full_type="type/md", datasource_id="12345", display_text="d2")
    s3 = chunk_repo.create(full_type="type/md", datasource_id="12345", display_text="d3")

    all_rows = chunk_repo.list()
    assert [s.chunk_id for s in all_rows] == [s3.chunk_id, s2.chunk_id, s1.chunk_id]
