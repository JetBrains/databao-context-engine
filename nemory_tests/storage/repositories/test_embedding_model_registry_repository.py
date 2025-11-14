import pytest
import duckdb

from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.services.table_name_policy import TableNamePolicy


@pytest.fixture
def registry_repo(conn) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


def test_create_and_get_roundtrip(registry_repo):
    table = TableNamePolicy().build(embedder="tests", model_id="model:v1", dim=768)

    created = registry_repo.create(embedder="tests", model_id="model:v1", dim=768, table_name=table)
    assert created.embedder == "tests"
    assert created.model_id == "model:v1"
    assert created.dim == 768
    assert created.table_name == table
    assert created.created_at is not None

    got = registry_repo.get(embedder="tests", model_id="model:v1")
    assert got is not None
    assert got.embedder == "tests"
    assert got.model_id == "model:v1"
    assert got.table_name == table
    assert got.dim == 768


def test_get_missing_returns_none(registry_repo):
    assert registry_repo.get(embedder="tests", model_id="missing:v1") is None


def test_pk_conflict_raises_constraint_error(registry_repo):
    table1 = TableNamePolicy().build(embedder="tests", model_id="dup:v1", dim=768)
    table2 = TableNamePolicy().build(embedder="tests", model_id="dup:v1", dim=768)

    registry_repo.create(embedder="tests", model_id="dup:v1", dim=768, table_name=table1)
    with pytest.raises(duckdb.ConstraintException):
        registry_repo.create(embedder="tests", model_id="dup:v1", dim=768, table_name=table2)


def test_unique_table_name_conflict_raises(registry_repo):
    table = TableNamePolicy().build(embedder="tests", model_id="unique-a:v1", dim=768)

    registry_repo.create(embedder="tests", model_id="unique-a:v1", dim=768, table_name=table)
    with pytest.raises(duckdb.ConstraintException):
        registry_repo.create(embedder="other", model_id="unique-b:v1", dim=768, table_name=table)


def test_invalid_table_name_rejected_by_repo_validation(registry_repo):
    with pytest.raises(ValueError):
        registry_repo.create(embedder="tests", model_id="badname:v1", dim=768, table_name="drop table x;--")


def test_delete_existing_returns_1_and_removes_row(registry_repo):
    table = TableNamePolicy().build(embedder="tests", model_id="to-delete:v1", dim=768)
    registry_repo.create(embedder="tests", model_id="to-delete:v1", dim=768, table_name=table)

    deleted = registry_repo.delete(embedder="tests", model_id="to-delete:v1")
    assert deleted == 1
    assert registry_repo.get(embedder="tests", model_id="to-delete:v1") is None


def test_delete_missing_returns_0(registry_repo):
    deleted = registry_repo.delete(embedder="tests", model_id="not-there:v9")
    assert deleted == 0
