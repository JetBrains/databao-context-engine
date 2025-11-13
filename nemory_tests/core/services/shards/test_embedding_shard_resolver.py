import pytest

from nemory.core.services.shards.embedding_shard_resolver import EmbeddingShardResolver
from nemory.core.services.shards.table_name_policy import TableNamePolicy


def test_resolve_existing_returns_table_name(conn, registry_repo):
    table_name = "embedding_tests__model_v1__768"
    registry_repo.create(embedder="tests", model_id="model:v1", dim=768, table_name=table_name)

    resolver = EmbeddingShardResolver(conn=conn, registry_repo=registry_repo)
    resolved_table_name = resolver.resolve(embedder="tests", model_id="model:v1")
    assert table_name == resolved_table_name


def test_resolve_or_create_creates_table_index_and_registry(conn, registry_repo, resolver):
    table_name = TableNamePolicy().build(embedder="ollama", model_id="nomic-embed-text:v1.5", dim=768)

    resolved_table_name = resolver.resolve_or_create(embedder="ollama", model_id="nomic-embed-text:v1.5", dim=768)

    assert table_name == resolved_table_name

    assert _table_exists(conn, table_name)
    assert _hnsw_index_exists(conn, table_name)

    got = registry_repo.get(embedder="ollama", model_id="nomic-embed-text:v1.5")
    assert got is not None
    assert got.table_name == table_name
    assert got.dim == 768


def test_resolve_or_create_is_idempotent(conn, registry_repo, resolver):
    table_name1 = resolver.resolve_or_create(embedder="tests", model_id="idempotent:v1", dim=768)
    table_name2 = resolver.resolve_or_create(embedder="tests", model_id="idempotent:v1", dim=768)

    assert table_name1 == table_name2

    rows = conn.execute(
        "SELECT COUNT(*) FROM embedding_model_registry WHERE embedder = ? AND model_id = ?",
        ["tests", "idempotent:v1"],
    ).fetchone()
    assert rows[0] == 1


def test_resolve_or_create_conflicting_dim_raises(conn, registry_repo, resolver):
    resolver.resolve_or_create(embedder="tests", model_id="conflict:v1", dim=768)

    with pytest.raises(ValueError):
        resolver.resolve_or_create(embedder="tests", model_id="conflict:v1", dim=1024)


def test_table_name_policy_replaces_unsafe_chars(conn, resolver):
    table_name = resolver.resolve_or_create(embedder="e", model_id="m-1:beta", dim=256)
    assert "m_1_beta" in table_name
    assert table_name.startswith("embedding_e__")
    assert table_name.endswith("__256")


def _table_exists(conn, name: str) -> bool:
    rows = conn.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchall()
    return bool(rows)


def _hnsw_index_exists(conn, table_name: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM duckdb_indexes() WHERE table_name = ? AND sql ILIKE '%HNSW%'",
        [table_name],
    ).fetchall()
    return bool(rows)
