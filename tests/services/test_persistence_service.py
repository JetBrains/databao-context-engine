from collections import deque, namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.models import ChunkEmbedding


def test_write_chunks_and_embeddings(persistence, datasource_context_hash_repo, chunk_repo, embedding_repo, table_name):
    chunks = [
        EmbeddableChunk(embeddable_text="A", content="a"),
        EmbeddableChunk(embeddable_text="B", content="b"),
        EmbeddableChunk(embeddable_text="C", content="c"),
    ]
    chunk_embeddings = [
        ChunkEmbedding(
            original_chunk=chunks[0],
            vec=_vec(0.0),
            embedded_text=chunks[0].embeddable_text,
            display_text=chunks[0].content,
        ),
        ChunkEmbedding(
            original_chunk=chunks[1],
            vec=_vec(1.0),
            embedded_text=chunks[1].embeddable_text,
            display_text=chunks[1].content,
        ),
        ChunkEmbedding(
            original_chunk=chunks[2],
            vec=_vec(2.0),
            embedded_text=chunks[2].embeddable_text,
            display_text=chunks[2].content,
        ),
    ]

    datasource_id = DatasourceId.from_string_repr("123.yaml")
    hashed_at = datetime.now()
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=chunk_embeddings,
        table_name=table_name,
        full_type="files/md",
        datasource_id=str(datasource_id),
        context_hash=DatasourceContextHash(
            datasource_id=datasource_id, hash="hash", hash_algorithm="test-algorithm", hashed_at=hashed_at
        ),
    )

    saved_hash = datasource_context_hash_repo.list()
    assert len(saved_hash) == 1
    assert saved_hash[0].datasource_id == "123.yaml"
    assert saved_hash[0].hash == "hash"
    assert saved_hash[0].hash_algorithm == "test-algorithm"
    assert saved_hash[0].hashed_at == hashed_at

    saved = chunk_repo.list()
    assert [c.display_text for c in saved] == ["c", "b", "a"]
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3
    assert rows[0].vec[0] in (0.0, 1.0, 2.0)


def test_empty_pairs_raises_value_error(persistence, table_name):
    with pytest.raises(ValueError):
        datasource_id = DatasourceId.from_string_repr("123.yaml")
        persistence.write_chunks_and_embeddings(
            chunk_embeddings=[],
            table_name=table_name,
            full_type="files/md",
            datasource_id=str(datasource_id),
            context_hash=DatasourceContextHash(
                datasource_id=datasource_id, hash="hash", hash_algorithm="test-algorithm", hashed_at=datetime.now()
            ),
        )


def test_mid_batch_failure_rolls_back(
    persistence, datasource_context_hash_repo, chunk_repo, embedding_repo, monkeypatch, table_name
):
    pairs = [
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="A", content="a"),
            _vec(0.0),
            embedded_text="A",
            display_text="a",
        ),
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="B", content="b"),
            _vec(1.0),
            embedded_text="B",
            display_text="b",
        ),
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="C", content="c"),
            _vec(2.0),
            embedded_text="C",
            display_text="c",
        ),
    ]

    def boom_bulk_insert(*, table_name: str, chunk_ids, vecs, dim):
        raise RuntimeError("boom")

    monkeypatch.setattr(embedding_repo, "bulk_insert", boom_bulk_insert)

    with pytest.raises(RuntimeError):
        datasource_id = DatasourceId.from_string_repr("123.yaml")
        persistence.write_chunks_and_embeddings(
            chunk_embeddings=pairs,
            table_name=table_name,
            full_type="files/md",
            datasource_id=str(datasource_id),
            context_hash=DatasourceContextHash(
                datasource_id=datasource_id, hash="hash", hash_algorithm="test-algorithm", hashed_at=datetime.now()
            ),
        )

    assert datasource_context_hash_repo.list() == []
    assert chunk_repo.list() == []
    assert embedding_repo.list(table_name=table_name) == []


def test_write_chunks_and_embeddings_with_complex_content(
    persistence, datasource_context_hash_repo, chunk_repo, embedding_repo, table_name
):
    class Status(Enum):
        ACTIVE = "active"
        DISABLED = "disabled"

    FileRef = namedtuple("FileRef", "path line")

    @dataclass
    class Owner:
        id: UUID
        email: str
        created_at: datetime

    class Widget:
        def __init__(self, name: str, tags: set[str]):
            self.name = name
            self.tags = tags

        def __repr__(self) -> str:
            return f"Widget(name={self.name!r}, tags={sorted(self.tags)!r})"

    now = datetime.now().replace(microsecond=0)
    owner = Owner(id=uuid4(), email="alice@example.com", created_at=now - timedelta(days=2))
    widget = Widget("w1", {"alpha", "beta"})

    complex_items = [
        (
            "dict",
            {
                "id": 123,
                "status": Status.ACTIVE,
                "owner": owner,
                "price": Decimal("19.99"),
                "path": Path("/srv/models/model.sql"),
                "when": now,
                "tags": {"dbt", "bi"},
                "alias": ("m1", "m2"),
                "file": FileRef(Path("/a/b/c.sql"), 42),
                "queue": deque([1, 2, 3], maxlen=10),
                "wid": uuid4(),
                "widget": widget,
                "bytes": b"\x00\x01\xff",
            },
        ),
        ("enum", Status.DISABLED),
        ("decimal", Decimal("0.000123")),
        ("uuid", uuid4()),
        ("datetime", now),
        ("path", Path("/opt/project/README.md")),
        ("set", {"x", "y", "z"}),
        ("tuple", (1, "two", 3.0)),
        ("namedtuple", FileRef(Path("file.txt"), 7)),
        ("deque", deque([3, 5, 8, 13], maxlen=8)),
        ("dataclass", owner),
        ("custom_repr", widget),
    ]

    pairs = [
        ChunkEmbedding(
            original_chunk=EmbeddableChunk(embeddable_text=et, content=obj),
            vec=_vec(float(i)),
            embedded_text=et,
            display_text=str(obj),
        )
        for i, (et, obj) in enumerate(complex_items)
    ]

    datasource_id = DatasourceId.from_string_repr("123.yaml")
    hashed_at = datetime.now()
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=pairs,
        table_name=table_name,
        full_type="files/md",
        datasource_id=str(datasource_id),
        context_hash=DatasourceContextHash(
            datasource_id=datasource_id, hash="hash", hash_algorithm="test-algorithm", hashed_at=hashed_at
        ),
    )

    saved_hash = datasource_context_hash_repo.list()
    assert len(saved_hash) == 1
    assert saved_hash[0].datasource_id == "123.yaml"
    assert saved_hash[0].hash == "hash"
    assert saved_hash[0].hash_algorithm == "test-algorithm"
    assert saved_hash[0].hashed_at == hashed_at

    saved = chunk_repo.list()
    assert len(saved) == len(complex_items)
    saved_sorted = sorted(saved, key=lambda c: c.chunk_id)
    assert all(isinstance(c.display_text, str) and len(c.display_text) > 0 for c in saved_sorted)
    assert [c.embeddable_text for c in saved_sorted] == [et for et, _ in complex_items]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == len(complex_items)


def test_write_chunks_and_embeddings_override_replaces_datasource_rows(
    persistence, datasource_context_hash_repo, chunk_repo, embedding_repo, table_name
):
    ds1_pairs = [
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="A", content="a"),
            _vec(0.0),
            embedded_text="A",
            display_text="a",
        ),
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="B", content="b"),
            _vec(1.0),
            embedded_text="B",
            display_text="b",
        ),
    ]
    ds2_pairs = [
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="X", content="x"),
            _vec(2.0),
            embedded_text="X",
            display_text="x",
        ),
    ]

    ds1_id = DatasourceId.from_string_repr("ds1.yaml")
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=ds1_pairs,
        table_name=table_name,
        full_type="files/md",
        datasource_id=str(ds1_id),
        context_hash=DatasourceContextHash(
            datasource_id=ds1_id, hash="hash-1", hash_algorithm="test-algorithm", hashed_at=datetime.now()
        ),
    )
    ds2_id = DatasourceId.from_string_repr("ds2.yaml")
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=ds2_pairs,
        table_name=table_name,
        full_type="files/md",
        datasource_id=str(ds2_id),
        context_hash=DatasourceContextHash(
            datasource_id=ds2_id, hash="hash-2", hash_algorithm="test-algorithm", hashed_at=datetime.now()
        ),
    )

    saved_before_hash = datasource_context_hash_repo.list()
    assert len(saved_before_hash) == 2
    assert {
        (datasource_context_hash.datasource_id, datasource_context_hash.hash)
        for datasource_context_hash in saved_before_hash
    } == {("ds2.yaml", "hash-2"), ("ds1.yaml", "hash-1")}

    saved_before = chunk_repo.list()
    old_ds1_chunk_ids = {c.chunk_id for c in saved_before if c.datasource_id == str(ds1_id)}
    assert len(old_ds1_chunk_ids) == 2

    new_ds1_pairs = [
        ChunkEmbedding(
            EmbeddableChunk(embeddable_text="C", content="c"),
            _vec(3.0),
            embedded_text="C",
            display_text="c",
        ),
    ]
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=new_ds1_pairs,
        table_name=table_name,
        full_type="files/md",
        datasource_id=str(ds1_id),
        context_hash=DatasourceContextHash(
            datasource_id=ds1_id, hash="hash-1", hash_algorithm="test-algorithm", hashed_at=datetime.now()
        ),
        override=True,
    )

    saved_after_hash = datasource_context_hash_repo.list()
    assert len(saved_after_hash) == 2
    assert {
        (datasource_context_hash.datasource_id, datasource_context_hash.hash)
        for datasource_context_hash in saved_after_hash
    } == {("ds2.yaml", "hash-2"), ("ds1.yaml", "hash-1")}

    saved_after = chunk_repo.list()

    ds1_rows = [c for c in saved_after if c.datasource_id == str(ds1_id)]
    assert [c.embeddable_text for c in ds1_rows] == ["C"]
    assert {c.chunk_id for c in ds1_rows}.isdisjoint(old_ds1_chunk_ids)

    ds2_rows = [c for c in saved_after if c.datasource_id == str(ds2_id)]
    assert [c.embeddable_text for c in ds2_rows] == ["X"]

    embedding_rows = embedding_repo.list(table_name=table_name)
    assert all(row.chunk_id not in old_ds1_chunk_ids for row in embedding_rows)
    assert len(embedding_rows) == len(ds1_rows) + len(ds2_rows)


def _vec(fill: float, dim: int = 768) -> list[float]:
    return [fill] * dim
