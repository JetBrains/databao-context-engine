from collections import deque, namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.models import ChunkEmbedding


def test_write_chunks_and_embeddings(persistence, chunk_repo, embedding_repo, table_name):
    chunks = [EmbeddableChunk("A", "a"), EmbeddableChunk("B", "b"), EmbeddableChunk("C", "c")]
    chunk_embeddings = [
        ChunkEmbedding(chunk=chunks[0], vec=_vec(0.0), display_text=chunks[0].content, generated_description="g1"),
        ChunkEmbedding(chunk=chunks[1], vec=_vec(1.0), display_text=chunks[1].content, generated_description="g2"),
        ChunkEmbedding(chunk=chunks[2], vec=_vec(2.0), display_text=chunks[2].content, generated_description="g3"),
    ]

    persistence.write_chunks_and_embeddings(
        chunk_embeddings=chunk_embeddings, table_name=table_name, full_type="files/md", datasource_id="123"
    )

    saved = chunk_repo.list()
    assert [c.display_text for c in saved] == ["c", "b", "a"]
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3
    assert rows[0].vec[0] in (0.0, 1.0, 2.0)


def test_empty_pairs_raises_value_error(persistence, table_name):
    with pytest.raises(ValueError):
        persistence.write_chunks_and_embeddings(
            chunk_embeddings=[], table_name=table_name, full_type="files/md", datasource_id="123"
        )


def test_mid_batch_failure_rolls_back(persistence, chunk_repo, embedding_repo, monkeypatch, table_name):
    pairs = [
        ChunkEmbedding(EmbeddableChunk("A", "a"), _vec(0.0), display_text="a", generated_description="a"),
        ChunkEmbedding(EmbeddableChunk("B", "b"), _vec(1.0), display_text="b", generated_description="b"),
        ChunkEmbedding(EmbeddableChunk("C", "c"), _vec(2.0), display_text="c", generated_description="c"),
    ]

    def boom_bulk_insert(*, table_name: str, chunk_ids, vecs, dim):
        raise RuntimeError("boom")

    monkeypatch.setattr(embedding_repo, "bulk_insert", boom_bulk_insert)

    with pytest.raises(RuntimeError):
        persistence.write_chunks_and_embeddings(
            chunk_embeddings=pairs, table_name=table_name, full_type="files/md", datasource_id="123"
        )

    assert chunk_repo.list() == []
    assert embedding_repo.list(table_name=table_name) == []


def test_write_chunks_and_embeddings_with_complex_content(persistence, chunk_repo, embedding_repo, table_name):
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
            chunk=EmbeddableChunk(et, obj), vec=_vec(float(i)), display_text=str(obj), generated_description="g1"
        )
        for i, (et, obj) in enumerate(complex_items)
    ]

    persistence.write_chunks_and_embeddings(
        chunk_embeddings=pairs, table_name=table_name, full_type="files/md", datasource_id="123"
    )

    saved = chunk_repo.list()
    assert len(saved) == len(complex_items)
    saved_sorted = sorted(saved, key=lambda c: c.chunk_id)
    assert all(isinstance(c.display_text, str) and len(c.display_text) > 0 for c in saved_sorted)
    assert [c.embeddable_text for c in saved_sorted] == [et for et, _ in complex_items]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == len(complex_items)


def test_write_chunks_and_embeddings_override_replaces_datasource_rows(
    persistence, chunk_repo, embedding_repo, table_name
):
    ds1_pairs = [
        ChunkEmbedding(EmbeddableChunk("A", "a"), _vec(0.0), display_text="a", generated_description="g"),
        ChunkEmbedding(EmbeddableChunk("B", "b"), _vec(1.0), display_text="b", generated_description="g"),
    ]
    ds2_pairs = [
        ChunkEmbedding(EmbeddableChunk("X", "x"), _vec(2.0), display_text="x", generated_description="g"),
    ]

    persistence.write_chunks_and_embeddings(
        chunk_embeddings=ds1_pairs, table_name=table_name, full_type="files/md", datasource_id="ds1"
    )
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=ds2_pairs, table_name=table_name, full_type="files/md", datasource_id="ds2"
    )

    saved_before = chunk_repo.list()
    old_ds1_chunk_ids = {c.chunk_id for c in saved_before if c.datasource_id == "ds1"}
    assert len(old_ds1_chunk_ids) == 2

    new_ds1_pairs = [
        ChunkEmbedding(EmbeddableChunk("C", "c"), _vec(3.0), display_text="c", generated_description="g"),
    ]
    persistence.write_chunks_and_embeddings(
        chunk_embeddings=new_ds1_pairs,
        table_name=table_name,
        full_type="files/md",
        datasource_id="ds1",
        override=True,
    )

    saved_after = chunk_repo.list()

    ds1_rows = [c for c in saved_after if c.datasource_id == "ds1"]
    assert [c.embeddable_text for c in ds1_rows] == ["C"]
    assert {c.chunk_id for c in ds1_rows}.isdisjoint(old_ds1_chunk_ids)

    ds2_rows = [c for c in saved_after if c.datasource_id == "ds2"]
    assert [c.embeddable_text for c in ds2_rows] == ["X"]

    embedding_rows = embedding_repo.list(table_name=table_name)
    assert all(row.chunk_id not in old_ds1_chunk_ids for row in embedding_rows)
    assert len(embedding_rows) == len(ds1_rows) + len(ds2_rows)


def _vec(fill: float, dim: int = 768) -> list[float]:
    return [fill] * dim
