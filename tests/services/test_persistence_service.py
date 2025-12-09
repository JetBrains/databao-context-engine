from collections import namedtuple, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.storage.exceptions.exceptions import IntegrityError
from nemory.services.models import ChunkEmbedding
from tests.utils.factories import make_datasource_run
import pytest


def test_write_chunks_and_embeddings(
    persistence, run_repo, datasource_run_repo, chunk_repo, embedding_repo, table_name
):
    chunks = [EmbeddableChunk("A", "a"), EmbeddableChunk("B", "b"), EmbeddableChunk("C", "c")]
    chunk_embeddings = [
        ChunkEmbedding(chunk=chunks[0], vec=_vec(0.0), display_text=chunks[0].content, generated_description="g1"),
        ChunkEmbedding(chunk=chunks[1], vec=_vec(1.0), display_text=chunks[1].content, generated_description="g2"),
        ChunkEmbedding(chunk=chunks[2], vec=_vec(2.0), display_text=chunks[2].content, generated_description="g3"),
    ]

    datasource_run = make_datasource_run(run_repo=run_repo, datasource_run_repo=datasource_run_repo)

    persistence.write_chunks_and_embeddings(
        datasource_run_id=datasource_run.datasource_run_id, chunk_embeddings=chunk_embeddings, table_name=table_name
    )

    saved = [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id]
    assert [c.embeddable_text for c in saved] == ["C", "B", "A"]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3
    assert rows[0].vec[0] in (0.0, 1.0, 2.0)


def test_empty_pairs_raises_value_error(persistence, run_repo, datasource_run_repo, table_name):
    datasource_run = make_datasource_run(run_repo=run_repo, datasource_run_repo=datasource_run_repo)

    with pytest.raises(ValueError):
        persistence.write_chunks_and_embeddings(
            datasource_run_id=datasource_run.datasource_run_id, chunk_embeddings=[], table_name=table_name
        )


def test_invalid_fk_rolls_back_entire_batch(persistence, chunk_repo, embedding_repo, table_name):
    pairs = [
        ChunkEmbedding(chunk=EmbeddableChunk("X", "x"), vec=_vec(0.0), display_text="x", generated_description="g1"),
    ]

    with pytest.raises(IntegrityError):
        persistence.write_chunks_and_embeddings(
            datasource_run_id=9_999_999, chunk_embeddings=pairs, table_name=table_name
        )

    assert chunk_repo.list() == []
    assert embedding_repo.list(table_name=table_name) == []


def test_mid_batch_failure_rolls_back(
    persistence, run_repo, datasource_run_repo, chunk_repo, embedding_repo, monkeypatch, table_name
):
    datasource_run = make_datasource_run(run_repo=run_repo, datasource_run_repo=datasource_run_repo)

    pairs = [
        ChunkEmbedding(EmbeddableChunk("A", "a"), _vec(0.0), display_text="a", generated_description="a"),
        ChunkEmbedding(EmbeddableChunk("B", "b"), _vec(1.0), display_text="b", generated_description="b"),
        ChunkEmbedding(EmbeddableChunk("C", "c"), _vec(2.0), display_text="c", generated_description="c"),
    ]

    calls = {"n": 0}
    real_create = embedding_repo.create

    def flaky_create(*, table_name: str, chunk_id: int, vec):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real_create(table_name=table_name, chunk_id=chunk_id, vec=vec)

    monkeypatch.setattr(embedding_repo, "create", flaky_create)

    with pytest.raises(RuntimeError):
        persistence.write_chunks_and_embeddings(
            datasource_run_id=datasource_run.datasource_run_id, chunk_embeddings=pairs, table_name=table_name
        )

    assert [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id] == []
    assert embedding_repo.list(table_name=table_name) == []


def test_write_chunks_and_embeddings_with_complex_content(
    persistence, run_repo, datasource_run_repo, chunk_repo, embedding_repo, table_name
):
    datasource_run = make_datasource_run(
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        plugin="test-plugin",
        source_id="src-1",
        storage_directory="/tmp",
    )

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
        datasource_run_id=datasource_run.datasource_run_id,
        chunk_embeddings=pairs,
        table_name=table_name,
    )

    saved = [c for c in chunk_repo.list() if c.datasource_run_id == datasource_run.datasource_run_id]
    assert len(saved) == len(complex_items)
    saved_sorted = sorted(saved, key=lambda c: c.chunk_id)
    assert [c.embeddable_text for c in saved_sorted] == [et for et, _ in complex_items]
    assert all(isinstance(c.display_text, str) and len(c.display_text) > 0 for c in saved_sorted)

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == len(complex_items)


def _vec(fill: float, dim: int = 768) -> list[float]:
    return [fill] * dim
