from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb
import pytest

from nemory.build_sources.public.api import build_all_datasources
from nemory.storage.migrate import migrate


@dataclass(frozen=True)
class _FakeProvider:
    embedder: str = "fake"
    model_id: str = "dummy"
    dim: int = 768

    def embed(self, text: str) -> list[float]:
        seed = float(len(text) % 10)
        return [seed] * self.dim


def _write_project(project_dir: Path) -> None:
    src = project_dir / "src"
    (src / "files").mkdir(parents=True, exist_ok=True)

    (src / "files" / "note.md").write_text("# Hello\nworld\n", encoding="utf-8")

    (project_dir / "nemory.ini").write_text(
        "[DEFAULT]\nproject-id=" + str(uuid4()) + "\n",
        encoding="utf-8",
    )


def _shard_rows(conn, table_name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _duckdb_has_table(conn, name: str) -> bool:
    rows = conn.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchall()
    return bool(rows)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    _write_project(tmp_path)
    return tmp_path


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "nemory_test.duckdb"


@pytest.fixture(autouse=True)
def _force_test_db(monkeypatch, db_path: Path):
    monkeypatch.setattr(
        "nemory.build_sources.public.api.get_db_path",
        lambda *a, **k: db_path,
        raising=False,
    )
    monkeypatch.setattr(
        "nemory.build_sources.internal.build_wiring.get_db_path",
        lambda *a, **k: db_path,
        raising=False,
    )
    monkeypatch.setattr(
        "nemory.project.layout.get_db_path",
        lambda *a, **k: db_path,
        raising=False,
    )
    monkeypatch.setenv("NEMORY_DB_PATH", str(db_path))


@pytest.fixture
def conn(db_path: Path):
    """
    Open a connection to the SAME db file the app will use (db_path).
    Run migrations and load extensions so repo assertions work.
    """
    migrate(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("LOAD vss;")
        con.execute("SET hnsw_enable_experimental_persistence = true;")
        yield con
    finally:
        con.close()


@pytest.fixture
def fake_provider() -> _FakeProvider:
    return _FakeProvider()


@pytest.fixture
def use_fake_provider(mocker, fake_provider):
    return mocker.patch(
        "nemory.build_sources.internal.build_wiring.OllamaEmbeddingProvider",
        return_value=fake_provider,
    )


def test_e2e_build_with_fake_provider(
    project_dir, db_path, conn, run_repo, chunk_repo, embedding_repo, registry_repo, use_fake_provider, fake_provider
):
    build_all_datasources(project_dir=project_dir)

    runs = run_repo.list()
    assert len(runs) == 1
    assert runs[0].ended_at is not None

    chunks = chunk_repo.list()
    assert len(chunks) >= 1

    reg = registry_repo.get(embedder=fake_provider.embedder, model_id=fake_provider.model_id)
    assert reg is not None
    assert reg.dim == fake_provider.dim
    assert _duckdb_has_table(conn, reg.table_name)

    count = _shard_rows(conn, reg.table_name)
    assert count == len(chunks)


def test_one_source_fails_but_others_succeed(
    mocker, project_dir, conn, run_repo, chunk_repo, embedding_repo, registry_repo, use_fake_provider, fake_provider
):
    import nemory.build_sources.internal.plugin_execution as execmod

    original_execute = execmod.execute

    def flaky_execute(source, plugin):
        if source.path.name.endswith("pg.yaml"):
            raise RuntimeError("boom")
        return original_execute(source, plugin)

    mocker.patch.object(execmod, "execute", side_effect=flaky_execute)

    build_all_datasources(project_dir=project_dir)

    runs = run_repo.list()
    assert len(runs) == 1 and runs[0].ended_at is not None

    chunks = chunk_repo.list()
    assert len(chunks) >= 1

    reg = registry_repo.get(embedder=fake_provider.embedder, model_id=fake_provider.model_id)
    assert reg is not None
    assert _shard_rows(conn, reg.table_name) == len(chunks)
