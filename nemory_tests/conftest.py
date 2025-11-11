from __future__ import annotations

import shutil
from pathlib import Path
import duckdb
import pytest

from nemory.core.db.embedding_repository import EmbeddingRepository
from nemory.core.db.entity_repository import EntityRepository
from nemory.core.db.migrate import migrate
from nemory.core.db.run_repository import RunRepository
from nemory.core.db.segment_repository import SegmentRepository


@pytest.fixture(scope="session")
def _template_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    template = tmp_path_factory.mktemp("db_template") / "nemory_template.duckdb"
    migrate(template)
    return template


@pytest.fixture
def conn(_template_db: Path, tmp_path: Path):
    db_path = tmp_path / "nemory_test.duckdb"
    shutil.copy(_template_db, db_path)
    conn = duckdb.connect(str(db_path))

    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def run_repo(conn) -> RunRepository:
    return RunRepository(conn)


@pytest.fixture
def entity_repo(conn) -> EntityRepository:
    return EntityRepository(conn)


@pytest.fixture
def segment_repo(conn) -> SegmentRepository:
    return SegmentRepository(conn)


@pytest.fixture
def embedding_repo(conn) -> EmbeddingRepository:
    conn.execute("LOAD vss;")
    return EmbeddingRepository(conn)
