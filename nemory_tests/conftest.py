from __future__ import annotations

import shutil
from pathlib import Path
import duckdb
import pytest

from nemory.core.db.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.core.db.embedding_repository import EmbeddingRepository
from nemory.core.db.entity_repository import EntityRepository
from nemory.core.db.migrate import migrate
from nemory.core.db.run_repository import RunRepository
from nemory.core.db.segment_repository import SegmentRepository
from nemory.core.services.shards.embedding_shard_resolver import EmbeddingShardResolver
from nemory.core.services.shards.table_name_policy import TableNamePolicy


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


@pytest.fixture
def registry_repo(conn) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


@pytest.fixture
def resolver(conn, registry_repo):
    return EmbeddingShardResolver(conn=conn, registry_repo=registry_repo, table_name_policy=TableNamePolicy())


@pytest.fixture
def table_name(conn):
    name = "embedding_tests__dummy_model__768"
    conn.execute("LOAD vss;")
    conn.execute("SET hnsw_enable_experimental_persistence = true;")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            segment_id BIGINT NOT NULL REFERENCES segment(segment_id),
            vec        FLOAT[768] NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (segment_id)
        );
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS emb_hnsw_{name}
        ON {name} USING HNSW (vec) WITH (metric='cosine');
    """)
    return name
