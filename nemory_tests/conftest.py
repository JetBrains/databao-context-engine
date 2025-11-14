from __future__ import annotations

import shutil
from pathlib import Path
import duckdb
import pytest

from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.migrate import migrate
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.chunk_repository import ChunkRepository
from nemory.services.persistence_service import PersistenceService
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.table_name_policy import TableNamePolicy


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
def datasource_run_repo(conn) -> DatasourceRunRepository:
    return DatasourceRunRepository(conn)


@pytest.fixture
def chunk_repo(conn) -> ChunkRepository:
    return ChunkRepository(conn)


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
def persistence(conn, chunk_repo, embedding_repo):
    return PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)


@pytest.fixture
def table_name(conn):
    name = "embedding_tests__dummy_model__768"
    conn.execute("LOAD vss;")
    conn.execute("SET hnsw_enable_experimental_persistence = true;")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            chunk_id BIGINT NOT NULL REFERENCES chunk(chunk_id),
            vec        FLOAT[768] NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chunk_id)
        );
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS emb_hnsw_{name}
        ON {name} USING HNSW (vec) WITH (metric='cosine');
    """)
    return name
