from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from nemory.project.init_project import init_project_dir
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.persistence_service import PersistenceService
from nemory.services.run_name_policy import RunNamePolicy
from nemory.services.table_name_policy import TableNamePolicy
from nemory.storage.migrate import migrate
from nemory.storage.repositories.chunk_repository import ChunkRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.run_repository import RunRepository
from nemory.system.properties import get_db_path


@pytest.fixture(scope="session")
def _template_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    template = tmp_path_factory.mktemp("db_template") / "nemory_template.duckdb"
    migrate(template)
    return template


@pytest.fixture()
def nemory_path(mocker, tmp_path: Path):
    mocker.patch("nemory.system.properties._nemory_path", new=tmp_path)
    yield tmp_path


@pytest.fixture
def db_path(nemory_path: Path) -> Path:
    return get_db_path(nemory_path)


@pytest.fixture
def create_db(_template_db: Path, db_path: Path) -> None:
    shutil.copy(_template_db, db_path)


@pytest.fixture
def conn(db_path, create_db):
    conn = duckdb.connect(str(db_path))

    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def run_repo(conn) -> RunRepository:
    return RunRepository(conn, run_name_policy=RunNamePolicy())


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


@pytest.fixture
def project_path(tmp_path) -> Path:
    tmp_project_dir = tmp_path.joinpath("project_dir")
    tmp_project_dir.mkdir(parents=True, exist_ok=True)
    init_project_dir(project_dir=str(tmp_project_dir))

    return tmp_project_dir
