from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from databao_context_engine.project.init_project import init_project_dir
from databao_context_engine.project.layout import ProjectLayout, get_config_file
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.services.persistence_service import PersistenceService
from databao_context_engine.services.table_name_policy import TableNamePolicy
from databao_context_engine.storage.migrate import migrate
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.embedding_model_registry_repository import (
    EmbeddingModelRegistryRepository,
)
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository
from databao_context_engine.system.properties import get_db_path

RUN_RECOMMENDED_EXTRAS_OPTION = "--run-recommended-extras"


def pytest_addoption(parser):
    parser.addoption(RUN_RECOMMENDED_EXTRAS_OPTION, action="store_true", default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption(RUN_RECOMMENDED_EXTRAS_OPTION):
        # --run_recommended_extras given in cli: do not skip recommended_extras tests
        return
    # dynamically at pytest.mark.skip annotation to tests marked with recommended_extras
    skip_recommended_extras = pytest.mark.skip(reason=f"need {RUN_RECOMMENDED_EXTRAS_OPTION} option to run")
    for item in items:
        if "recommended_extras" in item.keywords:
            item.add_marker(skip_recommended_extras)


@pytest.fixture(scope="session")
def _template_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    template = tmp_path_factory.mktemp("db_template") / "dce_template.duckdb"
    migrate(template)
    return template


@pytest.fixture()
def dce_path(mocker, tmp_path: Path):
    mocker.patch("databao_context_engine.system.properties._dce_path", new=tmp_path)
    yield tmp_path


@pytest.fixture
def db_path(dce_path: Path) -> Path:
    return get_db_path(dce_path)


@pytest.fixture
def create_db(_template_db: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(_template_db, db_path)


@pytest.fixture
def conn(db_path, create_db):
    conn = duckdb.connect(str(db_path))

    try:
        yield conn
    finally:
        conn.close()


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
    init_project_dir(project_dir=tmp_project_dir)

    return tmp_project_dir


@pytest.fixture
def project_layout(project_path) -> ProjectLayout:
    return ProjectLayout(project_dir=project_path, config_file=get_config_file(project_path))
