from pathlib import Path

import pytest

from databao_context_engine import DatabaoContextDomainManager, DatasourceId, SQLiteConfigFile, SQLiteConnectionConfig
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.datasource_context_repository import DatasourceContextHashRepository
from tests.integration.sqlite_integration_test_utils import create_sqlite_with_base_schema, execute_sqlite_queries
from tests.utils.ollama_test_fakes import FakeOllamaEmbeddingProvider
from tests.utils.project_creation import given_datasource_config_file


@pytest.fixture
def fake_provider() -> FakeOllamaEmbeddingProvider:
    return FakeOllamaEmbeddingProvider()


@pytest.fixture
def use_fake_embedding_provider(mocker, fake_provider):
    mocker.patch("databao_context_engine.build_sources.build_wiring.create_ollama_service", return_value=object())
    mocker.patch(
        "databao_context_engine.build_sources.build_wiring.create_ollama_embedding_provider",
        return_value=fake_provider,
    )


def test_indexing_is_done_in_build_when_no_context_exist(
    project_layout: ProjectLayout, tmp_path: Path, use_fake_embedding_provider
) -> None:
    sqlite1_path = tmp_path / "sqlite1.db"
    create_sqlite_with_base_schema(sqlite1_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite1",
        SQLiteConfigFile(
            name="my_sqlite1", connection=SQLiteConnectionConfig(database_path=str(sqlite1_path))
        ).model_dump(),
    )

    sqlite2_path = tmp_path / "sqlite2.db"
    create_sqlite_with_base_schema(sqlite2_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite2",
        SQLiteConfigFile(
            name="my_sqlite2", connection=SQLiteConnectionConfig(database_path=str(sqlite2_path))
        ).model_dump(),
    )

    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)

    assert not project_layout.output_dir.is_dir()

    domain_manager.build_context()

    sqlite1_datasource_id = DatasourceId.from_string_repr("my_sqlite1.yaml")
    sqlite2_datasource_id = DatasourceId.from_string_repr("my_sqlite2.yaml")

    assert set(project_layout.output_dir.iterdir()) == {
        sqlite1_datasource_id.absolute_path_to_context_file(project_layout),
        sqlite2_datasource_id.absolute_path_to_context_file(project_layout),
        project_layout.db_path,
    }
    with open_duckdb_connection(project_layout.db_path) as conn:
        # Assert we have a hash stored in the DB for each context built
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 2
        assert {context_hash.datasource_id for context_hash in context_hashes_in_db} == {
            str(sqlite1_datasource_id),
            str(sqlite2_datasource_id),
        }

        # Assert we have chunks for each context hash
        chunks = ChunkRepository(conn).list()
        assert {chunk.datasource_context_hash_id for chunk in chunks} == {
            context_hash.datasource_context_hash_id for context_hash in context_hashes_in_db
        }


def test_no_indexing_done_when_context_already_exist(
    project_layout: ProjectLayout, tmp_path: Path, use_fake_embedding_provider
) -> None:
    sqlite1_path = tmp_path / "sqlite1.db"
    create_sqlite_with_base_schema(sqlite1_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite1",
        SQLiteConfigFile(
            name="my_sqlite1", connection=SQLiteConnectionConfig(database_path=str(sqlite1_path))
        ).model_dump(),
    )

    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)
    domain_manager.build_context()

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 1
        sqlite1_initial_hash = context_hashes_in_db[0]
        initial_chunks = ChunkRepository(conn).list()

    # Build again, no new index should have happened
    domain_manager.build_context()

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 1
        # Still the same hash (including the same id and hashed_at that makes sure it wasn't re-inserted)
        assert context_hashes_in_db[0] == sqlite1_initial_hash
        # Same, the chunks shouldn't have changed (same IDs and created_at)
        assert ChunkRepository(conn).list() == initial_chunks

    # Checks that there are no changes when indexing-only
    domain_manager.index_built_contexts()

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 1
        # Still the same hash (including the same id and hashed_at that makes sure it wasn't re-inserted)
        assert context_hashes_in_db[0] == sqlite1_initial_hash
        # Same, the chunks shouldn't have changed (same IDs and created_at)
        assert ChunkRepository(conn).list() == initial_chunks


def test_reindexing_done_when_context_has_changed(
    project_layout: ProjectLayout, tmp_path: Path, use_fake_embedding_provider
) -> None:
    sqlite1_path = tmp_path / "sqlite1.db"
    create_sqlite_with_base_schema(sqlite1_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite1",
        SQLiteConfigFile(
            name="my_sqlite1", connection=SQLiteConnectionConfig(database_path=str(sqlite1_path))
        ).model_dump(),
    )

    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)
    domain_manager.build_context()

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 1
        sqlite1_initial_hash = context_hashes_in_db[0]
        initial_chunks = ChunkRepository(conn).list()

    # Modify the schema of the DB to get the context changed
    execute_sqlite_queries(
        sqlite1_path,
        """
        CREATE TABLE products (
            product_id  INTEGER NOT NULL,
            sku         VARCHAR NOT NULL,
            price       DECIMAL(10,2) NOT NULL,

            CONSTRAINT pk_products PRIMARY KEY (product_id),
            CONSTRAINT uq_products_sku UNIQUE (sku),
            CONSTRAINT chk_products_price CHECK (price >= 0)
        );
        """.strip(),
    )

    domain_manager.build_context()

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        assert len(context_hashes_in_db) == 2
        assert sqlite1_initial_hash in context_hashes_in_db
        assert {context_hash.datasource_id for context_hash in context_hashes_in_db} == {
            sqlite1_initial_hash.datasource_id,
        }
        new_chunks = ChunkRepository(conn).list()
        assert all(initial_chunk in new_chunks for initial_chunk in initial_chunks)
        assert {chunk.datasource_context_hash_id for chunk in new_chunks} == {
            context_hash.datasource_context_hash_id for context_hash in context_hashes_in_db
        }
