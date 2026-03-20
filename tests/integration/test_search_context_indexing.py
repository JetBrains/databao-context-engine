from pathlib import Path

import pytest

from databao_context_engine import (
    DatabaoContextDomainManager,
    DatasourceId,
    SQLiteConfigFile,
    SQLiteConnectionConfig,
)
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.search_context.search_service import ContextSearchMode
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
    mocker.patch("databao_context_engine.search_context.search_wiring.create_ollama_service", return_value=object())
    mocker.patch(
        "databao_context_engine.build_sources.build_wiring.create_ollama_embedding_provider",
        return_value=fake_provider,
    )
    mocker.patch(
        "databao_context_engine.search_context.search_wiring.create_ollama_embedding_provider",
        return_value=fake_provider,
    )


def _assert_current_hash_is_not_indexed(*, indexed_hashes, current_context_hash) -> None:
    assert all(
        not (
            context_hash.datasource_id == str(current_context_hash.datasource_id)
            and context_hash.hash == current_context_hash.hash
            and context_hash.hash_algorithm == current_context_hash.hash_algorithm
        )
        for context_hash in indexed_hashes
    )


def _find_indexed_hash(*, indexed_hashes, current_context_hash):
    return next(
        context_hash
        for context_hash in indexed_hashes
        if context_hash.datasource_id == str(current_context_hash.datasource_id)
        and context_hash.hash == current_context_hash.hash
        and context_hash.hash_algorithm == current_context_hash.hash_algorithm
    )


def _assert_chunks_exist_for_hashes(*, chunks, indexed_hashes) -> None:
    assert {chunk.datasource_context_hash_id for chunk in chunks}.issuperset(
        {context_hash.datasource_context_hash_id for context_hash in indexed_hashes}
    )


def test_search_context_skips_reindex_when_requested_contexts_are_already_indexed(
    project_layout: ProjectLayout,
    tmp_path: Path,
    use_fake_embedding_provider,
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

    # Build and index both datasources
    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)
    domain_manager.build_context()

    with open_duckdb_connection(project_layout.db_path) as conn:
        initial_context_hashes = DatasourceContextHashRepository(conn).list()
        initial_chunks = ChunkRepository(conn).list()

    engine = domain_manager.get_engine_for_domain()

    sqlite1_datasource_id = DatasourceId.from_string_repr("my_sqlite1.yaml")

    # Search the context in sqlite1 should not trigger a re-index (chunks and hashes won't change)
    results = engine.search_context(
        "users",
        datasource_ids=[sqlite1_datasource_id],
        context_search_mode=ContextSearchMode.KEYWORD_SEARCH,
    )

    assert results
    assert {result.datasource_id for result in results} == {sqlite1_datasource_id}

    with open_duckdb_connection(project_layout.db_path) as conn:
        assert DatasourceContextHashRepository(conn).list() == initial_context_hashes
        assert ChunkRepository(conn).list() == initial_chunks


def test_search_context_indexes_unindexed_and_outdated_contexts(
    project_layout: ProjectLayout,
    tmp_path: Path,
    use_fake_embedding_provider,
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

    # Build and index only my_sqlite1
    domain_manager = DatabaoContextDomainManager(domain_dir=project_layout.project_dir)
    domain_manager.build_context()

    sqlite1_datasource_id = DatasourceId.from_string_repr("my_sqlite1.yaml")

    sqlite2_path = tmp_path / "sqlite2.db"
    create_sqlite_with_base_schema(sqlite2_path)
    given_datasource_config_file(
        project_layout,
        "my_sqlite2",
        SQLiteConfigFile(
            name="my_sqlite2", connection=SQLiteConnectionConfig(database_path=str(sqlite2_path))
        ).model_dump(),
    )

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

    # Build my_sqlite2 and my_sqlite1 after the schema change, but don't index
    domain_manager.build_context(should_index=False)
    engine = domain_manager.get_engine_for_domain()

    sqlite2_datasource_id = DatasourceId.from_string_repr("my_sqlite2.yaml")
    sqlite1_current_context = engine.get_datasource_context(sqlite1_datasource_id)
    sqlite2_current_context = engine.get_datasource_context(sqlite2_datasource_id)

    with open_duckdb_connection(project_layout.db_path) as conn:
        hash_repo = DatasourceContextHashRepository(conn)
        chunk_repo = ChunkRepository(conn)
        initial_context_hashes = hash_repo.list()
        initial_chunks = chunk_repo.list()

        assert len(initial_context_hashes) == 1
        _assert_current_hash_is_not_indexed(
            indexed_hashes=initial_context_hashes,
            current_context_hash=sqlite1_current_context.context_hash,
        )
        _assert_current_hash_is_not_indexed(
            indexed_hashes=initial_context_hashes,
            current_context_hash=sqlite2_current_context.context_hash,
        )

    # Searching should reindex both datasources
    results = engine.search_context(
        "users",
        context_search_mode=ContextSearchMode.KEYWORD_SEARCH,
    )

    assert results
    assert {result.datasource_id for result in results}.issubset({sqlite1_datasource_id, sqlite2_datasource_id})

    with open_duckdb_connection(project_layout.db_path) as conn:
        context_hashes_in_db = DatasourceContextHashRepository(conn).list()
        new_chunks = ChunkRepository(conn).list()

    assert len(context_hashes_in_db) == 3
    assert len(new_chunks) > len(initial_chunks)

    sqlite1_current_hash_in_db = _find_indexed_hash(
        indexed_hashes=context_hashes_in_db,
        current_context_hash=sqlite1_current_context.context_hash,
    )
    sqlite2_current_hash_in_db = _find_indexed_hash(
        indexed_hashes=context_hashes_in_db,
        current_context_hash=sqlite2_current_context.context_hash,
    )

    _assert_chunks_exist_for_hashes(
        chunks=new_chunks,
        indexed_hashes=[sqlite1_current_hash_in_db, sqlite2_current_hash_in_db],
    )


def test_search_context_fails_when_requested_datasource_has_no_context_file(
    project_layout: ProjectLayout,
    tmp_path: Path,
    use_fake_embedding_provider,
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
    engine = domain_manager.get_engine_for_domain()

    missing_datasource_id = DatasourceId.from_string_repr("missing.yaml")

    with pytest.raises(ValueError, match=r"Context file not found for datasource missing\.yaml"):
        engine.search_context(
            "users",
            datasource_ids=[missing_datasource_id],
            context_search_mode=ContextSearchMode.KEYWORD_SEARCH,
        )
