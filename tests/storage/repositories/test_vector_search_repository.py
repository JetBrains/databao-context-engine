import pytest

from databao_context_engine import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.storage.repositories.vector_search_repository import (
    SearchResult,
    VectorSearchRepository,
)
from tests.utils.factories import make_chunk_and_embedding

DIM = 768


def test_similarity_returns_display_and_distance(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    make_chunk_and_embedding(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        table_name=table_name,
        dimension=DIM,
        full_type="test_type",
        datasource_id="databases/test_clickhouse_db.yaml",
        embeddable_text="raw embeddable",
        display_text="nice description",
    )

    repo = VectorSearchRepository(conn)

    retrieve_vec = [1.0] + [0.0] * (DIM - 1)
    results = repo.search_chunks_by_vector_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=10,
    )

    assert len(results) == 1
    r = results[0]
    assert r.display_text == "nice description"
    assert r.embeddable_text == "raw embeddable"
    assert r.datasource_type == DatasourceType(full_type="test_type")
    assert r.datasource_id == DatasourceId.from_string_repr("databases/test_clickhouse_db.yaml")
    assert r.score.score == pytest.approx(0.0, abs=1e-6)


def test_limit_is_applied(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    for i in range(3):
        make_chunk_and_embedding(
            chunk_repo=chunk_repo,
            embedding_repo=embedding_repo,
            table_name=table_name,
            dimension=DIM,
            full_type="f/type",
            datasource_id="databases/test_clickhouse_db.yaml",
            embeddable_text=f"e{i}",
            display_text=f"c{i}",
        )

    repo = VectorSearchRepository(conn)
    retrieve_vec = [1.0] + [0.0] * (DIM - 1)

    results = repo.search_chunks_by_vector_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=2,
    )

    assert len(results) == 2


def test_search_over_multiple_dataources(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    # Create 2 clickhouse chunks
    for i in range(2):
        make_chunk_and_embedding(
            chunk_repo=chunk_repo,
            embedding_repo=embedding_repo,
            table_name=table_name,
            dimension=DIM,
            full_type="f/type",
            datasource_id="databases/test_clickhouse_db.yaml",
            embeddable_text=f"e{i}",
            display_text=f"c{i}",
        )

    # Create postgres chunk
    make_chunk_and_embedding(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        table_name=table_name,
        dimension=DIM,
        full_type="f/type",
        datasource_id="databases/test_postgres_db.yaml",
        embeddable_text="Embeddable Postgres Chunk",
        display_text="Display Postgres Chunk",
    )

    # Create snowflake chunk
    make_chunk_and_embedding(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        table_name=table_name,
        dimension=DIM,
        full_type="f/type",
        datasource_id="databases/test_snowflake.yaml",
        embeddable_text="Embeddable Snowflake Chunk",
        display_text="Display Snowflake Chunk",
    )

    repo = VectorSearchRepository(conn)
    retrieve_vec = [1.0] + [0.0] * (DIM - 1)

    results = repo.search_chunks_by_vector_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=10,
    )

    assert len(results) == 4
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_snowflake.yaml")
            )
        )
        == 1
    )
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_postgres_db.yaml")
            )
        )
        == 1
    )
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_clickhouse_db.yaml")
            )
        )
        == 2
    )


def test_search_over_multiple_dataources_with_datasource_filter(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    # Create 2 clickhouse chunks
    for i in range(2):
        make_chunk_and_embedding(
            chunk_repo=chunk_repo,
            embedding_repo=embedding_repo,
            table_name=table_name,
            dimension=DIM,
            full_type="f/type",
            datasource_id="databases/test_clickhouse_db.yaml",
            embeddable_text=f"e{i}",
            display_text=f"c{i}",
        )

    # Create postgres chunk
    make_chunk_and_embedding(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        table_name=table_name,
        dimension=DIM,
        full_type="f/type",
        datasource_id="databases/test_postgres_db.yaml",
        embeddable_text="Embeddable Postgres Chunk",
        display_text="Display Postgres Chunk",
    )

    # Create snowflake chunk
    make_chunk_and_embedding(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        table_name=table_name,
        dimension=DIM,
        full_type="f/type",
        datasource_id="databases/test_snowflake.yaml",
        embeddable_text="Embeddable Snowflake Chunk",
        display_text="Display Snowflake Chunk",
    )

    repo = VectorSearchRepository(conn)
    retrieve_vec = [1.0] + [0.0] * (DIM - 1)

    results = repo.search_chunks_by_vector_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=10,
        datasource_ids=[
            DatasourceId.from_string_repr("databases/test_snowflake.yaml"),
            DatasourceId.from_string_repr("databases/test_postgres_db.yaml"),
        ],
    )

    assert len(results) == 2
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_snowflake.yaml")
            )
        )
        == 1
    )
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_postgres_db.yaml")
            )
        )
        == 1
    )
    assert (
        len(
            _get_all_results_for_datasource_id(
                results, datasource_id=DatasourceId.from_string_repr("databases/test_clickhouse_db.yaml")
            )
        )
        == 0
    )


def _get_all_results_for_datasource_id(results: list[SearchResult], datasource_id: DatasourceId) -> list[SearchResult]:
    return [result for result in results if result.datasource_id == datasource_id]
