import pytest

from databao_context_engine import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchRepository
from tests.utils.factories import make_chunk, make_embedding

DIM = 768


def test_similarity_returns_display_and_distance(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    chunk = make_chunk(
        chunk_repo,
        full_type="f/type",
        datasource_id="databases/test_clickhouse_db.yml",
        display_text="nice description",
    )
    make_embedding(
        chunk_repo,
        embedding_repo,
        table_name=table_name,
        chunk_id=chunk.chunk_id,
        dim=DIM,
        vec=[1.0] + [0.0] * (DIM - 1),
    )

    repo = VectorSearchRepository(conn)

    retrieve_vec = [1.0] + [0.0] * (DIM - 1)
    results = repo.get_display_texts_by_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=10,
    )

    assert len(results) == 1
    r = results[0]
    assert r.display_text == "nice description"
    assert r.datasource_type == DatasourceType.from_main_and_subtypes("f", "type")
    assert r.datasource_id == DatasourceId.from_string_repr("databases/test_clickhouse_db.yml")
    assert r.cosine_distance == pytest.approx(0.0, abs=1e-6)


def test_limit_is_applied(
    conn,
    chunk_repo,
    embedding_repo,
    table_name,
):
    for i in range(3):
        chunk = make_chunk(
            chunk_repo,
            full_type="f/type",
            datasource_id="databases/test_clickhouse_db.yml",
            display_text=f"c{i}",
        )
        make_embedding(
            chunk_repo,
            embedding_repo,
            table_name=table_name,
            chunk_id=chunk.chunk_id,
            dim=DIM,
            vec=[1.0] + [0.0] * (DIM - 1),
        )

    repo = VectorSearchRepository(conn)
    retrieve_vec = [1.0] + [0.0] * (DIM - 1)

    results = repo.get_display_texts_by_similarity(
        table_name=table_name,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=2,
    )

    assert len(results) == 2
