import pytest

from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchRepository
from tests.utils.factories import make_chunk, make_datasource_run, make_embedding

DIM = 768


def test_similarity_returns_display_and_distance(
    conn,
    run_repo,
    datasource_run_repo,
    chunk_repo,
    embedding_repo,
    table_name,
):
    ds_run = make_datasource_run(run_repo, datasource_run_repo, source_id="src-1")
    chunk = make_chunk(
        run_repo,
        datasource_run_repo,
        chunk_repo,
        datasource_run_id=ds_run.datasource_run_id,
        embeddable_text="raw embeddable",
        display_text="nice description",
    )
    make_embedding(
        run_repo,
        datasource_run_repo,
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
        run_id=ds_run.run_id,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=10,
    )

    assert len(results) == 1
    r = results[0]
    assert r.display_text == "nice description"
    assert r.embeddable_text == "raw embeddable"
    assert r.cosine_distance == pytest.approx(0.0, abs=1e-6)


def test_limit_is_applied(
    conn,
    run_repo,
    datasource_run_repo,
    chunk_repo,
    embedding_repo,
    table_name,
):
    ds = make_datasource_run(run_repo, datasource_run_repo, source_id="src-limit")

    for i in range(3):
        chunk = make_chunk(
            run_repo,
            datasource_run_repo,
            chunk_repo,
            datasource_run_id=ds.datasource_run_id,
            display_text=f"c{i}",
            embeddable_text=f"e{i}",
        )
        make_embedding(
            run_repo,
            datasource_run_repo,
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
        run_id=ds.run_id,
        retrieve_vec=retrieve_vec,
        dimension=DIM,
        limit=2,
    )

    assert len(results) == 2
