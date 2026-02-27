from unittest.mock import Mock

from databao_context_engine import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.retrieve_embeddings.retrieve_service import RAG_MODE, ContextSearchMode, RetrieveService
from databao_context_engine.storage.repositories.chunk_search_repository import (
    KeywordSearchScore,
    RrfScore,
    SearchResult,
    VectorSearchScore,
)


def test_retrieve_returns_results():
    run_repo = Mock()
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 42
    run_repo.get_by_run_name.return_value = run

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    expected = [
        SearchResult(
            chunk_id=1,
            display_text="a",
            embeddable_text="a",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
            score=RrfScore(rrf_score=0.5),
        ),
        SearchResult(
            chunk_id=2,
            display_text="b",
            embeddable_text="b",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
            score=RrfScore(rrf_score=0.51),
        ),
    ]
    chunk_search_repo.search_chunks_with_hybrid_search.return_value = expected

    retrieve_service = RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )

    result = retrieve_service.retrieve(
        text="hello world",
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.HYBRID_SEARCH,
    )

    shard_resolver.resolve.assert_called_once_with(
        embedder="ollama",
        model_id="nomic-embed-text",
    )

    provider.embed.assert_called_once_with("hello world")

    chunk_search_repo.search_chunks_with_hybrid_search.assert_called_once_with(
        table_name="emb_tbl",
        retrieve_vec=[0.1, 0.2],
        query_text="hello world",
        dimension=768,
        limit=10,
        datasource_ids=None,
    )

    assert result == expected


def test_retrieve_uses_run_name_if_provided():
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    chunk_search_repo.search_chunks_with_hybrid_search.return_value = [
        SearchResult(
            chunk_id=1,
            display_text="a",
            embeddable_text="a",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
            score=RrfScore(rrf_score=0.5),
        ),
        SearchResult(
            chunk_id=2,
            display_text="b",
            embeddable_text="b",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
            score=RrfScore(rrf_score=0.51),
        ),
    ]

    retrieve_service = RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )

    retrieve_service.retrieve(
        text="hello world",
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.HYBRID_SEARCH,
    )


def test_retrieve_honors_limit():
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    expected = [
        SearchResult(
            chunk_id=1,
            display_text="x",
            embeddable_text="x",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/x.yaml"),
            score=RrfScore(rrf_score=0.5),
        ),
    ]
    chunk_search_repo.search_chunks_with_hybrid_search.return_value = expected

    retrieve_service = RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )

    result = retrieve_service.retrieve(
        text="q",
        limit=3,
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.HYBRID_SEARCH,
    )

    chunk_search_repo.search_chunks_with_hybrid_search.assert_called_once()
    _, kwargs = chunk_search_repo.search_chunks_with_hybrid_search.call_args
    assert kwargs["limit"] == 3
    assert result == expected


def test_retrieve_keyword_mode_calls_bm25_search():
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    expected = [
        SearchResult(
            chunk_id=1,
            display_text="kw",
            embeddable_text="kw",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/kw.yaml"),
            score=KeywordSearchScore(bm25_score=0.8),
        ),
    ]
    chunk_search_repo.search_chunks_by_keyword_relevance.return_value = expected

    retrieve_service = RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )

    result = retrieve_service.retrieve(
        text="q", limit=3, rag_mode=RAG_MODE.RAW_QUERY, context_search_mode=ContextSearchMode.KEYWORD_SEARCH
    )

    shard_resolver.resolve.assert_not_called()
    provider.embed.assert_not_called()
    chunk_search_repo.search_chunks_by_keyword_relevance.assert_called_once_with(
        query_text="q",
        limit=3,
        datasource_ids=None,
    )
    assert result == expected


def test_retrieve_vector_mode_calls_vector_search():
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    expected = [
        SearchResult(
            chunk_id=1,
            display_text="vec",
            embeddable_text="vec",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/vec.yaml"),
            score=VectorSearchScore(vector_distance=0.2),
        ),
    ]
    chunk_search_repo.search_chunks_by_vector_similarity.return_value = expected

    retrieve_service = RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )

    result = retrieve_service.retrieve(
        text="q", limit=3, rag_mode=RAG_MODE.RAW_QUERY, context_search_mode=ContextSearchMode.VECTOR_SEARCH
    )

    chunk_search_repo.search_chunks_by_vector_similarity.assert_called_once_with(
        table_name="tbl",
        retrieve_vec=[0.5] * 768,
        dimension=768,
        limit=3,
        datasource_ids=None,
    )
    assert result == expected
