from datetime import datetime
from unittest.mock import Mock

from databao_context_engine import DatasourceId
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.llm.config import EmbeddingModelDetails
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.search_context.chunk_search_repository import (
    ChunkType,
    KeywordSearchScore,
    RrfScore,
    SearchResult,
    VectorSearchScore,
)
from databao_context_engine.search_context.search_service import RAG_MODE, ContextSearchMode, SearchContextService


def test_retrieve_returns_results():
    chunk_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.embedding_model_details = EmbeddingModelDetails.default()
    provider.embed.return_value = [0.1, 0.2]

    expected = [
        SearchResult(
            chunk_id=1,
            chunk_type=None,
            display_text="a",
            embeddable_text="a",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
            score=RrfScore(rrf_score=0.5),
        ),
        SearchResult(
            chunk_id=2,
            chunk_type=None,
            display_text="b",
            embeddable_text="b",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
            score=RrfScore(rrf_score=0.51),
        ),
    ]
    chunk_search_repo.search_chunks_with_hybrid_search.return_value = expected

    retrieve_service = SearchContextService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )
    datasource_context_hashes = [_make_datasource_context_hash("full/a.yaml")]

    result = retrieve_service.search(
        search_text="hello world",
        datasource_context_hashes=datasource_context_hashes,
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.HYBRID_SEARCH,
    )

    shard_resolver.resolve.assert_called_once_with(
        embedder="ollama",
        embedding_model_details=EmbeddingModelDetails.default(),
    )

    provider.embed.assert_called_once_with("hello world")

    chunk_search_repo.search_chunks_with_hybrid_search.assert_called_once_with(
        table_name="emb_tbl",
        search_vec=[0.1, 0.2],
        search_text="hello world",
        dimension=768,
        limit=10,
        datasource_context_hashes=datasource_context_hashes,
        chunk_types=None,
    )

    assert result == expected


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
            chunk_type=None,
            display_text="x",
            embeddable_text="x",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/x.yaml"),
            score=RrfScore(rrf_score=0.5),
        ),
    ]
    chunk_search_repo.search_chunks_with_hybrid_search.return_value = expected

    retrieve_service = SearchContextService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )
    datasource_context_hashes = [_make_datasource_context_hash("full/x.yaml")]

    result = retrieve_service.search(
        search_text="q",
        limit=3,
        datasource_context_hashes=datasource_context_hashes,
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.HYBRID_SEARCH,
    )

    chunk_search_repo.search_chunks_with_hybrid_search.assert_called_once()
    _, kwargs = chunk_search_repo.search_chunks_with_hybrid_search.call_args
    assert kwargs["limit"] == 3
    assert result == expected


def test_retrieve_keyword_mode_calls_bm25_search(chunk_types: list[ChunkType] | None, valid_types, raises):
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
            chunk_type=None,
            display_text="kw",
            embeddable_text="kw",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/kw.yaml"),
            score=KeywordSearchScore(bm25_score=0.8),
        ),
    ]
    chunk_search_repo.search_chunks_by_keyword_relevance.return_value = expected
    chunk_search_repo.get_available_chunk_types.return_value = valid_types

    retrieve_service = SearchContextService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )
    datasource_context_hashes = [_make_datasource_context_hash("full/kw.yaml")]
    result = retrieve_service.search(
        search_text="q",
        limit=3,
        datasource_context_hashes=datasource_context_hashes,
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.KEYWORD_SEARCH,
    )

    shard_resolver.resolve.assert_not_called()
    provider.embed.assert_not_called()
    chunk_search_repo.search_chunks_by_keyword_relevance.assert_called_once_with(
        query_text="q",
        limit=3,
        datasource_context_hashes=datasource_context_hashes,
        chunk_types=None,
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
            chunk_type=None,
            display_text="vec",
            embeddable_text="vec",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/vec.yaml"),
            score=VectorSearchScore(vector_distance=0.2),
        ),
    ]
    chunk_search_repo.search_chunks_by_vector_similarity.return_value = expected

    retrieve_service = SearchContextService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=provider,
        prompt_provider=None,
    )
    datasource_context_hashes = [_make_datasource_context_hash("full/vec.yaml")]

    result = retrieve_service.search(
        search_text="q",
        limit=3,
        datasource_context_hashes=datasource_context_hashes,
        rag_mode=RAG_MODE.RAW_QUERY,
        context_search_mode=ContextSearchMode.VECTOR_SEARCH,
    )

    chunk_search_repo.search_chunks_by_vector_similarity.assert_called_once_with(
        table_name="tbl",
        search_vec=[0.5] * 768,
        dimension=768,
        limit=3,
        datasource_context_hashes=datasource_context_hashes,
        chunk_types=None,
    )
    assert result == expected


def _make_datasource_context_hash(datasource_id: str) -> DatasourceContextHash:
    return DatasourceContextHash(
        datasource_id=DatasourceId.from_string_repr(datasource_id),
        hash="hash",
        hash_algorithm="xxh3",
        hashed_at=datetime.now(),
    )
