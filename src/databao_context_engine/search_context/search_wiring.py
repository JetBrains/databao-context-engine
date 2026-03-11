import os

from duckdb import DuckDBPyConnection

import databao_context_engine.perf.core as perf
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.factory import (
    create_ollama_embedding_provider,
    create_ollama_prompt_provider,
    create_ollama_service,
)
from databao_context_engine.llm.prompts.provider import PromptProvider
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.search_context.chunk_search_repository import ChunkSearchRepository, SearchResult
from databao_context_engine.search_context.search_service import RAG_MODE, ContextSearchMode, SearchContextService
from databao_context_engine.services.factories import create_shard_resolver
from databao_context_engine.storage.connection import open_duckdb_connection


@perf.perf_run(
    operation="search_context",
    attrs=lambda *, search_text, limit, datasource_ids, context_search_mode, **_: {
        "search_text_length": len(search_text),
        "limit": limit,
        "datasources_number": len(datasource_ids) if datasource_ids else -1,
        "context_search_mode": context_search_mode.value,
    },
)
@perf.perf_span("search_context.total")
def search_context(
    project_layout: ProjectLayout,
    search_text: str,
    limit: int | None,
    datasource_ids: list[DatasourceId] | None,
    context_search_mode: ContextSearchMode,
) -> list[SearchResult]:
    with open_duckdb_connection(project_layout.db_path) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(
            ollama_service, model_details=project_layout.project_config.ollama_embedding_model_details
        )
        rag_mode = _get_rag_mode()
        prompt_provider = create_ollama_prompt_provider(ollama_service) if rag_mode == RAG_MODE.REWRITE_QUERY else None

        search_context_service = _create_search_context_service(
            conn, embedding_provider=embedding_provider, prompt_provider=prompt_provider
        )
        return search_context_service.search(
            search_text=search_text,
            limit=limit,
            datasource_ids=datasource_ids,
            rag_mode=rag_mode,
            context_search_mode=context_search_mode,
        )


def _get_rag_mode() -> RAG_MODE:
    rag_mode_env_var = os.environ.get("DATABAO_CONTEXT_RAG_MODE")
    if rag_mode_env_var:
        try:
            return RAG_MODE(rag_mode_env_var)
        except ValueError:
            pass

    return RAG_MODE.RAW_QUERY


def _create_search_context_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
    prompt_provider: PromptProvider | None,
) -> SearchContextService:
    chunk_search_repo = _create_chunk_search_repository(conn)
    shard_resolver = create_shard_resolver(conn)

    return SearchContextService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=embedding_provider,
        prompt_provider=prompt_provider,
    )


def _create_chunk_search_repository(conn: DuckDBPyConnection) -> ChunkSearchRepository:
    return ChunkSearchRepository(conn)
