import logging

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.retrieve_embeddings.retrieve_service import RAG_MODE, ContextSearchMode, RetrieveService
from databao_context_engine.storage.repositories.chunk_search_repository import SearchResult

logger = logging.getLogger(__name__)


def retrieve(
    *,
    retrieve_service: RetrieveService,
    text: str,
    limit: int | None,
    datasource_ids: list[DatasourceId] | None = None,
    rag_mode: RAG_MODE | None,
    context_search_mode: ContextSearchMode,
) -> list[SearchResult]:
    return retrieve_service.retrieve(
        text=text,
        limit=limit,
        datasource_ids=datasource_ids,
        rag_mode=rag_mode,
        context_search_mode=context_search_mode,
    )
