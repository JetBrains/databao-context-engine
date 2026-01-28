import logging

from databao_context_engine.retrieve_embeddings.retrieve_service import RetrieveService
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult

logger = logging.getLogger(__name__)


def retrieve(
    *,
    retrieve_service: RetrieveService,
    project_id: str,
    text: str,
    limit: int | None,
) -> list[VectorSearchResult]:
    return retrieve_service.retrieve(project_id=project_id, text=text, limit=limit)
