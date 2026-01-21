import logging
from collections.abc import Sequence

from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.storage.repositories.vector_search_repository import (
    VectorSearchRepository,
    VectorSearchResult,
)

logger = logging.getLogger(__name__)


class RetrieveService:
    def __init__(
        self,
        *,
        vector_search_repo: VectorSearchRepository,
        shard_resolver: EmbeddingShardResolver,
        provider: EmbeddingProvider,
    ):
        self._shard_resolver = shard_resolver
        self._provider = provider
        self._vector_search_repo = vector_search_repo

    def retrieve(self, *, project_id: str, text: str, limit: int | None = None) -> list[VectorSearchResult]:
        if limit is None:
            limit = 10

        table_name, dimension = self._shard_resolver.resolve(
            embedder=self._provider.embedder, model_id=self._provider.model_id
        )

        retrieve_vec: Sequence[float] = self._provider.embed(text)

        logger.debug(f"Retrieving display texts \nTODO: fix log message\n in table {table_name}")

        search_results = self._vector_search_repo.get_display_texts_by_similarity(
            table_name=table_name,
            retrieve_vec=retrieve_vec,
            dimension=dimension,
            limit=limit,
        )

        logger.debug(f"Retrieved {len(search_results)} display texts for \nTODO: fix log msg\n in table {table_name}")

        if logger.isEnabledFor(logging.DEBUG):
            closest_result = min(search_results, key=lambda result: result.cosine_distance)
            logger.debug(f"Best result: ({closest_result.cosine_distance}, {closest_result.display_text})")

            farthest_result = max(search_results, key=lambda result: result.cosine_distance)
            logger.debug(f"Worst result: ({farthest_result.cosine_distance}, {farthest_result.display_text})")

        return search_results
