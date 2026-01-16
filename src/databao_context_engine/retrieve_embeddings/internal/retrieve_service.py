import logging
from collections.abc import Sequence

from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.project.runs import resolve_run_name_from_repo
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.storage.repositories.run_repository import RunRepository
from databao_context_engine.storage.repositories.vector_search_repository import (
    VectorSearchRepository,
    VectorSearchResult,
)

logger = logging.getLogger(__name__)


class RetrieveService:
    def __init__(
        self,
        *,
        run_repo: RunRepository,
        vector_search_repo: VectorSearchRepository,
        shard_resolver: EmbeddingShardResolver,
        provider: EmbeddingProvider,
    ):
        self._run_repo = run_repo
        self._shard_resolver = shard_resolver
        self._provider = provider
        self._vector_search_repo = vector_search_repo

    def retrieve(
        self, *, project_id: str, text: str, run_name: str, limit: int | None = None
    ) -> list[VectorSearchResult]:
        if limit is None:
            limit = 10

        run = self._run_repo.get_by_run_name(project_id=project_id, run_name=run_name)
        if run is None:
            raise LookupError(f"Run '{run_name}' not found for project '{project_id}'.")

        table_name, dimension = self._shard_resolver.resolve(
            embedder=self._provider.embedder, model_id=self._provider.model_id
        )

        retrieve_vec: Sequence[float] = self._provider.embed(text)

        logger.debug(f"Retrieving display texts for run {run.run_id} in table {table_name}")

        search_results = self._vector_search_repo.get_display_texts_by_similarity(
            table_name=table_name,
            run_id=run.run_id,
            retrieve_vec=retrieve_vec,
            dimension=dimension,
            limit=limit,
        )

        logger.debug(f"Retrieved {len(search_results)} display texts for run {run.run_id} in table {table_name}")

        if logger.isEnabledFor(logging.DEBUG):
            closest_result = min(search_results, key=lambda result: result.cosine_distance)
            logger.debug(f"Best result: ({closest_result.cosine_distance}, {closest_result.embeddable_text})")

            farthest_result = max(search_results, key=lambda result: result.cosine_distance)
            logger.debug(f"Worst result: ({farthest_result.cosine_distance}, {farthest_result.embeddable_text})")

        return search_results

    def resolve_run_name(self, *, project_id: str, run_name: str | None) -> str:
        return resolve_run_name_from_repo(run_repository=self._run_repo, project_id=project_id, run_name=run_name)
