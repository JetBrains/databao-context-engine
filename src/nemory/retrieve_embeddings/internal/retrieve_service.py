import logging
from collections.abc import Sequence

from nemory.embeddings.provider import EmbeddingProvider
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.vector_search_repository import VectorSearchRepository

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

    def retrieve(self, *, project_id: str, text: str, run_name: str, limit: int | None = None) -> list[str]:
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

        return [result.display_text for result in search_results]

    def resolve_run_name(self, *, project_id: str, run_name: str | None) -> str:
        if run_name is None:
            latest = self._run_repo.get_latest_run_for_project(project_id=project_id)
            if latest is None:
                raise LookupError(f"No runs found for project '{project_id}'. Run a build first.")
            return latest.run_name
        else:
            run = self._run_repo.get_by_run_name(project_id=project_id, run_name=run_name)
            if run is None:
                raise LookupError(f"Run '{run_name}' not found for project '{project_id}'.")
            return run.run_name
