from collections.abc import Sequence

from nemory.embeddings.provider import EmbeddingProvider
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.vector_search_repository import VectorSearchRepository


class QueryService:
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

    def query(self, *, project_id: str, query_text: str, run_name: str | None = None, limit: int = 50) -> list[str]:
        if run_name is None:
            run = self._run_repo.get_latest_for_project(project_id)
            if run is None:
                raise LookupError(f"No runs found for project '{project_id}'. Run a build first.")
        else:
            # TODO: this logic is wrong because we don't have a run_name parameter yet
            # Once we have a run_name parameter, add the find_by_name method to the run_repo and call it here
            run = self._run_repo.get_latest_for_project(project_id)
            if run is None:
                raise LookupError(f"No runs found for project '{project_id}'. Run a build first.")

        table_name, dimension = self._shard_resolver.resolve(
            embedder=self._provider.embedder, model_id=self._provider.model_id
        )

        query_vec: Sequence[float] = self._provider.embed(query_text)

        display_texts = self._vector_search_repo.get_display_texts_by_similarity(
            table_name=table_name,
            run_id=run.run_id,
            query_vec=query_vec,
            dimension=dimension,
            limit=limit,
        )

        return display_texts
