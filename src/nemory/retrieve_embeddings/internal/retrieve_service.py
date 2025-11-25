from collections.abc import Sequence

from nemory.embeddings.provider import EmbeddingProvider
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.vector_search_repository import VectorSearchRepository


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

    def retrieve(self, *, project_id: str, text: str, run_name: str, limit: int = 50) -> list[str]:
        run = self._run_repo.get_by_run_name(project_id=project_id, run_name=run_name)
        if run is None:
            raise LookupError(f"Run '{run_name}' not found for project '{project_id}'.")

        table_name, dimension = self._shard_resolver.resolve(
            embedder=self._provider.embedder, model_id=self._provider.model_id
        )

        retrieve_vec: Sequence[float] = self._provider.embed(text)

        display_texts = self._vector_search_repo.get_display_texts_by_similarity(
            table_name=table_name,
            run_id=run.run_id,
            retrieve_vec=retrieve_vec,
            dimension=dimension,
            limit=limit,
        )

        return display_texts

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
