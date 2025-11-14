from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.services.models import ChunkEmbedding
from nemory.services.persistence_service import PersistenceService
from nemory.embeddings.provider import EmbeddingProvider
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver

class ChunkEmbeddingService:
    def __init__(
        self,
        *,
        persistence_service: PersistenceService,
        provider: EmbeddingProvider,
        shard_resolver: EmbeddingShardResolver,
    ):
        self._persistence_service = persistence_service
        self._provider = provider
        self._shard_resolver = shard_resolver

    def embed_chunks(self, *, datasource_run_id: int, chunks: list[EmbeddableChunk]) -> None:
        """
        Turn plugin chunks into persisted chunks and embeddings

        Flow:
        1) Embed each chunk into an embedded vector
        2) Get or create embedding table for the appropriate model and embedding dimensions
        3) Persist chunks and embeddings vectors in a single transaction
        """

        if not chunks:
            return

        chunk_embeddings: list[ChunkEmbedding] = [
            ChunkEmbedding(chunk=chunk, vec = self._provider.embed(chunk.embeddable_text)) for chunk in chunks
        ]

        table_name = self._shard_resolver.resolve_or_create(
            embedder=self._provider.embedder,
            model_id=self._provider.model_id,
            dim=self._provider.dim,
        )

        self._persistence_service.write_chunks_and_embeddings(
            datasource_run_id=datasource_run_id,
            chunk_embeddings=chunk_embeddings,
            table_name=table_name,
        )
