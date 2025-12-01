from nemory.embeddings.providers.ollama.service import OllamaService
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
        ollama_service: OllamaService,
    ):
        self._persistence_service = persistence_service
        self._provider = provider
        self._shard_resolver = shard_resolver
        self._ollama_service = ollama_service

    def embed_chunks(self, *, datasource_run_id: int, chunks: list[EmbeddableChunk], result: str) -> None:
        """
        Turn plugin chunks into persisted chunks and embeddings

        Flow:
        1) Embed each chunk into an embedded vector
        2) Get or create embedding table for the appropriate model and embedding dimensions
        3) Persist chunks and embeddings vectors in a single transaction
        """

        if not chunks:
            return

        enriched_embeddings: list[ChunkEmbedding] = []
        for chunk in chunks:
            generated_description = self._ollama_service.describe(text=repr(chunk.content), context=result)

            embedding_text = generated_description + "\n" + chunk.embeddable_text

            vec = self._provider.embed(embedding_text)

            enriched_embeddings.append(
                ChunkEmbedding(
                    chunk=chunk,
                    vec=vec,
                    generated_description=generated_description,
                )
            )

        table_name = self._shard_resolver.resolve_or_create(
            embedder=self._provider.embedder,
            model_id=self._provider.model_id,
            dim=self._provider.dim,
        )

        self._persistence_service.write_chunks_and_embeddings(
            datasource_run_id=datasource_run_id,
            chunk_embeddings=enriched_embeddings,
            table_name=table_name,
        )
