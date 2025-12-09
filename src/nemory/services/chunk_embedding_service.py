from nemory.llm.descriptions.provider import DescriptionProvider
from nemory.llm.embeddings.provider import EmbeddingProvider
from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.serialisation.yaml import to_yaml_string
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.models import ChunkEmbedding
from nemory.services.persistence_service import PersistenceService


class ChunkEmbeddingService:
    def __init__(
        self,
        *,
        persistence_service: PersistenceService,
        embedding_provider: EmbeddingProvider,
        description_provider: DescriptionProvider,
        shard_resolver: EmbeddingShardResolver,
    ):
        self._persistence_service = persistence_service
        self._embedding_provider = embedding_provider
        self._description_provider = description_provider
        self._shard_resolver = shard_resolver

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
            chunk_display_text = to_yaml_string(chunk.content)
            generated_description = self._description_provider.describe(text=chunk_display_text, context=result)

            embedding_text = generated_description + "\n" + chunk.embeddable_text

            vec = self._embedding_provider.embed(embedding_text)

            enriched_embeddings.append(
                ChunkEmbedding(
                    chunk=chunk,
                    vec=vec,
                    display_text=chunk_display_text,
                    generated_description=generated_description,
                )
            )

        table_name = self._shard_resolver.resolve_or_create(
            embedder=self._embedding_provider.embedder,
            model_id=self._embedding_provider.model_id,
            dim=self._embedding_provider.dim,
        )

        self._persistence_service.write_chunks_and_embeddings(
            datasource_run_id=datasource_run_id,
            chunk_embeddings=enriched_embeddings,
            table_name=table_name,
        )
