import logging
from enum import Enum
from typing import cast

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.serialisation.yaml import to_yaml_string
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.services.models import ChunkEmbedding
from databao_context_engine.services.persistence_service import PersistenceService

logger = logging.getLogger(__name__)


class ChunkEmbeddingMode(Enum):
    EMBEDDABLE_TEXT_ONLY = "EMBEDDABLE_TEXT_ONLY"
    GENERATED_DESCRIPTION_ONLY = "GENERATED_DESCRIPTION_ONLY"
    EMBEDDABLE_TEXT_AND_GENERATED_DESCRIPTION = "EMBEDDABLE_TEXT_AND_GENERATED_DESCRIPTION"

    def should_generate_description(self) -> bool:
        return self in (
            ChunkEmbeddingMode.GENERATED_DESCRIPTION_ONLY,
            ChunkEmbeddingMode.EMBEDDABLE_TEXT_AND_GENERATED_DESCRIPTION,
        )


class ChunkEmbeddingService:
    def __init__(
        self,
        *,
        persistence_service: PersistenceService,
        embedding_provider: EmbeddingProvider,
        description_provider: DescriptionProvider | None,
        shard_resolver: EmbeddingShardResolver,
        chunk_embedding_mode: ChunkEmbeddingMode = ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
    ):
        self._persistence_service = persistence_service
        self._embedding_provider = embedding_provider
        self._description_provider = description_provider
        self._shard_resolver = shard_resolver
        self._chunk_embedding_mode = chunk_embedding_mode

        if self._chunk_embedding_mode.should_generate_description() and description_provider is None:
            raise ValueError("A DescriptionProvider must be provided when generating descriptions")

    def embed_chunks(self, *, chunks: list[EmbeddableChunk], result: str, full_type: str, datasource_id: str) -> None:
        """Turn plugin chunks into persisted chunks and embeddings.

        Flow:
        1) Embed each chunk into an embedded vector.
        2) Get or create embedding table for the appropriate model and embedding dimensions.
        3) Persist chunks and embeddings vectors in a single transaction.
        """
        if not chunks:
            return

        logger.debug(
            f"Embedding {len(chunks)} chunks for datasource {datasource_id}, with chunk_embedding_mode={self._chunk_embedding_mode}"
        )

        enriched_embeddings: list[ChunkEmbedding] = []
        for chunk in chunks:
            chunk_display_text = chunk.content if isinstance(chunk.content, str) else to_yaml_string(chunk.content)

            generated_description = ""
            match self._chunk_embedding_mode:
                case ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY:
                    embedding_text = chunk.embeddable_text
                case ChunkEmbeddingMode.GENERATED_DESCRIPTION_ONLY:
                    generated_description = cast(DescriptionProvider, self._description_provider).describe(
                        text=chunk_display_text, context=result
                    )
                    embedding_text = generated_description
                case ChunkEmbeddingMode.EMBEDDABLE_TEXT_AND_GENERATED_DESCRIPTION:
                    generated_description = cast(DescriptionProvider, self._description_provider).describe(
                        text=chunk_display_text, context=result
                    )
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
            chunk_embeddings=enriched_embeddings,
            table_name=table_name,
            full_type=full_type,
            datasource_id=datasource_id,
        )
