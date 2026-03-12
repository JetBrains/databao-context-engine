import logging

import databao_context_engine.perf.core as perf
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.progress.progress import ProgressCallback, ProgressEmitter, ProgressStep
from databao_context_engine.serialization.yaml import to_yaml_string
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.services.models import ChunkEmbedding
from databao_context_engine.services.persistence_service import PersistenceService

logger = logging.getLogger(__name__)


class ChunkEmbeddingService:
    def __init__(
        self,
        *,
        persistence_service: PersistenceService,
        embedding_provider: EmbeddingProvider,
        shard_resolver: EmbeddingShardResolver,
    ):
        self._persistence_service = persistence_service
        self._embedding_provider = embedding_provider
        self._shard_resolver = shard_resolver

    def embed_chunks(
        self,
        *,
        chunks: list[EmbeddableChunk],
        context_hash: DatasourceContextHash,
        full_type: str,
        datasource_id: str,
        override: bool = False,
        progress: ProgressCallback | None = None,
    ) -> None:
        """Turn plugin chunks into persisted chunks and embeddings.

        Flow:
        1) Embed each chunk into an embedded vector.
        2) Get or create embedding table for the appropriate model and embedding dimensions.
        3) Persist chunks and embeddings vectors in a single transaction.
        """
        if not chunks:
            return

        emitter = ProgressEmitter(progress)

        logger.debug(f"Embedding {len(chunks)} chunks for datasource {datasource_id}")

        chunk_display_texts: list[str] = [
            (chunk.content if isinstance(chunk.content, str) else to_yaml_string(chunk.content)) for chunk in chunks
        ]
        embedding_texts = [chunk.embeddable_text for chunk in chunks]

        vecs = self._embed_many(embedding_texts)

        emitter.datasource_step_completed(
            datasource_id=datasource_id,
            step=ProgressStep.EMBEDDING,
        )

        enriched_embeddings: list[ChunkEmbedding] = [
            ChunkEmbedding(
                original_chunk=chunk,
                vec=vec,
                embedded_text=embedding_text,
                display_text=display_text,
            )
            for chunk, vec, display_text, embedding_text in zip(chunks, vecs, chunk_display_texts, embedding_texts)
        ]

        table_name = self._shard_resolver.resolve_or_create(
            embedder=self._embedding_provider.embedder,
            embedding_model_details=self._embedding_provider.embedding_model_details,
        )

        self._persistence_service.write_chunks_and_embeddings(
            chunk_embeddings=enriched_embeddings,
            table_name=table_name,
            full_type=full_type,
            datasource_id=datasource_id,
            context_hash=context_hash,
            override=override,
        )

        emitter.datasource_step_completed(
            datasource_id=datasource_id,
            step=ProgressStep.PERSISTENCE,
        )

    @perf.perf_span("embedding.embed_many")
    def _embed_many(self, embedding_texts: list[str]) -> list[list[float]]:
        return self._embedding_provider.embed_many(embedding_texts)

    def is_index_up_to_date(self, context_hash: DatasourceContextHash) -> bool:
        return self._persistence_service.has_datasource_context_hash(context_hash=context_hash)
