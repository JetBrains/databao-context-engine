from databao_context_engine.storage.models import ChunkDTO, EmbeddingDTO
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository


def make_chunk(
    chunk_repo: ChunkRepository,
    *,
    full_type: str = "sample embeddable",
    datasource_id: str = "some-datasource-id",
    embeddable_text: str = "sample embeddable",
    display_text: str = "display text",
    keyword_index_text: str = "keyword index",
) -> ChunkDTO:
    return chunk_repo.create(
        full_type=full_type,
        datasource_id=datasource_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
        keyword_index_text=keyword_index_text,
    )


def make_embedding(
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    *,
    table_name: str,
    chunk_id: int | None = None,
    dim: int = 768,
    vec: list[float] | None = None,
) -> EmbeddingDTO:
    vec = vec or [0.0] * dim
    if chunk_id is None:
        chunk = make_chunk(chunk_repo)
        chunk_id = chunk.chunk_id

    return embedding_repo.create(
        chunk_id=chunk_id,
        table_name=table_name,
        vec=vec,
    )


def make_chunk_and_embedding(
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    table_name: str,
    dimension: int,
    full_type: str,
    datasource_id: str,
    embeddable_text: str,
    display_text: str,
):
    chunk = make_chunk(
        chunk_repo,
        full_type=full_type,
        datasource_id=datasource_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
    )
    make_embedding(
        chunk_repo,
        embedding_repo,
        table_name=table_name,
        chunk_id=chunk.chunk_id,
        dim=dimension,
        vec=[1.0] + [0.0] * (dimension - 1),
    )
