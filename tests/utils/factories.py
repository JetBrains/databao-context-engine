from datetime import datetime

from databao_context_engine.storage.models import ChunkDTO, DatasourceContextHashDTO, EmbeddingDTO
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.datasource_context_repository import DatasourceContextHashRepository
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository


def make_datasource_context_hash(
    datasource_context_hash_repo: DatasourceContextHashRepository,
    *,
    datasource_id: str = "test.yaml",
    hash_algorithm: str = "test-algorithm",
    hash_: str = "hash",
    hashed_at: datetime = datetime.now(),
) -> DatasourceContextHashDTO:
    return datasource_context_hash_repo.insert(
        datasource_id=datasource_id, hash_algorithm=hash_algorithm, hash_=hash_, hashed_at=hashed_at
    )


def make_or_get_datasource_context_hash(
    datasource_context_hash_repo: DatasourceContextHashRepository,
    *,
    datasource_id: str = "test.yaml",
    hash_algorithm: str = "test-algorithm",
    hash_: str = "hash",
    hashed_at: datetime = datetime.now(),
):
    datasource_context_hash = datasource_context_hash_repo.get_by_datasource_id_and_hash(
        datasource_id=datasource_id, hash_algorithm=hash_algorithm, hash_=hash_
    )

    if datasource_context_hash is not None:
        return datasource_context_hash

    return make_datasource_context_hash(
        datasource_context_hash_repo,
        datasource_id=datasource_id,
        hash_algorithm=hash_algorithm,
        hash_=hash_,
        hashed_at=hashed_at,
    )


def make_chunk(
    chunk_repo: ChunkRepository,
    *,
    datasource_context_hash_id: int,
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
        datasource_context_hash_id=datasource_context_hash_id,
    )


def make_embedding(
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    *,
    datasource_context_hash_id: int,
    table_name: str,
    chunk_id: int | None = None,
    dim: int,
    vec: list[float] | None = None,
) -> EmbeddingDTO:
    vec = vec or [0.0] * dim
    if chunk_id is None:
        chunk = make_chunk(chunk_repo, datasource_context_hash_id=datasource_context_hash_id)
        chunk_id = chunk.chunk_id

    return embedding_repo.create(
        chunk_id=chunk_id,
        table_name=table_name,
        vec=vec,
    )


def make_chunk_and_embedding_for_datasource_context_hash(
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    datasource_context_hash_id: int,
    table_name: str,
    dimension: int,
    full_type: str,
    datasource_id: str,
    embeddable_text: str,
    display_text: str,
):
    chunk = make_chunk(
        chunk_repo,
        datasource_context_hash_id=datasource_context_hash_id,
        full_type=full_type,
        datasource_id=datasource_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
    )
    make_embedding(
        chunk_repo,
        embedding_repo,
        datasource_context_hash_id=datasource_context_hash_id,
        table_name=table_name,
        chunk_id=chunk.chunk_id,
        dim=dimension,
        vec=[1.0] + [0.0] * (dimension - 1),
    )


def make_chunk_and_embedding(
    datasource_context_hash_repo: DatasourceContextHashRepository,
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    table_name: str,
    dimension: int,
    full_type: str,
    datasource_id: str,
    embeddable_text: str,
    display_text: str,
):
    datasource_context_hash = make_or_get_datasource_context_hash(
        datasource_context_hash_repo, datasource_id=datasource_id
    )

    make_chunk_and_embedding_for_datasource_context_hash(
        chunk_repo=chunk_repo,
        embedding_repo=embedding_repo,
        datasource_context_hash_id=datasource_context_hash.datasource_context_hash_id,
        table_name=table_name,
        dimension=dimension,
        full_type=full_type,
        datasource_id=datasource_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
    )
