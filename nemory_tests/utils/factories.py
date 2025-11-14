from nemory.storage.models import RunDTO, DatasourceRunDTO, ChunkDTO, EmbeddingDTO
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.chunk_repository import ChunkRepository


def make_run(
    run_repo: RunRepository,
    *,
    project_id: str = "project-id",
    nemory_version: str | None = None,
) -> RunDTO:
    return run_repo.create(project_id=project_id, nemory_version=nemory_version)


def make_datasource_run(
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
    *,
    plugin: str = "test-plugin",
    source_id: str = "src-id",
    storage_directory: str = "storage/path",
) -> DatasourceRunDTO:
    run = make_run(run_repo)
    return datasource_run_repo.create(
        run_id=run.run_id,
        plugin=plugin,
        source_id=source_id,
        storage_directory=storage_directory,
    )


def make_chunk(
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
    chunk_repo: ChunkRepository,
    *,
    embeddable_text: str = "sample embeddable",
    display_text: str = "display text",
    datasource_run_id: int | None = None,
) -> ChunkDTO:
    if datasource_run_id is None:
        datasource_run = make_datasource_run(run_repo, datasource_run_repo)
        datasource_run_id = datasource_run.datasource_run_id

    return chunk_repo.create(
        datasource_run_id=datasource_run_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
    )


def make_embedding(
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
    chunk_repo: ChunkRepository,
    embedding_repo: EmbeddingRepository,
    *,
    table_name: str,
    chunk_id: int = None,
    dim: int = 768,
    vec: list[float] = None,
) -> EmbeddingDTO:
    vec = vec or [0.0] * dim
    if chunk_id is None:
        chunk = make_chunk(run_repo, datasource_run_repo, chunk_repo)
        chunk_id = chunk.chunk_id

    return embedding_repo.create(
        chunk_id=chunk_id,
        table_name=table_name,
        vec=vec,
    )
