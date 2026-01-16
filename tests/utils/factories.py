from databao_context_engine.storage.models import ChunkDTO, DatasourceRunDTO, EmbeddingDTO, RunDTO
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.datasource_run_repository import DatasourceRunRepository
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository
from databao_context_engine.storage.repositories.run_repository import RunRepository


def make_run(
    run_repo: RunRepository,
    *,
    project_id: str = "project-id",
    dce_version: str | None = None,
) -> RunDTO:
    return run_repo.create(project_id=project_id, dce_version=dce_version)


def make_datasource_run(
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
    *,
    plugin: str = "test-plugin",
    full_type: str = "folder/type",
    source_id: str = "folder/name.yaml",
    storage_directory: str = "storage/path",
) -> DatasourceRunDTO:
    run = make_run(run_repo)
    return datasource_run_repo.create(
        run_id=run.run_id,
        plugin=plugin,
        full_type=full_type,
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
    generated_description: str = "generated description",
    datasource_run_id: int | None = None,
) -> ChunkDTO:
    if datasource_run_id is None:
        datasource_run = make_datasource_run(run_repo, datasource_run_repo)
        datasource_run_id = datasource_run.datasource_run_id

    return chunk_repo.create(
        datasource_run_id=datasource_run_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
        generated_description=generated_description,
    )


def make_embedding(
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
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
        chunk = make_chunk(run_repo, datasource_run_repo, chunk_repo)
        chunk_id = chunk.chunk_id

    return embedding_repo.create(
        chunk_id=chunk_id,
        table_name=table_name,
        vec=vec,
    )
