from nemory.core.db.dtos import RunStatus, RunDTO, EntityDTO, SegmentDTO, EmbeddingDTO
from nemory.core.db.embedding_repository import EmbeddingRepository
from nemory.core.db.entity_repository import EntityRepository
from nemory.core.db.run_repository import RunRepository
from nemory.core.db.segment_repository import SegmentRepository


def make_run(
    run_repo: RunRepository,
    *,
    status: RunStatus = RunStatus.RUNNING,
    project_id: str = "project-id",
    nemory_version: str | None = None,
) -> RunDTO:
    return run_repo.create(status=status, project_id=project_id, nemory_version=nemory_version)


def make_entity(
    run_repo: RunRepository,
    entity_repo: EntityRepository,
    *,
    plugin: str = "test-plugin",
    source_id: str = "src-id",
    storage_directory: str = "storage/path",
) -> EntityDTO:
    run = make_run(run_repo)
    return entity_repo.create(
        run_id=run.run_id,
        plugin=plugin,
        source_id=source_id,
        storage_directory=storage_directory,
    )


def make_segment(
    run_repo: RunRepository,
    entity_repo: EntityRepository,
    segment_repo: SegmentRepository,
    *,
    embeddable_text: str = "sample embeddable",
    display_text: str = "display text",
    entity_id: int | None = None,
) -> SegmentDTO:
    if entity_id is None:
        entity = make_entity(run_repo, entity_repo)
        entity_id = entity.entity_id

    return segment_repo.create(
        entity_id=entity_id,
        embeddable_text=embeddable_text,
        display_text=display_text,
    )


def make_embedding(
    run_repo: RunRepository,
    entity_repo: EntityRepository,
    segment_repo: SegmentRepository,
    embedding_repo: EmbeddingRepository,
    *,
    table_name: str,
    segment_id: int = None,
    dim: int = 768,
    vec: list[float] = None,
) -> EmbeddingDTO:
    vec = vec or [0.0] * dim
    if segment_id is None:
        segment = make_segment(run_repo, entity_repo, segment_repo)
        segment_id = segment.segment_id

    return embedding_repo.create(
        segment_id=segment_id,
        table_name=table_name,
        vec=vec,
    )
