from nemory.core.services.persistence_service import PersistenceService
from nemory.core.services.segment_embedding_service import SegmentEmbeddingService
from nemory.core.services.shards.table_name_policy import TableNamePolicy
from nemory.features.build_sources.plugin_lib.build_plugin import EmbeddableChunk
from nemory.core.db.run_repository import RunStatus


def test_embed_flow_persists_segments_and_embeddings(
    conn, run_repo, entity_repo, segment_repo, embedding_repo, registry_repo, resolver
):
    run = run_repo.create(status=RunStatus.RUNNING, project_id="project-id")
    entity = entity_repo.create(
        run_id=run.run_id,
        plugin="test-plugin",
        source_id="src-1",
        storage_directory="/path",
    )

    persistence = PersistenceService(conn=conn, segment_repo=segment_repo, embedding_repo=embedding_repo)
    provider = _StubProvider(dim=768, model_id="dummy:v1", embedder="tests")
    segment_embedding_service = SegmentEmbeddingService(
        persistence_service=persistence, provider=provider, shard_resolver=resolver
    )

    chunks = [
        EmbeddableChunk("alpha", "Alpha"),
        EmbeddableChunk("beta", "Beta"),
        EmbeddableChunk("gamma", "Gamma"),
    ]
    segment_embedding_service.embed_chunks(entity_id=entity.entity_id, chunks=chunks)

    table_name = TableNamePolicy().build(embedder="tests", model_id="dummy:v1", dim=768)
    reg = registry_repo.get(embedder="tests", model_id="dummy:v1")
    assert reg.table_name == table_name

    segments = segment_repo.list()
    segments_for_entity = [s for s in segments if s.entity_id == entity.entity_id]
    assert len(segments_for_entity) == 3
    assert [s.embeddable_text for s in segments_for_entity] == ["gamma", "beta", "alpha"]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3


def test_embed_flow_is_idempotent_on_resolver(
    conn, run_repo, entity_repo, segment_repo, embedding_repo, registry_repo, resolver
):
    entity = entity_repo.create(
        run_id=run_repo.create(status=RunStatus.RUNNING, project_id="project-id").run_id,
        plugin="p",
        source_id="s",
        storage_directory="/path",
    )
    provider = _StubProvider(embedder="tests", model_id="idempotent:v1", dim=768)
    persistence = PersistenceService(conn, segment_repo, embedding_repo)
    svc = SegmentEmbeddingService(
        persistence_service=persistence,
        provider=provider,
        shard_resolver=resolver,
    )

    svc.embed_chunks(entity_id=entity.entity_id, chunks=[EmbeddableChunk("x", "...")])
    svc.embed_chunks(entity_id=entity.entity_id, chunks=[EmbeddableChunk("y", "...")])

    (count,) = conn.execute(
        "SELECT COUNT(*) FROM embedding_model_registry WHERE embedder=? AND model_id=?",
        ["tests", "idempotent:v1"],
    ).fetchone()
    assert count == 1


class _StubProvider:
    def __init__(self, dim=768, model_id="stub-model", embedder="ollama"):
        self.dim = dim
        self.model_id = model_id
        self.embedder = embedder
        self._calls = 0

    def embed(self, text: str):
        self._calls += 1
        return [float(self._calls)] * self.dim
