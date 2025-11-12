from nemory.core.services.persistence_service import PersistenceService
from nemory.core.services.segment_embedding_service import SegmentEmbeddingService
from nemory.features.build_sources.plugin_lib.build_plugin import EmbeddableChunk
from nemory.core.db.run_repository import RunStatus


def test_embed_flow_persists_segments_and_embeddings(conn, run_repo, entity_repo, segment_repo, embedding_repo):
    run = run_repo.create(status=RunStatus.RUNNING, project_id="project-id")
    entity = entity_repo.create(
        run_id=run.run_id,
        plugin="test-plugin",
        source_id="src-1",
        storage_directory="/path",
    )

    persistence = PersistenceService(conn=conn, segment_repo=segment_repo, embedding_repo=embedding_repo)
    provider = _StubProvider(dim=768, model_id="nomic-embed-text", embedder="ollama")
    segment_embedding_service = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    chunks = [
        EmbeddableChunk("alpha", "Alpha"),
        EmbeddableChunk("beta", "Beta"),
        EmbeddableChunk("gamma", "Gamma"),
    ]

    segment_embedding_service.embed_chunks(entity_id=entity.entity_id, chunks=chunks)

    seg_count = conn.execute(
        """
        SELECT 
            COUNT(*) 
        FROM 
            segment 
        WHERE 
            entity_id = ?
        """,
        [entity.entity_id],
    ).fetchone()[0]
    assert seg_count == 3

    emb_count = conn.execute(
        """
        SELECT 
            COUNT(*)
        FROM 
            embedding e
            JOIN segment s ON s.segment_id = e.segment_id
        WHERE 
            s.entity_id = ?
            AND e.embedder = ?
            AND e.model_id = ?
        """,
        [entity.entity_id, provider.embedder, provider.model_id],
    ).fetchone()[0]
    assert emb_count == 3


class _StubProvider:
    def __init__(self, dim=768, model_id="stub-model", embedder="ollama"):
        self.dim = dim
        self.model_id = model_id
        self.embedder = embedder
        self._calls = 0

    def embed(self, text: str):
        self._calls += 1
        return [float(self._calls)] * self.dim
