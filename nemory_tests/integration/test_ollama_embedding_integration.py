import os

from nemory.core.db.dtos import RunStatus
from nemory.core.services.persistence_service import PersistenceService
from nemory.core.services.providers.ollama.config import OllamaConfig
from nemory.core.services.providers.ollama.provider import OllamaEmbeddingProvider
from nemory.core.services.providers.ollama.runtime import OllamaRuntime
from nemory.core.services.providers.ollama.service import OllamaService
from nemory.core.services.segment_embedding_service import SegmentEmbeddingService
from nemory.features.build_sources.plugin_lib.build_plugin import EmbeddableChunk

MODEL = os.getenv("OLLAMA_MODEL", "nomic-embed-text")
HOST = os.getenv("OLLAMA_HOST", "127.0.0.1")
PORT = int(os.getenv("OLLAMA_PORT", "11434"))


def test_service_embed_returns_vector():
    cfg = OllamaConfig(host=HOST, port=PORT)
    svc = OllamaService(cfg)

    svc.pull_model(model=MODEL, timeout=120)

    vec = svc.embed(model=MODEL, text="hello world")
    assert isinstance(vec, list)
    assert len(vec) == 768
    assert all(isinstance(x, float) for x in vec)


def test_ollama_embed_and_persist_e2e(conn, run_repo, entity_repo, segment_repo, embedding_repo, tmp_path):
    config = OllamaConfig(host=HOST, port=PORT)
    svc = OllamaService(config)
    rt = OllamaRuntime(service=svc, config=config)

    rt.start_and_await(timeout=60, poll_interval=0.5)
    svc.pull_model(model=MODEL, timeout=180)

    provider = OllamaEmbeddingProvider(service=svc, model_id=MODEL, dim=768)
    persistence = PersistenceService(conn=conn, segment_repo=segment_repo, embedding_repo=embedding_repo)
    seg_embed = SegmentEmbeddingService(persistence_service=persistence, provider=provider)

    run = run_repo.create(status=RunStatus.RUNNING, project_id="project-id")
    entity = entity_repo.create(run_id=run.run_id, plugin="integration-test", source_id="src-ollama", storage_directory="/some/path")

    chunks = [EmbeddableChunk("alpha", "Alpha"), EmbeddableChunk("beta", "Beta")]
    seg_embed.embed_chunks(entity_id=entity.entity_id, chunks=chunks)

    seg_rows = conn.execute(
        "SELECT segment_id FROM segment WHERE entity_id = ? ORDER BY segment_id", [entity.entity_id]
    ).fetchall()
    assert len(seg_rows) == 2

    emb_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM embedding e
        JOIN segment s ON s.segment_id = e.segment_id
        WHERE s.entity_id = ?
          AND e.embedder = 'ollama'
          AND e.model_id = ?
        """,
        [entity.entity_id, provider.model_id],
    ).fetchone()[0]
    assert emb_count == 2
