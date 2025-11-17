import os

from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.embeddings.providers.ollama.config import OllamaConfig
from nemory.embeddings.providers.ollama.provider import OllamaEmbeddingProvider
from nemory.services.persistence_service import PersistenceService
from nemory.embeddings.providers.ollama.runtime import OllamaRuntime
from nemory.embeddings.providers.ollama.service import OllamaService
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.table_name_policy import TableNamePolicy

MODEL = os.getenv("OLLAMA_MODEL", "nomic-embed-text")
HOST = os.getenv("OLLAMA_HOST", "127.0.0.1")
PORT = int(os.getenv("OLLAMA_PORT", "11434"))


def test_service_embed_returns_vector():
    cfg = OllamaConfig(host=HOST, port=PORT)
    service = OllamaService(cfg)

    service.pull_model(model=MODEL, timeout=120)

    vec = service.embed(model=MODEL, text="hello world")
    assert isinstance(vec, list)
    assert len(vec) == 768
    assert all(isinstance(x, float) for x in vec)


def test_ollama_embed_and_persist_e2e(
    conn, run_repo, datasource_run_repo, chunk_repo, embedding_repo, tmp_path, registry_repo, resolver
):
    config = OllamaConfig(host=HOST, port=PORT)
    service = OllamaService(config)
    rt = OllamaRuntime(service=service, config=config)

    rt.start_and_await(timeout=60, poll_interval=0.5)
    service.pull_model(model=MODEL, timeout=180)

    provider = OllamaEmbeddingProvider(service=service, model_id=MODEL, dim=768)

    persistence = PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)
    chunk_embedding_service = ChunkEmbeddingService(
        persistence_service=persistence, shard_resolver=resolver, provider=provider
    )

    run = run_repo.create(project_id="project-id")
    datasource_run = datasource_run_repo.create(
        run_id=run.run_id, plugin="integration-test", source_id="src-ollama", storage_directory="/some/path"
    )

    chunks = [EmbeddableChunk("alpha", "Alpha"), EmbeddableChunk("beta", "Beta")]
    chunk_embedding_service.embed_chunks(datasource_run_id=datasource_run.datasource_run_id, chunks=chunks)

    chunk_rows = conn.execute(
        "SELECT chunk_id FROM chunk WHERE datasource_run_id = ? ORDER BY chunk_id", [datasource_run.datasource_run_id]
    ).fetchall()
    assert len(chunk_rows) == 2
    chunk_ids = [r[0] for r in chunk_rows]

    expected_table = TableNamePolicy().build(embedder=provider.embedder, model_id=provider.model_id, dim=provider.dim)
    reg = registry_repo.get(embedder=provider.embedder, model_id=provider.model_id)
    assert reg and reg.table_name == expected_table and reg.dim == 768

    (emb_count,) = conn.execute(
        f"""
            SELECT COUNT(*)
            FROM {expected_table} e
            WHERE e.chunk_id IN ({",".join("?" for _ in chunk_ids)})
            """,
        chunk_ids,
    ).fetchone()
    assert emb_count == 2
