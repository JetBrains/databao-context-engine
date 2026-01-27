import os

from databao_context_engine.llm.config import OllamaConfig
from databao_context_engine.llm.descriptions.ollama import OllamaDescriptionProvider
from databao_context_engine.llm.embeddings.ollama import OllamaEmbeddingProvider
from databao_context_engine.llm.runtime import OllamaRuntime
from databao_context_engine.llm.service import OllamaService
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingService
from databao_context_engine.services.persistence_service import PersistenceService
from databao_context_engine.services.table_name_policy import TableNamePolicy

MODEL = os.getenv("OLLAMA_MODEL", "nomic-embed-text")
CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL", "llama3.2:1b")
HOST = os.getenv("OLLAMA_HOST", "127.0.0.1")
PORT = int(os.getenv("OLLAMA_PORT", "11434"))


def test_service_embed_returns_vector():
    cfg = OllamaConfig(host=HOST, port=PORT)
    service = OllamaService(cfg)

    service.pull_model_if_needed(model=MODEL, timeout=120)

    vec = service.embed(model=MODEL, text="hello world")
    assert isinstance(vec, list)
    assert len(vec) == 768
    assert all(isinstance(x, float) for x in vec)


def test_ollama_embed_and_persist_e2e(conn, chunk_repo, embedding_repo, tmp_path, registry_repo, resolver):
    config = OllamaConfig(host=HOST, port=PORT)
    service = OllamaService(config)
    rt = OllamaRuntime(service=service, config=config)

    rt.start_and_await(timeout=60, poll_interval=0.5)
    service.pull_model_if_needed(model=MODEL, timeout=180)
    service.pull_model_if_needed(model=CHAT_MODEL, timeout=300)

    embedding_provider = OllamaEmbeddingProvider(service=service, model_id=MODEL, dim=768)
    description_provider = OllamaDescriptionProvider(service=service, model_id=CHAT_MODEL)

    persistence = PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)
    chunk_embedding_service = ChunkEmbeddingService(
        persistence_service=persistence,
        shard_resolver=resolver,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
    )

    chunks = [EmbeddableChunk("alpha", "Alpha"), EmbeddableChunk("beta", "Beta")]
    chunk_embedding_service.embed_chunks(chunks=chunks, result="", full_type="type/md", datasource_id="some-id")

    chunk_rows = chunk_repo.list()
    assert len(chunk_rows) == 2
    chunk_ids = [r.chunk_id for r in chunk_rows]

    expected_table = TableNamePolicy().build(
        embedder=embedding_provider.embedder, model_id=embedding_provider.model_id, dim=embedding_provider.dim
    )
    reg = registry_repo.get(embedder=embedding_provider.embedder, model_id=embedding_provider.model_id)
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
