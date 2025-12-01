from nemory.pluginlib.build_plugin import EmbeddableChunk
from nemory.services.persistence_service import PersistenceService
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.table_name_policy import TableNamePolicy


def test_embed_flow_persists_chunks_and_embeddings(
    conn, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo, resolver
):
    run = run_repo.create(project_id="project-id")
    datasource_run = datasource_run_repo.create(
        run_id=run.run_id,
        plugin="test-plugin",
        full_type="folder/type",
        source_id="src-1",
        storage_directory="/path",
    )

    persistence = PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)
    provider = _StubProvider(dim=768, model_id="dummy:v1", embedder="tests")
    ollama_service = _StubOllamaService()
    chunk_embedding_service = ChunkEmbeddingService(
        persistence_service=persistence, provider=provider, shard_resolver=resolver, ollama_service=ollama_service
    )

    chunks = [
        EmbeddableChunk("alpha", "Alpha"),
        EmbeddableChunk("beta", "Beta"),
        EmbeddableChunk("gamma", "Gamma"),
    ]
    chunk_embedding_service.embed_chunks(datasource_run_id=datasource_run.datasource_run_id, chunks=chunks, result="")

    table_name = TableNamePolicy().build(embedder="tests", model_id="dummy:v1", dim=768)
    reg = registry_repo.get(embedder="tests", model_id="dummy:v1")
    assert reg.table_name == table_name

    chunks = chunk_repo.list()
    chunks_for_datasource_run = [s for s in chunks if s.datasource_run_id == datasource_run.datasource_run_id]
    assert len(chunks_for_datasource_run) == 3
    assert [s.embeddable_text for s in chunks_for_datasource_run] == ["gamma", "beta", "alpha"]

    for chunk in chunks_for_datasource_run:
        assert chunk.generated_description is not None
        assert chunk.display_text in chunk.generated_description

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3


def test_embed_flow_is_idempotent_on_resolver(
    conn, run_repo, datasource_run_repo, chunk_repo, embedding_repo, registry_repo, resolver
):
    datasource_run = datasource_run_repo.create(
        run_id=run_repo.create(project_id="project-id").run_id,
        plugin="p",
        full_type="folder/type",
        source_id="s",
        storage_directory="/path",
    )
    provider = _StubProvider(embedder="tests", model_id="idempotent:v1", dim=768)
    ollama_service = _StubOllamaService()
    persistence = PersistenceService(conn, chunk_repo, embedding_repo)
    service = ChunkEmbeddingService(
        persistence_service=persistence,
        provider=provider,
        shard_resolver=resolver,
        ollama_service=ollama_service,
    )

    service.embed_chunks(
        datasource_run_id=datasource_run.datasource_run_id, chunks=[EmbeddableChunk("x", "...")], result=""
    )
    service.embed_chunks(
        datasource_run_id=datasource_run.datasource_run_id, chunks=[EmbeddableChunk("y", "...")], result=""
    )

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


class _StubOllamaService:
    def __init__(self, *, fail_at=None):
        self._fail_at = set(fail_at or [])
        self.calls: list[tuple[str, str]] = []

    def describe(self, *, model: str, text: str, context: str) -> str:
        i = len(self.calls)
        self.calls.append((text, context))

        if i in self._fail_at:
            raise RuntimeError("fake describe failure")

        return f"desc-{i}-{text}"
