from datetime import datetime

import pytest

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode, ChunkEmbeddingService
from databao_context_engine.services.persistence_service import PersistenceService
from databao_context_engine.services.table_name_policy import TableNamePolicy


@pytest.mark.parametrize("chunk_embedding_mode", [mode for mode in ChunkEmbeddingMode])
def test_embed_flow_persists_chunks_and_embeddings(
    conn, chunk_repo, embedding_repo, registry_repo, resolver, chunk_embedding_mode
):
    persistence = PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)
    embedding_provider = _StubProvider(dim=768, model_id="dummy:v1", embedder="tests")
    description_provider = _StubDescriptionProvider()

    chunk_embedding_service = ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        shard_resolver=resolver,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )

    chunks = [
        EmbeddableChunk(embeddable_text="alpha", content="Alpha"),
        EmbeddableChunk(embeddable_text="beta", content="Beta"),
        EmbeddableChunk(embeddable_text="gamma", content="Gamma"),
    ]
    chunk_embedding_service.embed_chunks(
        chunks=chunks,
        result=BuiltDatasourceContext(
            datasource_id="", datasource_type="", context="", context_built_at=datetime.now()
        ),
        full_type="folder/type",
        datasource_id="src-1",
    )

    table_name = TableNamePolicy().build(embedder="tests", model_id="dummy:v1", dim=768)
    reg = registry_repo.get(embedder="tests", model_id="dummy:v1")
    assert reg.table_name == table_name

    chunks = chunk_repo.list()
    assert len(chunks) == 3
    assert [s.display_text for s in chunks] == ["Gamma", "Beta", "Alpha"]

    match chunk_embedding_mode:
        case ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY:
            assert [s.embeddable_text for s in chunks] == ["gamma", "beta", "alpha"]
        case ChunkEmbeddingMode.GENERATED_DESCRIPTION_ONLY:
            assert [s.embeddable_text for s in chunks] == ["desc-2-gamma", "desc-1-beta", "desc-0-alpha"]
        case ChunkEmbeddingMode.EMBEDDABLE_TEXT_AND_GENERATED_DESCRIPTION:
            assert [s.embeddable_text for s in chunks] == [
                "desc-2-gamma\ngamma",
                "desc-1-beta\nbeta",
                "desc-0-alpha\nalpha",
            ]

    rows = embedding_repo.list(table_name=table_name)
    assert len(rows) == 3


def test_embed_flow_is_idempotent_on_resolver(conn, chunk_repo, embedding_repo, registry_repo, resolver):
    embedding_provider = _StubProvider(embedder="tests", model_id="idempotent:v1", dim=768)
    description_provider = _StubDescriptionProvider()
    persistence = PersistenceService(conn, chunk_repo, embedding_repo)
    service = ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        shard_resolver=resolver,
        description_provider=description_provider,
    )

    service.embed_chunks(
        chunks=[EmbeddableChunk(embeddable_text="x", content="...")],
        result="",
        full_type="folder/type",
        datasource_id="s",
    )
    service.embed_chunks(
        chunks=[EmbeddableChunk(embeddable_text="y", content="...")],
        result="",
        full_type="folder/type",
        datasource_id="s",
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

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for t in texts:
            out.append(self.embed(t))
        return out


class _StubDescriptionProvider(DescriptionProvider):
    def __init__(self, *, fail_at: set[int] | None = None):
        self._fail_at = set(fail_at or [])
        self.calls: list[tuple[str, str]] = []  # (text, context)

    def describe(self, text: str, context: str) -> str:
        i = len(self.calls)
        self.calls.append((text, context))

        if i in self._fail_at:
            raise RuntimeError("fake describe failure")

        return f"desc-{i}-{text}"
