from __future__ import annotations

from dataclasses import dataclass

import pytest

from databao_context_engine import ChunkEmbeddingMode, DatabaoContextDomainManager
from databao_context_engine.llm.config import EmbeddingModelDetails
from tests.utils.project_creation import given_raw_source_file


@dataclass(frozen=True)
class _FakeProvider:
    embedder: str = "fake"
    embedding_model_details: EmbeddingModelDetails = EmbeddingModelDetails(model_id="dummy", model_dim=768)

    def embed(self, text: str) -> list[float]:
        seed = float(len(text) % 10)
        return [seed] * self.embedding_model_details.model_dim

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]


def _shard_rows(conn, table_name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _duckdb_has_table(conn, name: str) -> bool:
    rows = conn.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchall()
    return bool(rows)


@pytest.fixture
def fake_provider() -> _FakeProvider:
    return _FakeProvider()


@pytest.fixture
def use_fake_provider(mocker, fake_provider):
    return mocker.patch(
        "databao_context_engine.build_sources.build_wiring.create_ollama_embedding_provider",
        return_value=fake_provider,
    )


def test_e2e_build_with_fake_provider(
    project_path, db_path, conn, chunk_repo, embedding_repo, registry_repo, use_fake_provider, fake_provider
):
    given_raw_source_file(project_path, "note.md", "# Hello\nworld\n")

    result = DatabaoContextDomainManager(domain_dir=project_path).build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert len(result) == 1

    chunks = chunk_repo.list()
    assert len(chunks) >= 1

    reg = registry_repo.get(embedder=fake_provider.embedder, model_id=fake_provider.embedding_model_details.model_id)
    assert reg is not None
    assert reg.dim == fake_provider.embedding_model_details.model_dim
    assert _duckdb_has_table(conn, reg.table_name)

    count = _shard_rows(conn, reg.table_name)
    assert count == len(chunks)


def test_one_source_fails_but_others_succeed(
    mocker, project_path, conn, chunk_repo, embedding_repo, registry_repo, use_fake_provider, fake_provider
):
    import databao_context_engine.build_sources.plugin_execution as execmod

    original_execute_plugin = execmod.execute_plugin

    def flaky_execute(source, plugin):
        if source.path.name.endswith("pg.yaml"):
            raise RuntimeError("boom")
        return original_execute_plugin(source, plugin)

    mocker.patch.object(execmod, "execute_plugin", side_effect=flaky_execute)

    given_raw_source_file(project_path, "note.md", "# Hello\nworld\n")

    result = DatabaoContextDomainManager(domain_dir=project_path).build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY
    )

    assert len(result) == 1

    chunks = chunk_repo.list()
    assert len(chunks) >= 1

    reg = registry_repo.get(embedder=fake_provider.embedder, model_id=fake_provider.embedding_model_details.model_id)
    assert reg is not None
    assert _shard_rows(conn, reg.table_name) == len(chunks)
