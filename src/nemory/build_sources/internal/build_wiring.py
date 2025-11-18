from pathlib import Path

import duckdb

from nemory.build_sources.internal.build_runner import build
from nemory.embeddings.providers.ollama.config import OllamaConfig
from nemory.embeddings.providers.ollama.provider import OllamaEmbeddingProvider
from nemory.embeddings.providers.ollama.service import OllamaService
from nemory.project.layout import read_config_file, get_db_path
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.persistence_service import PersistenceService
from nemory.services.table_name_policy import TableNamePolicy
from nemory.storage.repositories.chunk_repository import ChunkRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.run_repository import RunRepository


def build_all_datasources(project_dir: str | Path):
    """
    Public build entrypoint
    - Opens DuckDB connection and loads VSS
    - Instantiates repositories, sharding resolver, embedding provider, and services
    - Delegates the actual build logic to the build runner
    """
    project_dir = Path(project_dir)

    conn = duckdb.connect(get_db_path())
    conn.execute("LOAD vss;")
    conn.execute("SET hnsw_enable_experimental_persistence = true;")

    try:
        run_repo = RunRepository(conn)
        datasource_run_repo = DatasourceRunRepository(conn)
        chunk_repo = ChunkRepository(conn)
        embedding_repo = EmbeddingRepository(conn)
        registry_repo = EmbeddingModelRegistryRepository(conn)

        table_name_policy = TableNamePolicy()
        shard_resolver = EmbeddingShardResolver(
            conn=conn, registry_repo=registry_repo, table_name_policy=table_name_policy
        )

        ollama_cfg = OllamaConfig(host="127.0.0.1", port=11434)
        ollama_service = OllamaService(ollama_cfg)
        provider = OllamaEmbeddingProvider(service=ollama_service, model_id="nomic-embed-text:latest", dim=768)

        persistence = PersistenceService(conn=conn, chunk_repo=chunk_repo, embedding_repo=embedding_repo)
        chunk_embedding_service = ChunkEmbeddingService(
            persistence_service=persistence, provider=provider, shard_resolver=shard_resolver
        )

        nemory_config = read_config_file(project_dir)
        build(
            project_dir=project_dir,
            run_repo=run_repo,
            datasource_run_repo=datasource_run_repo,
            chunk_embedding_service=chunk_embedding_service,
            project_id=str(nemory_config.project_id),
            nemory_version="UNRELEASED",  # TODO: implement nemory version
        )
    finally:
        conn.close()
