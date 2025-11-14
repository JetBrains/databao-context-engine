import duckdb

from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.services.table_name_policy import TableNamePolicy


class EmbeddingShardResolver:
    def __init__(
        self,
        *,
        conn: duckdb.DuckDBPyConnection,
        registry_repo: EmbeddingModelRegistryRepository,
        table_name_policy: TableNamePolicy | None = None,
    ):
        self._conn = conn
        self._registry = registry_repo
        self._policy = table_name_policy or TableNamePolicy()

    def resolve(self, *, embedder: str, model_id: str) -> str:
        row = self._registry.get(embedder=embedder, model_id=model_id)
        if not row:
            raise ValueError(f"Model not registered: {embedder}:{model_id}")
        return row.table_name

    def resolve_or_create(self, *, embedder: str, model_id: str, dim: int) -> str:
        row = self._registry.get(embedder=embedder, model_id=model_id)
        if row:
            if row.dim != dim:
                raise ValueError(f"Model already registered with dim={row.dim}, requested dim={dim}")
            return row.table_name

        table_name = self._policy.build(embedder=embedder, model_id=model_id, dim=dim)
        self._create_table_and_index(table_name, dim)

        self._registry.create(
            embedder=embedder,
            model_id=model_id,
            dim=dim,
            table_name=table_name,
        )

        return table_name

    def _create_table_and_index(self, table_name: str, dim: int) -> None:
        self._conn.execute("LOAD vss;")
        self._conn.execute("SET hnsw_enable_experimental_persistence = true;")

        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                chunk_id BIGINT NOT NULL REFERENCES chunk(chunk_id),
                vec FLOAT[{dim}] NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chunk_id)
            )
            """
        )
        self._conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS emb_hnsw_{table_name} ON {table_name} USING HNSW (vec) WITH (metric='cosine');
            """
        )
