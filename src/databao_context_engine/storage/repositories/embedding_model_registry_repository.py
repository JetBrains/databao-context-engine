from typing import Any, Optional

import duckdb

from databao_context_engine.plugins.duckdb_tools import fetchone_dicts
from databao_context_engine.services.table_name_policy import TableNamePolicy
from databao_context_engine.storage.models import EmbeddingModelRegistryDTO


class EmbeddingModelRegistryRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        embedder: str,
        model_id: str,
        dim: int,
        table_name: str,
    ) -> EmbeddingModelRegistryDTO:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = fetchone_dicts(
            cur=self._conn,
            sql="""
        INSERT INTO
            embedding_model_registry(embedder, model_id, dim, table_name)
        VALUES
            (?, ?, ?, ?)
        RETURNING
            *
        """,
            params=[embedder, model_id, dim, table_name],
        )
        if row is None:
            raise RuntimeError("Embedding_model_registry creatuib returned no object")
        return self._row_to_dto(row)

    def get(
        self,
        *,
        embedder: str,
        model_id: str,
    ) -> Optional[EmbeddingModelRegistryDTO]:
        row = fetchone_dicts(
            cur=self._conn,
            sql="""
        SELECT
            *
        FROM
            embedding_model_registry
        WHERE
            embedder = ?
            AND model_id = ?
        """,
            params=[embedder, model_id],
        )
        return self._row_to_dto(row) if row else None

    def delete(
        self,
        *,
        embedder: str,
        model_id: str,
    ) -> int:
        row = self._conn.execute(
            """
            DELETE FROM
                embedding_model_registry
            WHERE
                embedder = ?
                AND model_id = ?
            RETURNING
                model_id
            """,
            [embedder, model_id],
        ).fetchone()
        return 1 if row else 0

    @staticmethod
    def _row_to_dto(row: dict[str, Any]) -> EmbeddingModelRegistryDTO:
        return EmbeddingModelRegistryDTO(
            embedder=str(row["embedder"]),
            model_id=str(row["model_id"]),
            dim=int(row["dim"]),
            table_name=str(row["table_name"]),
            created_at=row["created_at"],
        )
