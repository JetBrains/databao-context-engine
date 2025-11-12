from datetime import datetime
from typing import Optional

import duckdb

from nemory.core.db.dtos import EmbeddingModelRegistryDTO
from nemory.core.services.shards.table_name_policy import TableNamePolicy


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
        row = self._conn.execute(
            """
            INSERT INTO
                embedding_model_registry(embedder, model_id, dim, table_name)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
            [embedder, model_id, dim, table_name],
        ).fetchone()
        return self._row_to_dto(row)

    def get(
        self,
        *,
        embedder: str,
        model_id: str,
    ) -> Optional[EmbeddingModelRegistryDTO]:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                embedding_model_registry
            WHERE
                embedder = ?
                AND model_id = ?
            """,
            [embedder, model_id],
        ).fetchone()
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
    def _row_to_dto(row: tuple) -> EmbeddingModelRegistryDTO:
        embedder, model_id, dim, table_name, created_at = row
        return EmbeddingModelRegistryDTO(
            embedder=str(embedder),
            model_id=str(model_id),
            dim=int(dim),
            table_name=str(table_name),
            created_at=created_at if isinstance(created_at, datetime) else created_at,
        )
