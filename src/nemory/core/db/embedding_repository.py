from typing import Tuple, Sequence, Optional
import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import EmbeddingDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


class EmbeddingRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        segment_id: int,
        embedder: str,
        model_id: str,
        vec: Sequence[float],
    ) -> EmbeddingDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                embedding(segment_id, embedder, model_id, vec)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [segment_id, embedder, model_id, vec],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, segment_id: int) -> Optional[EmbeddingDTO]:
        row = self._conn.execute(
            """
            SELECT 
                *
            FROM 
                embedding
            WHERE 
                segment_id = ?
            """,
            [segment_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        segment_id: int,
        embedder: str,
        model_id: str,
        *,
        vec: Sequence[float],
    ) -> Optional[EmbeddingDTO]:
        row = self._conn.execute(
            """
            UPDATE 
                embedding
            SET 
                vec = ?
            WHERE 
                segment_id = ? AND 
                embedder = ? AND
                model_id = ?
            RETURNING
                *
            """,
            [list(vec), segment_id, embedder, model_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def delete(self, segment_id: int, embedder: str, model_id: str) -> int:
        row = self._conn.execute(
            """
            DELETE FROM 
                embedding
            WHERE 
                segment_id = ? AND
                embedder = ? AND
                model_id = ?
            RETURNING 
                segment_id
            """,
            [segment_id, embedder, model_id],
        ).fetchone()
        return 1 if row else 0

    def list(self) -> list[EmbeddingDTO]:
        rows = self._conn.execute(
            """
            SELECT
                *
            FROM 
                embedding
            ORDER BY 
                segment_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> EmbeddingDTO:
        segment_id, embedder, model_id, vec, created_at = row
        return EmbeddingDTO(
            segment_id=int(segment_id),
            embedder=str(embedder),
            model_id=str(model_id),
            vec=list(vec) if not isinstance(vec, list) else vec,
            created_at=created_at,
        )
