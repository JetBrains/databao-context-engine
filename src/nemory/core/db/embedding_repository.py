from typing import Tuple, Sequence, Optional
import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import EmbeddingDTO
from nemory.core.db.exceptions.exceptions import IntegrityError
from nemory.core.services.shards.table_name_policy import TableNamePolicy


class EmbeddingRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        table_name: str,
        segment_id: int,
        vec: Sequence[float],
    ) -> EmbeddingDTO:
        try:
            TableNamePolicy.validate_table_name(table_name=table_name)
            row = self._conn.execute(
                f"""
            INSERT INTO
                {table_name} (segment_id, vec)
            VALUES
                (?, ?)
            RETURNING
                *
            """,
                [segment_id, vec],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, *, table_name: str, segment_id: int) -> Optional[EmbeddingDTO]:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = self._conn.execute(
            f"""
            SELECT 
                *
            FROM 
                {table_name}
            WHERE 
                segment_id = ?
            """,
            [segment_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        *,
        table_name: str,
        segment_id: int,
        vec: Sequence[float],
    ) -> Optional[EmbeddingDTO]:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = self._conn.execute(
            f"""
            UPDATE 
                {table_name}
            SET 
                vec = ?
            WHERE 
                segment_id = ?
            RETURNING
                *
            """,
            [list(vec), segment_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def delete(self, *, table_name: str, segment_id: int) -> int:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = self._conn.execute(
            f"""
            DELETE FROM 
                {table_name}
            WHERE 
                segment_id = ?
            RETURNING 
                segment_id
            """,
            [segment_id],
        ).fetchone()
        return 1 if row else 0

    def list(self, table_name: str) -> list[EmbeddingDTO]:
        TableNamePolicy.validate_table_name(table_name=table_name)
        rows = self._conn.execute(
            f"""
            SELECT
                *
            FROM                
                {table_name}
            ORDER BY 
                segment_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> EmbeddingDTO:
        segment_id, vec, created_at = row
        return EmbeddingDTO(
            segment_id=int(segment_id),
            vec=list(vec) if not isinstance(vec, list) else vec,
            created_at=created_at,
        )
