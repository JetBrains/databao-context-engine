from typing import Optional, Sequence, Tuple

import duckdb
from _duckdb import ConstraintException

from databao_context_engine.services.table_name_policy import TableNamePolicy
from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import EmbeddingDTO


class EmbeddingRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        table_name: str,
        chunk_id: int,
        vec: Sequence[float],
    ) -> EmbeddingDTO:
        try:
            TableNamePolicy.validate_table_name(table_name=table_name)
            row = self._conn.execute(
                f"""
            INSERT INTO
                {table_name} (chunk_id, vec)
            VALUES
                (?, ?)
            RETURNING
                *
            """,
                [chunk_id, vec],
            ).fetchone()
            if row is None:
                raise RuntimeError("Embedding creation returned no object")
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, *, table_name: str, chunk_id: int) -> Optional[EmbeddingDTO]:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = self._conn.execute(
            f"""
            SELECT 
                *
            FROM 
                {table_name}
            WHERE 
                chunk_id = ?
            """,
            [chunk_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        *,
        table_name: str,
        chunk_id: int,
        vec: Sequence[float],
    ) -> Optional[EmbeddingDTO]:
        TableNamePolicy.validate_table_name(table_name=table_name)
        self._conn.execute(
            f"""
            UPDATE 
                {table_name}
            SET 
                vec = ?
            WHERE 
                chunk_id = ?
            """,
            [list(vec), chunk_id],
        )
        return self.get(table_name=table_name, chunk_id=chunk_id)

    def delete(self, *, table_name: str, chunk_id: int) -> int:
        TableNamePolicy.validate_table_name(table_name=table_name)
        row = self._conn.execute(
            f"""
            DELETE FROM 
                {table_name}
            WHERE 
                chunk_id = ?
            RETURNING 
                chunk_id
            """,
            [chunk_id],
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
                chunk_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> EmbeddingDTO:
        chunk_id, vec, created_at = row
        return EmbeddingDTO(
            chunk_id=int(chunk_id),
            vec=list(vec) if not isinstance(vec, list) else vec,
            created_at=created_at,
        )
