from array import array
from typing import Optional, Sequence, Tuple

import duckdb
import pyarrow  # type: ignore[import-untyped]
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

    def delete_by_datasource_id(self, *, table_name: str, datasource_id: str) -> int:
        TableNamePolicy.validate_table_name(table_name=table_name)

        deleted = self._conn.execute(
            f"""
            DELETE FROM
                {table_name}
            WHERE
                chunk_id IN (
                    SELECT
                        chunk_id
                    FROM
                        chunk
                    WHERE
                        datasource_id = ?
                )
            """,
            [datasource_id],
        ).rowcount
        return int(deleted or 0)

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

    def bulk_insert(
        self,
        *,
        table_name: str,
        chunk_ids: Sequence[int],
        vecs: Sequence[Sequence[float]],
        dim: int,
    ) -> None:
        """Bulk insert embeddings efficiently.

        DuckDB has a fast ingestion path for Arrow/columnar data. By registering a pyarrow.Table as a temporary view
        and inserting via INSERT ... SELECT, DuckDB ingests the data in native code and avoids the conversion from the
        Python binder at each float, which is very slow for large vectors.
        """
        flat = array("f")
        for v in vecs:
            flat.extend(v)

        tbl = pyarrow.table(
            {
                "chunk_id": pyarrow.array(chunk_ids, type=pyarrow.int64()),
                "vec": pyarrow.FixedSizeListArray.from_arrays(pyarrow.array(flat), dim),
            }
        )

        view_name = "__tmp_embeddings"
        self._conn.register(view_name, tbl)
        try:
            self._conn.execute(
                f"""
                INSERT INTO {table_name} (chunk_id, vec)
                SELECT chunk_id, vec
                FROM {view_name}
                """
            )
        finally:
            self._conn.unregister(view_name)

    @staticmethod
    def _row_to_dto(row: Tuple) -> EmbeddingDTO:
        chunk_id, vec, created_at = row
        return EmbeddingDTO(
            chunk_id=int(chunk_id),
            vec=list(vec) if not isinstance(vec, list) else vec,
            created_at=created_at,
        )
