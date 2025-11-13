from typing import Tuple, Optional

import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import ChunkDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


class ChunkRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        datasource_run_id: int,
        embeddable_text: str,
        display_text: Optional[str],
    ) -> ChunkDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                chunk(datasource_run_id, embeddable_text, display_text)
            VALUES
                (?, ?, ?)
            RETURNING
                *
            """,
                [datasource_run_id, embeddable_text, display_text],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, chunk_id: int) -> Optional[ChunkDTO]:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                chunk
            WHERE
                chunk_id = ?
        """,
            [chunk_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(self, chunk_id: int, *, embeddable_text: Optional[str], display_text: Optional[str]) -> ChunkDTO:
        sets, params = [], []

        if embeddable_text is not None:
            sets.append("embeddable_text = ?")
            params.append(embeddable_text)
        if display_text is not None:
            sets.append("display_text = ?")
            params.append(display_text)

        if not sets:
            return self.get(chunk_id)

        params.append(chunk_id)
        row = self._conn.execute(
            f"""
            UPDATE
                chunk
            SET
                {", ".join(sets)}
            WHERE
                chunk_id = ?
            RETURNING
                *
        """,
            params,
        ).fetchone()

        return self._row_to_dto(row) if row else None

    def delete(self, chunk_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM
                chunk
            WHERE
                chunk_id = ?
            RETURNING
                chunk_id
            """,
            [chunk_id],
        )
        return 1 if row else 0

    def list(self) -> list[ChunkDTO]:
        rows = self._conn.execute(
            """
            SELECT
                *
            FROM
                chunk
            ORDER BY
                chunk_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> ChunkDTO:
        chunk_id, datasource_run_id, embeddable_text, display_text, created_at = row
        return ChunkDTO(
            chunk_id=int(chunk_id),
            datasource_run_id=int(datasource_run_id),
            embeddable_text=str(embeddable_text),
            display_text=display_text,
            created_at=created_at,
        )
