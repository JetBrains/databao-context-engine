from typing import Tuple, Optional

import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import SegmentDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


class SegmentRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        entity_id: int,
        embeddable_text: str,
        display_text: Optional[str],
    ) -> SegmentDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                segment(entity_id, embeddable_text, display_text)
            VALUES
                (?, ?, ?)
            RETURNING
                *
            """,
                [entity_id, embeddable_text, display_text],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, segment_id: int) -> Optional[SegmentDTO]:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                segment
            WHERE
                segment_id = ?
        """,
            [segment_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(self, segment_id: int, *, embeddable_text: Optional[str], display_text: Optional[str]) -> SegmentDTO:
        sets, params = [], []

        if embeddable_text is not None:
            sets.append("embeddable_text = ?")
            params.append(embeddable_text)
        if display_text is not None:
            sets.append("display_text = ?")
            params.append(display_text)

        if not sets:
            return self.get(segment_id)

        params.append(segment_id)
        row = self._conn.execute(
            f"""
            UPDATE
                segment
            SET
                {", ".join(sets)}
            WHERE
                segment_id = ?
            RETURNING
                *
        """,
            params,
        ).fetchone()

        return self._row_to_dto(row) if row else None

    def delete(self, segment_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM
                segment
            WHERE
                segment_id = ?
            RETURNING
                segment_id
            """,
            [segment_id],
        )
        return 1 if row else 0

    def list(self) -> list[SegmentDTO]:
        rows = self._conn.execute(
            """
            SELECT
                *
            FROM
                segment
            ORDER BY
                segment_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> SegmentDTO:
        segment_id, entity_id, embeddable_text, display_text, created_at = row
        return SegmentDTO(
            segment_id=int(segment_id),
            entity_id=int(entity_id),
            embeddable_text=str(embeddable_text),
            display_text=display_text,
            created_at=created_at,
        )
