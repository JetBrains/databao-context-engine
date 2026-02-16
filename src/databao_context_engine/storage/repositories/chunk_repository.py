from typing import Any, Optional, Sequence, Tuple

import duckdb
from _duckdb import ConstraintException

from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import ChunkDTO


class ChunkRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        full_type: str,
        datasource_id: str,
        embeddable_text: str,
        display_text: Optional[str],
    ) -> ChunkDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                chunk(full_type, datasource_id, embeddable_text, display_text)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [full_type, datasource_id, embeddable_text, display_text],
            ).fetchone()
            if row is None:
                raise RuntimeError("chunk creation returned no object")
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

    def update(
        self,
        chunk_id: int,
        *,
        full_type: Optional[str] = None,
        datasource_id: Optional[str] = None,
        embeddable_text: Optional[str] = None,
        display_text: Optional[str] = None,
    ) -> Optional[ChunkDTO]:
        sets: list[Any] = []
        params: list[Any] = []

        if full_type is not None:
            sets.append("full_type = ?")
            params.append(full_type)
        if datasource_id is not None:
            sets.append("datasource_id = ?")
            params.append(datasource_id)
        if embeddable_text is not None:
            sets.append("embeddable_text = ?")
            params.append(embeddable_text)
        if display_text is not None:
            sets.append("display_text = ?")
            params.append(display_text)

        if not sets:
            return self.get(chunk_id)

        params.append(chunk_id)
        self._conn.execute(
            f"""
            UPDATE
                chunk
            SET
                {", ".join(sets)}
            WHERE
                chunk_id = ?
        """,
            params,
        )

        return self.get(chunk_id)

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

    def delete_by_datasource_id(self, *, datasource_id: str) -> int:
        deleted = self._conn.execute(
            """
            DELETE FROM
                chunk
            WHERE
                datasource_id = ?
            """,
            [datasource_id],
        ).rowcount
        return int(deleted or 0)

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

    def bulk_insert(
        self,
        *,
        full_type: str,
        datasource_id: str,
        embeddable_texts: Sequence[str],
        display_texts: Sequence[Optional[str]],
    ) -> Sequence[int]:
        values_sql = ", ".join(["(?, ?, ?, ?)"] * len(embeddable_texts))
        sql = f"""
            INSERT INTO
                chunk(full_type, datasource_id, embeddable_text, display_text)
            VALUES
                {values_sql}
            RETURNING
                chunk_id
        """

        params: list[Any] = []
        for embeddable_text, display_text in zip(embeddable_texts, display_texts):
            params.extend([full_type, datasource_id, embeddable_text, display_text])

        rows = self._conn.execute(sql, params).fetchall()
        return [int(r[0]) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> ChunkDTO:
        chunk_id, full_type, datasource_id, embeddable_text, display_text, created_at = row
        return ChunkDTO(
            chunk_id=int(chunk_id),
            full_type=full_type,
            datasource_id=datasource_id,
            embeddable_text=embeddable_text,
            display_text=display_text,
            created_at=created_at,
        )
