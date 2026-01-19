from typing import Any, Optional, Tuple

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
        datasource_run_id: int,
        embeddable_text: str,
        display_text: Optional[str],
        generated_description: str,
    ) -> ChunkDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                chunk(datasource_run_id, embeddable_text, display_text, generated_description)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [datasource_run_id, embeddable_text, display_text, generated_description],
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
        embeddable_text: Optional[str] = None,
        display_text: Optional[str] = None,
        generated_description: Optional[str] = None,
    ) -> Optional[ChunkDTO]:
        sets: list[Any] = []
        params: list[Any] = []

        if embeddable_text is not None:
            sets.append("embeddable_text = ?")
            params.append(embeddable_text)
        if display_text is not None:
            sets.append("display_text = ?")
            params.append(display_text)
        if generated_description is not None:
            sets.append("generated_description = ?")
            params.append(generated_description)

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
        chunk_id, datasource_run_id, embeddable_text, display_text, created_at, generated_description = row
        return ChunkDTO(
            chunk_id=int(chunk_id),
            datasource_run_id=int(datasource_run_id),
            embeddable_text=str(embeddable_text),
            display_text=display_text,
            generated_description=str(generated_description),
            created_at=created_at,
        )
