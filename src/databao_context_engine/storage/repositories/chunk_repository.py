from typing import Any, Optional, Sequence, Tuple

import duckdb
from _duckdb import ConstraintException

import databao_context_engine.perf.core as perf
from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import ChunkDTO


class ChunkRepository:
    _BM25_CHUNK_COLUMN = "embeddable_text"

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

            self._refresh_fts_index()
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

        self._refresh_fts_index()
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

        self._refresh_fts_index()
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
        self._refresh_fts_index()
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
        chunk_contents: Sequence[Tuple[str, Optional[str]]],
    ) -> Sequence[int]:
        values_sql = ", ".join(["(?, ?, ?, ?)"] * len(chunk_contents))
        sql = f"""
            INSERT INTO
                chunk(full_type, datasource_id, embeddable_text, display_text)
            VALUES
                {values_sql}
            RETURNING
                chunk_id
        """

        params: list[Any] = []
        for embeddable_text, display_text in chunk_contents:
            params.extend([full_type, datasource_id, embeddable_text, display_text])

        rows = self._conn.execute(sql, params).fetchall()

        self._refresh_fts_index()

        return [int(r[0]) for r in rows]

    @perf.perf_span("chunk_repo.refresh_keyword_index")
    def _refresh_fts_index(self) -> None:
        """Refreshs the Full Text Search index on the chunks in DuckDB.

        This function needs to be called each time there are any changes in the chunk table.
        The FTS index is unfortunately not a standard index and needs to be rebuilt manually with each change.
        """
        self._conn.execute(f"PRAGMA create_fts_index('chunk', 'chunk_id', '{self._BM25_CHUNK_COLUMN}', overwrite=1);")

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
