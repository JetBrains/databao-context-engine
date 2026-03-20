from typing import Any, Optional, Sequence, Tuple

import duckdb
from _duckdb import ConstraintException

import databao_context_engine.perf.core as perf
from databao_context_engine.plugins.duckdb_tools import fetchall_dicts, fetchone_dicts
from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import ChunkDTO


class ChunkRepository:
    _BM25_CHUNK_COLUMN = "keyword_index_text"

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        full_type: str,
        datasource_id: str,
        embeddable_text: str,
        display_text: Optional[str],
        keyword_index_text: str,
        datasource_context_hash_id: int,
    ) -> ChunkDTO:
        try:
            row = fetchone_dicts(
                cur=self._conn,
                sql="""
            INSERT INTO
                chunk(full_type, datasource_id, embeddable_text, display_text, keyword_index_text, datasource_context_hash_id)
            VALUES
                (?, ?, ?, ?, ?, ?)
            RETURNING
                *
            """,
                params=[
                    full_type,
                    datasource_id,
                    embeddable_text,
                    display_text,
                    keyword_index_text,
                    datasource_context_hash_id,
                ],
            )
            if row is None:
                raise RuntimeError("chunk creation returned no object")

            self._refresh_fts_index()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, chunk_id: int) -> Optional[ChunkDTO]:
        row = fetchone_dicts(
            cur=self._conn,
            sql="""
            SELECT
                *
            FROM
                chunk
            WHERE
                chunk_id = ?
        """,
            params=[chunk_id],
        )
        return self._row_to_dto(row) if row else None

    def update(
        self,
        chunk_id: int,
        *,
        full_type: Optional[str] = None,
        datasource_id: Optional[str] = None,
        embeddable_text: Optional[str] = None,
        display_text: Optional[str] = None,
        keyword_index_text: Optional[str] = None,
        datasource_context_hash_id: Optional[int] = None,
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
        if keyword_index_text is not None:
            sets.append("keyword_index_text = ?")
            params.append(keyword_index_text)
        if datasource_context_hash_id is not None:
            sets.append("datasource_context_hash_id = ?")
            params.append(datasource_context_hash_id)

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

    def delete_by_datasource_context_hash_id(self, *, datasource_context_hash_id: int) -> int:
        deleted = self._conn.execute(
            """
            DELETE FROM
                chunk
            WHERE
                datasource_context_hash_id = ?
            """,
            [datasource_context_hash_id],
        ).rowcount
        self._refresh_fts_index()
        return int(deleted or 0)

    def list(self) -> list[ChunkDTO]:
        rows = fetchall_dicts(
            cur=self._conn,
            sql="""
            SELECT
                *
            FROM
                chunk
            ORDER BY
                chunk_id DESC
            """,
        )
        return [self._row_to_dto(r) for r in rows]

    def bulk_insert(
        self,
        *,
        full_type: str,
        datasource_id: str,
        datasource_context_hash_id: int,
        chunk_contents: Sequence[Tuple[str, Optional[str], str]],
    ) -> Sequence[int]:
        values_sql = ", ".join(["(?, ?, ?, ?, ?, ?)"] * len(chunk_contents))
        sql = f"""
            INSERT INTO
                chunk(full_type, datasource_id, embeddable_text, display_text, keyword_index_text, datasource_context_hash_id)
            VALUES
                {values_sql}
            RETURNING
                chunk_id
        """

        params: list[Any] = []
        for embeddable_text, display_text, keyword_index_text in chunk_contents:
            params.extend(
                [
                    full_type,
                    datasource_id,
                    embeddable_text,
                    display_text,
                    keyword_index_text,
                    datasource_context_hash_id,
                ]
            )

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
    def _row_to_dto(row: dict[str, Any]) -> ChunkDTO:
        return ChunkDTO(
            chunk_id=int(row["chunk_id"]),
            full_type=row["full_type"],
            datasource_id=row["datasource_id"],
            embeddable_text=row["embeddable_text"],
            display_text=row["display_text"],
            created_at=row["created_at"],
            keyword_index_text=row["keyword_index_text"],
            datasource_context_hash_id=row["datasource_context_hash_id"],
        )
