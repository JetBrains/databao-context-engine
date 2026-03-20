from datetime import datetime
from typing import Any

from duckdb import ConstraintException, DuckDBPyConnection

from databao_context_engine.plugins.duckdb_tools import fetchall_dicts, fetchone_dicts
from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import DatasourceContextHashDTO


class DatasourceContextHashRepository:
    def __init__(self, conn: DuckDBPyConnection):
        self._conn = conn

    def insert(
        self, *, datasource_id: str, hash_algorithm: str, hash_: str, hashed_at: datetime
    ) -> DatasourceContextHashDTO:
        try:
            row = fetchone_dicts(
                cur=self._conn,
                sql="""
            INSERT INTO
                datasource_context_hash(datasource_id, hash_algorithm, hash, hashed_at)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                params=[datasource_id, hash_algorithm, hash_, hashed_at],
            )
            if row is None:
                raise RuntimeError("datasource_context_hash creation returned no object")

            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def list(self) -> list[DatasourceContextHashDTO]:
        rows = fetchall_dicts(
            cur=self._conn,
            sql="""
            SELECT
                *
            FROM
                datasource_context_hash
            ORDER BY
                datasource_context_hash_id DESC
            """,
        )
        return [self._row_to_dto(r) for r in rows]

    def get_by_datasource_id_and_hash(
        self, *, datasource_id: str, hash_algorithm: str, hash_: str
    ) -> DatasourceContextHashDTO | None:
        row = fetchone_dicts(
            cur=self._conn,
            sql="""
            SELECT
                *
            FROM
                datasource_context_hash
            WHERE
                datasource_id = ?
                AND hash_algorithm = ?
                AND hash = ?
            """,
            params=[datasource_id, hash_algorithm, hash_],
        )
        return self._row_to_dto(row) if row else None

    def delete(self, *, datasource_context_hash_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM
                datasource_context_hash
            WHERE
                datasource_context_hash_id = ?
                RETURNING
                    datasource_context_hash_id
            """,
            [datasource_context_hash_id],
        )

        return 1 if row else 0

    def delete_by_datasource_id_and_hash(self, *, datasource_id: str, hash_algorithm: str, hash_: str) -> int:
        rows = self._conn.execute(
            """
            DELETE FROM
                datasource_context_hash
            WHERE
                datasource_id = ?
                AND hash_algorithm = ?
                AND hash = ?
            RETURNING
                datasource_context_hash_id
            """,
            [datasource_id, hash_algorithm, hash_],
        ).fetchall()

        return len(rows)

    @staticmethod
    def _row_to_dto(row: dict[str, Any]) -> DatasourceContextHashDTO:
        return DatasourceContextHashDTO(
            datasource_context_hash_id=row["datasource_context_hash_id"],
            datasource_id=row["datasource_id"],
            hash_algorithm=row["hash_algorithm"],
            hash=row["hash"],
            hashed_at=row["hashed_at"],
        )
