from datetime import datetime
from typing import Tuple

from duckdb import ConstraintException, DuckDBPyConnection

from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import DatasourceContextHashDTO


class DatasourceContextHashRepository:
    def __init__(self, conn: DuckDBPyConnection):
        self._conn = conn

    def insert(
        self, *, datasource_id: str, hash_algorithm: str, hash_: str, hashed_at: datetime
    ) -> DatasourceContextHashDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                datasource_context_hash(datasource_id, hash_algorithm, hash, hashed_at)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [datasource_id, hash_algorithm, hash_, hashed_at],
            ).fetchone()
            if row is None:
                raise RuntimeError("datasource_context_hash creation returned no object")

            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def list(self) -> list[DatasourceContextHashDTO]:
        rows = self._conn.execute(
            """
            SELECT
                *
            FROM
                datasource_context_hash
            ORDER BY
                datasource_context_hash_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    def get_by_datasource_id_and_hash(
        self, *, datasource_id: str, hash_algorithm: str, hash_: str
    ) -> DatasourceContextHashDTO | None:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                datasource_context_hash
            WHERE
                datasource_id = ?
                AND hash_algorithm = ?
                AND hash = ?
            """,
            [datasource_id, hash_algorithm, hash_],
        ).fetchone()
        return self._row_to_dto(row) if row else None

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
    def _row_to_dto(row: Tuple) -> DatasourceContextHashDTO:
        (datasource_context_hash_id, datasource_id, hash_algorithm, hash_, hashed_at) = row
        return DatasourceContextHashDTO(
            datasource_context_hash_id=datasource_context_hash_id,
            datasource_id=datasource_id,
            hash_algorithm=hash_algorithm,
            hash=hash_,
            hashed_at=hashed_at,
        )
