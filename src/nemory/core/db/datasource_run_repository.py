from typing import Tuple, Optional

import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import DatasourceRunDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


class DatasourceRunRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        run_id: int,
        plugin: str,
        source_id: str,
        storage_directory: str,
    ) -> DatasourceRunDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                datasource_run(run_id, plugin, source_id, storage_directory)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [run_id, plugin, source_id, storage_directory],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, datasource_run_id: int) -> Optional[DatasourceRunDTO]:
        row = self._conn.execute(
            """
        SELECT
            *
        FROM
            datasource_run 
        WHERE
            datasource_run_id = ?
        """,
            [datasource_run_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        datasource_run_id: int,
        *,
        plugin: Optional[str] = None,
        source_id: Optional[str] = None,
        storage_directory: Optional[str] = None,
    ) -> Optional[DatasourceRunDTO]:
        sets, params = [], []

        if plugin is not None:
            sets.append("plugin = ?")
            params.append(plugin)
        if source_id is not None:
            sets.append("source_id = ?")
            params.append(source_id)
        if storage_directory is not None:
            sets.append("storage_directory = ?")
            params.append(storage_directory)

        if not sets:
            return self.get(datasource_run_id)

        params.append(datasource_run_id)
        row = self._conn.execute(
            f"""
            UPDATE 
                datasource_run
            SET 
                {", ".join(sets)}
            WHERE 
                datasource_run_id = ?
            RETURNING
             *
            """,
            params,
        ).fetchone()

        return self._row_to_dto(row) if row else None

    def delete(self, datasource_run_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM 
                datasource_run 
            WHERE 
                datasource_run_id = ? 
            RETURNING 
                datasource_run_id
            """,
            [datasource_run_id],
        ).fetchone()
        return 1 if row else 0

    def list(self) -> list[DatasourceRunDTO]:
        rows = self._conn.execute(
            """
            SELECT
              *
            FROM 
                datasource_run
            ORDER BY
                datasource_run_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> DatasourceRunDTO:
        datasource_run_id, run_id, plugin, source_id, storage_directory, created_at = row
        return DatasourceRunDTO(
            datasource_run_id=int(datasource_run_id),
            run_id=int(run_id),
            plugin=str(plugin),
            source_id=str(source_id),
            storage_directory=str(storage_directory),
            created_at=created_at,
        )
