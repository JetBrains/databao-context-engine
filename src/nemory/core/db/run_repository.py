from datetime import datetime
from typing import Optional
import duckdb
from nemory.core.db.dtos import RunDTO, RunStatus


class RunRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self, *, status: RunStatus = RunStatus.RUNNING, project_id: str, nemory_version: Optional[str] = None
    ) -> RunDTO:
        row = self._conn.execute(
            """
            INSERT INTO 
                run (status, project_id, nemory_version)
            VALUES 
                (?, ?, ?)
            RETURNING 
                *
            """,
            [status.value, project_id, nemory_version],
        ).fetchone()
        return self._row_to_dto(row)

    def get(self, run_id: int) -> Optional[RunDTO]:
        row = self._conn.execute(
            """
            SELECT 
                *
            FROM
                run
            WHERE
                run_id = ?
            """,
            [run_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        run_id: int,
        *,
        status: Optional[RunStatus] = None,
        project_id: Optional[str] = None,
        ended_at: Optional[datetime] = None,
        nemory_version: Optional[str] = None,
    ) -> Optional[RunDTO]:
        sets, params = [], []

        if status is not None:
            sets.append("status = ?")
            params.append(status.value)
        if project_id is not None:
            sets.append("project_id = ?")
            params.append(project_id)
        if ended_at is not None:
            sets.append("ended_at = ?")
            params.append(ended_at)
        if nemory_version is not None:
            sets.append("nemory_version = ?")
            params.append(nemory_version)

        if not sets:
            return self.get(run_id)

        params.append(run_id)
        row = self._conn.execute(
            f"""
            UPDATE
                run
            SET 
                {", ".join(sets)}
            WHERE
                run_id = ?
            RETURNING
                *
            """,
            params,
        ).fetchone()

        return self._row_to_dto(row) if row else None

    def delete(self, run_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM
                run
            WHERE
                run_id = ?
            RETURNING
                run_id
            """,
            [run_id],
        ).fetchone()
        return 1 if row else 0

    def list(self) -> list[RunDTO]:
        rows = self._conn.execute(
            """
            SELECT
                *
            FROM
                run
            ORDER BY
                run_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: tuple) -> RunDTO:
        run_id, status, project_id, started_at, ended_at, nemory_version = row
        return RunDTO(
            run_id=int(run_id),
            status=RunStatus(status),
            project_id=str(project_id),
            started_at=started_at,
            ended_at=ended_at,
            nemory_version=nemory_version,
        )
