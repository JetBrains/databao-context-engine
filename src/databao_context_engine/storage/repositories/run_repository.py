from datetime import datetime
from typing import Any, Optional

import duckdb

from databao_context_engine.services.run_name_policy import RunNamePolicy
from databao_context_engine.storage.models import RunDTO


class RunRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection, run_name_policy: RunNamePolicy):
        self._conn = conn
        self._run_name_policy = run_name_policy

    def create(
        self, *, project_id: str, dce_version: Optional[str] = None, started_at: datetime | None = None
    ) -> RunDTO:
        if started_at is None:
            started_at = datetime.now()
        run_name = self._run_name_policy.build(run_started_at=started_at)

        row = self._conn.execute(
            """
            INSERT INTO 
                run (project_id, nemory_version, started_at, run_name)
            VALUES 
                (?, ?, ?, ?)
            RETURNING 
                *
            """,
            [project_id, dce_version, started_at, run_name],
        ).fetchone()
        if row is None:
            raise RuntimeError("Run creation returned no object")
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

    def get_by_run_name(self, *, project_id: str, run_name: str) -> RunDTO | None:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                run
            WHERE
                run.project_id = ? AND run_name = ?
            """,
            [project_id, run_name],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def get_latest_run_for_project(self, project_id: str) -> RunDTO | None:
        row = self._conn.execute(
            """
            SELECT
                *
            FROM
                run
            WHERE
                run.project_id = ?
            ORDER BY run.started_at DESC
            LIMIT 1
            """,
            [project_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        run_id: int,
        *,
        project_id: Optional[str] = None,
        ended_at: Optional[datetime] = None,
        dce_version: Optional[str] = None,
    ) -> Optional[RunDTO]:
        sets: list[Any] = []
        params: list[Any] = []

        if project_id is not None:
            sets.append("project_id = ?")
            params.append(project_id)
        if ended_at is not None:
            sets.append("ended_at = ?")
            params.append(ended_at)
        if dce_version is not None:
            sets.append("nemory_version = ?")
            params.append(dce_version)

        if not sets:
            return self.get(run_id)

        params.append(run_id)
        self._conn.execute(
            f"""
                    UPDATE
                        run
                    SET 
                        {", ".join(sets)}
                    WHERE
                        run_id = ?
                    """,
            params,
        )

        return self.get(run_id)

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
        run_id, project_id, started_at, ended_at, dce_version, run_name = row
        return RunDTO(
            run_id=int(run_id),
            run_name=run_name,
            project_id=str(project_id),
            started_at=started_at,
            ended_at=ended_at,
            nemory_version=dce_version,
        )
