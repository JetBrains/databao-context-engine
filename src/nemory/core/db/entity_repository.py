from typing import Tuple, Optional

import duckdb
from _duckdb import ConstraintException

from nemory.core.db.dtos import EntityDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


class EntityRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def create(
        self,
        *,
        run_id: int,
        plugin: str,
        source_id: str,
        document: str,
    ) -> EntityDTO:
        try:
            row = self._conn.execute(
                """
            INSERT INTO
                entity(run_id, plugin, source_id, document)
            VALUES
                (?, ?, ?, ?)
            RETURNING
                *
            """,
                [run_id, plugin, source_id, document],
            ).fetchone()
            return self._row_to_dto(row)
        except ConstraintException as e:
            raise IntegrityError from e

    def get(self, entity_id: int) -> Optional[EntityDTO]:
        row = self._conn.execute(
            """
        SELECT
            *
        FROM
            entity
        WHERE
            entity_id = ?
        """,
            [entity_id],
        ).fetchone()
        return self._row_to_dto(row) if row else None

    def update(
        self,
        entity_id: int,
        *,
        plugin: Optional[str] = None,
        source_id: Optional[str] = None,
        document: Optional[str] = None,
    ) -> Optional[EntityDTO]:
        sets, params = [], []

        if plugin is not None:
            sets.append("plugin = ?")
            params.append(plugin)
        if source_id is not None:
            sets.append("source_id = ?")
            params.append(source_id)
        if document is not None:
            sets.append("document = ?")
            params.append(document)

        if not sets:
            return self.get(entity_id)

        params.append(entity_id)
        row = self._conn.execute(
            f"""
            UPDATE 
                entity
            SET 
                {", ".join(sets)}
            WHERE 
                entity_id = ?
            RETURNING
             *
            """,
            params,
        ).fetchone()

        return self._row_to_dto(row) if row else None

    def delete(self, entity_id: int) -> int:
        row = self._conn.execute(
            """
            DELETE FROM 
                entity 
            WHERE 
                entity_id = ? 
            RETURNING 
                entity_id
            """,
            [entity_id],
        ).fetchone()
        return 1 if row else 0

    def list(self) -> list[EntityDTO]:
        rows = self._conn.execute(
            """
            SELECT
              *
            FROM 
                entity
            ORDER BY
                entity_id DESC
            """
        ).fetchall()
        return [self._row_to_dto(r) for r in rows]

    @staticmethod
    def _row_to_dto(row: Tuple) -> EntityDTO:
        entity_id, run_id, plugin, source_id, document, created_at = row
        return EntityDTO(
            entity_id=int(entity_id),
            run_id=int(run_id),
            plugin=str(plugin),
            source_id=str(source_id),
            document=str(document),
            created_at=created_at,
        )
