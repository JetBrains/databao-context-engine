from collections.abc import Sequence
from dataclasses import dataclass

import duckdb


@dataclass(kw_only=True, frozen=True)
class VectorSearchResult:
    display_text: str
    embeddable_text: str
    cosine_distance: float


class VectorSearchRepository:
    _DEFAULT_DISTANCE_THRESHOLD = 0.75

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def get_display_texts_by_similarity(
        self, *, table_name: str, run_id: int, retrieve_vec: Sequence[float], dimension: int, limit: int
    ) -> list[VectorSearchResult]:
        """
        Read only similarity search on a specific embedding shard table.
        Returns the display text for the closest matches in a given run
        """
        rows = self._conn.execute(
            f"""
            SELECT
                COALESCE(c.display_text, c.embeddable_text) AS display_text,
                c.embeddable_text AS embeddable_text,
                array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) AS cosine_distance,
            FROM
                {table_name} e
                JOIN chunk c ON e.chunk_id = c.chunk_id
                JOIN datasource_run dr ON c.datasource_run_id = dr.datasource_run_id
            WHERE
                dr.run_id = ?
                AND cosine_distance < ?
            ORDER BY
                array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) ASC
            LIMIT ?
            """,
            [list(retrieve_vec), run_id, list(retrieve_vec), limit, self._DEFAULT_DISTANCE_THRESHOLD],
        ).fetchall()

        return [VectorSearchResult(display_text=row[0], embeddable_text=row[1], cosine_distance=row[2]) for row in rows]
