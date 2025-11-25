from collections.abc import Sequence

import duckdb


class VectorSearchRepository:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def get_display_texts_by_similarity(
        self, *, table_name: str, run_id: int, retrieve_vec: Sequence[float], dimension: int, limit: int
    ) -> list[str]:
        """
        Read only similarity search on a specific embedding shard table.
        Returns the display text for the closest matches in a given run
        """
        rows = self._conn.execute(
            f"""
            SELECT
                COALESCE(c.display_text, c.embeddable_text) AS display_text,
            FROM
                {table_name} e
                JOIN chunk c ON e.chunk_id = c.chunk_id
                JOIN datasource_run dr ON c.datasource_run_id = dr.datasource_run_id
            WHERE
                dr.run_id = ?
            ORDER BY
                array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) ASC
            LIMIT ?
            """,
            [run_id, list(retrieve_vec), limit],
        ).fetchall()

        return [r[0] for r in rows]
