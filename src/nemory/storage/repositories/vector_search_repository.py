from collections.abc import Sequence
from dataclasses import dataclass

import duckdb

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.project.datasource_discovery import DatasourceId


@dataclass(kw_only=True, frozen=True)
class VectorSearchResult:
    display_text: str
    embeddable_text: str
    cosine_distance: float
    datasource_type: DatasourceType
    datasource_id: DatasourceId


def get_search_results_display_text(search_results: list[VectorSearchResult]) -> list[str]:
    return [result.display_text for result in search_results]


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
                dr.full_type,
                dr.source_id,
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
            [list(retrieve_vec), run_id, self._DEFAULT_DISTANCE_THRESHOLD, list(retrieve_vec), limit],
        ).fetchall()

        return [
            VectorSearchResult(
                display_text=row[0],
                embeddable_text=row[1],
                cosine_distance=row[2],
                datasource_type=DatasourceType(full_type=row[3]),
                datasource_id=row[4],
            )
            for row in rows
        ]
