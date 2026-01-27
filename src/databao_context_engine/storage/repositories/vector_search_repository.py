from collections.abc import Sequence
from dataclasses import dataclass

import duckdb

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType


@dataclass(kw_only=True, frozen=True)
class VectorSearchResult:
    display_text: str
    embeddable_text: str
    cosine_distance: float
    datasource_type: DatasourceType
    datasource_id: DatasourceId


class VectorSearchRepository:
    _DEFAULT_DISTANCE_THRESHOLD = 0.75

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def get_display_texts_by_similarity(
        self, *, table_name: str, retrieve_vec: Sequence[float], dimension: int, limit: int
    ) -> list[VectorSearchResult]:
        """Read only similarity search on a specific embedding shard table."""
        rows = self._conn.execute(
            f"""
            SELECT
                COALESCE(c.display_text, c.embeddable_text) AS display_text,
                c.embeddable_text,
                array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) AS cosine_distance,
                c.full_type,
                c.datasource_id,
            FROM
                {table_name} e
                JOIN chunk c ON e.chunk_id = c.chunk_id
            WHERE
                cosine_distance < ?
            ORDER BY
                array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) ASC
            LIMIT ?
            """,
            [list(retrieve_vec), self._DEFAULT_DISTANCE_THRESHOLD, list(retrieve_vec), limit],
        ).fetchall()

        return [
            VectorSearchResult(
                display_text=row[0],
                embeddable_text=row[1],
                cosine_distance=row[2],
                datasource_type=DatasourceType(full_type=row[3]),
                datasource_id=DatasourceId.from_string_repr(row[4]),
            )
            for row in rows
        ]
