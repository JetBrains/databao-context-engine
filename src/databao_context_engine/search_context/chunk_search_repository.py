import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

import duckdb

import databao_context_engine.perf.core as perf
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType

logger = logging.getLogger(__name__)


class ChunkType(str, Enum):
    """Enum of the supported chunk types to search."""

    TABLE = "table"
    COLUMN = "column"


@dataclass(kw_only=True, frozen=True)
class VectorSearchCandidate:
    chunk_id: int
    chunk_type: ChunkType | None
    display_text: str
    embeddable_text: str
    cosine_distance: float
    datasource_type: DatasourceType
    datasource_id: DatasourceId


@dataclass(kw_only=True, frozen=True)
class Bm25SearchCandidate:
    chunk_id: int
    chunk_type: ChunkType | None
    display_text: str
    embeddable_text: str
    bm25_score: float
    datasource_type: DatasourceType
    datasource_id: DatasourceId


@dataclass(kw_only=True, frozen=True)
class RrfScore:
    vector_distance: float | None = None
    bm25_score: float | None = None
    rrf_score: float

    @property
    def score(self):
        return self.rrf_score


@dataclass(kw_only=True, frozen=True)
class VectorSearchScore:
    vector_distance: float

    @property
    def score(self):
        return self.vector_distance


@dataclass(kw_only=True, frozen=True)
class KeywordSearchScore:
    bm25_score: float

    @property
    def score(self):
        return self.bm25_score


@dataclass(kw_only=True, frozen=True)
class SearchResult:
    chunk_id: int
    chunk_type: ChunkType | None
    display_text: str
    embeddable_text: str
    datasource_type: DatasourceType
    datasource_id: DatasourceId
    score: RrfScore | VectorSearchScore | KeywordSearchScore


class ChunkSearchRepository:
    _DEFAULT_DISTANCE_THRESHOLD = 0.75
    _DEFAULT_RRF_K = 60
    _DEFAULT_CANDIDATE_MULTIPLIER = 3
    _BM25_FTS_SCHEMA = "fts_main_chunk"

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    @perf.perf_span("chunk_search.search_chunks_by_vector_similarity")
    def search_chunks_by_vector_similarity(
        self,
        *,
        table_name: str,
        search_vec: Sequence[float],
        dimension: int,
        limit: int,
        datasource_context_hashes: list[DatasourceContextHash],
        chunk_types: list[ChunkType] | None = None,
    ) -> list[SearchResult]:
        """Read only similarity search on a specific embedding shard table."""
        vector_candidates = self._get_vector_candidates(
            table_name=table_name,
            search_vec=search_vec,
            dimension=dimension,
            limit=limit,
            datasource_context_hashes=datasource_context_hashes,
            chunk_types=chunk_types,
        )
        return [
            SearchResult(
                chunk_id=candidate.chunk_id,
                chunk_type=candidate.chunk_type,
                display_text=candidate.display_text,
                embeddable_text=candidate.embeddable_text,
                datasource_type=candidate.datasource_type,
                datasource_id=candidate.datasource_id,
                score=VectorSearchScore(vector_distance=candidate.cosine_distance),
            )
            for candidate in vector_candidates
        ]

    @perf.perf_span("chunk_search._get_vector_candidates")
    def _get_vector_candidates(
        self,
        *,
        table_name: str,
        search_vec: Sequence[float],
        dimension: int,
        limit: int,
        datasource_context_hashes: list[DatasourceContextHash],
        chunk_types: list[ChunkType] | None = None,
    ) -> list[VectorSearchCandidate]:
        """Read only vector candidates on a specific embedding shard table."""
        if not datasource_context_hashes:
            return []

        allowed_hashes_sql, hash_params = self._build_allowed_hashes_values(datasource_context_hashes)

        chunk_types_param: list[list[ChunkType]]
        if chunk_types:
            search_candidates_chunk_type_filter = "WHERE c.chunk_type IN ?"
            chunk_types_param = [chunk_types]
        else:
            chunk_types_param = []
            search_candidates_chunk_type_filter = ""

        params: list[Any] = [
            *hash_params,
            list(search_vec),
            *chunk_types_param,
            self._DEFAULT_DISTANCE_THRESHOLD,
            limit,
        ]

        rows = self._conn.execute(
            f"""
            WITH allowed_hashes(datasource_id, hash, hash_algorithm) AS (
                VALUES {allowed_hashes_sql}
            ),
            vector_candidates AS (
                SELECT
                    c.chunk_id,
                    c.chunk_type,
                    COALESCE(c.display_text, c.embeddable_text) AS display_text,
                    c.embeddable_text,
                    array_cosine_distance(e.vec, CAST(? AS FLOAT[{dimension}])) AS cosine_distance,
                    c.full_type,
                    c.datasource_id
                FROM
                    {table_name} e
                    JOIN chunk c ON e.chunk_id = c.chunk_id
                    JOIN datasource_context_hash h ON c.datasource_context_hash_id = h.datasource_context_hash_id
                    JOIN allowed_hashes ah
                        ON h.datasource_id = ah.datasource_id
                        AND h.hash = ah.hash
                        AND h.hash_algorithm = ah.hash_algorithm
                {search_candidates_chunk_type_filter}
            )
            SELECT
                vc.chunk_id,
                vc.chunk_type,
                vc.display_text,
                vc.embeddable_text,
                vc.cosine_distance,
                vc.full_type,
                vc.datasource_id
            FROM
                vector_candidates vc
            WHERE
                vc.cosine_distance < ?
            ORDER BY
                vc.cosine_distance ASC
            LIMIT ?
            """,
            params,
        ).fetchall()

        return [
            VectorSearchCandidate(
                chunk_id=row[0],
                chunk_type=ChunkType(row[1]) if row[1] else None,
                display_text=row[2],
                embeddable_text=row[3],
                cosine_distance=row[4],
                datasource_type=DatasourceType(full_type=row[5]),
                datasource_id=DatasourceId.from_string_repr(row[6]),
            )
            for row in rows
        ]

    @perf.perf_span("chunk_search.search_chunks_with_hybrid_search")
    def search_chunks_with_hybrid_search(
        self,
        *,
        table_name: str,
        search_vec: Sequence[float],
        search_text: str,
        dimension: int,
        limit: int,
        datasource_context_hashes: list[DatasourceContextHash],
        chunk_types: list[ChunkType] | None = None,
    ) -> list[SearchResult]:
        """Hybrid retrieval combining vector similarity and BM25 with Reciprocal Rank Fusion (RRF).

        Returns:
            A list of ranked search results.
        """
        candidate_limit = max(limit, limit * self._DEFAULT_CANDIDATE_MULTIPLIER)
        vector_candidates = self._get_vector_candidates(
            table_name=table_name,
            search_vec=search_vec,
            dimension=dimension,
            limit=candidate_limit,
            datasource_context_hashes=datasource_context_hashes,
            chunk_types=chunk_types,
        )

        bm25_candidates = self._get_bm25_candidates(
            query_text=search_text,
            limit=candidate_limit,
            datasource_context_hashes=datasource_context_hashes,
            chunk_types=chunk_types,
        )
        return self._fuse_by_rrf(
            vector_candidates=vector_candidates,
            bm25_candidates=bm25_candidates,
            limit=limit,
        )

    @perf.perf_span("chunk_search.search_chunks_by_keyword_relevance")
    def search_chunks_by_keyword_relevance(
        self,
        *,
        query_text: str,
        limit: int,
        datasource_context_hashes: list[DatasourceContextHash],
        chunk_types: list[ChunkType] | None = None,
    ) -> list[SearchResult]:
        """Read only BM25 search over chunk text."""
        bm25_candidates = self._get_bm25_candidates(
            query_text=query_text,
            limit=limit,
            datasource_context_hashes=datasource_context_hashes,
            chunk_types=chunk_types,
        )

        return [
            SearchResult(
                chunk_id=candidate.chunk_id,
                chunk_type=candidate.chunk_type,
                display_text=candidate.display_text,
                embeddable_text=candidate.embeddable_text,
                datasource_type=candidate.datasource_type,
                datasource_id=candidate.datasource_id,
                score=KeywordSearchScore(bm25_score=candidate.bm25_score),
            )
            for candidate in bm25_candidates
        ]

    @perf.perf_span("chunk_search._get_bm25_candidates")
    def _get_bm25_candidates(
        self,
        *,
        query_text: str,
        limit: int,
        datasource_context_hashes: list[DatasourceContextHash],
        chunk_types: list[ChunkType] | None = None,
    ) -> list[Bm25SearchCandidate]:
        if not datasource_context_hashes:
            return []

        allowed_hashes_sql, hash_params = self._build_allowed_hashes_values(datasource_context_hashes)
        if chunk_types:
            search_candidates_chunk_type_filter = "WHERE c.chunk_type IN ?"
            chunk_types = [chunk_types]
        else:
            chunk_types = []
            search_candidates_chunk_type_filter = ""

        params: list[Any] = [*hash_params, query_text, *chunk_types, limit]

        rows = self._conn.execute(
            f"""
            WITH allowed_hashes(datasource_id, hash, hash_algorithm) AS (
                VALUES {allowed_hashes_sql}
            ),
            bm25_candidates AS (
                SELECT
                    c.chunk_id,
                    c.chunk_type,
                    COALESCE(c.display_text, c.embeddable_text) AS display_text,
                    c.embeddable_text,
                    c.full_type,
                    c.datasource_id,
                    {self._BM25_FTS_SCHEMA}.match_bm25(
                        c.chunk_id,
                        ?
                    ) AS bm25_score
                FROM
                    chunk c
                    JOIN datasource_context_hash h ON c.datasource_context_hash_id = h.datasource_context_hash_id
                    JOIN allowed_hashes ah
                        ON h.datasource_id = ah.datasource_id
                        AND h.hash = ah.hash
                        AND h.hash_algorithm = ah.hash_algorithm
                {search_candidates_chunk_type_filter}
            )
            SELECT
                b.chunk_id,
                b.chunk_type,
                b.display_text,
                b.embeddable_text,
                b.bm25_score,
                b.full_type,
                b.datasource_id
            FROM
                bm25_candidates b
            WHERE
                b.bm25_score IS NOT NULL
            ORDER BY
                b.bm25_score DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

        return [
            Bm25SearchCandidate(
                chunk_id=row[0],
                chunk_type=ChunkType(row[1]) if row[1] else None,
                display_text=row[2],
                embeddable_text=row[3],
                bm25_score=row[4],
                datasource_type=DatasourceType(full_type=row[5]),
                datasource_id=DatasourceId.from_string_repr(row[6]),
            )
            for row in rows
        ]

    @staticmethod
    def _build_allowed_hashes_values(datasource_context_hashes: list[DatasourceContextHash]) -> tuple[str, list[Any]]:
        allowed_hashes_sql = ", ".join(["(?, ?, ?)"] * len(datasource_context_hashes))
        params: list[Any] = []
        for datasource_context_hash in datasource_context_hashes:
            params.extend(
                [
                    str(datasource_context_hash.datasource_id),
                    datasource_context_hash.hash,
                    datasource_context_hash.hash_algorithm,
                ]
            )
        return allowed_hashes_sql, params

    @perf.perf_span("chunk_search._fuse_by_rrf")
    def _fuse_by_rrf(
        self,
        *,
        vector_candidates: list[VectorSearchCandidate],
        bm25_candidates: list[Bm25SearchCandidate],
        limit: int,
    ) -> list[SearchResult]:
        scores_by_chunk_id: dict[int, float] = {}
        vector_by_chunk_id = {candidate.chunk_id: candidate for candidate in vector_candidates}
        bm25_by_chunk_id = {candidate.chunk_id: candidate for candidate in bm25_candidates}

        for rank, candidate in enumerate(vector_candidates, start=1):
            chunk_id = candidate.chunk_id
            scores_by_chunk_id[chunk_id] = scores_by_chunk_id.get(chunk_id, 0.0) + (1.0 / (self._DEFAULT_RRF_K + rank))

        for rank, candidate in enumerate(bm25_candidates, start=1):
            chunk_id = candidate.chunk_id
            scores_by_chunk_id[chunk_id] = scores_by_chunk_id.get(chunk_id, 0.0) + (1.0 / (self._DEFAULT_RRF_K + rank))

        ranked_chunk_ids = sorted(
            scores_by_chunk_id.keys(),
            key=lambda chunk_id: scores_by_chunk_id[chunk_id],
            reverse=True,
        )
        results: list[SearchResult] = []
        for chunk_id in ranked_chunk_ids[0:limit]:
            vector_candidate = vector_by_chunk_id.get(chunk_id)
            bm25_candidate = bm25_by_chunk_id.get(chunk_id)
            data_candidate = vector_candidate or bm25_candidate
            if data_candidate is None:
                continue
            results.append(
                SearchResult(
                    chunk_id=chunk_id,
                    chunk_type=data_candidate.chunk_type,
                    display_text=data_candidate.display_text,
                    embeddable_text=data_candidate.embeddable_text,
                    datasource_type=data_candidate.datasource_type,
                    datasource_id=data_candidate.datasource_id,
                    score=RrfScore(
                        vector_distance=vector_candidate.cosine_distance if vector_candidate is not None else None,
                        bm25_score=bm25_candidate.bm25_score if bm25_candidate is not None else None,
                        rrf_score=scores_by_chunk_id[chunk_id],
                    ),
                )
            )
        return results
