import logging
from collections.abc import Sequence
from enum import Enum

import databao_context_engine.perf.core as perf
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.prompts.provider import PromptProvider
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.storage.repositories.chunk_search_repository import (
    ChunkSearchRepository,
    SearchResult,
)

logger = logging.getLogger(__name__)


class RAG_MODE(Enum):
    RAW_QUERY = "RAW_QUERY"
    QUERY_WITH_INSTRUCTION = "QUERY_WITH_INSTRUCTION"
    REWRITE_QUERY = "REWRITE_QUERY"


class ContextSearchMode(Enum):
    HYBRID_SEARCH = "HYBRID_SEARCH"
    KEYWORD_SEARCH = "KEYWORD_SEARCH"
    VECTOR_SEARCH = "VECTOR_SEARCH"


class RetrieveService:
    def __init__(
        self,
        *,
        chunk_search_repo: ChunkSearchRepository,
        shard_resolver: EmbeddingShardResolver,
        embedding_provider: EmbeddingProvider,
        prompt_provider: PromptProvider | None,
    ):
        self._shard_resolver = shard_resolver
        self._provider = embedding_provider
        self._chunk_search_repo = chunk_search_repo
        self._prompt_provider = prompt_provider

    @perf.perf_span(
        "search_context.do_search",
        attrs=lambda *_, rag_mode, **__: {
            "rag_mode": rag_mode.value,
        },
    )
    def retrieve(
        self,
        *,
        text: str,
        limit: int | None = None,
        datasource_ids: list[DatasourceId] | None = None,
        rag_mode: RAG_MODE,
        context_search_mode: ContextSearchMode,
    ) -> list[SearchResult]:
        if limit is None:
            limit = 10

        search_results = self._do_retrieve(
            text=text,
            limit=limit,
            datasource_ids=datasource_ids,
            rag_mode=rag_mode,
            context_search_mode=context_search_mode,
        )

        logger.debug(f"Retrieved {len(search_results)} display texts")

        if logger.isEnabledFor(logging.DEBUG):
            if search_results:
                top_index = min(10, limit)
                top_results = search_results[0:top_index]
                msg = "\n".join([f"({r.score.score}, {r.embeddable_text})" for r in top_results])
                logger.debug(f"Top {top_index} results:\n{msg}")
                lowest_score = min(search_results, key=lambda result: result.score.score or 0.0)
                logger.debug(f"Worst result: ({lowest_score.score.score}, {lowest_score.embeddable_text})")
            else:
                logger.debug("No results found")

        return search_results

    def _do_retrieve(
        self,
        *,
        text: str,
        limit: int,
        datasource_ids: list[DatasourceId] | None = None,
        rag_mode: RAG_MODE,
        context_search_mode: ContextSearchMode,
    ) -> list[SearchResult]:
        if context_search_mode == ContextSearchMode.KEYWORD_SEARCH:
            return self._chunk_search_repo.search_chunks_by_keyword_relevance(
                query_text=text,
                limit=limit,
                datasource_ids=datasource_ids,
            )

        table_name, dimension = self._shard_resolver.resolve(
            embedder=self._provider.embedder, model_id=self._provider.model_id
        )

        match rag_mode:
            case RAG_MODE.QUERY_WITH_INSTRUCTION:
                task_description = "Generate an embedding aware of the named entities such as to be useful for a semantic search on database table and column names"
                embeddable_query = f"Instruct: {task_description}\nQuery:{text}"
            case RAG_MODE.REWRITE_QUERY:
                embeddable_query = self._rewrite_retrieve_query(text)
            case _:
                embeddable_query = text

        with perf.span(
            "search_context.embed_search_text",
            attrs={"model_id": self._provider.model_id, "model_dim": self._provider.dim},
        ):
            retrieve_vec: Sequence[float] = self._provider.embed(embeddable_query)

        match context_search_mode:
            case ContextSearchMode.VECTOR_SEARCH:
                return self._chunk_search_repo.search_chunks_by_vector_similarity(
                    table_name=table_name,
                    retrieve_vec=retrieve_vec,
                    dimension=dimension,
                    limit=limit,
                    datasource_ids=datasource_ids,
                )
            case ContextSearchMode.HYBRID_SEARCH:
                return self._chunk_search_repo.search_chunks_with_hybrid_search(
                    table_name=table_name,
                    retrieve_vec=retrieve_vec,
                    query_text=text,
                    dimension=dimension,
                    limit=limit,
                    datasource_ids=datasource_ids,
                )

    @perf.perf_span("search_context.rewrite_query")
    def _rewrite_retrieve_query(self, text: str) -> str:
        if not self._prompt_provider:
            raise ValueError(f"Prompt provider should never be None when rag_mode is {RAG_MODE.REWRITE_QUERY.value}")

        prompt = f"""You are an AI language model assistant. 
        Your task is to use NLP (Natural Language Processing) and NER (Named Entity Recognition) to extract named entities from a given question.
        Those entities will be used as metadata in a semantic search.
        Do not try to answer the question or get more information about the entities you find. 
        
        Output each entity separated by a newline in the following format, without any other explanations: 
        "extracted entity": "entity classification or tag"
        
        Examples:
        1. From the question "Where did Apple CEO Tim Cook announced the latest iPhone models last September?", you should respond with:
        "Apple": "Organization"
        "Tim Cook": "Person"
        "iPhone": "Product"
        "last September": "Date"
        
        2. From the question "How many accounts in North Bohemia has made a transaction with the partner's bank being AB?", you should respond with:
        "North Bohemia": "Location"
        "partner": "Person"
        "AB": "Organization"
        
        3. From the question "List out top 10 Spanish drivers who were born before 1982 and have the latest lap time.", you should respond with:
        "Spanish": "NORP (Nationalities, Religious, or Political groups)"
        "1982": "Date"
        
        Here is the question:
        {text}
        """

        try:
            extracted_named_entities = self._prompt_provider.prompt(prompt=prompt)
        except Exception:
            logger.debug(f"Failed to prompt rewritten query for question: {text}")
            return text

        return f"{text}\n{extracted_named_entities}"
