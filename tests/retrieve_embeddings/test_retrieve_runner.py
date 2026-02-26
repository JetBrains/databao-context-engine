from unittest.mock import Mock

from databao_context_engine import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.retrieve_embeddings.retrieve_runner import retrieve
from databao_context_engine.retrieve_embeddings.retrieve_service import RAG_MODE
from databao_context_engine.storage.repositories.chunk_search_repository import RrfScore, SearchResult


def test_retrieve_without_export(capsys):
    service = Mock()
    expected = [
        SearchResult(
            chunk_id=1,
            display_text="a",
            embeddable_text="a",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
            score=RrfScore(rrf_score=0.9),
        ),
        SearchResult(
            chunk_id=2,
            display_text="b",
            embeddable_text="b",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
            score=RrfScore(rrf_score=0.8),
        ),
        SearchResult(
            chunk_id=3,
            display_text="c",
            embeddable_text="c",
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/c.yaml"),
            score=RrfScore(rrf_score=0.7),
        ),
    ]
    service.retrieve.return_value = expected

    result = retrieve(
        retrieve_service=service,
        text="hello",
        limit=5,
        rag_mode=RAG_MODE.RAW_QUERY,
    )

    service.retrieve.assert_called_once_with(
        text="hello",
        limit=5,
        datasource_ids=None,
        rag_mode=RAG_MODE.RAW_QUERY,
    )

    assert result == expected
