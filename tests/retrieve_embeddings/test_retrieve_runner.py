from unittest.mock import Mock

from databao_context_engine import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.retrieve_embeddings.retrieve_runner import retrieve
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult


def test_retrieve_without_export(capsys):
    service = Mock()
    expected = [
        VectorSearchResult(
            display_text="a",
            embeddable_text="a",
            cosine_distance=0.0,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
        ),
        VectorSearchResult(
            display_text="b",
            embeddable_text="b",
            cosine_distance=0.1,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
        ),
        VectorSearchResult(
            display_text="c",
            embeddable_text="c",
            cosine_distance=0.2,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id=DatasourceId.from_string_repr("full/c.yaml"),
        ),
    ]
    service.retrieve.return_value = expected

    result = retrieve(
        retrieve_service=service,
        text="hello",
        limit=5,
    )

    service.retrieve.assert_called_once_with(text="hello", limit=5, datasource_ids=None)

    assert result == expected
