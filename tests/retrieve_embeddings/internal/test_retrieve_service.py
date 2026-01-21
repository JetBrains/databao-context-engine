from unittest.mock import Mock

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.retrieve_embeddings.internal.retrieve_service import RetrieveService
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult


def test_retrieve_returns_results():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 42
    run_repo.get_by_run_name.return_value = run

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    expected = [
        VectorSearchResult(
            display_text="a",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/a",
        ),
        VectorSearchResult(
            display_text="b",
            cosine_distance=0.51,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/b",
        ),
    ]
    vector_search_repo.get_display_texts_by_similarity.return_value = expected

    retrieve_service = RetrieveService(
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="hello world")

    shard_resolver.resolve.assert_called_once_with(
        embedder="ollama",
        model_id="nomic-embed-text",
    )

    provider.embed.assert_called_once_with("hello world")

    vector_search_repo.get_display_texts_by_similarity.assert_called_once_with(
        table_name="emb_tbl",
        retrieve_vec=[0.1, 0.2],
        dimension=768,
        limit=10,
    )

    assert result == expected


def test_retrieve_uses_run_name_if_provided():
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    vector_search_repo.get_display_texts_by_similarity.return_value = [
        VectorSearchResult(
            display_text="a",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/a",
        ),
        VectorSearchResult(
            display_text="b",
            cosine_distance=0.51,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/b",
        ),
    ]

    retrieve_service = RetrieveService(
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    retrieve_service.retrieve(project_id="proj-1", text="hello world")


def test_retrieve_honors_limit():
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    expected = [
        VectorSearchResult(
            display_text="x",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/x",
        ),
    ]
    vector_search_repo.get_display_texts_by_similarity.return_value = expected

    retrieve_service = RetrieveService(
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="q", limit=3)

    vector_search_repo.get_display_texts_by_similarity.assert_called_once()
    _, kwargs = vector_search_repo.get_display_texts_by_similarity.call_args
    assert kwargs["limit"] == 3
    assert result == expected
