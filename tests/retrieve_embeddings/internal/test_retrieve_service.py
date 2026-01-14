from unittest.mock import Mock

import pytest

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.retrieve_embeddings.internal.retrieve_service import RetrieveService
from nemory.storage.repositories.vector_search_repository import VectorSearchResult


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
            embeddable_text="a",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/a",
        ),
        VectorSearchResult(
            display_text="b",
            embeddable_text="b",
            cosine_distance=0.51,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/b",
        ),
    ]
    vector_search_repo.get_display_texts_by_similarity.return_value = expected

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="hello world", run_name="run-123")

    shard_resolver.resolve.assert_called_once_with(
        embedder="ollama",
        model_id="nomic-embed-text",
    )

    provider.embed.assert_called_once_with("hello world")

    vector_search_repo.get_display_texts_by_similarity.assert_called_once_with(
        table_name="emb_tbl",
        run_id=42,
        retrieve_vec=[0.1, 0.2],
        dimension=768,
        limit=10,
    )

    assert result == expected


def test_retrieve_uses_run_name_if_provided():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 10
    run_repo.get_by_run_name.return_value = run

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    vector_search_repo.get_display_texts_by_similarity.return_value = [
        VectorSearchResult(
            display_text="a",
            embeddable_text="a",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/a",
        ),
        VectorSearchResult(
            display_text="b",
            embeddable_text="b",
            cosine_distance=0.51,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/b",
        ),
    ]

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    retrieve_service.retrieve(project_id="proj-1", text="hello world", run_name="run-123")

    run_repo.get_by_run_name.assert_called_once_with(project_id="proj-1", run_name="run-123")


def test_retrieve_honors_limit():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 7
    run_repo.get_by_run_name.return_value = run

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    expected = [
        VectorSearchResult(
            display_text="x",
            embeddable_text="x",
            cosine_distance=0.5,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/x",
        ),
    ]
    vector_search_repo.get_display_texts_by_similarity.return_value = expected

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="q", limit=3, run_name="run-123")

    vector_search_repo.get_display_texts_by_similarity.assert_called_once()
    _, kwargs = vector_search_repo.get_display_texts_by_similarity.call_args
    assert kwargs["limit"] == 3
    assert result == expected


def test_resolve_run_name_uses_given_name_when_run_exists():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_name = "run-123"
    run_repo.get_by_run_name.return_value = run

    service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    resolved = service.resolve_run_name(project_id="proj-1", run_name="run-123")

    run_repo.get_by_run_name.assert_called_once_with(project_id="proj-1", run_name="run-123")
    assert resolved == "run-123"


def test_resolve_run_name_raises_if_named_run_not_found():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run_repo.get_by_run_name.return_value = None

    service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    with pytest.raises(LookupError) as excinfo:
        service.resolve_run_name(project_id="proj-1", run_name="missing-run")

    run_repo.get_by_run_name.assert_called_once_with(project_id="proj-1", run_name="missing-run")
    assert "missing-run" in str(excinfo.value)
    assert "proj-1" in str(excinfo.value)


def test_resolve_run_name_uses_latest_when_none_given():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    latest = Mock()
    latest.run_name = "latest-run"
    run_repo.get_latest_run_for_project.return_value = latest

    service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    resolved = service.resolve_run_name(project_id="proj-1", run_name=None)

    run_repo.get_latest_run_for_project.assert_called_once_with(project_id="proj-1")
    assert resolved == "latest-run"


def test_resolve_run_name_raises_if_no_runs_for_project():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run_repo.get_latest_run_for_project.return_value = None

    service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    with pytest.raises(LookupError) as excinfo:
        service.resolve_run_name(project_id="proj-1", run_name=None)

    run_repo.get_latest_run_for_project.assert_called_once_with(project_id="proj-1")
    assert "proj-1" in str(excinfo.value)
