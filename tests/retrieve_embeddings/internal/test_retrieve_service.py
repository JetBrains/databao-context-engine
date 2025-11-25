from unittest.mock import Mock

import pytest

from nemory.retrieve_embeddings.internal.retrieve_service import RetrieveService


def test_retrieve_uses_latest_run_and_returns_display_texts():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 42
    run_repo.get_latest_run_for_project.return_value = run

    shard_resolver.resolve.return_value = ("emb_tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.1, 0.2]

    vector_search_repo.get_display_texts_by_similarity.return_value = ["a", "b"]

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="hello world")

    run_repo.get_latest_run_for_project.assert_called_once_with("proj-1")
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
        limit=50,
    )

    assert result == ["a", "b"]


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

    vector_search_repo.get_display_texts_by_similarity.return_value = ["a", "b"]

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    retrieve_service.retrieve(project_id="proj-1", text="hello world", run_name="run-123")

    run_repo.get_by_run_name.assert_called_once_with(project_id="proj-1", run_name="run-123")


def test_retrieve_raises_lookup_error_if_run_name_does_not_exist():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run_repo.get_by_run_name.return_value = None

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    with pytest.raises(LookupError):
        retrieve_service.retrieve(project_id="proj-1", text="hello world", run_name="run-123")


def test_retrieve_raises_when_no_run_exists():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run_repo.get_latest_run_for_project.return_value = None

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    with pytest.raises(LookupError) as exc:
        retrieve_service.retrieve(project_id="proj-1", text="hello")

    assert "No runs found for project 'proj-1'" in str(exc.value)
    vector_search_repo.get_display_texts_by_similarity.assert_not_called()
    provider.embed.assert_not_called()


def test_retrieve_honors_limit():
    run_repo = Mock()
    vector_search_repo = Mock()
    shard_resolver = Mock()
    provider = Mock()

    run = Mock()
    run.run_id = 7
    run_repo.get_latest_for_project.return_value = run

    shard_resolver.resolve.return_value = ("tbl", 768)
    provider.embedder = "ollama"
    provider.model_id = "nomic-embed-text"
    provider.embed.return_value = [0.5] * 768

    vector_search_repo.get_display_texts_by_similarity.return_value = ["x"]

    retrieve_service = RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=provider,
    )

    result = retrieve_service.retrieve(project_id="proj-1", text="q", limit=3)

    vector_search_repo.get_display_texts_by_similarity.assert_called_once()
    _, kwargs = vector_search_repo.get_display_texts_by_similarity.call_args
    assert kwargs["limit"] == 3
    assert result == ["x"]
