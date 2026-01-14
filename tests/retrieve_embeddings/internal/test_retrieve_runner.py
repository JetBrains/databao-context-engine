from pathlib import Path
from unittest.mock import Mock, patch

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.retrieve_embeddings.internal.retrieve_runner import retrieve
from nemory.storage.repositories.vector_search_repository import VectorSearchResult


def test_retrieve_without_export(capsys):
    service = Mock()
    expected = [
        VectorSearchResult(
            display_text="a",
            embeddable_text="a",
            cosine_distance=0.0,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/a",
        ),
        VectorSearchResult(
            display_text="b",
            embeddable_text="b",
            cosine_distance=0.1,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/b",
        ),
        VectorSearchResult(
            display_text="c",
            embeddable_text="c",
            cosine_distance=0.2,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/c",
        ),
    ]
    service.retrieve.return_value = expected
    service.resolve_run_name.return_value = "run-1"

    project_dir = Path("/project")
    run_name = "run-1"

    with patch("nemory.retrieve_embeddings.internal.retrieve_runner.export_retrieve_results") as mock_export:
        result = retrieve(
            project_dir=project_dir,
            retrieve_service=service,
            project_id="proj-123",
            text="hello",
            run_name=run_name,
            limit=5,
            export_to_file=False,
        )

    service.retrieve.assert_called_once_with(project_id="proj-123", text="hello", run_name=run_name, limit=5)

    mock_export.assert_not_called()

    assert result == expected


def test_retrieve_file_with_export(tmp_path, capsys):
    service = Mock()
    service.retrieve.return_value = [
        VectorSearchResult(
            display_text="x",
            embeddable_text="x",
            cosine_distance=0.01,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/x",
        ),
        VectorSearchResult(
            display_text="y",
            embeddable_text="y",
            cosine_distance=0.02,
            datasource_type=DatasourceType(full_type="full/type"),
            datasource_id="full/y",
        ),
    ]
    service.resolve_run_name.return_value = "run-123"

    project_dir = tmp_path
    run_name = "run-123"

    with (
        patch("nemory.retrieve_embeddings.internal.retrieve_runner.get_run_dir") as mock_get_run_dir,
        patch("nemory.retrieve_embeddings.internal.retrieve_runner.export_retrieve_results") as mock_export,
    ):
        export_dir = tmp_path / "out"
        mock_get_run_dir.return_value = export_dir

        retrieve(
            project_dir=project_dir,
            retrieve_service=service,
            project_id="proj-123",
            text="hello",
            run_name=run_name,
            limit=10,
            export_to_file=True,
        )

    mock_get_run_dir.assert_called_once_with(project_dir=project_dir, run_name=run_name)

    mock_export.assert_called_once_with(export_dir, ["x", "y"])
