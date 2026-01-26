from pathlib import Path
from unittest.mock import Mock, patch

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.retrieve_embeddings.internal.retrieve_runner import retrieve
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult


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

    project_dir = Path("/project")

    with patch(
        "databao_context_engine.retrieve_embeddings.internal.retrieve_runner.export_retrieve_results"
    ) as mock_export:
        result = retrieve(
            project_dir=project_dir,
            retrieve_service=service,
            project_id="proj-123",
            text="hello",
            limit=5,
            export_to_file=False,
        )

    service.retrieve.assert_called_once_with(project_id="proj-123", text="hello", limit=5)

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

    project_dir = tmp_path

    with (
        patch(
            "databao_context_engine.retrieve_embeddings.internal.retrieve_runner.get_output_dir"
        ) as mock_get_output_dir,
        patch(
            "databao_context_engine.retrieve_embeddings.internal.retrieve_runner.export_retrieve_results"
        ) as mock_export,
    ):
        export_dir = tmp_path / "out"
        mock_get_output_dir.return_value = export_dir

        retrieve(
            project_dir=project_dir,
            retrieve_service=service,
            project_id="proj-123",
            text="hello",
            limit=10,
            export_to_file=True,
        )

    mock_export.assert_called_once_with(export_dir, ["x", "y"])
