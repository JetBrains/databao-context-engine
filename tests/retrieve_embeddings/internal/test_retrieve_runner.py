from pathlib import Path
from unittest.mock import Mock, patch

from nemory.retrieve_embeddings.internal.retrieve_runner import retrieve


def test_retrieve_streamed_prints_and_does_not_export(capsys):
    service = Mock()
    service.retrieve.return_value = ["a", "b", "c"]
    service.resolve_run_name.return_value = "run-1"

    project_dir = Path("/project")
    run_name = "run-1"

    with patch("nemory.retrieve_embeddings.internal.retrieve_runner.export_retrieve_results") as mock_export:
        retrieve(
            project_dir=project_dir,
            retrieve_service=service,
            project_id="proj-123",
            text="hello",
            run_name=run_name,
            limit=5,
            output_format="streamed",
        )

    service.retrieve.assert_called_once_with(project_id="proj-123", text="hello", run_name=run_name, limit=5)

    mock_export.assert_not_called()

    captured = capsys.readouterr()
    assert captured.out == "a\nb\nc\n"


def test_retrieve_file_output_calls_export_with_run_dir(tmp_path, capsys):
    service = Mock()
    service.retrieve.return_value = ["x", "y"]
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
            output_format="file",
        )

    mock_get_run_dir.assert_called_once_with(project_dir=project_dir, run_name=run_name)

    mock_export.assert_called_once_with(export_dir, ["x", "y"])

    captured = capsys.readouterr()
    assert captured.out == ""
