from pathlib import Path

from nemory.project.layout import get_run_dir
from nemory.retrieve_embeddings.internal.export_results import export_retrieve_results
from nemory.retrieve_embeddings.internal.retrieve_service import RetrieveService


def retrieve(
    project_dir: Path,
    *,
    retrieve_service: RetrieveService,
    project_id: str,
    text: str,
    run_name: str | None,
    limit: int | None,
    output_format: str,
):
    resolved_run_name = retrieve_service.resolve_run_name(project_id=project_id, run_name=run_name)
    display_texts = retrieve_service.retrieve(project_id=project_id, text=text, run_name=resolved_run_name, limit=limit)

    if output_format == "streamed":
        print("\n".join(display_texts))
    else:
        export_directory = get_run_dir(project_dir=project_dir, run_name=resolved_run_name)
        export_retrieve_results(export_directory, display_texts)
