from pathlib import Path

from nemory.query_embeddings.internal.export_results import export_query_results
from nemory.query_embeddings.internal.query_service import QueryService


def query(
    project_dir: Path,
    *,
    query_service: QueryService,
    project_id: str,
    query_text: str,
    run_name: str | None,
    limit: int,
):
    display_texts = query_service.query(project_id=project_id, query_text=query_text, run_name=run_name, limit=limit)

    # TODO: this is wrong
    # TODO: need the link from run to the folder to properly do this - Julien is working on it
    export_query_results(project_dir, display_texts)
