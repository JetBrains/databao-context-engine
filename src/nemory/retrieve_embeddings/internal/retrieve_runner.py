import logging
from pathlib import Path

from nemory.project.runs import get_run_dir
from nemory.retrieve_embeddings.internal.export_results import export_retrieve_results
from nemory.retrieve_embeddings.internal.retrieve_service import RetrieveService
from nemory.storage.repositories.vector_search_repository import VectorSearchResult

logger = logging.getLogger(__name__)


def retrieve(
    project_dir: Path,
    *,
    retrieve_service: RetrieveService,
    project_id: str,
    text: str,
    run_name: str | None,
    limit: int | None,
    export_to_file: bool,
) -> list[VectorSearchResult]:
    resolved_run_name = retrieve_service.resolve_run_name(project_id=project_id, run_name=run_name)
    retrieve_results = retrieve_service.retrieve(
        project_id=project_id, text=text, run_name=resolved_run_name, limit=limit
    )

    if export_to_file:
        export_directory = get_run_dir(project_dir=project_dir, run_name=resolved_run_name)

        display_texts = [result.display_text for result in retrieve_results]
        export_file = export_retrieve_results(export_directory, display_texts)
        logger.info(f"Exported results to {export_file}")

    return retrieve_results
