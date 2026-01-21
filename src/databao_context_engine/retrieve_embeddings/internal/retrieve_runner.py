import logging
from pathlib import Path

from databao_context_engine.project.layout import get_output_dir
from databao_context_engine.retrieve_embeddings.internal.export_results import export_retrieve_results
from databao_context_engine.retrieve_embeddings.internal.retrieve_service import RetrieveService
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult

logger = logging.getLogger(__name__)


def retrieve(
    project_dir: Path,
    *,
    retrieve_service: RetrieveService,
    project_id: str,
    text: str,
    limit: int | None,
    export_to_file: bool,
) -> list[VectorSearchResult]:
    retrieve_results = retrieve_service.retrieve(project_id=project_id, text=text, limit=limit)

    if export_to_file:
        export_directory = get_output_dir(project_dir)

        display_texts = [result.display_text for result in retrieve_results]
        export_file = export_retrieve_results(export_directory, display_texts)
        logger.info(f"Exported results to {export_file}")

    return retrieve_results
