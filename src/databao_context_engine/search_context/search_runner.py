from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.datasources.datasource_context import (
    get_all_datasource_context_hashes,
    get_datasource_context_hashes,
)
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.search_context.search_service import RAG_MODE, ContextSearchMode, SearchContextService


def run_context_search(
    *,
    project_layout: ProjectLayout,
    search_context_service: SearchContextService,
    build_service: BuildService,
    search_text: str,
    limit: int | None,
    datasource_ids: list[DatasourceId] | None,
    rag_mode: RAG_MODE,
    context_search_mode: ContextSearchMode,
):
    context_hashes = (
        get_datasource_context_hashes(project_layout, datasource_ids)
        if datasource_ids
        else get_all_datasource_context_hashes(project_layout)
    )

    build_service.index_context_if_necessary(datasource_context_hashes=context_hashes)

    return search_context_service.search(
        search_text=search_text,
        limit=limit,
        datasource_context_hashes=context_hashes,
        rag_mode=rag_mode,
        context_search_mode=context_search_mode,
    )
