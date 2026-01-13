from pathlib import Path

from nemory.llm.factory import create_ollama_embedding_provider, create_ollama_service
from nemory.project.layout import ensure_project_dir, read_config_file
from nemory.retrieve_embeddings.internal.retrieve_runner import retrieve
from nemory.services.factories import create_retrieve_service
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path


def retrieve_embeddings(
    project_dir: Path,
    retrieve_text: str,
    run_name: str | None,
    limit: int | None,
    export_to_file: bool,
) -> list[str]:
    ensure_project_dir(project_dir)

    with open_duckdb_connection(get_db_path()) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(ollama_service)
        retrieve_service = create_retrieve_service(conn, embedding_provider=embedding_provider)
        return retrieve(
            project_dir=project_dir,
            retrieve_service=retrieve_service,
            project_id=str(read_config_file(project_dir).project_id),
            text=retrieve_text,
            run_name=run_name,
            limit=limit,
            export_to_file=export_to_file,
        )
