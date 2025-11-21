from pathlib import Path

from nemory.embeddings.providers.ollama.factory import create_ollama_provider
from nemory.project.layout import ensure_project_dir, read_config_file
from nemory.query_embeddings.internal.query_runner import query
from nemory.services.factories import create_query_service
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path


def query_embeddings(project_dir: str | Path, query_text: str, run_name: str | None, limit: int):
    project_dir = Path(project_dir)
    ensure_project_dir(str(project_dir))

    with open_duckdb_connection(get_db_path()) as conn:
        provider = create_ollama_provider()
        query_service = create_query_service(conn, provider=provider)
        query(
            project_dir=project_dir,
            query_service=query_service,
            project_id=str(read_config_file(project_dir).project_id),
            query_text=query_text,
            run_name=run_name,
            limit=limit,
        )
