import datetime
from pathlib import Path

from nemory.llm.factory import create_ollama_embedding_provider, create_ollama_service
from nemory.project.layout import read_config_file
from nemory.services.factories import create_retrieve_service
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path


def run_retrieve_tool(project_dir: Path, *, run_name: str | None = None, text: str, limit: int | None = None) -> str:
    """
    Execute the retrieve flow for MCP and return the matching display texts
    Adds the current date to the end
    """
    with open_duckdb_connection(get_db_path()) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(ollama_service)
        service = create_retrieve_service(conn, embedding_provider=embedding_provider)

        run_name = service.resolve_run_name(project_id=str(read_config_file(project_dir).project_id), run_name=run_name)

        retrieve_results = service.retrieve(
            project_id=str(read_config_file(project_dir).project_id),
            text=text,
            run_name=run_name,
            limit=limit,
        )

        retrieve_results.append(f"\nToday's date is {datetime.date.today()}")

        return "\n".join(retrieve_results)
