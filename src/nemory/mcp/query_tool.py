import datetime
from pathlib import Path

from nemory.embeddings.providers.ollama.factory import create_ollama_provider
from nemory.project.layout import read_config_file
from nemory.services.factories import create_query_service
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path


def run_query_tool(project_dir: Path, *, run_name: str | None = None, text: str, limit: int) -> str:
    """
    Execute the query flow for MCP and return the matching display texts
    Adds the current date to the end
    """
    with open_duckdb_connection(get_db_path()) as conn:
        provider = create_ollama_provider()
        service = create_query_service(conn, provider=provider)

        query_results = service.query(
            project_id=str(read_config_file(project_dir).project_id),
            query_text=text,
            run_name=run_name,
            limit=limit,
        )

        query_results.append(f"\nToday's date is {datetime.date.today()}")

        return "\n".join(query_results)
