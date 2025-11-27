import logging
from importlib.metadata import version
from pathlib import Path

from nemory.build_sources.internal.build_runner import build
from nemory.embeddings.providers.ollama.factory import create_ollama_provider
from nemory.project.layout import read_config_file, ensure_project_dir
from nemory.services.factories import (
    create_build_service,
)
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path


logger = logging.getLogger(__name__)


def build_all_datasources(project_dir: str | Path):
    """
    Public build entrypoint
    - Instantiates the build service
    - Delegates the actual build logic to the build runner
    """
    project_dir = Path(project_dir)
    ensure_project_dir(str(project_dir))

    logger.debug(f"Starting to build datasources in project {project_dir.resolve()}")

    with open_duckdb_connection(get_db_path()) as conn:
        provider = create_ollama_provider(host="127.0.0.1", port=11434, model_id="nomic-embed-text:latest", dim=768)
        build_service = create_build_service(conn, provider=provider)
        nemory_config = read_config_file(project_dir)
        build(
            project_dir=project_dir,
            build_service=build_service,
            project_id=str(nemory_config.project_id),
            nemory_version=version("nemory"),
        )
