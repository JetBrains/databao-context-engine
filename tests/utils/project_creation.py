from datetime import datetime
from pathlib import Path
from typing import Any

from databao_context_engine.datasource_config.datasource_context import DatasourceContext
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.project.layout import create_datasource_config_file, get_output_dir, read_config_file
from databao_context_engine.serialisation.yaml import to_yaml_string
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.repositories.factories import create_run_repository


def with_config_file(project_dir: Path, full_type: str, datasource_name: str, config_content: dict[str, Any]) -> Path:
    return create_datasource_config_file(
        project_dir=project_dir,
        datasource_type=DatasourceType(full_type=full_type),
        datasource_name=datasource_name,
        config_content=to_yaml_string(config_content),
    )


def with_run_dir(
    db_path: Path, project_dir: Path, datasource_contexts: list[DatasourceContext], started_at: datetime | None = None
) -> Path:
    output_dir = get_output_dir(project_dir)
    output_dir.mkdir(exist_ok=True)

    project_id = str(read_config_file(project_dir).project_id)

    with open_duckdb_connection(db_path) as conn:
        run_repo = create_run_repository(conn)
        if started_at is None:
            started_at = datetime.now()

        run = run_repo.create(project_id=str(project_id), dce_version="1.0", started_at=started_at)

        run_dir = output_dir.joinpath(run.run_name)
        run_dir.mkdir()

        for context in datasource_contexts:
            _create_output_context(run_dir, context.datasource_id, context.context)

    return run_dir


def _create_output_context(run_dir: Path, datasource_id: DatasourceId, output: str):
    output_file = run_dir.joinpath(datasource_id.relative_path_to_context_file())
    output_file.parent.mkdir(exist_ok=True)
    output_file.write_text(output)
