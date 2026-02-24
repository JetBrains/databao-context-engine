from pathlib import Path

import click

from databao_context_engine import (
    DatabaoContextEngine,
    DatasourceId,
)


def run_sql_query_cli(project_dir: Path, *, datasource_id: DatasourceId, sql: str) -> None:
    databao_engine = DatabaoContextEngine(project_dir=project_dir)
    result = databao_engine.run_sql(datasource_id=datasource_id, sql=sql, params=None)

    # save somewhere or pretty print
    click.echo(f"Found {len(result.rows)} rows for query: {sql}")
    for row in result.rows:
        click.echo(row)

    click.echo(f"Columns are: {result.columns}")
