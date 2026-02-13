import os
from pathlib import Path

import click

from databao_context_engine import (
    CheckDatasourceConnectionResult,
    DatabaoContextEngine,
    DatabaoContextProjectManager,
    DatasourceConnectionStatus,
    DatasourceId,
)
from databao_context_engine.cli.add_datasource_config import add_datasource_config_interactive


def add_datasource_config_cli(project_dir: Path) -> None:
    datasource_id = add_datasource_config_interactive(project_dir)

    if click.confirm("\nDo you want to check the connection to this new datasource?"):
        check_datasource_connection_cli(project_dir, datasource_ids=[datasource_id])


def check_datasource_connection_cli(project_dir: Path, *, datasource_ids: list[DatasourceId] | None) -> None:
    results = DatabaoContextProjectManager(project_dir=project_dir).check_datasource_connection(
        datasource_ids=datasource_ids
    )

    _print_check_datasource_connection_results(results)


def _print_check_datasource_connection_results(results: list[CheckDatasourceConnectionResult]) -> None:
    if len(results) > 0:
        valid_datasources = [
            result for result in results if result.connection_status == DatasourceConnectionStatus.VALID
        ]
        invalid_datasources = [
            result for result in results if result.connection_status == DatasourceConnectionStatus.INVALID
        ]
        unknown_datasources = [
            result for result in results if result.connection_status == DatasourceConnectionStatus.UNKNOWN
        ]

        # Print all errors
        for check_result in invalid_datasources:
            click.echo(
                f"Error for datasource {str(check_result.datasource_id)}:{os.linesep}{check_result.full_message}{os.linesep}"
            )

        results_summary = (
            os.linesep.join(
                [
                    f"{str(check_result.datasource_id)}: {check_result.format(show_summary_only=True)}"
                    for check_result in results
                ]
            )
            if results
            else "No datasource found"
        )

        click.echo(
            f"Validation completed with {len(valid_datasources)} valid datasource(s) and {len(invalid_datasources) + len(unknown_datasources)} invalid (or unknown status) datasource(s)"
            f"{os.linesep}{results_summary}"
        )
    else:
        click.echo("No datasource found")


def run_sql_query_cli(project_dir: Path, *, datasource_id: DatasourceId, sql: str) -> None:
    databao_engine = DatabaoContextEngine(project_dir=project_dir)
    result = databao_engine.run_sql(datasource_id=datasource_id, sql=sql, params=None)

    # save somewhere or pretty print
    click.echo(f"Found {len(result.rows)} rows for query: {sql}")
    for row in result.rows:
        click.echo(row)

    click.echo(f"Columns are: {result.columns}")
