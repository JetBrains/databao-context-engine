import os
from pathlib import Path
from typing import Iterable

import click

from databao_context_engine import (
    CheckDatasourceConnectionResult,
    DatabaoContextDomainManager,
    DatabaoContextEngine,
    DatabaoContextPluginLoader,
    DatasourceConnectionStatus,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.cli.user_input_cb_impl import ClickUserInputCallback


def add_datasource_config_interactive_impl(project_dir: Path) -> None:
    plugin_loader = DatabaoContextPluginLoader()
    domain_manager = DatabaoContextDomainManager(domain_dir=project_dir, plugin_loader=plugin_loader)

    click.echo(f"We will guide you to add a new datasource at {project_dir.resolve()}")

    datasource_type = _ask_for_datasource_type(
        plugin_loader.get_all_supported_datasource_types(exclude_file_plugins=True)
    )

    datasource_name = click.prompt("Datasource name?", type=str)

    datasource_id = domain_manager.datasource_config_exists(datasource_name=datasource_name)
    if datasource_id is not None:
        click.confirm(
            f"A config file already exists for this datasource {datasource_id.relative_path_to_config_file()}. "
            f"Do you want to overwrite it?",
            abort=True,
            default=False,
        )
    created_datasource = domain_manager.create_datasource_config_interactively(
        datasource_type, datasource_name, ClickUserInputCallback(), overwrite_existing=True
    )

    datasource_id = created_datasource.datasource.id
    click.echo(
        f"{os.linesep}We've created a new config file for your datasource at: "
        f"{domain_manager.get_config_file_path_for_datasource(datasource_id)}"
    )
    if click.confirm("\nDo you want to check the connection to this new datasource?"):
        results = domain_manager.check_datasource_connection(datasource_ids=[datasource_id])
        _print_connection_check_results(results.values())


def _ask_for_datasource_type(supported_datasource_types: set[DatasourceType]) -> DatasourceType:
    all_datasource_types = sorted([ds_type.full_type for ds_type in supported_datasource_types])
    config_type = click.prompt(
        "What type of datasource do you want to add?",
        type=click.Choice(all_datasource_types),
        default=all_datasource_types[0] if len(all_datasource_types) == 1 else None,
    )
    click.echo(f"Selected type: {config_type}")

    return DatasourceType(full_type=config_type)


def check_datasource_connection_impl(project_dir: Path, *, datasource_ids: list[DatasourceId] | None) -> None:
    results = DatabaoContextDomainManager(domain_dir=project_dir).check_datasource_connection(
        datasource_ids=datasource_ids
    )

    _print_connection_check_results(results.values())


def _print_connection_check_results(results: Iterable[CheckDatasourceConnectionResult]) -> None:
    if len(list(results)) > 0:
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
    databao_engine = DatabaoContextEngine(domain_dir=project_dir)
    result = databao_engine.run_sql(datasource_id=datasource_id, sql=sql, params=None)

    # save somewhere or pretty print
    click.echo(f"Found {len(result.rows)} rows for query: {sql}")
    for row in result.rows:
        click.echo(row)

    click.echo(f"Columns are: {result.columns}")
