import os
from pathlib import Path

import click

from nemory.datasource_config.add_config import add_datasource_config
from nemory.datasource_config.validate_config import (
    ValidationStatus,
    validate_datasource_config,
    ValidationResult,
)


def add_datasource_config_cli(project_dir: Path) -> None:
    datasource_config_file = add_datasource_config(project_dir)

    if click.confirm("\nDo you want to check the connection to this new datasource?"):
        validate_datasource_config_cli(project_dir, datasource_config_files=[datasource_config_file])


def validate_datasource_config_cli(project_dir: Path, *, datasource_config_files: list[str] | None) -> None:
    results = validate_datasource_config(project_dir, datasource_config_files=datasource_config_files)

    _print_datasource_validation_results(results)


def _print_datasource_validation_results(results: dict[str, ValidationResult]) -> None:
    if len(results) > 0:
        valid_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.VALID
        }
        invalid_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.INVALID
        }
        unknown_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.UNKNOWN
        }

        # Print all errors
        for datasource_path, validation_result in invalid_datasources.items():
            click.echo(
                f"Error for datasource {datasource_path}:{os.linesep}{validation_result.full_message}{os.linesep}"
            )

        results_summary = (
            os.linesep.join(
                [
                    f"{datasource_path}: {validation_result.format(show_summary_only=True)}"
                    for datasource_path, validation_result in results.items()
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
