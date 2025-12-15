from pathlib import Path

import click

from nemory.datasource_config.add_config import add_datasource_config as add_datasource_config_internal
from nemory.datasource_config.validate_config import validate_datasource_config as validate_datasource_config_internal


def add_datasource_config_cli(project_dir: Path) -> None:
    datasource_config_file = add_datasource_config_internal(project_dir)

    if click.confirm("\nDo you want to check the connection to this new datasource?"):
        validate_datasource_config_cli(project_dir, datasource_config_files=[datasource_config_file])


def validate_datasource_config_cli(project_dir: Path, *, datasource_config_files: list[str] | None) -> None:
    validate_datasource_config_internal(project_dir, datasource_config_files=datasource_config_files)
