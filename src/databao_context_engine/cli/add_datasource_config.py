import os
from pathlib import Path
from typing import Any, Iterable

import click

from databao_context_engine import (
    ConfigPropertyDefinition,
    DatabaoContextPluginLoader,
    DatabaoContextProjectManager,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.datasources.config_wizard import UserInputCallback, build_config_content_interactively


class ClickUserInputCallback(UserInputCallback):
    def prompt(
        self,
        text: str,
        type: Iterable[str] | Any | None = None,
        default: Any | None = None,
        hide_input: bool = False,
        show_default: bool = True,
    ) -> str:
        final_type = click.Choice(type) if isinstance(type, Iterable) else type
        return click.prompt(
            text=text, default=default, hide_input=hide_input, type=final_type, show_default=show_default
        )

    def confirm(self, text: str) -> bool:
        return click.confirm(text=text)


def add_datasource_config_interactive(
    project_dir: Path, plugin_loader: DatabaoContextPluginLoader | None = None
) -> DatasourceId:
    plugin_loader = plugin_loader if plugin_loader else DatabaoContextPluginLoader()
    project_manager = DatabaoContextProjectManager(project_dir=project_dir, plugin_loader=plugin_loader)

    click.echo(
        f"We will guide you to add a new datasource in your Databao Context Engine project, at {project_dir.resolve()}"
    )

    datasource_type = _ask_for_datasource_type(
        plugin_loader.get_all_supported_datasource_types(exclude_file_plugins=True)
    )
    datasource_name = click.prompt("Datasource name?", type=str)

    datasource_id = project_manager.datasource_config_exists(datasource_name=datasource_name)
    if datasource_id is not None:
        click.confirm(
            f"A config file already exists for this datasource {datasource_id.relative_path_to_config_file()}. Do you want to overwrite it?",
            abort=True,
            default=False,
        )

    created_datasource = project_manager.create_datasource_config_interactively(
        datasource_type,
        datasource_name,
        ClickUserInputCallback(),
        overwrite_existing=True,
        validate_config_content=False,
    )

    click.echo(
        f"{os.linesep}We've created a new config file for your datasource at: {project_manager.get_config_file_path_for_datasource(created_datasource.datasource.id)}"
    )

    return created_datasource.datasource.id


def _ask_for_datasource_type(supported_datasource_types: set[DatasourceType]) -> DatasourceType:
    all_datasource_types = sorted([ds_type.full_type for ds_type in supported_datasource_types])
    config_type = click.prompt(
        "What type of datasource do you want to add?",
        type=click.Choice(all_datasource_types),
        default=all_datasource_types[0] if len(all_datasource_types) == 1 else None,
    )
    click.echo(f"Selected type: {config_type}")

    return DatasourceType(full_type=config_type)


def _ask_for_config_details(config_file_structure: list[ConfigPropertyDefinition]) -> dict[str, Any]:
    # Adds a new line before asking for the config values specific to the plugin
    click.echo("")

    if len(config_file_structure) == 0:
        click.echo(
            "The plugin for this datasource doesn't declare its configuration. Please check the documentation and fill the config file manually."
        )
        return {}

    return build_config_content_interactively(config_file_structure, ClickUserInputCallback())
