import os
from typing import Any, cast

import click
import yaml

from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin
from nemory.project.layout import create_datasource_config_file, ensure_project_dir


def add_datasource_config(project_dir: str) -> None:
    project_path = ensure_project_dir(project_dir)

    click.echo(f"We will guide you to add a new datasource in your Nemory project, at {project_path.resolve()}")

    all_datasource_plugins = load_plugins(exclude_file_plugins=True)

    config_full_type = click.prompt(
        "What type of datasource do you want to add?", type=click.Choice(all_datasource_plugins.keys())
    )
    click.echo(f"Selected type: {config_full_type}")

    datasource_name = click.prompt("Datasource name?", type=str)

    config_folder, config_type = config_full_type.split("/")
    basic_config = {"type": config_type, "name": datasource_name}

    config_for_plugin = _create_config_for_plugin(cast(BuildDatasourcePlugin, all_datasource_plugins[config_full_type]))

    config_content = config_content_to_yaml_string(basic_config | config_for_plugin)
    config_file_path = create_datasource_config_file(project_path, config_folder, datasource_name, config_content)

    click.echo(f"{os.linesep}We've created a new config file for your datasource at: {config_file_path}")


def config_content_to_yaml_string(config_content: dict[str, Any]) -> str:
    return yaml.safe_dump(config_content, sort_keys=False, default_flow_style=False)


def _create_config_for_plugin(plugin: BuildDatasourcePlugin) -> dict[str, Any]:
    config_file_structure = plugin.get_mandatory_config_file_structure()

    # Adds a new line before asking for the config values specific to the plugin
    click.echo("")

    if len(config_file_structure) == 0:
        click.echo(
            "The plugin for this datasource doesn't declare its configuration. Please check the documentation and fill the config file manually."
        )

    config_content: dict[str, Any] = {}
    for config_file_property in config_file_structure:
        default_value: str | None
        if config_file_property.default_value:
            default_value = config_file_property.default_value
        else:
            # We need to add an empty string default value for non-required fields
            default_value = None if config_file_property.required else ""

        property_value = click.prompt(
            f"{config_file_property.property_key}? {'(Optional)' if not config_file_property.required else ''}",
            type=str,
            default=default_value,
            show_default=default_value is not None and default_value != "",
        )
        if property_value.strip():
            if config_file_property.nested_in:
                config_content.setdefault(config_file_property.nested_in, dict())[config_file_property.property_key] = (
                    property_value
                )
            else:
                config_content[config_file_property.property_key] = property_value

    return config_content
