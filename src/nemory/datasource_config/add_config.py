import os
from typing import Any, cast

import click
import yaml

from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.introspection.property_extract import get_property_list_from_type
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin
from nemory.pluginlib.config_properties import ConfigPropertyDefinition, CustomiseConfigProperties
from nemory.project.layout import (
    create_datasource_config_file,
    ensure_datasource_config_file_doesnt_exist,
    ensure_project_dir,
)


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
    ensure_datasource_config_file_doesnt_exist(project_path, config_folder, datasource_name)
    basic_config = {"type": config_type, "name": datasource_name}

    config_for_plugin = _create_config_for_plugin(cast(BuildDatasourcePlugin, all_datasource_plugins[config_full_type]))

    config_content = config_content_to_yaml_string(basic_config | config_for_plugin)
    config_file_path = create_datasource_config_file(project_path, config_folder, datasource_name, config_content)

    click.echo(f"{os.linesep}We've created a new config file for your datasource at: {config_file_path}")


def config_content_to_yaml_string(config_content: dict[str, Any]) -> str:
    return yaml.safe_dump(config_content, sort_keys=False, default_flow_style=False)


def _create_config_for_plugin(plugin: BuildDatasourcePlugin) -> dict[str, Any]:
    if isinstance(plugin, CustomiseConfigProperties):
        config_file_structure = plugin.get_config_file_properties()
    else:
        config_file_structure = get_property_list_from_type(plugin.config_file_type)

    # Adds a new line before asking for the config values specific to the plugin
    click.echo("")

    if len(config_file_structure) == 0:
        click.echo(
            "The plugin for this datasource doesn't declare its configuration. Please check the documentation and fill the config file manually."
        )

    return _create_config_for_properties(config_file_structure, properties_prefix="")


def _create_config_for_properties(properties: list[ConfigPropertyDefinition], properties_prefix: str) -> dict[str, Any]:
    config_content: dict[str, Any] = {}
    for config_file_property in properties:
        if config_file_property.property_key in ["type", "name"] and len(properties_prefix) == 0:
            # We ignore type and name properties as they've already been filled
            continue

        if config_file_property.nested_properties is not None and len(config_file_property.nested_properties) > 0:
            nested_content = _create_config_for_properties(
                config_file_property.nested_properties,
                properties_prefix=f"{properties_prefix}.{config_file_property.property_key}."
                if properties_prefix
                else f"{config_file_property.property_key}.",
            )
            if len(nested_content.keys()) > 0:
                config_content[config_file_property.property_key] = nested_content
        else:
            default_value: str | None
            if config_file_property.default_value:
                default_value = config_file_property.default_value
            else:
                # We need to add an empty string default value for non-required fields
                default_value = None if config_file_property.required else ""

            property_value = click.prompt(
                f"{properties_prefix}{config_file_property.property_key}? {'(Optional)' if not config_file_property.required else ''}",
                type=str,
                default=default_value,
                show_default=default_value is not None and default_value != "",
            )

            if property_value.strip():
                config_content[config_file_property.property_key] = property_value

    return config_content
