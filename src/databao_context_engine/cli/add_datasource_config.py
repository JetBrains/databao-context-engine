import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import click

from databao_context_engine import (
    ConfigPropertyDefinition,
    DatabaoContextPluginLoader,
    DatabaoContextProjectManager,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.pluginlib.config import ConfigUnionPropertyDefinition


def add_datasource_config_interactive(project_dir: Path) -> DatasourceId:
    project_manager = DatabaoContextProjectManager(project_dir=project_dir)
    plugin_loader = DatabaoContextPluginLoader()

    click.echo(
        f"We will guide you to add a new datasource in your Databao Context Engine project, at {project_dir.resolve()}"
    )

    datasource_type = _ask_for_datasource_type(
        plugin_loader.get_all_supported_datasource_types(exclude_file_plugins=True)
    )
    datasource_name = click.prompt("Datasource name?", type=str)

    is_datasource_existing = project_manager.datasource_config_exists(
        datasource_type=datasource_type, datasource_name=datasource_name
    )
    if is_datasource_existing:
        click.confirm(
            f"A config file already exists for this datasource ({datasource_type.config_folder}/{datasource_name}). Do you want to overwrite it?",
            abort=True,
            default=False,
        )

    config_content = _ask_for_config_details(
        plugin_loader.get_config_file_structure_for_datasource_type(datasource_type)
    )

    config_file = project_manager.create_datasource_config(
        datasource_type, datasource_name, config_content, overwrite_existing=True
    )

    click.echo(f"{os.linesep}We've created a new config file for your datasource at: {config_file.config_file_path}")

    return config_file.datasource_id


def _ask_for_datasource_type(supported_datasource_types: set[DatasourceType]) -> DatasourceType:
    supported_types_by_folder = _group_supported_types_by_folder(supported_datasource_types)

    all_config_folders = sorted(supported_types_by_folder.keys())
    config_folder = click.prompt(
        "What type of datasource do you want to add?",
        type=click.Choice(all_config_folders),
        default=all_config_folders[0] if len(all_config_folders) == 1 else None,
    )
    click.echo(f"Selected type: {config_folder}")

    all_subtypes_for_folder = sorted(supported_types_by_folder[config_folder])
    config_type = click.prompt(
        "What is the subtype of this datasource?",
        type=click.Choice(all_subtypes_for_folder),
        default=all_subtypes_for_folder[0] if len(all_subtypes_for_folder) == 1 else None,
    )
    click.echo(f"Selected subtype: {config_type}")

    return DatasourceType.from_main_and_subtypes(config_folder, config_type)


def _ask_for_config_details(config_file_structure: list[ConfigPropertyDefinition]) -> dict[str, Any]:
    # Adds a new line before asking for the config values specific to the plugin
    click.echo("")

    if len(config_file_structure) == 0:
        click.echo(
            "The plugin for this datasource doesn't declare its configuration. Please check the documentation and fill the config file manually."
        )
        return {}

    return _build_config_content_from_properties(config_file_structure, properties_prefix="")


def _build_config_content_from_properties(
    properties: list[ConfigPropertyDefinition], properties_prefix: str, in_union: bool = False
) -> dict[str, Any]:
    config_content: dict[str, Any] = {}
    for config_file_property in properties:
        if config_file_property.property_key in ["type", "name"] and len(properties_prefix) == 0:
            # We ignore type and name properties as they've already been filled
            continue
        if in_union and config_file_property.property_key == "type":
            continue

        if isinstance(config_file_property, ConfigUnionPropertyDefinition):
            choices = {t.__name__: t for t in config_file_property.types}

            chosen = click.prompt(
                f"{properties_prefix}{config_file_property.property_key}.type?",
                type=click.Choice(sorted(choices.keys())),
            )

            chosen_type = choices[chosen]

            nested_props = config_file_property.type_properties[chosen_type]
            nested_content = _build_config_content_from_properties(
                nested_props, f"{properties_prefix}{config_file_property.property_key}.", in_union=True
            )

            config_content[config_file_property.property_key] = {
                **nested_content,
            }
            continue

        if config_file_property.nested_properties is not None and len(config_file_property.nested_properties) > 0:
            nested_content = _build_config_content_from_properties(
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
                hide_input=config_file_property.secret,
            )

            if property_value.strip():
                config_content[config_file_property.property_key] = property_value

    return config_content


def _group_supported_types_by_folder(all_plugin_types: set[DatasourceType]) -> dict[str, list[str]]:
    main_to_subtypes = defaultdict(list)
    for datasource_type in all_plugin_types:
        main_to_subtypes[datasource_type.main_type].append(datasource_type.subtype)

    return main_to_subtypes
