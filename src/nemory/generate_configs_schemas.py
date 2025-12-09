import logging
import os
from typing import cast

import click

from nemory.plugins.plugin_loader import load_plugins
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin
from nemory.pluginlib.plugin_utils import format_json_schema_for_output, generate_json_schema

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-i",
    "--include-plugins",
    multiple=True,
    type=str,
    help="""
    Plugin ID for the plugins to include in the json schema generation (-i can be specified multiple times to include multiple plugins). 
    When set, only the plugins in this list are included in the json schema generation.
    This can't be used in conjunction with --exclude-plugins  
    """,
)
@click.option(
    "-e",
    "--exclude-plugins",
    multiple=True,
    help="""
    Plugin ID for the plugins to exclude from the json schema generation (-e can be specified multiple times to exclude multiple plugins). 
    This can't be used in conjunction with --include-plugins  
    """,
)
def generate_configs_schemas(
    include_plugins: tuple[str, ...] | None = None, exclude_plugins: tuple[str, ...] | None = None
):
    try:
        schema_list = _generate_json_schema_output_for_plugins(include_plugins, exclude_plugins)

        print(f"{os.linesep}{os.linesep}".join(schema_list))
    except Exception as e:
        print(str(e))


def _generate_json_schema_output_for_plugins(
    include_plugins: tuple[str, ...] | None = None, exclude_plugins: tuple[str, ...] | None = None
) -> list[str]:
    if include_plugins and exclude_plugins:
        raise ValueError("Can't use --include-plugins and --exclude-plugins together")

    filtered_plugins = _get_plugins_for_schema_generation(include_plugins, exclude_plugins)

    if len(filtered_plugins) == 0:
        if include_plugins:
            raise ValueError(f"No plugin found with id in {include_plugins}")
        elif exclude_plugins:
            raise ValueError(f"No plugin found when excluding {exclude_plugins}")
        else:
            raise ValueError("No plugin found")

    results = []
    for plugin in filtered_plugins:
        if not isinstance(plugin, BuildDatasourcePlugin):
            continue

        json_schema = generate_json_schema(plugin)
        if json_schema is not None:
            results.append(format_json_schema_for_output(plugin, json_schema))

    return results


def _get_plugins_for_schema_generation(
    include_plugins: tuple[str, ...] | None = None, exclude_plugins: tuple[str, ...] | None = None
) -> list[BuildDatasourcePlugin]:
    all_plugins = load_plugins(exclude_file_plugins=True).values()

    return [
        cast(BuildDatasourcePlugin, plugin)
        for plugin in all_plugins
        if (plugin.id in include_plugins if include_plugins else True)
        and (plugin.id not in exclude_plugins if exclude_plugins else True)
    ]


def main():
    generate_configs_schemas()


if __name__ == "__main__":
    main()
