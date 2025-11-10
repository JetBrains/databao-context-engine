import logging
from pathlib import Path

import yaml

from nemory.features.build_sources.internal.types import BuildPlugin, PluginList
from nemory.features.build_sources.plugin_lib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
)

logger = logging.getLogger(__name__)


def _get_plugin_to_execute(plugins_per_type: PluginList, full_type: str) -> BuildPlugin | None:
    return plugins_per_type[full_type]


def _execute_plugin_for_datasource(
    datasource_file: Path, main_type: str, plugins_per_type: PluginList
) -> tuple[BuildExecutionResult, BuildPlugin] | None:
    if datasource_file.suffix in {".yaml", ".yml"}:
        return _execute_plugin_for_config_file(
            config_file=datasource_file, main_type=main_type, plugins_per_type=plugins_per_type
        )
    else:
        return _execute_plugin_for_file(file=datasource_file, main_type=main_type, plugins_per_type=plugins_per_type)


def _execute_plugin_for_config_file(
    config_file: Path, main_type: str, plugins_per_type: PluginList
) -> tuple[BuildExecutionResult, BuildPlugin] | None:
    if config_file.suffix not in {".yaml", ".yml"}:
        return None

    with config_file.open("r") as yaml_stream:
        file_config = yaml.safe_load(yaml_stream)
        file_subtype = file_config["type"]
        if not file_subtype:
            logger.warning(f"Found a data source with no type at: {str(config_file.resolve())}")
            return None

        full_type = f"{main_type}/{file_subtype}"

        plugin = _get_plugin_to_execute(plugins_per_type, full_type)

        if plugin is None or not isinstance(plugin, BuildDatasourcePlugin):
            logger.warning(
                f"No plugin found for configuration file of type {full_type}. Make sure you have installed a plugin that can handle that type of data source."
            )
            return None

        return (plugin.execute(full_type=full_type, file_config=file_config), plugin)


def _execute_plugin_for_file(
    file: Path, main_type: str, plugins_per_type: PluginList
) -> tuple[BuildExecutionResult, BuildPlugin] | None:
    raise NotImplementedError("Files datasources are not supported yet")


def execute_plugins_for_all_datasource_files(
    project_dir: Path, plugins_per_type: PluginList
) -> list[tuple[BuildExecutionResult, BuildPlugin]]:
    source_folder = project_dir.joinpath("src")
    if not source_folder.exists() or not source_folder.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    results = []
    for current_folder_directory in source_folder.iterdir():
        if not current_folder_directory.exists() or not current_folder_directory.is_dir():
            continue

        for file in current_folder_directory.iterdir():
            result = _execute_plugin_for_datasource(file, current_folder_directory.name, plugins_per_type)
            if result is not None:
                results.append(result)

    return results
