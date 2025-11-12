import logging
from pathlib import Path

import yaml

from nemory.build_sources.internal.types import PluginList
from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    BuildFilePlugin,
    BuildPlugin,
)
from nemory.project.layout import get_source_dir

logger = logging.getLogger(__name__)


def _get_plugin_to_execute(plugins_per_type: PluginList, full_type: str) -> BuildPlugin | None:
    return plugins_per_type.get(full_type)


def _execute_plugin_for_datasource(
    datasource_file: Path, main_type: str, plugins_per_type: PluginList
) -> tuple[BuildExecutionResult, BuildPlugin] | None:
    if datasource_file.suffix in {".yaml", ".yml"} and not main_type == "files":
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

        logger.info(f"Config file found at src/{main_type}/{config_file.name}, with type {full_type}")
        logger.info(f"Plugin {plugin.name} will be executed")

        config_file_name_without_extension = config_file.name.split(".")[0]
        plugin_result = plugin.execute(
            full_type=full_type, datasource_name=config_file_name_without_extension, file_config=file_config
        )

        return plugin_result, plugin


def _execute_plugin_for_file(
    file: Path, main_type: str, plugins_per_type: PluginList
) -> tuple[BuildExecutionResult, BuildPlugin] | None:
    file_extension = file.suffix[1:]
    full_type = f"{main_type}/{file_extension}"

    plugin = _get_plugin_to_execute(plugins_per_type, full_type)

    if plugin is None or not isinstance(plugin, BuildFilePlugin):
        logger.warning(
            f"No plugin found for configuration file of type {full_type}. Make sure you have installed a plugin that can handle that type of data source."
        )
        return None

    logger.info(f"Raw file found at src/{main_type}/{file.name}, with type {full_type}")
    logger.info(f"Plugin {plugin.name} will be executed")

    with file.open("rb") as file_stream:
        plugin_result = plugin.execute(full_type=full_type, file_name=file.name, file_buffer=file_stream)
        return plugin_result, plugin


def execute_plugins_for_all_datasource_files(
    project_dir: Path, plugins_per_type: PluginList
) -> list[tuple[BuildExecutionResult, BuildPlugin]]:
    source_folder = get_source_dir(project_dir)
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
