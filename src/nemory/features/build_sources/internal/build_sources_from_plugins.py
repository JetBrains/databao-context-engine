import logging
from pathlib import Path

import yaml

from nemory.features.build_sources.plugin_lib.build_plugin import (
    BuildPlugin,
    BuildExecutionResult,
)

logger = logging.getLogger(__name__)

CONFIG_SRC_FOLDERS = ["databases", "dbt", "others"]


def _get_all_build_plugins() -> dict[str, BuildPlugin]:
    # TODO: Load plugins:
    #  1. Load internal plugins (dynamically importing all plugins in a specific folder? or statically importing all the plugins?)
    #  2. Load external plugins (using entry points?)
    # TODO: How do we deal with multiple plugins handling the same type? Simply taking the latest installed? Priorizing internal plugins over externals?
    return dict()


def _get_plugin_to_execute(
    plugins_per_type: dict[str, BuildPlugin], full_type: str
) -> BuildPlugin | None:
    return plugins_per_type[full_type]


def _execute_plugin_for_config_file(
    config_file: Path, main_type: str, plugins_per_type: dict[str, BuildPlugin]
) -> BuildExecutionResult | None:
    if config_file.suffix not in {".yaml", ".yml"}:
        return None

    with config_file.open("r") as yaml_stream:
        file_config = yaml.safe_load(yaml_stream)
        file_subtype = file_config["type"]
        if not file_subtype:
            logger.warning(
                f"Found a data source with no type at: {str(config_file.resolve())}"
            )
            return None

        full_type = f"{main_type}/{file_subtype}"

        plugin = _get_plugin_to_execute(plugins_per_type, full_type)

        if plugin is None:
            logger.warning(
                f"No plugin found for configuration file of type {full_type}. Make sure you have installed a plugin that can handle that type of data source."
            )
            return None

        return plugin.execute(full_type=full_type, file_config=file_config)


def _execute_plugins_for_all_config_files(
    project_dir: Path, plugins_per_type: dict[str, BuildPlugin]
) -> list[BuildExecutionResult]:
    source_folder = project_dir.joinpath("src")
    if not source_folder.exists() or not source_folder.is_dir():
        raise ValueError(f"src directory does not exist in {project_dir}")

    results = []
    for folder in CONFIG_SRC_FOLDERS:
        current_folder_directory = source_folder.joinpath(folder)
        if (
            not current_folder_directory.exists()
            or not current_folder_directory.is_dir()
        ):
            continue

        for file in current_folder_directory.iterdir():
            result = _execute_plugin_for_config_file(file, folder, plugins_per_type)
            if result is not None:
                results.append(result)

    return results


def _export_results(results: list[BuildExecutionResult]) -> None:
    # TODO: Implement writing the results in files
    pass


def _build_embeddings(results: list[BuildExecutionResult]) -> None:
    # TODO: Use the get_chunks method of each result to create embeddings
    pass


def build_all_datasources(project_dir: str) -> None:
    # 1. Find all plugins that can be run
    plugins_per_type = _get_all_build_plugins()

    # 2. Browse the src directory to find all config file and execute the right plugin for each
    results = _execute_plugins_for_all_config_files(Path(project_dir), plugins_per_type)

    _export_results(results)

    _build_embeddings(results)
