import logging
from pathlib import Path

from nemory.features.build_sources.internal.execute_plugins import (
    execute_plugins_for_all_datasource_files,
)
from nemory.features.build_sources.internal.types import PluginList
from nemory.features.build_sources.plugin_lib.build_plugin import (
    BuildExecutionResult,
    BuildPlugin,
)

logger = logging.getLogger(__name__)


def _get_all_build_plugins() -> PluginList:
    # TODO: Load plugins:
    #  1. Load internal plugins (dynamically importing all plugins in a specific folder? or statically importing all the plugins?)
    #  2. Load external plugins (using entry points?)
    # TODO: How do we deal with multiple plugins handling the same type? Simply taking the latest installed? Priorizing internal plugins over externals?
    return dict()


def _export_results(results: list[tuple[BuildExecutionResult, BuildPlugin]]) -> None:
    # TODO: Implement writing the results in files
    pass


def _build_embeddings(results: list[tuple[BuildExecutionResult, BuildPlugin]]) -> None:
    # TODO: Use the get_chunks method of each result to create embeddings
    pass


def build_all_datasources(project_dir: str) -> None:
    # 1. Find all plugins that can be run
    plugins_per_type = _get_all_build_plugins()

    # 2. Browse the src directory to find all config file and execute the right plugin for each
    results = execute_plugins_for_all_datasource_files(Path(project_dir), plugins_per_type)

    _export_results(results)

    _build_embeddings(results)
