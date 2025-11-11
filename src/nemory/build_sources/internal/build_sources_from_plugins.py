import logging

from nemory.build_sources.internal.execute_plugins import (
    execute_plugins_for_all_datasource_files,
)
from nemory.build_sources.internal.types import PluginList
from nemory.pluginlib.build_plugin import (
    BuildExecutionResult,
    BuildPlugin,
)
from nemory.project.layout import ensure_project_dir

logger = logging.getLogger(__name__)


def _add_plugin(plugin_list: PluginList, plugin: BuildPlugin) -> None:
    """
    Adds a plugin to the plugin list.

    /!\ This overrides any supported type that was already declared as supported in the original plugin list.
    ie. Plugins added last take precedence over plugins added early
    """
    plugin_list.update(
        {
            # fmt: skip
            supported_type: plugin
            for supported_type in plugin.supported_types()
        }
    )


def _get_all_build_plugins() -> PluginList:
    from nemory.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    plugin_list: PluginList = {}

    # TODO: Statically load more internal plugins
    _add_plugin(plugin_list, InternalUnstructuredFilesPlugin())

    # TODO: Load external plugins using entry points

    return plugin_list


def _export_results(results: list[tuple[BuildExecutionResult, BuildPlugin]]) -> None:
    # TODO: Implement writing the results in files
    pass


def _build_embeddings(results: list[tuple[BuildExecutionResult, BuildPlugin]]) -> None:
    # TODO: Use the get_chunks method of each result to create embeddings
    pass


def build_all_datasources(project_dir: str) -> None:
    project_path = ensure_project_dir(project_dir)

    # 1. Find all plugins that can be run
    plugins_per_type = _get_all_build_plugins()

    # 2. Browse the src directory to find all config file and execute the right plugin for each
    results = execute_plugins_for_all_datasource_files(project_path, plugins_per_type)

    _export_results(results)

    _build_embeddings(results)
