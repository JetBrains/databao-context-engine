import logging

from nemory.build_sources.internal.types import PluginList
from nemory.pluginlib.build_plugin import BuildPlugin

logger = logging.getLogger(__name__)


class DuplicatePluginTypeError(RuntimeError):
    """Raised when two plugins register the same <main>/<sub> plugin key."""


def load_plugins() -> PluginList:
    """
    Loads both builtin and external plugins and merges them into one list
    """
    builtin_plugins = load_builtin_plugins()
    external_plugins = load_external_plugins()
    plugins = merge_plugins(builtin_plugins, external_plugins)

    return plugins


def load_builtin_plugins() -> list[BuildPlugin]:
    """
    Statically register built-in plugins
    """
    from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin
    from nemory.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    builtin_plugins: list[BuildPlugin] = [
        PostgresqlDbPlugin(),
        InternalUnstructuredFilesPlugin(),
    ]

    return builtin_plugins


def load_external_plugins() -> list[BuildPlugin]:
    """
    Discover external plugins via entry points
    """
    # TODO: implement external plugin loading
    return []


def merge_plugins(*plugin_lists: list[BuildPlugin]) -> PluginList:
    """
    Merge multiple plugin maps
    """
    registry: PluginList = {}
    for plugins in plugin_lists:
        for plugin in plugins:
            for full_type in plugin.supported_types():
                if full_type in registry:
                    raise DuplicatePluginTypeError(
                        f"Plugin type '{full_type}' is provided by both {type(registry[full_type]).__name__} and {type(plugin).__name__}"
                    )
                registry[full_type] = plugin
    return registry
