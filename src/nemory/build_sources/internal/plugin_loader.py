import logging
import operator
from collections import Counter
from functools import reduce

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


def load_builtin_plugins() -> PluginList:
    """
    Statically register built-in plugins
    """
    from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin
    from nemory.plugins.unstructured_files_plugin import InternalUnstructuredFilesPlugin

    builtin_plugins: list[BuildPlugin] = [
        PostgresqlDbPlugin(),
        InternalUnstructuredFilesPlugin(),
    ]

    registry: PluginList = {}
    for plugin in builtin_plugins:
        for full_type in plugin.supported_types():
            registry[full_type] = plugin

    return registry


def load_external_plugins() -> PluginList:
    """
    Discover external plugins via entry points
    """
    # TODO: implement external plugin loading
    return {}


def merge_plugins(*maps: PluginList) -> PluginList:
    """
    Merge multiple plugin maps
    """
    counts = Counter(k for m in maps for k in m)
    dupes = [k for k, c in counts.items() if c > 1]
    if dupes:
        raise DuplicatePluginTypeError(f"Duplicate plugin keys: {', '.join(sorted(dupes))}")

    return reduce(operator.or_, maps, {})
