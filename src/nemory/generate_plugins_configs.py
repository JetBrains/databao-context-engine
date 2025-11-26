import json
import logging

from pydantic import TypeAdapter

from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin

logger = logging.getLogger(__name__)


def generate_plugins_configs():
    all_plugins = load_plugins().values()

    results = []
    for plugin in all_plugins:
        if not isinstance(plugin, BuildDatasourcePlugin):
            continue

        if plugin.config_file_type is None:
            logger.debug(f"No type provided for plugin {plugin.id}")
            continue

        results.append(json.dumps(TypeAdapter(plugin.config_file_type).json_schema(mode="serialization"), indent=4))

    print("\n".join(results))


if __name__ == "__main__":
    generate_plugins_configs()
