import logging
import os

from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin
from nemory.pluginlib.plugin_utils import generate_json_schema, format_json_schema_for_output

logger = logging.getLogger(__name__)


def generate_plugins_configs():
    all_plugins = load_plugins().values()

    results = []
    for plugin in all_plugins:
        if not isinstance(plugin, BuildDatasourcePlugin):
            continue

        json_schema = generate_json_schema(plugin)
        if json_schema is not None:
            results.append(format_json_schema_for_output(plugin, json_schema))

    print(f"{os.linesep}{os.linesep}".join(results))


if __name__ == "__main__":
    generate_plugins_configs()
