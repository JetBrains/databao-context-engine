import yaml

from nemory.build_sources.internal.source_discovery import SourceDescriptor, full_type_of
from nemory.pluginlib.build_plugin import (
    BuildPlugin,
    BuildExecutionResult,
    BuildDatasourcePlugin,
    BuildFilePlugin,
    EmbeddableChunk,
)


def execute(source: SourceDescriptor, plugin: BuildPlugin) -> BuildExecutionResult:
    """
    Run a source through the plugin
    """
    full_type = full_type_of(source)
    suffix = source.path.suffix.lower()

    if suffix in {".yml", ".yaml"}:
        if not isinstance(plugin, BuildDatasourcePlugin):
            raise TypeError(
                f"Plugin {type(plugin).__name__} does not implement BuildDatasourcePlugin "
                f"required for YAML datasource configs ({source.path.name})"
            )
        datasource_name = source.path.name
        with source.path.open("r", encoding="utf-8") as fh:
            file_config = yaml.safe_load(fh) or {}
        return plugin.execute(
            full_type=full_type,
            datasource_name=datasource_name,
            file_config=file_config,
        )

    if not isinstance(plugin, BuildFilePlugin):
        raise TypeError(
            f"Plugin {type(plugin).__name__} does not implement BuildFilePlugin "
            f"required for raw files ({source.path.name})"
        )
    with source.path.open("rb") as fh:
        return plugin.execute(
            full_type=full_type,
            file_name=source.path.name,
            file_buffer=fh,
        )


def divide_into_chunks(plugin: BuildPlugin, result: BuildExecutionResult) -> list[EmbeddableChunk]:
    """
    Concert a plugin's build result into embeddable chunks
    """
    return plugin.divide_result_into_chunks(result)
