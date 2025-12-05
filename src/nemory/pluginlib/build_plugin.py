from dataclasses import asdict, dataclass, replace
from datetime import datetime
from io import BufferedReader
from typing import Any, Protocol, runtime_checkable

from nemory.pluginlib.config_properties import ConfigPropertyDefinition


@dataclass
class EmbeddableChunk:
    """
    A chunk that will be embedded as a vector and used when searching context from a given AI prompt
    """

    embeddable_text: str
    """
    The text to embed as a vector for search usage
    """
    content: Any
    """
    The content to return as a response when the embeddings has been selected in a search
    """


@dataclass()
class BuildExecutionResult:
    """
    Dataclass defining the result of the execution of a build plugin.

    The implementing class must contain the defined attributes
    as well as a method that allows to create chunks from the result.
    """

    id: str
    """
    The id of the built data source
    """

    name: str
    """
    The name of the built data source
    """

    type: str
    """
    The type of the built data source
    """

    description: str | None
    """
    A description of the data source
    """

    version: str | None
    """
    The version number of the data source when it was built
    """

    executed_at: datetime
    """
    The time of execution of the build plugin
    """

    result: Any
    """
    A dictionary containing the actual result that should be stored as context for the data source.
    This dictionary should be serializable in JSON or YAML format.
    """

    def _to_yaml_serializable(self) -> dict[str, Any]:
        return asdict(replace(self, result=repr(self.result)))


class BaseBuildPlugin(Protocol):
    id: str
    name: str

    def supported_types(self) -> set[str]: ...

    """
    Returns the list of all supported types for this plugin.
    If the plugin supports multiple types, they should check the type given in the `full_type` argument when `execute` is called.
    """

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]: ...

    """
    A method dividing the data source context into meaninful chunks that will be used when searching the context from an AI prompt.
    """


@runtime_checkable
class BuildDatasourcePlugin[T](BaseBuildPlugin, Protocol):
    config_file_type: type[T]

    def execute(self, full_type: str, datasource_name: str, file_config: T) -> BuildExecutionResult: ...

    """
    The method that will be called when a config file has been found for a data source supported by this plugin.
    """

    def get_mandatory_config_file_structure(self) -> list[ConfigPropertyDefinition]:
        return []


class DefaultBuildDatasourcePlugin(BuildDatasourcePlugin[dict[str, Any]], Protocol):
    """
    Use this as a base class for plugins that don't need a specific config file type.
    """

    config_file_type: type[dict[str, Any]] = dict[str, Any]


@runtime_checkable
class BuildFilePlugin(BaseBuildPlugin, Protocol):
    def execute(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> BuildExecutionResult: ...

    """
    The method that will be called when a file has been found as a data source supported by this plugin.
    """


BuildPlugin = BuildDatasourcePlugin | BuildFilePlugin
