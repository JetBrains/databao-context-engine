from dataclasses import dataclass
from io import BufferedReader
from typing import Any, Protocol, runtime_checkable


@dataclass
class EmbeddableChunk:
    """A chunk that will be embedded as a vector and used when searching context from a given AI prompt."""

    embeddable_text: str
    """
    The text to embed as a vector for search usage
    """
    content: Any
    """
    The content to return as a response when the embeddings has been selected in a search
    """


class BaseBuildPlugin(Protocol):
    id: str
    name: str

    def supported_types(self) -> set[str]: ...

    """
    Returns the list of all supported types for this plugin.
    If the plugin supports multiple types, they should check the type given in the `full_type` argument when `execute` is called.
    """

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]: ...

    """
    A method dividing the data source context into meaningful chunks that will be used when searching the context from an AI prompt.
    """


@runtime_checkable
class BuildDatasourcePlugin[T](BaseBuildPlugin, Protocol):
    config_file_type: type[T]

    def build_context(self, full_type: str, datasource_name: str, file_config: T) -> Any: ...

    """
    The method that will be called when a config file has been found for a data source supported by this plugin.
    """

    def check_connection(self, full_type: str, datasource_name: str, file_config: T) -> None:
        """Check whether the configuration to the datasource is working.

        The function is expected to succeed without a result if the connection is working.
        If something is wrong with the connection, the function should raise an Exception

        Raises:
            NotSupportedError: If the plugin doesn't support this method.
        """
        raise NotSupportedError("This method is not implemented for this plugin")


class DefaultBuildDatasourcePlugin(BuildDatasourcePlugin[dict[str, Any]], Protocol):
    """Use this as a base class for plugins that don't need a specific config file type."""

    config_file_type: type[dict[str, Any]] = dict[str, Any]


@runtime_checkable
class BuildFilePlugin(BaseBuildPlugin, Protocol):
    def build_file_context(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> Any: ...

    """
    The method that will be called when a file has been found as a data source supported by this plugin.
    """


class NotSupportedError(RuntimeError):
    """Exception raised by methods not supported by a plugin."""


BuildPlugin = BuildDatasourcePlugin | BuildFilePlugin


@dataclass(kw_only=True, frozen=True)
class DatasourceType:
    """The type of a Datasource.

    Attributes:
        full_type: The full type of the datasource, in the format `<main_type>/<subtype>`.
    """

    full_type: str
