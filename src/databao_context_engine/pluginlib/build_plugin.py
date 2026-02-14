from abc import ABC
from dataclasses import dataclass
from io import BufferedReader
from typing import Any, Mapping, Protocol, runtime_checkable

from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult


@dataclass
class EmbeddableChunk:
    """A chunk that will be embedded as a vector and used when searching context from a given AI prompt.

    Attributes:
        embeddable_text: The text to embed as a vector for search usage
        content: The content to return as a response when the embedding has been selected in a search
    """

    embeddable_text: str
    content: Any


class BaseBuildPlugin(Protocol):
    """The base protocol that all plugins inherit from.

    This should not be inherited directly, instead make sure to implement one of BuildDatasourcePlugin, DefaultBuildDatasourcePlugin or BuildFilePlugin.

    Attributes:
        id: The id of the plugin
        name: The human-readable name of the plugin
        context_type: The type returned when building the context
    """

    id: str
    name: str
    context_type: type[Any]

    def supported_types(self) -> set[str]:
        """Returns the set of all supported types for this plugin.

        If the plugin supports multiple types, they should check the type given in the `full_type` argument when `execute` is called.

        Returns:
            The set of supported types for this plugin.
        """
        ...

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        """Divides the datasource context into meaningful chunks.

        The returned chunks will be used when searching the context from an AI prompt.

        Args:
            context: The context to be divided into chunks. This argument will have the type defined in the context_type instance attribute.

        Returns:
            A list of EmbeddableChunk objects that will be used for searching context.
        """
        ...


@runtime_checkable
class BuildDatasourcePlugin[T: ConfigFile](BaseBuildPlugin, Protocol):
    """A plugin that can be used to build the context of datasource, using a config file.

    Attributes:
        id: The id of the plugin
        name: The human-readable name of the plugin
        context_type: The type returned when building the context
        config_file_type: The type of the config file that is expected when building a context.
          If you don't want to provide a type, you can directly use DefaultBuildDatasourcePlugin which uses a dict as the config_file_type.
          This type must be compatible with Pydantic, which is the library used to parse and validate the config file.
    """

    config_file_type: type[T]

    def build_context(self, full_type: str, datasource_name: str, file_config: T) -> Any:
        """The method that will be called when a config file has been found for a data source supported by this plugin.

        Args:
            full_type: The type of the datasource to build.
              This type should be exactly the same than the one found in the file_config
            datasource_name: The name of the datasource to build
            file_config: The config file of the datasource to build.
                This argument will be an object of type `self.config_file_type`.

        Returns:
            The context for this datasource as an object of type `self.context_type`
        """
        ...

    def check_connection(self, full_type: str, datasource_name: str, file_config: T) -> None:
        """Check whether the configuration to the datasource is working.

        The function is expected to succeed without a result if the connection is working.
        If something is wrong with the connection, the function should raise an Exception

        Args:
            full_type: The type of the datasource to build.
              This type should be exactly the same than the one found in the file_config
            datasource_name: The name of the datasource to build
            file_config: The config file of the datasource to build.
                This argument will be an object of type `self.config_file_type`.

        Raises:
            NotSupportedError: If the plugin doesn't support this method.
        """
        raise NotSupportedError("This method is not implemented for this plugin")

    def run_sql(
        self,
        file_config: T,
        sql: str,
        params: list[Any] | None = None,
        read_only: bool = True,
    ) -> SqlExecutionResult:
        """Execute SQL against the datasource represented by `file_config`.

        Implementations should honor `read_only=True` by default and refuse mutating statements
        unless explicitly allowed.

        Raises:
            NotSupportedError: If the plugin doesn't support this method.
        """
        raise NotSupportedError("This method is not implemented for this plugin")


class DefaultBuildDatasourcePlugin(BuildDatasourcePlugin[dict[str, Any]], Protocol):
    """Defines a protocol to implement for plugins that don't want to strongly type their config file.

    This is the same than BuildDatasourcePlugin but it offers a shortcut to always get the config file as a dict.
    """

    config_file_type: type[dict[str, Any]] = dict[str, Any]


@runtime_checkable
class BuildFilePlugin(BaseBuildPlugin, Protocol):
    """A plugin that can be used to build the context of a raw file datasource.

    Attributes:
        id: The id of the plugin
        name: The human-readable name of the plugin
        context_type: The type returned when building the context
    """

    def build_file_context(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> Any:
        """The method that will be called when a file has been found as a data source supported by this plugin.

        Args:
            full_type: The type of the file to build context for.
            file_name: The name of the file to build context for, including its suffix.
            file_buffer: A buffered reader to the file to build context for.

        Returns:
            The context for this datasource as an object of type `self.context_type`
        """
        ...


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


class AbstractConfigFile(ABC):
    type: str
    name: str


# Config files can either be defined:
# - as a class inheriting AbstractConfigFile
# - or as a Mappping for Plugins that didn't declare a config type or used a TypedDict
ConfigFile = Mapping[str, Any] | AbstractConfigFile
