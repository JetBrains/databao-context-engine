import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from databao_context_engine.pluginlib.build_plugin import DatasourceType


class DatasourceKind(StrEnum):
    CONFIG = "config"
    FILE = "file"


@dataclass(frozen=True)
class DatasourceDescriptor:
    path: Path
    kind: DatasourceKind
    main_type: str


@dataclass(frozen=True)
class PreparedConfig:
    datasource_type: DatasourceType
    path: Path
    config: dict[Any, Any]
    datasource_name: str


@dataclass(frozen=True)
class PreparedFile:
    datasource_type: DatasourceType
    path: Path


PreparedDatasource = PreparedConfig | PreparedFile


@dataclass(kw_only=True, frozen=True, eq=True)
class DatasourceId:
    """The ID of a datasource. The ID is the path to the datasource's config file relative to the src folder in the project.

    e.g: "databases/my_postgres_datasource.yaml"

    Use the provided factory methods `from_string_repr` and `from_datasource_config_file_path` to create a DatasourceId, rather than its constructor.

    Attributes:
        datasource_config_folder: The folder where the datasource's config file is located.
        datasource_name: The name of the datasource.
        config_file_suffix: The suffix of the config (or raw) file.
    """

    datasource_config_folder: str
    datasource_name: str
    config_file_suffix: str

    def __post_init__(self):
        if not self.datasource_config_folder.strip():
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_config_folder must not be empty")
        if not self.datasource_name.strip():
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_name must not be empty")
        if not self.config_file_suffix.strip():
            raise ValueError(f"Invalid DatasourceId ({str(self)}): config_file_suffix must not be empty")

        if os.sep in self.datasource_config_folder:
            raise ValueError(
                f"Invalid DatasourceId ({str(self)}): datasource_config_folder must not contain a path separator"
            )

        if os.sep in self.datasource_name:
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_name must not contain a path separator")

        if not self.config_file_suffix.startswith("."):
            raise ValueError(
                f'Invalid DatasourceId ({str(self)}): config_file_suffix must start with a dot "." (e.g.: .yaml)'
            )

        if self.datasource_name.endswith(self.config_file_suffix):
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_name must not contain the file suffix")

    def __str__(self):
        return str(self.relative_path_to_config_file())

    def relative_path_to_config_file(self) -> Path:
        """Return a path to the config file for this datasource.

        The returned path is relative to the src folder in the project.

        Returns:
            The path to the config file relative to the src folder in the project.
        """
        return Path(self.datasource_config_folder).joinpath(self.datasource_name + self.config_file_suffix)

    def relative_path_to_context_file(self) -> Path:
        """Return a path to the config file for this datasource.

        The returned path is relative to an output run folder in the project.

        Returns:
            The path to the context file relative to the output folder in the project.
        """
        # Keep the suffix in the filename if this datasource is a raw file, to handle multiple files with the same name and different extensions
        suffix = ".yaml" if self.config_file_suffix == ".yaml" else (self.config_file_suffix + ".yaml")

        return Path(self.datasource_config_folder).joinpath(self.datasource_name + suffix)

    @classmethod
    def from_string_repr(cls, datasource_id_as_string: str) -> "DatasourceId":
        """Create a DatasourceId from a string representation.

        Args:
            datasource_id_as_string: The string representation of a DatasourceId.
                This is the path to the datasource's config file relative to the src folder in the project.
                (e.g. "databases/my_postgres_datasource.yaml")

        Returns:
            The DatasourceId instance created from the string representation.

        Raises:
            ValueError: If the string representation is invalid (e.g. too many parent folders).
        """
        config_file_path = Path(datasource_id_as_string)

        if len(config_file_path.parents) > 2:
            raise ValueError(
                f"Invalid string representation of a DatasourceId: too many parent folders defined in {datasource_id_as_string}"
            )

        return DatasourceId.from_datasource_config_file_path(config_file_path)

    @classmethod
    def from_datasource_config_file_path(cls, datasource_config_file: Path) -> "DatasourceId":
        """Create a DatasourceId from a config file path.

        Args:
            datasource_config_file: The path to the datasource config file.
                This path can either be the config file path relative to the src folder or the full path to the config file.

        Returns:
            The DatasourceId instance created from the config file path.
        """
        return DatasourceId(
            datasource_config_folder=datasource_config_file.parent.name,
            datasource_name=datasource_config_file.stem,
            config_file_suffix=datasource_config_file.suffix,
        )

    @classmethod
    def from_datasource_context_file_path(cls, datasource_context_file: Path) -> "DatasourceId":
        """Create a DatasourceId from a context file path.

        This factory handles the case where the context was generated from a raw file rather than from a config.
        In that case, the context file name will look like "<my_datasource_name>.<raw_file_extension>.yaml"

        Args:
            datasource_context_file: The path to the datasource context file.

        Returns:
            The DatasourceId instance created from the context file path.
        """
        if len(datasource_context_file.suffixes) > 1 and datasource_context_file.suffix == ".yaml":
            # If there is more than 1 suffix, we remove the latest suffix (.yaml) to keep only the actual datasource file suffix
            context_file_name_without_yaml_extension = datasource_context_file.name[: -len(".yaml")]
            datasource_context_file = datasource_context_file.with_name(context_file_name_without_yaml_extension)

        return DatasourceId(
            datasource_config_folder=datasource_context_file.parent.name,
            datasource_name=datasource_context_file.stem,
            config_file_suffix=datasource_context_file.suffix,
        )


@dataclass
class Datasource:
    """A datasource contained in the project.

    Attributes:
        id: The unique identifier of the datasource.
        type: The type of the datasource.
    """

    id: DatasourceId
    type: DatasourceType
