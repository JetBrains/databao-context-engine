from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import ProjectLayout


class DatasourceKind(StrEnum):
    CONFIG = "config"
    FILE = "file"


@dataclass(frozen=True)
class PreparedConfig:
    datasource_id: "DatasourceId"
    datasource_type: DatasourceType
    config: dict[str, Any]
    datasource_name: str


@dataclass(frozen=True)
class PreparedFile:
    datasource_id: "DatasourceId"
    datasource_type: DatasourceType


PreparedDatasource = PreparedConfig | PreparedFile


@dataclass(kw_only=True, frozen=True, eq=True)
class DatasourceId:
    """The ID of a datasource. The ID is the path to the datasource's config file relative to the src folder in the project.

    e.g: "databases/my_postgres_datasource.yaml"

    Use the provided factory methods `from_string_repr` and `from_datasource_config_file_path` to create a DatasourceId, rather than its constructor.

    Attributes:
        datasource_path: The path to the datasource relative to project's src folder.
        config_file_suffix: The suffix of the config (or raw) file.
    """

    ALLOWED_YAML_SUFFIXES = [".yaml", ".yml"]
    CONTEXT_FILE_SUFFIX = ".yaml"

    datasource_path: str
    config_file_suffix: str

    @property
    def kind(self) -> DatasourceKind:
        parts = self.datasource_path.split("/")
        if len(parts) == 2 and parts[0] == "files":
            return DatasourceKind.FILE
        if self.config_file_suffix in {".yaml", ".yml"}:
            return DatasourceKind.CONFIG
        if self.config_file_suffix:
            return DatasourceKind.FILE
        raise ValueError("Unknown datasource kind %s" % self)

    @property
    def name(self) -> str:
        match self.kind:
            case DatasourceKind.CONFIG:
                return self.datasource_path.split("/")[-1]
            case DatasourceKind.FILE:
                return (self.datasource_path + self.config_file_suffix).split("/")[-1]

    def __post_init__(self):
        if not self.datasource_path.strip():
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_path must not be empty")
        if not self.config_file_suffix.strip():
            raise ValueError(f"Invalid DatasourceId ({str(self)}): config_file_suffix must not be empty")

        if not self.config_file_suffix.startswith("."):
            raise ValueError(
                f'Invalid DatasourceId ({str(self)}): config_file_suffix must start with a dot "." (e.g.: .yaml)'
            )

        if self.datasource_path.endswith(self.config_file_suffix):
            raise ValueError(f"Invalid DatasourceId ({str(self)}): datasource_path must not contain the file suffix")

    def __str__(self):
        return str(self.relative_path_to_config_file())

    def relative_path_to_config_file(self) -> Path:
        """Return a path to the config file for this datasource.

        The returned path is relative to the src folder in the project.

        Returns:
            The path to the config file relative to the src folder in the project.
        """
        return Path(self.datasource_path + self.config_file_suffix)

    def absolute_path_to_config_file(self, project_layout: ProjectLayout) -> Path:
        """Return an absolute path to the config file for this datasource.

        Args:
            project_layout: The databao context engine project layout.

        Returns:
            The absolute path to the config file.
        """
        return project_layout.src_dir / self.relative_path_to_config_file()

    def relative_path_to_context_file(self) -> Path:
        """Return a path to the config file for this datasource.

        The returned path is relative to an output run folder in the project.

        Returns:
            The path to the context file relative to the output folder in the project.
        """
        # Keep the suffix in the filename if this datasource is a raw file, to handle multiple files with the same name and different extensions
        suffix = (
            DatasourceId.CONTEXT_FILE_SUFFIX
            if self.config_file_suffix in DatasourceId.ALLOWED_YAML_SUFFIXES
            else (self.config_file_suffix + DatasourceId.CONTEXT_FILE_SUFFIX)
        )

        return Path(self.datasource_path + suffix)

    def absolute_path_to_context_file(self, project_layout: ProjectLayout) -> Path:
        """Return an absolute path to the context file for this datasource.

        Args:
            project_layout: The databao context engine project layout.

        Returns:
            The absolute path to the context file.
        """
        return project_layout.output_dir / self.relative_path_to_context_file()

    @classmethod
    def from_string_repr(cls, datasource_id_as_string: str) -> "DatasourceId":
        """Create a DatasourceId from a string representation.

        Args:
            datasource_id_as_string: The string representation of a DatasourceId.
                This is the path to the datasource's config file relative to the src folder in the project.
                (e.g. "databases/my_postgres_datasource.yaml")

        Returns:
            The DatasourceId instance created from the string representation.
        """
        config_file_path = Path(datasource_id_as_string)
        return DatasourceId._from_relative_datasource_config_file_path(config_file_path)

    @classmethod
    def from_datasource_config_file_path(
        cls, project_layout: ProjectLayout, datasource_config_file: Path
    ) -> "DatasourceId":
        """Create a DatasourceId from a config file path.

        Args:
            project_layout: The databao context engine project layout.
            datasource_config_file: The path to the datasource config file.
                This path can either be the config file path relative to the src folder or the full path to the config file.

        Returns:
            The DatasourceId instance created from the config file path.
        """
        relative_datasource_config_file = datasource_config_file.relative_to(project_layout.src_dir)
        return DatasourceId._from_relative_datasource_config_file_path(relative_datasource_config_file)

    @classmethod
    def _from_relative_datasource_config_file_path(cls, relative_datasource_config_file: Path) -> "DatasourceId":
        if relative_datasource_config_file.is_absolute():
            raise ValueError(
                f"Path to datasource config file should be relative to project's src folder: {relative_datasource_config_file}"
            )
        return DatasourceId(
            datasource_path=_extract_datasource_path(relative_datasource_config_file),
            config_file_suffix=relative_datasource_config_file.suffix,
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

        Raises:
            ValueError: If the wrong datasource_context_file is provided.
        """
        if (
            len(datasource_context_file.suffixes) > 1
            and datasource_context_file.suffix == DatasourceId.CONTEXT_FILE_SUFFIX
        ):
            # If there is more than 1 suffix, we remove the latest suffix (.yaml) to keep only the actual datasource file suffix
            context_file_name_without_yaml_extension = datasource_context_file.name[
                : -len(DatasourceId.CONTEXT_FILE_SUFFIX)
            ]
            datasource_context_file = datasource_context_file.with_name(context_file_name_without_yaml_extension)
        if datasource_context_file.is_absolute():
            raise ValueError(f"Path to datasource context file should be a relative path: {datasource_context_file}")
        return DatasourceId(
            datasource_path=_extract_datasource_path(datasource_context_file),
            config_file_suffix=datasource_context_file.suffix,
        )


def _extract_datasource_path(datasource_file: Path) -> str:
    return str(datasource_file.with_suffix(""))


@dataclass
class Datasource:
    """A datasource contained in the project.

    Attributes:
        id: The unique identifier of the datasource.
        type: The type of the datasource.
    """

    id: DatasourceId
    type: DatasourceType


@dataclass
class ConfiguredDatasource:
    """A datasource configured in the project.

    Attributes:
        datasource: An object describing the datasource.
        config: The config dictionary for the datasource, or None if the datasource is a raw file.
    """

    datasource: Datasource
    config: dict[str, Any] | None
