import logging
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import ValidationError

from nemory.datasource_config.utils import get_datasource_config_relative_path
from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, NotSupportedError
from nemory.pluginlib.plugin_utils import check_connection_for_datasource
from nemory.plugins.plugin_loader import load_plugins
from nemory.project.datasource_discovery import (
    discover_datasources,
    get_datasource_descriptors,
    prepare_source,
)
from nemory.project.layout import ensure_project_dir
from nemory.project.types import PreparedConfig

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    VALID = "Valid"
    INVALID = "Invalid"
    UNKNOWN = "Unknown"


@dataclass(kw_only=True)
class ValidationResult:
    validation_status: ValidationStatus
    summary: str | None
    full_message: str | None = None

    def format(self, show_summary_only: bool = True) -> str:
        formatted_string = str(self.validation_status.value)
        if self.summary:
            formatted_string += f" - {self.summary}"
        if not show_summary_only and self.full_message:
            formatted_string += f"{os.linesep}{self.full_message}"

        return formatted_string


def validate_datasource_config(
    project_dir: Path, *, datasource_config_files: list[str] | None = None
) -> dict[str, ValidationResult]:
    ensure_project_dir(project_dir)

    if datasource_config_files:
        logger.info(f"Validating datasource(s): {datasource_config_files}")
        datasources_to_traverse = get_datasource_descriptors(project_dir, datasource_config_files)
    else:
        datasources_to_traverse = discover_datasources(project_dir)

    plugins = load_plugins(exclude_file_plugins=True)

    result = {}
    for discovered_datasource in datasources_to_traverse:
        result_key = get_datasource_config_relative_path(project_dir, discovered_datasource.path)

        try:
            prepared_source = prepare_source(discovered_datasource)
        except Exception as e:
            result[result_key] = ValidationResult(
                validation_status=ValidationStatus.INVALID,
                summary="Failed to prepare source",
                full_message=str(e),
            )
            continue

        plugin = plugins.get(prepared_source.datasource_type)
        if plugin is None:
            logger.debug(
                "No plugin for '%s' (datasource=%s) — skipping.",
                prepared_source.datasource_type.full_type,
                prepared_source.path,
            )
            result[result_key] = ValidationResult(
                validation_status=ValidationStatus.INVALID, summary="No compatible plugin found"
            )
            continue

        if isinstance(prepared_source, PreparedConfig) and isinstance(plugin, BuildDatasourcePlugin):
            try:
                check_connection_for_datasource(
                    plugin=plugin,
                    datasource_type=prepared_source.datasource_type,
                    config=prepared_source.config,
                    datasource_name=prepared_source.datasource_name,
                )

                result[result_key] = ValidationResult(validation_status=ValidationStatus.VALID, summary=None)
            except Exception as e:
                logger.debug(
                    f"Connection failed for {prepared_source.datasource_name} with error: {str(e)}",
                    exc_info=True,
                    stack_info=True,
                )
                result[result_key] = get_validation_result_from_error(e)

    return result


def get_validation_result_from_error(e: Exception):
    if isinstance(e, ValidationError):
        return ValidationResult(
            validation_status=ValidationStatus.INVALID,
            summary="Config file is invalid",
            full_message=str(e),
        )
    elif isinstance(e, NotImplementedError | NotSupportedError):
        return ValidationResult(
            validation_status=ValidationStatus.UNKNOWN,
            summary="Plugin doesn't support validating its config",
        )
    else:
        return ValidationResult(
            validation_status=ValidationStatus.INVALID,
            summary="Connection with the datasource can not be established",
            full_message=str(e),
        )
