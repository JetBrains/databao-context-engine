import logging
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import ValidationError

from nemory.pluginlib.build_plugin import BuildDatasourcePlugin, NotSupportedError
from nemory.pluginlib.plugin_utils import check_connection_for_datasource
from nemory.plugins.plugin_loader import load_plugins
from nemory.project.datasource_discovery import get_datasource_descriptors, traverse_datasources
from nemory.project.layout import ensure_project_dir, get_source_dir
from nemory.project.types import PreparedConfig, PreparedDatasourceError
from nemory.utils.result import is_err

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


def validate_datasource_config(project_dir: Path, *, datasource_config_files: list[str] | None = None):
    ensure_project_dir(project_dir)

    results = _validate_datasource_config(project_dir, datasource_config_files=datasource_config_files)

    if len(results) > 0:
        valid_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.VALID
        }
        invalid_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.INVALID
        }
        unknown_datasources = {
            key: value for key, value in results.items() if value.validation_status == ValidationStatus.UNKNOWN
        }

        # Print all errors
        for datasource_path, validation_result in invalid_datasources.items():
            logger.info(
                f"Error for datasource {datasource_path}:{os.linesep}{validation_result.full_message}{os.linesep}"
            )

        results_summary = (
            os.linesep.join(
                [
                    f"{datasource_path}: {validation_result.format(show_summary_only=True)}"
                    for datasource_path, validation_result in results.items()
                ]
            )
            if results
            else "No datasource found"
        )

        logger.info(
            f"Validation completed with {len(valid_datasources)} valid datasource(s) and {len(invalid_datasources) + len(unknown_datasources)} invalid (or unknown status) datasource(s)"
            f"{os.linesep}{results_summary}"
        )
    else:
        logger.info("No datasource found")


def _validate_datasource_config(
    project_dir: Path, *, datasource_config_files: list[str] | None = None
) -> dict[str, ValidationResult]:
    src_dir = get_source_dir(project_dir)

    datasources_to_traverse = None
    if datasource_config_files:
        logger.info(f"Validating datasource(s): {datasource_config_files}")
        datasources_to_traverse = get_datasource_descriptors(project_dir, datasource_config_files)

    plugins = load_plugins(exclude_file_plugins=True)

    result = {}
    for source_result in traverse_datasources(project_dir, datasources_to_traverse=datasources_to_traverse):
        if is_err(source_result):
            error = source_result.err_value
            result[str(error.path.relative_to(src_dir))] = get_validation_result_from_error(error)
            continue

        prepared_source = source_result.ok_value

        result_key = str(prepared_source.path.relative_to(src_dir))

        plugin = plugins.get(prepared_source.full_type)
        if plugin is None:
            logger.debug(
                "No plugin for '%s' (datasource=%s) — skipping.", prepared_source.full_type, prepared_source.path
            )
            result[result_key] = ValidationResult(
                validation_status=ValidationStatus.INVALID, summary="No compatible plugin found"
            )
            continue

        if isinstance(prepared_source, PreparedConfig) and isinstance(plugin, BuildDatasourcePlugin):
            try:
                check_connection_for_datasource(
                    plugin=plugin,
                    full_type=prepared_source.full_type,
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
    if isinstance(e, PreparedDatasourceError):
        return ValidationResult(
            validation_status=ValidationStatus.INVALID,
            summary=str(e),
            full_message=str(e) + os.linesep + str(e.__cause__),
        )
    elif isinstance(e, ValidationError):
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
