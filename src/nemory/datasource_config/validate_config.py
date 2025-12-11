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


def validate_datasource_config(project_dir: Path, *, datasource_config_files: list[str] | None = None):
    ensure_project_dir(project_dir)

    results = _validate_datasource_config(project_dir, datasource_config_files=datasource_config_files)

    resuls_as_string = (
        os.linesep.join(
            [
                f"{datasource_path}: {validation_result.format(show_summary_only=len(results) > 1)}"
                for datasource_path, validation_result in results.items()
            ]
        )
        if results
        else "No datasource found"
    )
    logger.info(f"Validation complete: {os.linesep}{resuls_as_string}")

    if len(results) > 1 and any(
        result for result in results.values() if result.validation_status == ValidationStatus.INVALID
    ):
        logger.info(
            'To get more details about the invalid configuration, try re-running this command specifying only one datasource to validate.\n e.g: nemory datasource validate "my-source-folder/my-config-name.yaml"'
        )


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
    for prepared_source in traverse_datasources(project_dir, datasources_to_traverse=datasources_to_traverse):
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
                if isinstance(e, ValidationError):
                    result[result_key] = ValidationResult(
                        validation_status=ValidationStatus.INVALID,
                        summary="Config file is invalid",
                        full_message=str(e),
                    )
                elif isinstance(e, NotImplementedError | NotSupportedError):
                    result[result_key] = ValidationResult(
                        validation_status=ValidationStatus.UNKNOWN,
                        summary="Plugin doesn't support validating its config",
                    )
                else:
                    result[result_key] = ValidationResult(
                        validation_status=ValidationStatus.INVALID,
                        summary="Connection with the datasource can not be established",
                        full_message=str(e),
                    )

    return result
