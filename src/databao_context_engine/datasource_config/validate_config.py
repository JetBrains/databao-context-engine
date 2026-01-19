import logging
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import ValidationError

from databao_context_engine.pluginlib.build_plugin import BuildDatasourcePlugin, NotSupportedError
from databao_context_engine.pluginlib.plugin_utils import check_connection_for_datasource
from databao_context_engine.plugins.plugin_loader import load_plugins
from databao_context_engine.project.datasource_discovery import (
    discover_datasources,
    get_datasource_descriptors,
    prepare_source,
)
from databao_context_engine.project.layout import ensure_project_dir
from databao_context_engine.project.types import PreparedConfig, DatasourceId

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    VALID = "Valid"
    INVALID = "Invalid"
    UNKNOWN = "Unknown"


@dataclass(kw_only=True)
class CheckDatasourceConnectionResult:
    datasource_id: DatasourceId
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
    project_dir: Path, *, datasource_ids: list[DatasourceId] | None = None
) -> dict[DatasourceId, CheckDatasourceConnectionResult]:
    ensure_project_dir(project_dir)

    if datasource_ids:
        logger.info(f"Validating datasource(s): {datasource_ids}")
        datasources_to_traverse = get_datasource_descriptors(project_dir, datasource_ids)
    else:
        datasources_to_traverse = discover_datasources(project_dir)

    plugins = load_plugins(exclude_file_plugins=True)

    result = {}
    for discovered_datasource in datasources_to_traverse:
        result_key = DatasourceId.from_datasource_config_file_path(discovered_datasource.path)

        try:
            prepared_source = prepare_source(discovered_datasource)
        except Exception as e:
            result[result_key] = CheckDatasourceConnectionResult(
                datasource_id=result_key,
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
            result[result_key] = CheckDatasourceConnectionResult(
                datasource_id=result_key,
                validation_status=ValidationStatus.INVALID,
                summary="No compatible plugin found",
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

                result[result_key] = CheckDatasourceConnectionResult(
                    datasource_id=result_key, validation_status=ValidationStatus.VALID, summary=None
                )
            except Exception as e:
                logger.debug(
                    f"Connection failed for {prepared_source.datasource_name} with error: {str(e)}",
                    exc_info=True,
                    stack_info=True,
                )
                result[result_key] = get_validation_result_from_error(result_key, e)

    return result


def get_validation_result_from_error(datasource_id: DatasourceId, e: Exception):
    if isinstance(e, ValidationError):
        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id,
            validation_status=ValidationStatus.INVALID,
            summary="Config file is invalid",
            full_message=str(e),
        )
    elif isinstance(e, NotImplementedError | NotSupportedError):
        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id,
            validation_status=ValidationStatus.UNKNOWN,
            summary="Plugin doesn't support validating its config",
        )
    else:
        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id,
            validation_status=ValidationStatus.INVALID,
            summary="Connection with the datasource can not be established",
            full_message=str(e),
        )
