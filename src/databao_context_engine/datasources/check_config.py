import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

from pydantic import ValidationError

from databao_context_engine.datasources.datasource_discovery import (
    discover_datasources,
    prepare_source,
    validate_datasource_ids,
)
from databao_context_engine.datasources.types import DatasourceId, PreparedConfig
from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    DatasourceType,
    NotSupportedError,
)
from databao_context_engine.pluginlib.plugin_utils import check_connection_for_datasource
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.project.layout import ProjectLayout

logger = logging.getLogger(__name__)


class DatasourceConnectionStatus(Enum):
    """Status of the connection to a datasource."""

    VALID = "Valid"
    INVALID = "Invalid"
    UNKNOWN = "Unknown"


@dataclass(kw_only=True)
class CheckDatasourceConnectionResult:
    """Result of checking the connection status of a datasource.

    Attributes:
        datasource_id: The id of the datasource. Applicable in cases when the project has been already created.
        connection_status: The connection status of the datasource.
        summary: A summary of the connection status' error, or None if the connection is valid.
        full_message: A detailed message about the connection status' error, or None if the connection is valid.
    """

    datasource_id: DatasourceId | None = None
    connection_status: DatasourceConnectionStatus
    summary: str | None
    full_message: str | None = None

    def format(self, show_summary_only: bool = True) -> str:
        """Format the connection result in a human-readable string.

        Args:
            show_summary_only: Whether to show only the summary or show everything.

        Returns:
            A formatted string of the connection result.
        """
        formatted_string = str(self.connection_status.value)
        if self.summary:
            formatted_string += f" - {self.summary}"
        if not show_summary_only and self.full_message:
            formatted_string += f"{os.linesep}{self.full_message}"

        return formatted_string


def check_datasource_connection(
    project_layout: ProjectLayout,
    *,
    plugin_loader: DatabaoContextPluginLoader,
    datasource_ids: list[DatasourceId] | None = None,
) -> dict[DatasourceId, CheckDatasourceConnectionResult]:
    if datasource_ids:
        logger.info(f"Validating datasource(s): {datasource_ids}")
        datasources_to_traverse = validate_datasource_ids(project_layout, datasource_ids)
    else:
        datasources_to_traverse = discover_datasources(project_layout)

    result = {}
    for datasource_id in datasources_to_traverse:
        result_key = datasource_id

        try:
            prepared_source = prepare_source(project_layout, datasource_id)
        except Exception as e:
            result[result_key] = CheckDatasourceConnectionResult(
                datasource_id=datasource_id,
                connection_status=DatasourceConnectionStatus.INVALID,
                summary="Failed to prepare source",
                full_message=str(e),
            )
            continue

        plugin = plugin_loader.get_plugin_for_datasource_type(prepared_source.datasource_type)
        if plugin is None:
            logger.debug(
                "No plugin for '%s' (datasource=%s) — skipping.",
                prepared_source.datasource_type.full_type,
                prepared_source.datasource_id.datasource_path,
            )
            result[result_key] = CheckDatasourceConnectionResult(
                datasource_id=datasource_id,
                connection_status=DatasourceConnectionStatus.INVALID,
                summary="No compatible plugin found",
            )
            continue

        if not isinstance(plugin, BuildDatasourcePlugin):
            result[result_key] = CheckDatasourceConnectionResult(
                datasource_id=datasource_id,
                connection_status=DatasourceConnectionStatus.UNKNOWN,
                summary="Plugin does not support connection verification",
            )
            continue
        if isinstance(prepared_source, PreparedConfig):
            result[result_key] = do_check_connection(
                plugin,
                prepared_source.datasource_type,
                prepared_source.datasource_name,
                prepared_source.config,
                datasource_id,
            )

    return result


def check_datasource_config_connection(
    *,
    plugin_loader: DatabaoContextPluginLoader,
    datasource_type: DatasourceType,
    datasource_name: str,
    config_content: dict[str, Any],
) -> CheckDatasourceConnectionResult:

    plugin = plugin_loader.get_plugin_for_datasource_type(datasource_type)
    if plugin is None:
        return CheckDatasourceConnectionResult(
            connection_status=DatasourceConnectionStatus.INVALID,
            summary="No compatible plugin found",
        )

    if not isinstance(plugin, BuildDatasourcePlugin):
        return CheckDatasourceConnectionResult(
            connection_status=DatasourceConnectionStatus.UNKNOWN,
            summary="Plugin does not support connection verification",
        )

    return do_check_connection(plugin, datasource_type, datasource_name, config_content)


def do_check_connection(
    plugin: BuildDatasourcePlugin,
    datasource_type: DatasourceType,
    datasource_name: str,
    config: dict[str, Any],
    datasource_id: DatasourceId | None = None,
) -> CheckDatasourceConnectionResult:
    try:
        check_connection_for_datasource(
            plugin=plugin,
            datasource_type=datasource_type,
            config=config,
        )

        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id, connection_status=DatasourceConnectionStatus.VALID, summary=None
        )
    except Exception as e:
        logger.debug(
            f"Connection failed for {datasource_name} with error: {str(e)}",
            exc_info=True,
            stack_info=True,
        )
        return _get_validation_result_from_error(e, datasource_id)


def _get_validation_result_from_error(
    e: Exception, datasource_id: DatasourceId | None
) -> CheckDatasourceConnectionResult:
    if isinstance(e, ValidationError):
        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id,
            connection_status=DatasourceConnectionStatus.INVALID,
            summary="Config file is invalid",
            full_message=str(e),
        )
    if isinstance(e, NotImplementedError | NotSupportedError):
        return CheckDatasourceConnectionResult(
            datasource_id=datasource_id,
            connection_status=DatasourceConnectionStatus.UNKNOWN,
            summary="Plugin doesn't support validating its config",
        )

    return CheckDatasourceConnectionResult(
        datasource_id=datasource_id,
        connection_status=DatasourceConnectionStatus.INVALID,
        summary="Connection with the datasource can not be established",
        full_message=str(e),
    )
