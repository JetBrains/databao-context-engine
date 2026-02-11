from typing import Any

from databao_context_engine.datasources.datasource_discovery import get_datasource_descriptors, prepare_source
from databao_context_engine.datasources.sql_read_only import is_read_only_sql
from databao_context_engine.datasources.types import DatasourceId, PreparedConfig
from databao_context_engine.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.pluginlib.build_plugin import NotSupportedError, SqlRunnablePlugin
from databao_context_engine.pluginlib.plugin_utils import execute_sql_for_datasource
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.project.layout import ProjectLayout, logger


def run_sql(
    project_layout: ProjectLayout,
    loader: DatabaoContextPluginLoader,
    datasource_id: DatasourceId,
    sql: str,
    params: list[Any] | None = None,
    read_only: bool = True,
) -> SqlExecutionResult:
    if read_only and not is_read_only_sql(sql):
        # we could use SqlReadOnlyDecision in the future
        raise PermissionError("SQL execution is only supported for read-only queries")

    logger.info(f"Running SQL query against datasource {datasource_id}: {sql}")
    datasource_descriptor = get_datasource_descriptors(project_layout, [datasource_id])[0]

    prepared = prepare_source(datasource_descriptor)
    if not isinstance(prepared, PreparedConfig):
        raise NotSupportedError("SQL execution is only supported for config-backed datasources")

    plugin = loader.get_plugin_for_datasource_type(prepared.datasource_type)
    if not isinstance(plugin, SqlRunnablePlugin):
        raise NotSupportedError("Plugin doesn't support SQL execution")

    return execute_sql_for_datasource(
        plugin=plugin,
        datasource_type=prepared.datasource_type,
        config=prepared.config,
        sql=sql,
        params=params,
        read_only=read_only,
    )
