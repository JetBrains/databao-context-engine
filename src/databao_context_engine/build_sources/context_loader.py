from __future__ import annotations

from dataclasses import replace
from typing import Any

import yaml
from pydantic import BaseModel, TypeAdapter

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.datasource_context import (
    DatasourceContext,
    get_datasource_context,
    read_datasource_type_from_context,
    read_datasource_type_from_context_file,
)
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import BuildPlugin, DatasourceType
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader, NoPluginFoundForDatasource
from databao_context_engine.project.layout import ProjectLayout


def get_plugin_for_context(plugin_loader: DatabaoContextPluginLoader, context: DatasourceContext) -> BuildPlugin:
    datasource_type = read_datasource_type_from_context(context)

    return get_plugin_for_datasource_type(plugin_loader=plugin_loader, datasource_type=datasource_type)


def get_plugin_for_datasource_type(
    plugin_loader: DatabaoContextPluginLoader, datasource_type: DatasourceType
) -> BuildPlugin:
    plugin = plugin_loader.get_plugin_for_datasource_type(datasource_type)
    if plugin is None:
        raise NoPluginFoundForDatasource(
            f"No plugin found for datasource type {datasource_type.full_type}", datasource_type=datasource_type
        )

    return plugin


def deserialize_built_context(
    *,
    context: DatasourceContext,
    context_type: type[Any],
) -> BuiltDatasourceContext:
    """Parse a datasource output YAML payload and type the embedded context."""
    raw_context = yaml.safe_load(context.context)
    built = TypeAdapter(BuiltDatasourceContext).validate_python(raw_context)

    if isinstance(context_type, type) and issubclass(context_type, BaseModel):
        typed_context: Any = context_type.model_validate(built.context)
    else:
        typed_context = TypeAdapter(context_type).validate_python(built.context)

    return replace(built, context=typed_context)


def _load_typed_built_context(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    datasource_id: DatasourceId,
) -> BuiltDatasourceContext:
    datasource_context = get_datasource_context(project_layout=project_layout, datasource_id=datasource_id)
    plugin = get_plugin_for_context(plugin_loader=plugin_loader, context=datasource_context)

    return deserialize_built_context(context=datasource_context, context_type=plugin.context_type)


def load_database_built_context(
    *,
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    datasource_id: DatasourceId,
) -> BuiltDatasourceContext:
    datasource_type = read_datasource_type_from_context_file(project_layout=project_layout, datasource_id=datasource_id)

    if datasource_type is not None and datasource_type not in plugin_loader.list_database_capable_datasource_types():
        raise ValueError(f"Datasource {datasource_id} is not database-capable")

    built = _load_typed_built_context(
        project_layout=project_layout,
        plugin_loader=plugin_loader,
        datasource_id=datasource_id,
    )
    if not isinstance(built.context, DatabaseIntrospectionResult):
        raise ValueError(f"Datasource {datasource_id} is not database-capable")
    return built
