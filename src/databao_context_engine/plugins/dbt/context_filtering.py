from collections import deque
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Collection

from pydantic import BaseModel, ConfigDict, Field, model_validator

from databao_context_engine.plugins.dbt.types_artifacts import (
    DbtManifestMetric,
    DbtManifestModel,
    DbtManifestNode,
    DbtManifestSemanticModel,
)


class DbtContextFilterStructuredRule(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    unique_id: str | None = None
    resource_type: str | None = None
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    name: str | None = None

    @model_validator(mode="after")
    def _validate_at_least_one_field(self):
        if (
            self.unique_id is None
            and self.resource_type is None
            and self.database is None
            and self.schema_name is None
            and self.name is None
        ):
            raise ValueError("At least one of unique_id, resource_type, database, schema, name must be provided")
        return self


DbtContextFilterRule = str | DbtContextFilterStructuredRule


class DbtContextFilter(BaseModel):
    include: list[DbtContextFilterRule] = []
    exclude: list[DbtContextFilterRule] = []


def filter_manifest_models(
    manifest_models: list[DbtManifestModel], resource_filter: DbtContextFilter | None
) -> list[DbtManifestModel]:
    if resource_filter is None:
        return manifest_models

    return [
        manifest_model for manifest_model in manifest_models if is_resource_in_scope(manifest_model, resource_filter)
    ]


def is_resource_in_scope(
    resource: DbtManifestNode | DbtManifestSemanticModel | DbtManifestMetric,
    resource_filter: DbtContextFilter | None,
) -> bool:
    if resource_filter is None:
        return True
    resource_fields = _extract_resource_fields(resource)

    include_rules = resource_filter.include
    if len(include_rules) > 0:
        included = any(
            _is_resource_matching_rule(
                rule=rule,
                unique_id=resource_fields.unique_id,
                resource_type=resource_fields.resource_type,
                database=resource_fields.database,
                schema=resource_fields.schema,
                name=resource_fields.name,
            )
            for rule in include_rules
        )
        if not included:
            return False

    exclude_rules = resource_filter.exclude
    if len(exclude_rules) > 0:
        excluded = any(
            _is_resource_matching_rule(
                rule=rule,
                unique_id=resource_fields.unique_id,
                resource_type=resource_fields.resource_type,
                database=resource_fields.database,
                schema=resource_fields.schema,
                name=resource_fields.name,
            )
            for rule in exclude_rules
        )
        if excluded:
            return False

    return True


@dataclass(frozen=True)
class _ResourceFields:
    unique_id: str
    resource_type: str | None = None
    database: str | None = None
    schema: str | None = None
    name: str | None = None


def _extract_resource_fields(
    resource: DbtManifestNode | DbtManifestSemanticModel | DbtManifestMetric,
) -> _ResourceFields:
    if isinstance(resource, DbtManifestModel):
        return _ResourceFields(
            unique_id=resource.unique_id,
            resource_type=resource.resource_type,
            database=resource.database,
            schema=resource.schema_,
            name=resource.name,
        )
    return _ResourceFields(unique_id=resource.unique_id, resource_type=resource.resource_type, name=resource.name)


def _is_resource_matching_rule(
    rule: DbtContextFilterRule,
    unique_id: str,
    resource_type: str | None = None,
    database: str | None = None,
    schema: str | None = None,
    name: str | None = None,
) -> bool:
    if isinstance(rule, str):
        return _match_wildcard_pattern(unique_id, rule)

    if rule.unique_id is not None and not _match_wildcard_pattern(unique_id, rule.unique_id):
        return False
    if rule.resource_type is not None and (
        resource_type is None or not _match_wildcard_pattern(resource_type, rule.resource_type)
    ):
        return False
    if rule.database is not None and (database is None or not _match_wildcard_pattern(database, rule.database)):
        return False
    if rule.schema_name is not None and (schema is None or not _match_wildcard_pattern(schema, rule.schema_name)):
        return False
    if rule.name is not None and (name is None or not _match_wildcard_pattern(name, rule.name)):
        return False

    return True


def _match_wildcard_pattern(value: str, pattern: str) -> bool:
    return fnmatchcase(value, pattern)


def filter_manifest_metrics(
    metrics: Collection[DbtManifestMetric],
    resource_filter,
    semantic_model_ids_filter: set[str] | None,
    child_map: dict[str, list[str]],
) -> Collection[DbtManifestMetric]:
    """Get the metrics to include if the semantic models were filtered.

    Metrics can depend on metrics and hence depend on semantic models only transitively.
    This method traverses the child_map dependency graph, starting at the semantic_model we are including, to find all metrics to include.

    Returns:
        A set of the metric ids to include.
    """
    if len(metrics) == 0:
        return metrics

    manifest_metrics_in_scope = [
        manifest_metric for manifest_metric in metrics if is_resource_in_scope(manifest_metric, resource_filter)
    ]

    if semantic_model_ids_filter is None or len(semantic_model_ids_filter) == 0:
        return manifest_metrics_in_scope

    metrics_ids = {manifest_metric.unique_id for manifest_metric in manifest_metrics_in_scope}

    selected_metric_ids: set[str] = set()
    visited_nodes: set[str] = set()
    nodes_to_visit = deque(semantic_model_ids_filter)

    # Traverse downstream graph from selected semantic models to discover reachable metrics.
    while len(nodes_to_visit) > 0:
        node_id = nodes_to_visit.popleft()
        if node_id in visited_nodes:
            continue
        visited_nodes.add(node_id)

        for child_node_id in child_map.get(node_id, []):
            if child_node_id in metrics_ids:
                selected_metric_ids.add(child_node_id)
            nodes_to_visit.append(child_node_id)

    return [
        manifest_metric
        for manifest_metric in manifest_metrics_in_scope
        if manifest_metric.unique_id in selected_metric_ids
    ]
