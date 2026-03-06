from dataclasses import dataclass
from fnmatch import fnmatchcase

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
    """Filter dbt resources via include/exclude rules.

    Rules can be:
    - A string glob pattern matched against resource `unique_id`.
    - A structured rule object with any combination of:
      `unique_id`, `resource_type`, `database`, `schema`, `name`.
    - A mix of both in the same include/exclude list.

    Matching uses Unix shell-style wildcard syntax (`*`, `?`, `[seq]`, `[!seq]`).
    Evaluation order: first apply include rules (if any), then apply exclude rules (if any) on the included set.
    """

    include: list[DbtContextFilterRule] = []
    exclude: list[DbtContextFilterRule] = []


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
