import fnmatch
from dataclasses import dataclass

from databao_context_engine.plugins.databases.introspection_scope import (
    IntrospectionScope,
    ScopeExcludeRule,
    ScopeIncludeRule,
)


@dataclass(frozen=True)
class ScopeSelection:
    """
    The final catalog+schema scope to introspect.
    """

    catalogs: list[str]
    schemas_per_catalog: dict[str, list[str]]


class IntrospectionScopeMatcher:
    """
    Applies include/exclude rules (glob matching, case-insensitive) to a discovered set of catalogs/schemas.

    Semantics:
    - If include is empty => start from "everything"
    - If include is non-empty => start from "only what include matches"
    - Then apply exclude (exclude wins)
    - except_schemas on an exclude rule prevents exclusion for that rule only
    """

    def __init__(
        self,
        scope: IntrospectionScope | None,
        *,
        ignored_schemas: set[str] | None = None,
    ) -> None:
        self._scope = scope or IntrospectionScope()
        self._ignored_schemas = {s.lower() for s in (ignored_schemas or set())}

    def filter_scopes(
        self,
        catalogs: list[str],
        schemas_per_catalog: dict[str, list[str]],
    ) -> ScopeSelection:
        include_rules = self._scope.include
        exclude_rules = self._scope.exclude
        has_includes = len(include_rules) > 0

        filtered: dict[str, list[str]] = {}

        for catalog in catalogs:
            kept_schemas: list[str] = []
            for schema in schemas_per_catalog.get(catalog, []):
                if schema.lower() in self._ignored_schemas:
                    continue

                if has_includes and not self._is_included(include_rules, catalog, schema):
                    continue

                if self._is_excluded(exclude_rules, catalog, schema):
                    continue

                kept_schemas.append(schema)

            if kept_schemas:
                filtered[catalog] = kept_schemas

        filtered_catalogs = [c for c in catalogs if c in filtered]
        return ScopeSelection(catalogs=filtered_catalogs, schemas_per_catalog=filtered)

    @staticmethod
    def _glob_match(pattern: str, value: str) -> bool:
        return fnmatch.fnmatchcase(value.lower(), pattern.lower())

    def _matches_any(self, patterns: list[str] | None, value: str) -> bool:
        if patterns is None:
            return True
        return any(self._glob_match(p, value) for p in patterns)

    def _include_rule_matches(self, rule: ScopeIncludeRule, catalog: str, schema: str) -> bool:
        if rule.catalog is not None and not self._glob_match(rule.catalog, catalog):
            return False
        if rule.schemas is not None and not self._matches_any(rule.schemas, schema):
            return False
        return True

    def _exclude_rule_excludes(self, rule: ScopeExcludeRule, catalog: str, schema: str) -> bool:
        if rule.catalog is not None and not self._glob_match(rule.catalog, catalog):
            return False
        if rule.schemas is not None and not self._matches_any(rule.schemas, schema):
            return False

        if rule.except_schemas is not None and self._matches_any(rule.except_schemas, schema):
            return False

        return True

    def _is_included(self, rules: list[ScopeIncludeRule], catalog: str, schema: str) -> bool:
        return any(self._include_rule_matches(r, catalog, schema) for r in rules)

    def _is_excluded(self, rules: list[ScopeExcludeRule], catalog: str, schema: str) -> bool:
        return any(self._exclude_rule_excludes(r, catalog, schema) for r in rules)
