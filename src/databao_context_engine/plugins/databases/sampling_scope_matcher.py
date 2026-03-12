import fnmatch

from databao_context_engine.plugins.databases.sampling_scope import (
    SamplingConfig,
    SamplingExcludeRule,
    SamplingIncludeRule,
    SamplingScope,
)


class SamplingScopeMatcher:
    """Decides whether to collect sample rows for a given table.

    Matching is glob-based (fnmatch) and case-insensitive.

    Semantics:
    - If sampling config is None => treat as enabled with an empty scope (sample everything),
      except ignored_schemas.
    - If sampling.enabled is False => sample nothing.
    - If include is empty => start from "everything"
    - If include is non-empty => start from "only what include matches"
    - Then apply exclude (exclude wins)
    - except_schemas / except_tables on an exclude rule prevents exclusion for that rule only
    """

    def __init__(
        self,
        sampling: SamplingConfig | None,
        *,
        ignored_schemas: set[str] | None = None,
    ) -> None:
        self._config = sampling or SamplingConfig()
        self._scope = self._config.scope or SamplingScope()
        self._ignored_schemas = {s.lower() for s in (ignored_schemas or set())}

    def should_sample(self, catalog: str, schema: str, table: str) -> bool:
        if not self._config.enabled:
            return False

        if schema.lower() in self._ignored_schemas:
            return False

        include_rules = self._scope.include
        exclude_rules = self._scope.exclude
        has_includes = bool(include_rules)

        if has_includes and not self._is_included(include_rules, catalog, schema, table):
            return False

        if self._is_excluded(exclude_rules, catalog, schema, table):
            return False

        return True

    @staticmethod
    def _glob_match(pattern: str, value: str) -> bool:
        """Case-insensitive glob match (fnmatch)."""
        return fnmatch.fnmatchcase(value.lower(), pattern.lower())

    def _matches_any(self, patterns: list[str] | None, value: str) -> bool:
        if patterns is None:
            return True
        return any(self._glob_match(p, value) for p in patterns)

    def _include_rule_matches(
        self,
        rule: SamplingIncludeRule,
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        if rule.catalog is not None and not self._glob_match(rule.catalog, catalog):
            return False
        if not self._matches_any(rule.schemas, schema):
            return False
        if not self._matches_any(rule.tables, table):
            return False
        return True

    def _exclude_rule_excludes(
        self,
        rule: SamplingExcludeRule,
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        if rule.catalog is not None and not self._glob_match(rule.catalog, catalog):
            return False
        if not self._matches_any(rule.schemas, schema):
            return False
        if not self._matches_any(rule.tables, table):
            return False

        if rule.except_schemas is not None and self._matches_any(rule.except_schemas, schema):
            return False
        if rule.except_tables is not None and self._matches_any(rule.except_tables, table):
            return False

        return True

    def _is_included(
        self,
        rules: list[SamplingIncludeRule],
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        return any(self._include_rule_matches(r, catalog, schema, table) for r in rules)

    def _is_excluded(
        self,
        rules: list[SamplingExcludeRule],
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        return any(self._exclude_rule_excludes(r, catalog, schema, table) for r in rules)
