from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


def _normalize_str_or_list(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return [v]
    return v


class SamplingIncludeRule(BaseModel):
    """Allowlist selector for sampling.

    Attributes:
        catalog: optional glob pattern
        schemas: optional list of glob patterns (string also accepted and normalized to a list)
        tables: optional list of glob patterns (string also accepted and normalized to a list)

    A rule must specify at least one of: catalog, schemas, tables.
    """

    model_config = ConfigDict(extra="forbid")

    catalog: str | None = None
    schemas: list[str] | None = None
    tables: list[str] | None = None

    @field_validator("schemas", "tables", mode="before")
    @classmethod
    def _normalize_lists(cls, v: Any) -> Any:
        return _normalize_str_or_list(v)

    @model_validator(mode="after")
    def _validate_rule(self) -> SamplingIncludeRule:
        if self.catalog is None and self.schemas is None and self.tables is None:
            raise ValueError("Sampling include rule must specify at least one of: catalog, schemas, tables")
        return self


class SamplingExcludeRule(BaseModel):
    """Denylist selector for sampling.

    Attributes:
        catalog: optional glob pattern
        schemas: optional list of glob patterns (string also accepted)
        tables: optional list of glob patterns (string also accepted)
        except_schemas: optional list of glob patterns (string also accepted)
        except_tables: optional list of glob patterns (string also accepted)

    If a target matches the rule but also matches an except_* selector, it is NOT excluded by this rule.
    """

    model_config = ConfigDict(extra="forbid")

    catalog: str | None = None
    schemas: list[str] | None = None
    tables: list[str] | None = None

    except_schemas: list[str] | None = None
    except_tables: list[str] | None = None

    @field_validator("schemas", "tables", "except_schemas", "except_tables", mode="before")
    @classmethod
    def _normalize_lists(cls, v: Any) -> Any:
        return _normalize_str_or_list(v)

    @model_validator(mode="after")
    def _validate_rule(self) -> SamplingExcludeRule:
        if self.catalog is None and self.schemas is None and self.tables is None:
            raise ValueError("Sampling exclude rule must specify at least one of: catalog, schemas, tables")
        return self


class SamplingScope(BaseModel):
    """Include/exclude rule set for sampling."""

    model_config = ConfigDict(extra="forbid")

    include: list[SamplingIncludeRule] = []
    exclude: list[SamplingExcludeRule] = []


class SamplingConfig(BaseModel):
    """Sampling configuration.

    Attributes:
        enabled: master switch. If False, sampling is disabled entirely.
        scope: include/exclude rules controlling which tables get sampled.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    scope: SamplingScope | None = None
