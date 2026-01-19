from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class ScopeIncludeRule(BaseModel):
    """
    Allowlist selector.
    - catalog: optional glob pattern
    - schemas: optional list of glob patterns (string also accepted and normalized to a list)

    A rule must specify at least one of: catalog, schemas.
    """

    model_config = ConfigDict(extra="forbid")

    catalog: str | None = None
    schemas: list[str] | None = None

    @field_validator("schemas", mode="before")
    @classmethod
    def _normalize_schemas(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        return v

    @model_validator(mode="after")
    def _validate_rule(self) -> ScopeIncludeRule:
        if self.catalog is None and self.schemas is None:
            raise ValueError("Include rule must specify at least 'catalog' or 'schemas'")
        return self


class ScopeExcludeRule(BaseModel):
    """
    Denylist selector.
    - catalog: optional glob pattern
    - schemas: optional list of glob patterns (string also accepted)
    - except_schemas: optional list of glob patterns (string also accepted)

    If a target matches the rule but also matches except_schemas, it is NOT excluded by this rule.
    """

    model_config = ConfigDict(extra="forbid")

    catalog: str | None = None
    schemas: list[str] | None = None
    except_schemas: list[str] | None = None

    @field_validator("schemas", "except_schemas", mode="before")
    @classmethod
    def _normalize_lists(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        return v

    @model_validator(mode="after")
    def _validate_rule(self) -> ScopeExcludeRule:
        if self.catalog is None and self.schemas is None:
            raise ValueError("Exclude rule must specify at least 'catalog' or 'schemas'")
        return self


class IntrospectionScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: list[ScopeIncludeRule] = []
    exclude: list[ScopeExcludeRule] = []
