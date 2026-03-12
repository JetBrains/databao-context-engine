import pytest
from pydantic import ValidationError

from databao_context_engine.plugins.databases.sampling_scope import (
    SamplingConfig,
    SamplingExcludeRule,
    SamplingIncludeRule,
    SamplingScope,
)


def test_include_rule_normalizes_strings_to_lists() -> None:
    rule = SamplingIncludeRule(schemas="public", tables="users")  # type: ignore[arg-type]
    assert rule.schemas == ["public"]
    assert rule.tables == ["users"]


def test_exclude_rule_normalizes_strings_to_lists() -> None:
    rule = SamplingExcludeRule(
        schemas="pii",  # type: ignore[arg-type]
        tables="customers",  # type: ignore[arg-type]
        except_schemas="public",  # type: ignore[arg-type]
        except_tables="customers_sanitized",  # type: ignore[arg-type]
    )
    assert rule.schemas == ["pii"]
    assert rule.tables == ["customers"]
    assert rule.except_schemas == ["public"]
    assert rule.except_tables == ["customers_sanitized"]


def test_include_rule_requires_at_least_one_selector() -> None:
    with pytest.raises(ValidationError):
        SamplingIncludeRule()


def test_exclude_rule_requires_at_least_one_selector() -> None:
    with pytest.raises(ValidationError):
        SamplingExcludeRule()


def test_scope_defaults_to_empty_lists() -> None:
    scope = SamplingScope()
    assert scope.include == []
    assert scope.exclude == []


def test_config_defaults_enabled_true_and_scope_none() -> None:
    cfg = SamplingConfig()
    assert cfg.enabled is True
    assert cfg.scope is None
