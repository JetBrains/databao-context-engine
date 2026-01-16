import pytest
from pydantic import ValidationError

from databao_context_engine.plugins.databases.introspection_scope import (
    IntrospectionScope,
    ScopeExcludeRule,
    ScopeIncludeRule,
)


def test_include_rule_requires_catalog_or_schemas():
    with pytest.raises(ValidationError):
        ScopeIncludeRule()


def test_exclude_rule_requires_catalog_or_schemas():
    with pytest.raises(ValidationError):
        ScopeExcludeRule()


def test_include_rule_normalizes_schemas_string_to_list():
    rule = ScopeIncludeRule(schemas="sales_*")
    assert rule.schemas == ["sales_*"]
    assert rule.catalog is None


def test_exclude_rule_normalizes_schemas_string_to_list():
    rule = ScopeExcludeRule(catalog="A", schemas="*")
    assert rule.schemas == ["*"]
    assert rule.catalog == "A"


def test_exclude_rule_normalizes_except_schemas_string_to_list():
    rule = ScopeExcludeRule(catalog="A", schemas="*", except_schemas="B")
    assert rule.except_schemas == ["B"]


def test_extra_fields_forbidden_on_include_rule():
    with pytest.raises(ValidationError):
        ScopeIncludeRule(catalog="A", schemas=["*"], unknown_key="nope")


def test_extra_fields_forbidden_on_exclude_rule():
    with pytest.raises(ValidationError):
        ScopeExcludeRule(catalog="A", schemas=["*"], unknown_key="nope")


def test_introspection_scope_defaults():
    scope = IntrospectionScope()
    assert scope.include == []
    assert scope.exclude == []


def test_introspection_scope_forbids_extra_fields():
    with pytest.raises(ValidationError):
        IntrospectionScope(include=[], exclude=[], extra_field=True)


def test_schemas_must_be_list_or_string():
    with pytest.raises(ValidationError):
        ScopeIncludeRule(schemas=123)  # type: ignore[arg-type]


def test_except_schemas_must_be_list_or_string():
    with pytest.raises(ValidationError):
        ScopeExcludeRule(catalog="A", schemas="*", except_schemas=123)  # type: ignore[arg-type]
