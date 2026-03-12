from databao_context_engine.plugins.databases.sampling_scope import (
    SamplingConfig,
    SamplingExcludeRule,
    SamplingIncludeRule,
    SamplingScope,
)
from databao_context_engine.plugins.databases.sampling_scope_matcher import SamplingScopeMatcher


def test_default_samples_everything() -> None:
    matcher = SamplingScopeMatcher(None)

    assert matcher.should_sample("catalog", "public", "users") is True
    assert matcher.should_sample("catalog", "analytics", "fact_orders") is True


def test_ignored_schemas_are_never_sampled() -> None:
    matcher = SamplingScopeMatcher(None, ignored_schemas={"information_schema"})

    assert matcher.should_sample("catalog", "public", "users") is True
    assert matcher.should_sample("catalog", "information_schema", "tables") is False


def test_enabled_false_disables_sampling() -> None:
    cfg = SamplingConfig(enabled=False)
    matcher = SamplingScopeMatcher(cfg)

    assert matcher.should_sample("catalog", "public", "users") is False


def test_include_rules_turn_on_allowlist_mode() -> None:
    cfg = SamplingConfig(
        scope=SamplingScope(
            include=[SamplingIncludeRule(schemas=["analytics"], tables=["fact_*"])],
        )
    )
    matcher = SamplingScopeMatcher(cfg)

    assert matcher.should_sample("catalog", "analytics", "fact_orders") is True
    assert matcher.should_sample("catalog", "analytics", "dim_users") is False
    assert matcher.should_sample("catalog", "public", "fact_orders") is False


def test_exclude_rules_override_includes() -> None:
    cfg = SamplingConfig(
        scope=SamplingScope(
            include=[SamplingIncludeRule(schemas=["analytics"])],
            exclude=[SamplingExcludeRule(tables=["secret_*"])],
        )
    )
    matcher = SamplingScopeMatcher(cfg)

    assert matcher.should_sample("catalog", "analytics", "orders") is True
    assert matcher.should_sample("catalog", "analytics", "secret_keys") is False


def test_exclude_rule_exceptions_punch_holes() -> None:
    cfg = SamplingConfig(
        scope=SamplingScope(
            exclude=[SamplingExcludeRule(schemas=["pii"], except_tables=["allow_*"])],
        )
    )
    matcher = SamplingScopeMatcher(cfg)

    assert matcher.should_sample("catalog", "pii", "customers") is False
    assert matcher.should_sample("catalog", "pii", "allow_customers") is True


def test_matching_is_case_insensitive() -> None:
    cfg = SamplingConfig(
        scope=SamplingScope(
            include=[SamplingIncludeRule(schemas=["Analytics"], tables=["Fact_*"])],
        )
    )
    matcher = SamplingScopeMatcher(cfg)

    assert matcher.should_sample("catalog", "analytics", "fact_orders") is True
