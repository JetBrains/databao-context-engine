from databao_context_engine.plugins.databases.introspection_scope import (
    IntrospectionScope,
    ScopeExcludeRule,
    ScopeIncludeRule,
)
from databao_context_engine.plugins.databases.introspection_scope_matcher import IntrospectionScopeMatcher


def test_no_scope_keeps_all_except_ignored():
    catalogs = ["A", "B"]
    schemas_per_catalog = {
        "A": ["public", "information_schema"],
        "B": ["app"],
    }

    matcher = IntrospectionScopeMatcher(scope=None, ignored_schemas={"information_schema"})
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A", "B"]
    assert selection.schemas_per_catalog == {
        "A": ["public"],
        "B": ["app"],
    }


def test_catalog_removed_if_all_schemas_filtered_out():
    catalogs = ["A"]
    schemas_per_catalog = {"A": ["information_schema"]}

    matcher = IntrospectionScopeMatcher(scope=None, ignored_schemas={"information_schema"})
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == []
    assert selection.schemas_per_catalog == {}


def test_include_restricts_to_matching_scopes():
    catalogs = ["A", "B"]
    schemas_per_catalog = {"A": ["s1", "s2"], "B": ["s3"]}

    scope = IntrospectionScope(include=[ScopeIncludeRule(catalog="A", schemas=["s1"])])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A"]
    assert selection.schemas_per_catalog == {"A": ["s1"]}


def test_include_catalog_only_includes_all_schemas_in_catalog():
    catalogs = ["A", "B"]
    schemas_per_catalog = {"A": ["s1", "s2"], "B": ["s3"]}

    scope = IntrospectionScope(include=[ScopeIncludeRule(catalog="A")])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A"]
    assert selection.schemas_per_catalog == {"A": ["s1", "s2"]}


def test_exclude_removes_matching_schemas():
    catalogs = ["A", "B"]
    schemas_per_catalog = {"A": ["s1"], "B": ["s2", "s3"]}

    scope = IntrospectionScope(exclude=[ScopeExcludeRule(schemas=["s2"])])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A", "B"]
    assert selection.schemas_per_catalog == {"A": ["s1"], "B": ["s3"]}


def test_exclude_wins_over_include():
    catalogs = ["A"]
    schemas_per_catalog = {"A": ["s1"]}

    scope = IntrospectionScope(
        include=[ScopeIncludeRule(schemas=["s1"])],
        exclude=[ScopeExcludeRule(schemas=["s1"])],
    )
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == []
    assert selection.schemas_per_catalog == {}


def test_except_schemas_allows_exception_inside_exclude_rule():
    catalogs = ["A", "C"]
    schemas_per_catalog = {"A": ["B", "D"], "C": ["X"]}

    scope = IntrospectionScope(exclude=[ScopeExcludeRule(catalog="A", schemas=["*"], except_schemas=["B"])])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A", "C"]
    assert selection.schemas_per_catalog == {"A": ["B"], "C": ["X"]}


def test_exclude_catalog_only_excludes_all_schemas_in_that_catalog():
    catalogs = ["A", "B"]
    schemas_per_catalog = {"A": ["s1", "s2"], "B": ["s3"]}

    scope = IntrospectionScope(exclude=[ScopeExcludeRule(catalog="A")])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["B"]
    assert selection.schemas_per_catalog == {"B": ["s3"]}


def test_exclude_catalog_only_with_except_schemas_keeps_exception():
    catalogs = ["A"]
    schemas_per_catalog = {"A": ["keep_me", "drop_me"]}

    scope = IntrospectionScope(exclude=[ScopeExcludeRule(catalog="A", except_schemas=["keep_me"])])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A"]
    assert selection.schemas_per_catalog == {"A": ["keep_me"]}


def test_glob_is_case_insensitive():
    catalogs = ["A"]
    schemas_per_catalog = {"A": ["sales_2024", "SALES_TMP", "other"]}

    scope = IntrospectionScope(include=[ScopeIncludeRule(schemas=["SaLeS_*"])])
    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.schemas_per_catalog == {"A": ["sales_2024", "SALES_TMP"]}


def test_include_and_exclude_interaction_excludes_subset_of_included_scopes():
    catalogs = ["A", "B"]
    schemas_per_catalog = {
        "A": ["s1", "s2"],
        "B": ["s3", "s4"],
    }

    scope = IntrospectionScope(
        include=[
            ScopeIncludeRule(catalog="A", schemas=["s*"]),
            ScopeIncludeRule(catalog="B", schemas=["s*"]),
        ],
        exclude=[
            ScopeExcludeRule(catalog="B", schemas=["s3"]),
        ],
    )

    matcher = IntrospectionScopeMatcher(scope, ignored_schemas=set())
    selection = matcher.filter_scopes(catalogs, schemas_per_catalog)

    assert selection.catalogs == ["A", "B"]
    assert selection.schemas_per_catalog == {
        "A": ["s1", "s2"],
        "B": ["s4"],
    }
