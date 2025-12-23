from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from nemory.plugins.databases.databases_types import DatabaseIntrospectionResult


class IntrospectionAsserter:
    def __init__(self, result: DatabaseIntrospectionResult):
        self.result = result

        self.catalogs = {c.name: c for c in result.catalogs}
        self.schemas = {}
        self.tables = {}

        for c in result.catalogs:
            for s in c.schemas:
                self.schemas[(c.name, s.name)] = s
                for t in getattr(s, "tables", []) or []:
                    self.tables[(c.name, s.name, t.name)] = t

    def fail(self, msg: str, path: Sequence[str]) -> None:
        full = ".".join(path)
        raise AssertionError(f"{msg} at {full}" if full else msg)

    def table(self, catalog: str, schema: str, table: str):
        key = (catalog, schema, table)
        if key not in self.tables:
            available = sorted(t for (c, s, t) in self.tables.keys() if c == catalog and s == schema)
            self.fail(f"Missing table {table!r}. Available={available}", [catalog, schema, table])
        return self.tables[key]

    def column(self, catalog: str, schema: str, table: str, column: str):
        t = self.table(catalog, schema, table)
        cols = getattr(t, "columns", []) or []
        col_map = {c.name: c for c in cols}
        if column not in col_map:
            self.fail(
                f"Missing column {column!r}. Available={sorted(col_map.keys())}",
                [catalog, schema, table, column],
            )
        return col_map[column]


class Fact:
    def check(self, a: IntrospectionAsserter) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class TableExists(Fact):
    catalog: str
    schema: str
    table: str

    def check(self, a: IntrospectionAsserter) -> None:
        a.table(self.catalog, self.schema, self.table)


@dataclass(frozen=True)
class TableKindIs(Fact):
    catalog: str
    schema: str
    table: str
    kind: str

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        actual = getattr(t, "kind", None)
        if actual is not None and hasattr(actual, "value"):
            actual = actual.value
        if str(actual) != self.kind:
            a.fail(f"Expected kind={self.kind!r}, got {actual!r}", [self.catalog, self.schema, self.table])


@dataclass(frozen=True)
class TableDescriptionContains(Fact):
    catalog: str
    schema: str
    table: str
    contains: str

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        actual = getattr(t, "description", None) or ""
        if self.contains not in actual:
            a.fail(
                f"Expected description to contain {self.contains!r}, got {actual!r}",
                [self.catalog, self.schema, self.table],
            )


@dataclass(frozen=True)
class ColumnIs(Fact):
    catalog: str
    schema: str
    table: str
    column: str

    type: str | None = None
    nullable: bool | None = None
    default_equals: str | None = None
    default_contains: str | None = None
    generated: str | None = None
    description_contains: str | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        c = a.column(self.catalog, self.schema, self.table, self.column)
        path = [self.catalog, self.schema, self.table, self.column]

        if self.type is not None and getattr(c, "type", None) != self.type:
            a.fail(f"Expected type={self.type!r}, got {getattr(c, 'type', None)!r}", path)

        if self.nullable is not None and getattr(c, "nullable", None) != self.nullable:
            a.fail(f"Expected nullable={self.nullable!r}, got {getattr(c, 'nullable', None)!r}", path)

        if self.generated is not None and getattr(c, "generated", None) != self.generated:
            a.fail(f"Expected generated={self.generated!r}, got {getattr(c, 'generated', None)!r}", path)

        if self.default_equals is not None and getattr(c, "default_expression", None) != self.default_equals:
            a.fail(
                f"Expected default_expression == {self.default_equals!r}, got {getattr(c, 'default_expression', None)!r}",
                path,
            )

        if self.default_contains is not None:
            actual = getattr(c, "default_expression", None) or ""
            if self.default_contains not in actual:
                a.fail(
                    f"Expected default_expression to contain {self.default_contains!r}, got {actual!r}",
                    path,
                )

        if self.description_contains is not None:
            actual = getattr(c, "description", None) or ""
            if self.description_contains not in actual:
                a.fail(
                    f"Expected description to contain {self.description_contains!r}, got {actual!r}",
                    path,
                )


@dataclass(frozen=True)
class PrimaryKeyIs(Fact):
    catalog: str
    schema: str
    table: str
    columns: Sequence[str]
    name: str | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        pk = getattr(t, "primary_key", None)
        path = [self.catalog, self.schema, self.table, "primary_key"]

        if pk is None:
            a.fail("Expected primary key, but none found", path)

        if list(getattr(pk, "columns", []) or []) != list(self.columns):
            a.fail(f"Expected PK columns={list(self.columns)!r}, got {getattr(pk, 'columns', None)!r}", path)

        if self.name is not None and getattr(pk, "name", None) != self.name:
            a.fail(f"Expected PK name={self.name!r}, got {getattr(pk, 'name', None)!r}", path)


@dataclass(frozen=True)
class UniqueConstraintExists(Fact):
    catalog: str
    schema: str
    table: str
    columns: Sequence[str]
    name: str | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        uqs = getattr(t, "unique_constraints", []) or []
        path = [self.catalog, self.schema, self.table, "unique_constraints"]

        for uq in uqs:
            if list(getattr(uq, "columns", []) or []) != list(self.columns):
                continue
            if self.name is not None and getattr(uq, "name", None) != self.name:
                continue
            return

        found = [(getattr(uq, "name", None), getattr(uq, "columns", None)) for uq in uqs]
        a.fail(f"Expected unique constraint on {list(self.columns)!r} not found. Found={found}", path)


@dataclass(frozen=True)
class ForeignKeyExists(Fact):
    catalog: str
    schema: str
    table: str

    from_columns: Sequence[str]
    ref_table: str
    ref_columns: Sequence[str]

    name: str | None = None
    on_update: str | None = None
    on_delete: str | None = None
    enforced: bool | None = None
    validated: bool | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        fks = getattr(t, "foreign_keys", []) or []
        path = [self.catalog, self.schema, self.table, "foreign_keys"]

        for fk in fks:
            if self.name is not None and getattr(fk, "name", None) != self.name:
                continue
            if getattr(fk, "referenced_table", None) != self.ref_table:
                continue

            mapping = getattr(fk, "mapping", []) or []
            act_from = [m.from_column for m in mapping]
            act_to = [m.to_column for m in mapping]
            if act_from != list(self.from_columns) or act_to != list(self.ref_columns):
                continue

            if self.on_update is not None and getattr(fk, "on_update", None) != self.on_update:
                continue
            if self.on_delete is not None and getattr(fk, "on_delete", None) != self.on_delete:
                continue
            if self.enforced is not None and getattr(fk, "enforced", None) != self.enforced:
                continue
            if self.validated is not None and getattr(fk, "validated", None) != self.validated:
                continue

            return

        found = []
        for fk in fks:
            mapping = getattr(fk, "mapping", []) or []
            found.append(
                {
                    "name": getattr(fk, "name", None),
                    "from": [m.from_column for m in mapping],
                    "ref_table": getattr(fk, "referenced_table", None),
                    "to": [m.to_column for m in mapping],
                    "on_update": getattr(fk, "on_update", None),
                    "on_delete": getattr(fk, "on_delete", None),
                    "enforced": getattr(fk, "enforced", None),
                    "validated": getattr(fk, "validated", None),
                }
            )

        a.fail(
            f"Expected FK {list(self.from_columns)!r} -> {self.ref_table}({list(self.ref_columns)!r}) not found. Found={found}",
            path,
        )


@dataclass(frozen=True)
class CheckConstraintExists(Fact):
    catalog: str
    schema: str
    table: str
    name: str | None = None
    expression_contains: str | None = None
    validated: bool | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        checks = list(getattr(t, "checks", []) or [])

        for col in getattr(t, "columns", []) or []:
            checks.extend(getattr(col, "checks", []) or [])

        path = [self.catalog, self.schema, self.table, "checks"]

        for chk in checks:
            if self.name is not None and getattr(chk, "name", None) != self.name:
                continue
            if self.validated is not None and getattr(chk, "validated", None) != self.validated:
                continue
            if self.expression_contains is not None:
                actual = getattr(chk, "expression", None) or ""
                if self.expression_contains not in actual:
                    continue
            return

        found = [
            (getattr(c, "name", None), getattr(c, "expression", None), getattr(c, "validated", None)) for c in checks
        ]
        a.fail(f"Expected check constraint not found. Found={found}", path)


@dataclass(frozen=True)
class IndexExists(Fact):
    catalog: str
    schema: str
    table: str
    columns: Sequence[str] | None = None
    name: str | None = None
    unique: bool | None = None
    method: str | None = None
    predicate_contains: str | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        idxs = getattr(t, "indexes", []) or []
        path = [self.catalog, self.schema, self.table, "indexes"]

        for idx in idxs:
            if self.name is not None and getattr(idx, "name", None) != self.name:
                continue
            if self.unique is not None and getattr(idx, "unique", None) != self.unique:
                continue
            if self.method is not None and getattr(idx, "method", None) != self.method:
                continue
            if self.columns is not None and list(getattr(idx, "columns", []) or []) != list(self.columns):
                continue
            if self.predicate_contains is not None:
                actual = getattr(idx, "predicate", None) or ""
                if self.predicate_contains not in actual:
                    continue
            return

        found = [
            {
                "name": getattr(i, "name", None),
                "columns": getattr(i, "columns", None),
                "unique": getattr(i, "unique", None),
                "method": getattr(i, "method", None),
                "predicate": getattr(i, "predicate", None),
            }
            for i in idxs
        ]
        a.fail(f"Expected index not found. Found={found}", path)


@dataclass(frozen=True)
class PartitionMetaContains(Fact):
    catalog: str
    schema: str
    table: str
    expected_meta: Mapping[str, Any]

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        p = getattr(t, "partition_info", None)
        path = [self.catalog, self.schema, self.table, "partition_info"]

        if p is None:
            a.fail("Expected partition_info, but none found", path)

        meta = getattr(p, "meta", None) or {}
        for k, v in self.expected_meta.items():
            if k not in meta:
                a.fail(f"Expected partition meta key {k!r} missing. Meta keys={sorted(meta.keys())}", path)
            if meta[k] != v:
                a.fail(f"Expected partition meta {k!r}={v!r}, got {meta[k]!r}", path)


def assert_contract(result: DatabaseIntrospectionResult, facts: Iterable[Fact]) -> None:
    a = IntrospectionAsserter(result)
    for fact in facts:
        try:
            fact.check(a)
        except AssertionError as e:
            raise AssertionError(f"{e}\nFact: {fact!r}") from e
