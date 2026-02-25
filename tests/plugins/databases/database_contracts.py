from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult

logger = logging.getLogger(__name__)


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

    def samples(self, catalog: str, schema: str, table: str):
        t = self.table(catalog, schema, table)
        samples = getattr(t, "samples", None)
        if samples is None:
            self.fail("Missing samples attribute", [catalog, schema, table, "samples"])
        return samples


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


@dataclass(frozen=True)
class SamplesEqual(Fact):
    catalog: str
    schema: str
    table: str
    rows: Sequence[Mapping[str, Any]]

    def check(self, a: IntrospectionAsserter) -> None:
        actual = a.samples(self.catalog, self.schema, self.table)
        if list(actual) != list(self.rows):
            a.fail(
                f"Expected samples == {list(self.rows)!r}, got {list(actual)!r}",
                [self.catalog, self.schema, self.table, "samples"],
            )


@dataclass(frozen=True)
class SamplesCountIs(Fact):
    catalog: str
    schema: str
    table: str
    count: int

    def check(self, a: IntrospectionAsserter) -> None:
        actual = a.samples(self.catalog, self.schema, self.table)
        if len(actual) != self.count:
            a.fail(
                f"Expected samples count={self.count}, got {len(actual)}",
                [self.catalog, self.schema, self.table, "samples"],
            )


@dataclass(frozen=True)
class TableStatsRowCountIs(Fact):
    catalog: str
    schema: str
    table: str
    row_count: int
    approximate: bool = True

    def check(self, a: IntrospectionAsserter) -> None:
        t = a.table(self.catalog, self.schema, self.table)
        stats = getattr(t, "stats", None)
        path = [self.catalog, self.schema, self.table, "stats"]

        if stats is None:
            a.fail("Expected table stats, but none found", path)

        actual_row_count = getattr(stats, "row_count", None)
        if actual_row_count != self.row_count:
            a.fail(f"Expected row_count={self.row_count}, got {actual_row_count}", path)

        actual_approximate = getattr(stats, "approximate", True)
        if actual_approximate != self.approximate:
            a.fail(f"Expected approximate={self.approximate}, got {actual_approximate}", path)


@dataclass(frozen=True)
class ColumnStatsExists(Fact):
    catalog: str
    schema: str
    table: str
    column: str

    null_count: int | None = None
    non_null_count: int | None = None
    distinct_count: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    has_top_values: bool | None = None
    top_values: dict[Any, int] | None = None
    total_row_count: int | None = None

    def check(self, a: IntrospectionAsserter) -> None:
        c = a.column(self.catalog, self.schema, self.table, self.column)
        stats = getattr(c, "stats", None)
        path = [self.catalog, self.schema, self.table, self.column, "stats"]

        if stats is None:
            a.fail("Expected column stats, but none found", path)

        if self.null_count is not None:
            actual = getattr(stats, "null_count", None)
            if actual != self.null_count:
                a.fail(f"Expected null_count={self.null_count}, got {actual}", path)

        if self.non_null_count is not None:
            actual = getattr(stats, "non_null_count", None)
            if actual != self.non_null_count:
                a.fail(f"Expected non_null_count={self.non_null_count}, got {actual}", path)

        if self.distinct_count is not None:
            actual = getattr(stats, "distinct_count", None)
            if actual != self.distinct_count:
                a.fail(f"Expected distinct_count={self.distinct_count}, got {actual}", path)

        if self.min_value is not None:
            actual = getattr(stats, "min_value", None)
            if actual != self.min_value:
                a.fail(f"Expected min_value={self.min_value}, got {actual}", path)

        if self.max_value is not None:
            actual = getattr(stats, "max_value", None)
            if actual != self.max_value:
                a.fail(f"Expected max_value={self.max_value}, got {actual}", path)

        if self.has_top_values is not None:
            actual_top_values = getattr(stats, "top_values", None)
            has_values = actual_top_values is not None and len(actual_top_values) > 0
            if has_values != self.has_top_values:
                a.fail(f"Expected has_top_values={self.has_top_values}, got {has_values}", path)

        if self.top_values is not None:
            actual_top_values = getattr(stats, "top_values", None)
            if actual_top_values is None:
                a.fail(f"Expected top_values={self.top_values}, but top_values is None", path)
                return

            actual_dict = {value: count for value, count in actual_top_values}
            for expected_value, expected_count in self.top_values.items():
                actual_count = actual_dict.get(expected_value)
                if actual_count != expected_count:
                    a.fail(
                        f"Expected top_values[{expected_value!r}]={expected_count}, got {actual_count}. "
                        f"Full actual: {actual_dict}",
                        path,
                    )

        if self.total_row_count is not None:
            actual = getattr(stats, "total_row_count", None)
            if actual != self.total_row_count:
                a.fail(f"Expected total_row_count={self.total_row_count}, got {actual}", path)


def assert_contract(result: DatabaseIntrospectionResult, facts: Iterable[Fact]) -> None:
    a = IntrospectionAsserter(result)
    for fact in facts:
        try:
            fact.check(a)
        except AssertionError as e:
            raise AssertionError(f"{e}\nFact: {fact!r}") from e


def log_introspection_result(result: DatabaseIntrospectionResult) -> None:
    """Log the full introspection result tree. Use with ``--log-cli-level=INFO``."""
    for catalog in result.catalogs:
        logger.info("Catalog: %s", catalog.name)
        for schema in catalog.schemas:
            logger.info("  Schema: %s (%d tables)", schema.name, len(schema.tables))
            for table in schema.tables:
                pk_cols = table.primary_key.columns if table.primary_key else []
                logger.info(
                    "    Table: %s (kind=%s, columns=%d, samples=%d, pk=%s)%s",
                    table.name,
                    table.kind.value,
                    len(table.columns),
                    len(table.samples),
                    pk_cols or "none",
                    f" — {table.description}" if table.description else "",
                )
                for col in table.columns:
                    extras = []
                    if col.nullable:
                        extras.append("nullable")
                    if col.default_expression:
                        extras.append(f"default={col.default_expression}")
                    if col.generated:
                        extras.append(f"generated={col.generated}")
                    if col.description:
                        extras.append(f"desc={col.description!r}")
                    suffix = f"  [{', '.join(extras)}]" if extras else ""
                    logger.info("      %s %s%s", col.name, col.type, suffix)
                for fk in table.foreign_keys:
                    mapping = ", ".join(f"{m.from_column}->{m.to_column}" for m in fk.mapping)
                    logger.info("      FK %s: (%s) -> %s", fk.name, mapping, fk.referenced_table)
                for idx in table.indexes:
                    logger.info(
                        "      IDX %s: %s (unique=%s, method=%s)", idx.name, idx.columns, idx.unique, idx.method
                    )
                if table.samples:
                    logger.info("      Samples (first row): %s", table.samples[0])
