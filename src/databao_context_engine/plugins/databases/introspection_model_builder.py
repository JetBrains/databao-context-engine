from collections import defaultdict
from typing import Any, Iterable, cast

from databao_context_engine.plugins.databases.databases_types import (
    CheckConstraint,
    ColumnStats,
    DatabaseColumn,
    DatabasePartitionInfo,
    DatabaseSchema,
    DatabaseTable,
    DatasetKind,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
    KeyConstraint,
    TableStats,
)


class IntrospectionModelBuilder:
    def __init__(self) -> None:
        self.by_table: dict[str, DatabaseTable] = {}

    @classmethod
    def build_schemas_from_components(
        cls,
        *,
        schemas: list[str],
        rels: list[dict] | None = None,
        cols: list[dict] | None = None,
        pk_cols: list[dict] | None = None,
        uq_cols: list[dict] | None = None,
        checks: list[dict] | None = None,
        fk_cols: list[dict] | None = None,
        idx_cols: list[dict] | None = None,
        partitions: list[dict] | None = None,
        table_stats: list[dict] | None = None,
        column_stats: list[dict] | None = None,
        schema_field: str = "schema_name",
    ) -> list[DatabaseSchema]:
        def group_by_schema(rows: list[dict] | None) -> dict[str, list[dict]]:
            g: dict[str, list[dict]] = defaultdict(list)
            for r in rows or []:
                s = r.get(schema_field)
                if isinstance(s, str) and s:
                    g[s].append(r)
            return g

        grouped = {
            "rels": group_by_schema(rels),
            "cols": group_by_schema(cols),
            "pk": group_by_schema(pk_cols),
            "uq": group_by_schema(uq_cols),
            "checks": group_by_schema(checks),
            "fks": group_by_schema(fk_cols),
            "idx": group_by_schema(idx_cols),
            "parts": group_by_schema(partitions),
            "table_stats": group_by_schema(table_stats),
            "column_stats": group_by_schema(column_stats),
        }

        out: list[DatabaseSchema] = []
        for schema in schemas:
            tables = (
                cls.build_tables_from_components(
                    rels=grouped["rels"].get(schema, []),
                    cols=grouped["cols"].get(schema, []),
                    pk_cols=grouped["pk"].get(schema, []),
                    uq_cols=grouped["uq"].get(schema, []),
                    checks=grouped["checks"].get(schema, []),
                    fk_cols=grouped["fks"].get(schema, []),
                    idx_cols=grouped["idx"].get(schema, []),
                    partitions=grouped["parts"].get(schema, []),
                    table_stats=grouped["table_stats"].get(schema, []),
                    column_stats=grouped["column_stats"].get(schema, []),
                )
                or []
            )

            if tables:
                out.append(DatabaseSchema(name=schema, tables=tables))

        return out

    @classmethod
    def build_tables_from_components(
        cls,
        *,
        rels: list[dict] | None = None,
        cols: list[dict] | None = None,
        pk_cols: list[dict] | None = None,
        uq_cols: list[dict] | None = None,
        checks: list[dict] | None = None,
        fk_cols: list[dict] | None = None,
        idx_cols: list[dict] | None = None,
        partitions: list[dict] | None = None,
        table_stats: list[dict] | None = None,
        column_stats: list[dict] | None = None,
    ) -> list[DatabaseTable]:
        b = cls()
        b.apply_relations(rels)
        b.apply_columns(cols)
        b.apply_primary_keys(pk_cols)
        b.apply_unique_constraints(uq_cols)
        b.apply_checks(checks)
        b.apply_foreign_keys(fk_cols)
        b.apply_indexes(idx_cols)
        b.apply_partitions(partitions)
        b.apply_table_stats(table_stats)
        b.apply_column_stats(column_stats)
        return b.finish()

    def get_or_create_table(self, table_name: str) -> DatabaseTable:
        t = self.by_table.get(table_name)
        if t is None:
            t = self.by_table[table_name] = DatabaseTable(
                name=table_name,
                columns=[],
                samples=[],
                partition_info=None,
                description=None,
                kind=DatasetKind.TABLE,
            )
        return t

    def apply_relations(self, rels: list[dict] | None) -> None:
        for r in rels or []:
            t = self.get_or_create_table(r["table_name"])
            t.kind = DatasetKind.from_raw((r.get("kind") or "table").lower())
            if desc := r.get("description"):
                t.description = desc

    def apply_columns(self, cols: list[dict] | None) -> None:
        cols_by_table = group_rows(cols, ("table_name",))
        for (table_name,), grp in cols_by_table.items():
            grp.sort(key=lambda r: (r.get("ordinal_position") is None, r.get("ordinal_position") or 0))
            t = self.get_or_create_table(table_name)
            for c in grp:
                t.columns.append(
                    DatabaseColumn(
                        name=c["column_name"],
                        type=c["data_type"],
                        nullable=bool(coerce_bool(c.get("is_nullable"), default=True)),
                        description=c.get("description"),
                        default_expression=c.get("default_expression"),
                        generated=c.get("generated"),
                        checks=[],
                    )
                )

    def apply_primary_keys(self, pk_cols: list[dict] | None) -> None:
        pk_groups = group_rows(pk_cols, ("table_name", "constraint_name"))
        for (table_name, cname), grp in pk_groups.items():
            grp.sort(key=lambda r: sort_position_by_key(r, "position"))
            self.get_or_create_table(table_name).primary_key = KeyConstraint(
                name=cname,
                columns=[r["column_name"] for r in grp if r.get("column_name") is not None],
                validated=True,
            )

    def apply_unique_constraints(self, uq_cols: list[dict] | None) -> None:
        uq_groups = group_rows(uq_cols, ("table_name", "constraint_name"))
        by_table: dict[str, list[KeyConstraint]] = defaultdict(list)

        for (table_name, cname), grp in uq_groups.items():
            grp.sort(key=lambda r: sort_position_by_key(r, "position"))
            by_table[table_name].append(
                KeyConstraint(
                    name=cname,
                    columns=[r["column_name"] for r in grp if r.get("column_name") is not None],
                    validated=True,
                )
            )

        for table_name, uqs in by_table.items():
            self.get_or_create_table(table_name).unique_constraints = uqs

    def apply_checks(self, checks: list[dict] | None) -> None:
        for r in checks or []:
            self.get_or_create_table(r["table_name"]).checks.append(
                CheckConstraint(
                    name=r["constraint_name"],
                    expression=cast(str, r.get("expression")),
                    validated=coerce_bool(r.get("validated"), default=True),
                )
            )

    def apply_foreign_keys(self, fk_cols: list[dict] | None) -> None:
        fk_groups = group_rows(fk_cols, ("table_name", "constraint_name"))
        by_table: dict[str, list[ForeignKey]] = defaultdict(list)

        for (table_name, cname), grp in fk_groups.items():
            grp.sort(key=lambda r: sort_position_by_key(r, "position"))
            first = grp[0]

            ref_schema = first.get("ref_schema")
            ref_table = first.get("ref_table")
            referenced = f"{ref_schema}.{ref_table}" if ref_schema and ref_table else ""

            by_table[table_name].append(
                ForeignKey(
                    name=cname,
                    mapping=[ForeignKeyColumnMap(from_column=r["from_column"], to_column=r["to_column"]) for r in grp],
                    referenced_table=referenced,
                    on_update=first.get("on_update"),
                    on_delete=first.get("on_delete"),
                    enforced=coerce_bool(first.get("enforced"), default=True),
                    validated=coerce_bool(first.get("validated"), default=True),
                )
            )

        for table_name, fks in by_table.items():
            self.get_or_create_table(table_name).foreign_keys = fks

    def apply_indexes(self, idx_cols: list[dict] | None) -> None:
        idx_groups = group_rows(idx_cols, ("table_name", "index_name"))
        by_table: dict[str, list[Index]] = defaultdict(list)

        for (table_name, idx_name), grp in idx_groups.items():
            grp.sort(key=lambda r: sort_position_by_key(r, "position"))
            first = grp[0]

            by_table[table_name].append(
                Index(
                    name=idx_name,
                    columns=[cast(str, r.get("expr")) for r in grp if r.get("expr") is not None],
                    unique=bool(coerce_bool(first.get("is_unique"), default=False)),
                    method=first.get("method"),
                    predicate=first.get("predicate"),
                )
            )

        for table_name, idxs in by_table.items():
            self.get_or_create_table(table_name).indexes = idxs

    def apply_partitions(self, partitions: list[dict] | None) -> None:
        for r in partitions or []:
            t = self.get_or_create_table(r["table_name"])

            meta = {k: v for k, v in r.items() if k not in ("table_name", "partition_tables", "schema_name")}
            part_tables = r.get("partition_tables") or []
            part_tables_list = [p for p in list(part_tables) if p is not None]

            t.partition_info = DatabasePartitionInfo(
                meta=meta,
                partition_tables=part_tables_list,
            )

    def apply_table_stats(self, table_stats: list[dict] | None) -> None:
        for r in table_stats or []:
            t = self.get_or_create_table(r["table_name"])
            row_count = r.get("row_count")
            if row_count is not None:
                t.stats = TableStats(row_count=int(row_count), approximate=True)

    def apply_column_stats(self, column_stats: list[dict] | None) -> None:
        stats_by_table_col: dict[tuple[str, str], dict] = {}
        for r in column_stats or []:
            table_name = r.get("table_name")
            column_name = r.get("column_name")
            if table_name and column_name:
                stats_by_table_col[(table_name, column_name)] = r

        for table_name, table in self.by_table.items():
            for col in table.columns:
                stat_row = stats_by_table_col.get((table_name, col.name))
                if not stat_row:
                    continue

                null_count = None
                non_null_count = None
                if table.stats and table.stats.row_count is not None:
                    null_frac = stat_row.get("null_frac")
                    if null_frac is not None:
                        row_count = table.stats.row_count
                        null_count = round(row_count * null_frac)
                        non_null_count = row_count - null_count

                distinct_count = None
                n_distinct = stat_row.get("n_distinct")
                if n_distinct is not None:
                    if n_distinct < 0 and table.stats and table.stats.row_count:
                        distinct_count = round(abs(n_distinct) * table.stats.row_count)
                    elif n_distinct > 0:
                        distinct_count = round(n_distinct)

                top_values = None
                top_n = 5
                mcv = stat_row.get("most_common_vals")
                mcf = stat_row.get("most_common_freqs")
                if mcv is not None and mcf is not None:
                    vals = _parse_pg_array_simple(mcv)
                    freqs = _parse_pg_array_simple(mcf)
                    if vals and freqs and len(vals) == len(freqs) and table.stats and table.stats.row_count:
                        try:
                            row_count = table.stats.row_count
                            top_values = [(str(v), round(float(f) * row_count)) for v, f in zip(vals, freqs)][:top_n]
                        except (ValueError, TypeError):
                            pass

                min_value = None
                max_value = None
                histogram_bounds = stat_row.get("histogram_bounds")
                if histogram_bounds:
                    bounds = _parse_pg_array_simple(histogram_bounds)
                    if bounds:
                        min_value = bounds[0]
                        max_value = bounds[-1]

                col.stats = ColumnStats(
                    null_count=null_count,
                    non_null_count=non_null_count,
                    distinct_count=distinct_count,
                    min_value=min_value,
                    max_value=max_value,
                    top_values=top_values,
                )

    def finish(self) -> list[DatabaseTable]:
        return [self.by_table[k] for k in sorted(self.by_table)]


def _parse_pg_array_simple(arr_str: str) -> list[str]:
    """Parse PostgreSQL array format like '{value1,value2,value3}'.

    Note: Simple parser that doesn't handle quoted strings with commas or escapes.

    Returns:
        List of string values from the array, or empty list if parsing fails.
    """
    if not arr_str or not isinstance(arr_str, str):
        return []
    if not arr_str.startswith("{") or not arr_str.endswith("}"):
        return []
    content = arr_str[1:-1]
    return [v.strip() for v in content.split(",") if v.strip()]


def coerce_bool(value: Any, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"yes", "true", "1"}:
            return True
        if v in {"no", "false", "0"}:
            return False
    return bool(value)


def sort_position_by_key(r: dict, pos_field: str) -> tuple[bool, int]:
    pos = r.get(pos_field)
    try:
        pos_val = int(pos) if pos is not None else 0
    except (TypeError, ValueError):
        pos_val = 0
    return pos is None, pos_val


def group_rows(rows: Iterable[dict] | None, key_fields: tuple[str, ...]) -> dict[tuple[Any, ...], list[dict]]:
    grouped: dict[tuple[Any, ...], list[dict]] = defaultdict(list)
    for r in rows or []:
        grouped[tuple(r.get(f) for f in key_fields)].append(r)
    return grouped
