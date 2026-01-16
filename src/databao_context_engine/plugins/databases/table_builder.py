from collections import defaultdict
from typing import Any, Iterable, cast

from databao_context_engine.plugins.databases.databases_types import (
    CheckConstraint,
    DatabaseColumn,
    DatabasePartitionInfo,
    DatabaseTable,
    DatasetKind,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
    KeyConstraint,
)


class TableBuilder:
    def __init__(self) -> None:
        self.by_table: dict[str, DatabaseTable] = {}

    @classmethod
    def build_from_components(
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

            meta = {k: v for k, v in r.items() if k not in ("table_name", "partition_tables")}
            part_tables = r.get("partition_tables") or []
            part_tables_list = [p for p in list(part_tables) if p is not None]

            t.partition_info = DatabasePartitionInfo(
                meta=meta,
                partition_tables=part_tables_list,
            )

    def finish(self) -> list[DatabaseTable]:
        return [self.by_table[k] for k in sorted(self.by_table)]


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
