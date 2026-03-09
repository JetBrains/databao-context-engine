from __future__ import annotations

import json
from collections.abc import Hashable
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

_TS_DISPLAY_LEN = 19


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _short_ts(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).replace("T", " ")
    return s[:_TS_DISPLAY_LEN] if len(s) >= _TS_DISPLAY_LEN else s


def _to_int(series: Any) -> pd.Series:
    if series is None:
        return pd.Series(dtype="Int64")
    return pd.to_numeric(series, errors="coerce").round(0).astype("Int64")


def read_perf_records(lines: Iterable[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:  # noqa: S112
            continue
        if isinstance(rec, dict):
            records.append(rec)
    return records


def load_perf_text(text: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    return records_to_frames(read_perf_records(text.splitlines()))


def load_perf_jsonl(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    return load_perf_text(path.read_text(encoding="utf-8"))


def records_to_frames(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    run_starts: list[dict[str, Any]] = []
    run_ends: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []

    for r in records:
        t = r.get("type")
        if t == "run_start":
            run_starts.append(r)
        elif t == "run_end":
            run_ends.append(r)
        elif t == "span":
            spans.append(r)

    runs_df = _build_runs_df(run_starts, run_ends)
    spans_df = _build_spans_df(spans, runs_df)
    return runs_df, spans_df


def _build_runs_df(run_starts: list[dict[str, Any]], run_ends: list[dict[str, Any]]) -> pd.DataFrame:
    starts = pd.DataFrame(run_starts) if run_starts else pd.DataFrame(columns=["run_id", "operation", "ts", "attrs"])
    ends = (
        pd.DataFrame(run_ends)
        if run_ends
        else pd.DataFrame(columns=["run_id", "ts", "status", "duration_ms", "error_type"])
    )

    if not starts.empty:
        starts = starts.rename(columns={"ts": "ts_start", "attrs": "run_attrs"})
    else:
        starts = pd.DataFrame(columns=["run_id", "operation", "ts_start", "run_attrs"])

    if not ends.empty:
        ends = ends.rename(columns={"ts": "ts_end", "status": "run_status"})
    else:
        ends = pd.DataFrame(columns=["run_id", "ts_end", "run_status", "duration_ms", "error_type"])

    runs = pd.merge(starts, ends, on="run_id", how="outer")

    if "operation" not in runs.columns:
        runs["operation"] = "unknown"
    runs["operation"] = runs["operation"].fillna("unknown")

    if "run_status" not in runs.columns:
        runs["run_status"] = "unknown"
    runs["run_status"] = runs["run_status"].fillna("unknown")

    if "ts_start" in runs.columns:
        ts_start_series = runs["ts_start"]
    else:
        ts_start_series = pd.Series([None] * len(runs), index=runs.index)

    if "ts_end" in runs.columns:
        ts_end_series = runs["ts_end"]
    else:
        ts_end_series = pd.Series([None] * len(runs), index=runs.index)

    runs["start_dt"] = pd.to_datetime(ts_start_series, errors="coerce", utc=True)
    runs["end_dt"] = pd.to_datetime(ts_end_series, errors="coerce", utc=True)

    runs["ts_start"] = ts_start_series.apply(_short_ts)
    runs["ts_end"] = ts_end_series.apply(_short_ts)

    if "run_attrs" in runs.columns:
        run_attrs_series = runs["run_attrs"]
    else:
        run_attrs_series = pd.Series([None] * len(runs), index=runs.index)

    run_attrs_norm = pd.json_normalize(run_attrs_series.apply(_as_dict).tolist()).add_prefix("run_attr.")
    runs = pd.concat([runs.drop(columns=["run_attrs"]), run_attrs_norm], axis=1)

    runs["duration_ms"] = _to_int(runs.get("duration_ms"))

    runs = runs.sort_values("start_dt", na_position="last").reset_index(drop=True)
    runs["run_index"] = np.arange(len(runs), dtype=int)
    return runs


def _build_spans_df(spans: list[dict[str, Any]], runs_df: pd.DataFrame) -> pd.DataFrame:
    df = (
        pd.DataFrame(spans)
        if spans
        else pd.DataFrame(
            columns=[
                "run_id",
                "span_id",
                "parent_span_id",
                "datasource_id",
                "name",
                "t_start_ms",
                "duration_ms",
                "status",
                "error_type",
                "attrs",
            ]
        )
    )

    if df.empty:
        for col in [
            "run_id",
            "span_id",
            "parent_span_id",
            "datasource_id",
            "name",
            "t_start_ms",
            "duration_ms",
            "status",
            "error_type",
            "attrs",
        ]:
            if col not in df.columns:
                df[col] = None
        return df

    if "status" in df.columns:
        df["status"] = df["status"].fillna("ok")
    else:
        df["status"] = "ok"

    df["t_start_ms"] = _to_int(df.get("t_start_ms"))
    df["duration_ms"] = _to_int(df.get("duration_ms"))
    df["t_end_ms"] = df["t_start_ms"] + df["duration_ms"]

    if "attrs" in df.columns:
        attrs_series = df["attrs"]
    else:
        attrs_series = pd.Series([None] * len(df), index=df.index)

    attrs_norm = pd.json_normalize(attrs_series.apply(_as_dict).tolist()).add_prefix("attr.")
    df = pd.concat([df.drop(columns=["attrs"]), attrs_norm], axis=1)

    if not runs_df.empty:
        run_meta = runs_df[["run_id", "operation", "start_dt", "run_index", "run_status", "duration_ms"]].rename(
            columns={"duration_ms": "run_duration_ms"}
        )
        df = df.merge(run_meta, on="run_id", how="left")

    return df


def datasource_metrics(spans_df: pd.DataFrame) -> pd.DataFrame:
    if spans_df.empty:
        return pd.DataFrame(columns=["run_id", "datasource_id", "chunk_count", "context_size_bytes", "datasource_type"])

    df = spans_df[spans_df["name"] == "datasource.total"].copy()

    chunk_col = "attr.chunk_count"
    ctx_col = "attr.context_size_bytes"
    type_col = "attr.datasource_type"

    out = (
        df.groupby(["run_id", "datasource_id"], dropna=False)
        .agg(
            chunk_count=(chunk_col, "max") if chunk_col in df.columns else ("name", "count"),
            context_size_bytes=(ctx_col, "max") if ctx_col in df.columns else ("name", "count"),
            datasource_type=(type_col, "first") if type_col in df.columns else ("name", "first"),
        )
        .reset_index()
    )

    if "chunk_count" in out.columns:
        out["chunk_count"] = _to_int(out["chunk_count"])
    if "context_size_bytes" in out.columns:
        out["context_size_bytes"] = _to_int(out["context_size_bytes"])

    return out


def step_totals(spans_df: pd.DataFrame) -> pd.DataFrame:
    if spans_df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "operation",
                "run_index",
                "start_dt",
                "datasource_id",
                "name",
                "step_ms",
                "span_count",
                "first_t_start_ms",
                "status",
            ]
        )

    df = spans_df.copy()
    if "status" in df.columns:
        df["status"] = df["status"].fillna("ok")
    else:
        df["status"] = "ok"

    extra_cols = [c for c in ["operation", "run_index", "start_dt"] if c in df.columns]
    group_cols = extra_cols + ["run_id", "datasource_id", "name"]

    out = (
        df.groupby(group_cols, dropna=False)
        .agg(
            step_ms=("duration_ms", "sum"),
            span_count=("span_id", "count"),
            first_t_start_ms=("t_start_ms", "min"),
            any_error=("status", lambda s: (s != "ok").any()),
        )
        .reset_index()
    )

    out["status"] = np.where(out["any_error"], "error", "ok")
    out = out.drop(columns=["any_error"])

    out["step_ms"] = _to_int(out["step_ms"])
    out["span_count"] = pd.to_numeric(out["span_count"], errors="coerce").astype("Int64")
    out["first_t_start_ms"] = _to_int(out["first_t_start_ms"])

    ds = datasource_metrics(spans_df)
    out = out.merge(ds, on=["run_id", "datasource_id"], how="left")

    if "chunk_count" in out.columns:
        denom = pd.to_numeric(out["chunk_count"], errors="coerce")
        numer = pd.to_numeric(out["step_ms"], errors="coerce")

        mask = denom.notna() & (denom > 0)
        out["ms_per_chunk"] = np.nan
        out.loc[mask, "ms_per_chunk"] = numer[mask] / denom[mask]

    return out


def summarize_step_stats(step_totals_df: pd.DataFrame) -> pd.DataFrame:
    if step_totals_df.empty:
        return pd.DataFrame(columns=["name", "count", "mean_ms", "median_ms", "p90_ms", "p99_ms", "max_ms"])

    g = step_totals_df.groupby("name")["step_ms"]

    out = pd.DataFrame(
        {
            "count": g.count(),
            "mean_ms": g.mean(),
            "median_ms": g.median(),
            "p90_ms": g.quantile(0.90),
            "p99_ms": g.quantile(0.99),
            "max_ms": g.max(),
        }
    ).reset_index()

    for c in ["mean_ms", "median_ms", "p90_ms", "p99_ms", "max_ms"]:
        out[c] = _to_int(out[c])

    out["count"] = pd.to_numeric(out["count"], errors="coerce").astype("Int64")
    return out.sort_values("p90_ms", ascending=False)


def filter_perf(
    runs_df: pd.DataFrame,
    spans_df: pd.DataFrame,
    *,
    selected_ops: list[str] | None = None,
    selected_statuses: list[str] | None = None,
    selected_ds: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ops = selected_ops or sorted(runs_df["operation"].dropna().unique().tolist())
    statuses = selected_statuses or sorted(runs_df["run_status"].dropna().unique().tolist())

    runs_f = runs_df.copy()
    runs_f = runs_f[runs_f["operation"].isin(ops)]
    runs_f = runs_f[runs_f["run_status"].isin(statuses)]
    runs_f = runs_f.sort_values("start_dt", na_position="last")

    selected_run_ids = set(runs_f["run_id"].dropna().tolist())
    spans_in_runs = spans_df[spans_df["run_id"].isin(selected_run_ids)].copy()

    spans_f = spans_in_runs
    if selected_ds:
        spans_f = spans_f[spans_f["datasource_id"].isin(selected_ds)]

    return runs_f, spans_in_runs, spans_f


def format_span_tree(spans_df_run: pd.DataFrame) -> list[str]:
    if spans_df_run.empty:
        return ["(no spans)"]

    rows = spans_df_run.to_dict(orient="records")
    by_id: dict[str, dict[Hashable, Any]] = {}
    children: dict[str, list[str]] = {}

    for r in rows:
        sid = r.get("span_id")
        if isinstance(sid, str) and sid:
            by_id = {}
            children.setdefault(sid, [])

    roots: list[str] = []
    for sid, r in by_id.items():
        pid = r.get("parent_span_id")
        if isinstance(pid, str) and pid in by_id:
            children[pid].append(sid)
        else:
            roots.append(sid)

    def start_key(sid: str) -> int:
        v = by_id[sid].get("t_start_ms")
        if v is None:
            return 0
        n = pd.to_numeric(v, errors="coerce")
        if pd.isna(n):
            return 0
        return int(n)

    roots.sort(key=start_key)
    for pid in list(children.keys()):
        children[pid].sort(key=start_key)

    def _fmt_ms(v: Any) -> str:
        try:
            return f"{int(v)}ms"
        except Exception:
            return "—"

    def line_for(sid: str) -> str:
        r = by_id[sid]
        name = r.get("name", "?")
        ds = r.get("datasource_id")
        status = r.get("status", "ok")
        ds_part = f" [{ds}]" if (name == "datasource.total" and isinstance(ds, str) and ds) else ""
        return f"{name}{ds_part} @ {_fmt_ms(r.get('t_start_ms'))}  {_fmt_ms(r.get('duration_ms'))}  ({status})"

    out_lines: list[str] = []

    def walk(sid: str, prefix: str, is_last: bool) -> None:
        connector = "└─ " if is_last else "├─ "
        out_lines.append(prefix + connector + line_for(sid))
        next_prefix = prefix + ("   " if is_last else "│  ")
        kids = children.get(sid, [])
        for i, cid in enumerate(kids):
            walk(cid, next_prefix, is_last=(i == len(kids) - 1))

    for i, rid in enumerate(roots):
        walk(rid, prefix="", is_last=(i == len(roots) - 1))

    return out_lines


def add_depth_labels(spans_df_run: pd.DataFrame) -> pd.DataFrame:
    if spans_df_run.empty:
        return spans_df_run.copy()

    df = spans_df_run.copy()

    parent_map: dict[str, Optional[str]] = {}
    for _, r in df.iterrows():
        sid = r.get("span_id")
        pid = r.get("parent_span_id")
        if isinstance(sid, str) and sid:
            parent_map[sid] = pid if isinstance(pid, str) and pid else None

    memo: dict[str, int] = {}

    def depth(sid: str) -> int:
        if sid in memo:
            return memo[sid]
        pid = parent_map.get(sid)
        if not pid or pid not in parent_map:
            memo[sid] = 0
            return 0
        memo[sid] = depth(pid) + 1
        return memo[sid]

    depths: list[int] = []
    labels: list[str] = []
    for _, r in df.iterrows():
        sid = r.get("span_id")
        d = depth(sid) if isinstance(sid, str) and sid else 0
        depths.append(d)
        labels.append(("  " * d) + str(r.get("name", "?")))

    df["depth"] = depths
    df["label"] = labels
    return df


def to_perfetto_trace(
    spans_df_run: pd.DataFrame,
    *,
    operation: Optional[str] = None,
    run_id: Optional[str] = None,
    lane_by: str = "datasource_id",
) -> dict:
    df = spans_df_run.copy()
    if df.empty:
        return {"traceEvents": [], "displayTimeUnit": "ms"}

    df = df[df["t_start_ms"].notna() & df["duration_ms"].notna()]
    if df.empty:
        return {"traceEvents": [], "displayTimeUnit": "ms"}

    pid = 1
    events: list[dict] = []

    rid_short = (run_id[:8] + "…") if isinstance(run_id, str) and run_id else ""
    proc_name = f"{operation or 'run'} {rid_short}".strip() if (operation or run_id) else "perf"
    events.append({"ph": "M", "pid": pid, "name": "process_name", "args": {"name": proc_name}})

    def lane_key(r: pd.Series) -> str:
        v = r.get(lane_by)
        return v if isinstance(v, str) and v else "pipeline"

    lanes = [lane_key(r) for _, r in df.iterrows()]
    lane_list = sorted(set(lanes))
    tid_map = {lane: i + 1 for i, lane in enumerate(lane_list)}

    for lane, tid in tid_map.items():
        events.append({"ph": "M", "pid": pid, "tid": tid, "name": "thread_name", "args": {"name": lane}})

    attr_cols = [c for c in df.columns if c.startswith("attr.")]
    for _, r in df.iterrows():
        ts_us = int(round(float(r["t_start_ms"]) * 1000.0))
        dur_us = int(round(float(r["duration_ms"]) * 1000.0))

        lane = lane_key(r)
        tid = tid_map[lane]

        args: dict[str, Any] = {
            "span_id": r.get("span_id"),
            "parent_span_id": r.get("parent_span_id"),
            "datasource_id": r.get("datasource_id"),
            "status": r.get("status"),
        }
        if pd.notna(r.get("error_type")):
            args["error_type"] = r.get("error_type")

        for c in attr_cols:
            v = r.get(c)
            if pd.isna(v):
                continue
            args[c[5:]] = v

        events.append(
            {
                "name": str(r.get("name", "?")),
                "ph": "X",
                "pid": pid,
                "tid": tid,
                "ts": ts_us,
                "dur": dur_us,
                "args": args,
            }
        )

    return {"traceEvents": events, "displayTimeUnit": "ms"}
