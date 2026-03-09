from __future__ import annotations

import json

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from databao_context_engine.perf.perf_analysis import (
    add_depth_labels,
    filter_perf,
    format_span_tree,
    load_perf_text,
    step_totals,
    summarize_step_stats,
    to_perfetto_trace,
)

st.set_page_config(page_title="Perf Dashboard", layout="wide")


def render_sidebar_upload() -> str | None:
    st.sidebar.header("Data source")
    uploaded = st.sidebar.file_uploader("Upload perf.jsonl", type=["jsonl"], key="perf_jsonl_uploader")
    if uploaded is None:
        return None
    return uploaded.getvalue().decode("utf-8", errors="replace")


def render_sidebar_filters(runs_df: pd.DataFrame, spans_df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    st.sidebar.header("Filters")

    ops_all = sorted(runs_df["operation"].dropna().unique().tolist())
    selected_ops = st.sidebar.multiselect(
        "Operation",
        options=ops_all,
        default=[],
        help="Leave empty to include all operations.",
        key="perf_ops",
    )
    if not selected_ops:
        selected_ops = ops_all

    statuses_all = sorted(runs_df["run_status"].dropna().unique().tolist())
    selected_statuses = st.sidebar.multiselect(
        "Run status",
        options=statuses_all,
        default=[],
        help="Leave empty to include all statuses.",
        key="perf_statuses",
    )
    if not selected_statuses:
        selected_statuses = statuses_all

    _, spans_in_runs, _ = filter_perf(
        runs_df,
        spans_df,
        selected_ops=selected_ops,
        selected_statuses=selected_statuses,
        selected_ds=None,
    )
    ds_options = sorted(spans_in_runs["datasource_id"].dropna().unique().tolist())

    selected_ds = st.sidebar.multiselect(
        "Datasource",
        options=ds_options,
        default=[],
        key="perf_ds",
    )

    return selected_ops, selected_statuses, selected_ds


@st.cache_data(show_spinner=False)
def _load_from_text(text: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    return load_perf_text(text)


def _run_label(row: pd.Series) -> str:
    op = row.get("operation", "?")
    ts = row.get("ts_start", "")
    dur = row.get("duration_ms", pd.NA)
    dur_s = "—" if pd.isna(dur) else f"{int(dur)}ms"
    return f"{op} | {ts} | {dur_s}"


def _bar_chart_avg_step_times_stacked_horizontal(pivot_display: pd.DataFrame) -> None:
    if pivot_display.empty:
        st.info("No data to chart.")
        return

    bar_df = (
        pivot_display.reset_index()
        .rename(columns={"name": "step"})
        .melt(id_vars=["step"], var_name="datasource_id", value_name="avg_step_ms")
        .dropna(subset=["avg_step_ms"])
    )
    if bar_df.empty:
        st.info("No data to chart.")
        return

    bar_df["avg_step_ms"] = pd.to_numeric(bar_df["avg_step_ms"], errors="coerce")
    bar_df = bar_df.dropna(subset=["avg_step_ms"])

    step_order = bar_df.groupby("step")["avg_step_ms"].sum().sort_values(ascending=False).index.tolist()

    height = min(900, 18 * len(step_order) + 120)

    chart = (
        alt.Chart(bar_df)
        .mark_bar()
        .encode(
            y=alt.Y("step:N", sort=step_order, title="", axis=alt.Axis(labelLimit=0)),
            x=alt.X(
                "avg_step_ms:Q",
                title="Avg step ms",
                stack="zero",
                axis=alt.Axis(format="d"),
            ),
            color=alt.Color("datasource_id:N", title="Datasource"),
            tooltip=[
                alt.Tooltip("step:N"),
                alt.Tooltip("datasource_id:N"),
                alt.Tooltip("avg_step_ms:Q", format=".0f"),
            ],
        )
        .properties(height=height)
    )

    st.altair_chart(chart, width="stretch")


def render_overview(runs_f: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Runs", len(runs_f))
    col2.metric("Errors", int((runs_f["run_status"] == "error").sum()))

    if "duration_ms" in runs_f.columns:
        duration_series = runs_f["duration_ms"]
    else:
        duration_series = pd.Series([None] * len(runs_f), index=runs_f.index)

    dur = pd.to_numeric(duration_series, errors="coerce")
    dur_non_na = dur.dropna()

    avg_ms = float(dur_non_na.mean()) if len(dur_non_na) else np.nan
    p90_ms = float(np.nanpercentile(dur_non_na.to_numpy(), 90)) if len(dur_non_na) else np.nan

    avg_s = avg_ms / 1000.0 if not np.isnan(avg_ms) else np.nan
    p90_s = p90_ms / 1000.0 if not np.isnan(p90_ms) else np.nan

    col3.metric("Avg duration (s)", f"{avg_s:.2f}" if not np.isnan(avg_s) else "—")
    col4.metric("P90 duration (s)", f"{p90_s:.2f}" if not np.isnan(p90_s) else "—")

    if runs_f.empty:
        st.info("No runs match the current filters.")
        return

    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown("### Run durations over time")
        chart_df_ms = runs_f.pivot_table(index="start_dt", columns="operation", values="duration_ms", aggfunc="mean")
        chart_df_s = (chart_df_ms / 1000.0).round(2)
        st.line_chart(chart_df_s)

    with right:
        st.markdown("### Recent runs")
        recent = (
            runs_f.sort_values("start_dt", ascending=False)
            .head(50)[["ts_start", "operation", "run_status", "duration_ms"]]
            .copy()
        )

        recent["duration_s"] = (pd.to_numeric(recent["duration_ms"], errors="coerce") / 1000.0).round(2)
        recent = recent.drop(columns=["duration_ms"])
        st.dataframe(recent, width="stretch", hide_index=True)


def render_step_stats(step_totals_df: pd.DataFrame) -> None:
    if step_totals_df.empty:
        st.info("No step data for selected filters.")
        return

    stats_df = summarize_step_stats(step_totals_df)
    st.dataframe(stats_df, width="stretch", hide_index=True)


def render_datasource_breakdown(step_totals_df: pd.DataFrame, step_totals_all_ds: pd.DataFrame) -> None:
    if step_totals_all_ds.empty:
        st.info("No datasource data for selected filters.")
        return

    ds_total = step_totals_all_ds[step_totals_all_ds["name"] == "datasource.total"].copy()
    ds_total = ds_total[ds_total["datasource_id"].notna()]
    if not ds_total.empty:
        top_ds = (
            ds_total.groupby("datasource_id")["step_ms"]
            .mean()
            .round(0)
            .astype("Int64")
            .rename("avg_datasource_total_ms")
            .reset_index()
        ).sort_values("avg_datasource_total_ms", ascending=False)

        st.markdown("### Top datasources by avg `datasource.total`")
        st.dataframe(top_ds.head(30), width="stretch", hide_index=True)

    st.markdown("### Average step time per datasource")

    ds_steps = step_totals_df.copy()
    ds_steps = ds_steps[ds_steps["datasource_id"].notna()]
    if ds_steps.empty:
        st.info("No datasource steps to display for the current filters.")
        return

    avg_long = (
        ds_steps.groupby(["name", "datasource_id"])["step_ms"]
        .mean()
        .reset_index()
        .rename(columns={"step_ms": "avg_step_ms"})
    )

    pivot = avg_long.pivot_table(index="name", columns="datasource_id", values="avg_step_ms", aggfunc="mean")
    pivot["_max"] = pivot.max(axis=1)
    pivot = pivot.sort_values("_max", ascending=False).drop(columns=["_max"])

    pivot_display = pivot.round(0).astype("Int64")
    st.dataframe(pivot_display, width="stretch", hide_index=True)
    _bar_chart_avg_step_times_stacked_horizontal(pivot_display)


def render_run_detail(
    runs_f: pd.DataFrame, runs_df: pd.DataFrame, spans_df: pd.DataFrame, selected_ds: list[str]
) -> None:
    if runs_f.empty:
        st.info("No runs match the current filters, so there is nothing to drill down into.")
        return

    runs_for_select = runs_f.copy().sort_values("start_dt", ascending=False)
    runs_for_select["_label"] = runs_for_select.apply(_run_label, axis=1)

    selected_label = st.selectbox("Select a run", options=runs_for_select["_label"].tolist())
    selected_run_id = runs_for_select[runs_for_select["_label"] == selected_label]["run_id"].iloc[0]
    operation = runs_df[runs_df["run_id"] == selected_run_id]["operation"].iloc[0]

    spans_run_all = spans_df[spans_df["run_id"] == selected_run_id].copy()
    if spans_run_all.empty:
        st.info("No spans for this run.")
        return

    spans_run = spans_run_all
    if selected_ds:
        spans_run = spans_run[spans_run["datasource_id"].isin(selected_ds)]

    if spans_run.empty:
        st.info("No spans match the current detail filters.")
        return

    st.markdown("### Span tree")
    st.code("\n".join(format_span_tree(spans_run)), language="text")

    st.markdown("### Spans table")
    cols = [
        c
        for c in ["name", "datasource_id", "t_start_ms", "duration_ms", "status", "error_type"]
        if c in spans_run.columns
    ]
    st.dataframe(spans_run.sort_values("t_start_ms")[cols], width="stretch", hide_index=True)

    st.markdown("### Timeline")
    tl = add_depth_labels(spans_run).copy()
    tl["t_end_ms"] = tl["t_start_ms"] + tl["duration_ms"]

    if tl["t_start_ms"].notna().any():
        offset = int(tl["t_start_ms"].min())
    else:
        st.info("No timing data for timeline.")
        return

    tl["x_start_ms"] = tl["t_start_ms"] - offset
    tl["x_end_ms"] = tl["t_end_ms"] - offset
    x_title = f"ms from first shown span (offset {offset}ms in run)"

    tl = tl.sort_values("t_start_ms", ascending=True)
    y_order = list(dict.fromkeys(tl["label"].tolist()))
    row_count = len(y_order)
    height = min(900, 18 * row_count + 120)

    tl = tl.rename(
        columns={
            "attr.chunk_count": "chunk_count",
            "attr.context_size_bytes": "context_size_bytes",
            "attr.datasource_type": "datasource_type",
        }
    )

    tooltips = [
        alt.Tooltip("name:N", title="step"),
        alt.Tooltip("datasource_id:N"),
        alt.Tooltip("t_start_ms:Q", format=".0f"),
        alt.Tooltip("duration_ms:Q", format=".0f"),
        alt.Tooltip("status:N"),
    ]
    if "error_type" in tl.columns:
        tooltips.append(alt.Tooltip("error_type:N"))
    if "chunk_count" in tl.columns:
        tooltips.append(alt.Tooltip("chunk_count:Q", format=".0f"))
    if "context_size_bytes" in tl.columns:
        tooltips.append(alt.Tooltip("context_size_bytes:Q", format=".0f"))
    if "datasource_type" in tl.columns:
        tooltips.append(alt.Tooltip("datasource_type:N"))

    chart = (
        alt.Chart(tl)
        .mark_bar()
        .encode(
            x=alt.X("x_start_ms:Q", title=x_title, scale=alt.Scale(zero=False), axis=alt.Axis(format="d")),
            x2="x_end_ms:Q",
            y=alt.Y(
                "label:N",
                sort=y_order,
                title="",
                axis=alt.Axis(labelLimit=0),
            ),
            color=alt.Color("status:N", legend=alt.Legend(title="status")),
            tooltip=tooltips,
        )
        .properties(height=height)
    )
    st.altair_chart(chart, width="stretch")

    st.markdown("### Export to Perfetto")

    if selected_ds:
        trace_filtered = to_perfetto_trace(
            spans_df_run=spans_run,
            operation=f"{operation} (filtered)",
            run_id=selected_run_id,
            lane_by="datasource_id",
        )
        st.download_button(
            "Download trace JSON (filtered datasources)",
            data=json.dumps(trace_filtered).encode("utf-8"),
            file_name=f"perf_trace_{selected_run_id[:8]}_filtered.json",
            mime="application/json",
        )

    trace_full = to_perfetto_trace(
        spans_df_run=spans_run_all,
        operation=str(operation),
        run_id=selected_run_id,
        lane_by="datasource_id",
    )
    st.download_button(
        "Download trace JSON (full run)",
        data=json.dumps(trace_full).encode("utf-8"),
        file_name=f"perf_trace_{selected_run_id[:8]}_full.json",
        mime="application/json",
    )

    st.caption("Open Perfetto UI and drag & drop the downloaded file: https://ui.perfetto.dev/")


def main() -> None:
    st.title("DCE Performance dashboard")

    uploaded_text = render_sidebar_upload()
    if uploaded_text is None:
        st.info("Upload a `perf.jsonl` file to start.")
        return

    runs_df, spans_df = _load_from_text(uploaded_text)
    if runs_df.empty:
        st.warning("No runs found in the uploaded perf.jsonl.")
        return

    selected_ops, selected_statuses, selected_ds = render_sidebar_filters(runs_df, spans_df)

    runs_f, spans_in_runs, spans_f = filter_perf(
        runs_df,
        spans_df,
        selected_ops=selected_ops,
        selected_statuses=selected_statuses,
        selected_ds=selected_ds,
    )

    step_totals_df = step_totals(spans_f)
    step_totals_all_ds = step_totals(spans_in_runs)

    tab_overview, tab_analysis, tab_detail = st.tabs(["Overview", "Analysis", "Detail"])

    with tab_overview:
        render_overview(runs_f)

    with tab_analysis:
        sub_steps, sub_datasources = st.tabs(["Steps", "Datasources"])
        with sub_steps:
            render_step_stats(step_totals_df)

        with sub_datasources:
            render_datasource_breakdown(step_totals_df, step_totals_all_ds)

    with tab_detail:
        render_run_detail(runs_f, runs_df, spans_df, selected_ds)


main()
