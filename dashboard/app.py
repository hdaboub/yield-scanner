#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DEFAULT_ARTIFACTS = Path("output_from_droplet/multichain_3w")


def _artifacts_dir() -> Path:
    raw = os.getenv("YIELD_SCANNER_ARTIFACTS", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_ARTIFACTS.resolve()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> None:
    st.set_page_config(page_title="Yield Scanner Dashboard", layout="wide")
    artifacts = _artifacts_dir()
    st.title("Yield Scanner Dashboard")
    st.caption(f"Artifacts: `{artifacts}`")

    state = _read_state(artifacts / "dashboard_state.json")
    spikes = pd.DataFrame(state.get("spikes", []))
    schedule = pd.DataFrame(state.get("schedule", {}).get("top_blocks", []))
    selected = pd.DataFrame(state.get("schedule", {}).get("selected_plan", []))
    frontier = pd.DataFrame(state.get("schedule", {}).get("curve", []))
    source_health = pd.DataFrame(state.get("source_health", []))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Spikes", len(spikes))
    c2.metric("Schedule Blocks", len(schedule))
    c3.metric("Selected Plan Rows", len(selected))
    c4.metric("Excluded Sources", int(source_health.get("excluded_from_schedule", pd.Series(dtype=bool)).sum()) if not source_health.empty else 0)

    st.subheader("Top Spikes")
    if spikes.empty:
        st.info("No spikes in dashboard_state.json")
    else:
        chain_filter = st.multiselect("Chains", sorted(spikes["chain"].dropna().unique().tolist()))
        df = spikes.copy()
        if chain_filter:
            df = df[df["chain"].isin(chain_filter)]
        st.dataframe(df.head(200), use_container_width=True)

    st.subheader("Selected Plan")
    if selected.empty:
        st.warning("Selected plan is empty for active scenario.")
    else:
        st.dataframe(selected, use_container_width=True)

    st.subheader("Moves/Day Frontier")
    if frontier.empty:
        st.info("No frontier rows.")
    else:
        st.dataframe(frontier, use_container_width=True)

    st.subheader("Source Health")
    if source_health.empty:
        st.info("No source health rows.")
    else:
        st.dataframe(source_health, use_container_width=True)

    st.subheader("Pool Drilldown")
    hourly = _read_csv(artifacts / "hourly_observations.csv")
    if hourly.empty:
        st.info("hourly_observations.csv not available.")
        return
    pool_ids = sorted(hourly["pool_id"].dropna().astype(str).unique().tolist())
    if not pool_ids:
        st.info("No pool IDs in hourly observations.")
        return
    selected_pool = st.selectbox("Pool ID", options=pool_ids, index=0)
    pool_df = hourly[hourly["pool_id"].astype(str) == str(selected_pool)].copy()
    if pool_df.empty:
        st.info("No rows for selected pool.")
        return
    pool_df["dt"] = pd.to_datetime(pool_df["ts"], unit="s", utc=True)
    pool_df = pool_df.sort_values("dt")
    fig_yield = px.line(pool_df, x="dt", y="hourly_yield", title="Hourly Yield")
    fig_swap = px.line(pool_df, x="dt", y="volume_usd", title="Swap Volume (USD)")
    st.plotly_chart(fig_yield, use_container_width=True)
    st.plotly_chart(fig_swap, use_container_width=True)


if __name__ == "__main__":
    main()
