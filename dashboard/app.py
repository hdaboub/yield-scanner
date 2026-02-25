#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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


def _fetch_json(url: str, timeout: int = 15) -> dict | list | None:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "yield-scanner-dashboard/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def _eth_price_series(artifacts: Path, days: int = 7) -> pd.DataFrame:
    cache_dir = artifacts / "_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"eth_usd_{days}d.json"
    now = int(time.time())
    if cache_file.exists():
        age = now - int(cache_file.stat().st_mtime)
        if age <= 600:
            try:
                raw = json.loads(cache_file.read_text(encoding="utf-8"))
                return _coingecko_prices_to_df(raw)
            except Exception:
                pass
    url = (
        "https://api.coingecko.com/api/v3/coins/ethereum/market_chart?"
        + urllib.parse.urlencode({"vs_currency": "usd", "days": str(days), "interval": "hourly"})
    )
    raw = _fetch_json(url)
    if isinstance(raw, dict):
        cache_file.write_text(json.dumps(raw), encoding="utf-8")
        return _coingecko_prices_to_df(raw)
    return pd.DataFrame()


def _coingecko_prices_to_df(raw: dict) -> pd.DataFrame:
    prices = raw.get("prices", []) if isinstance(raw, dict) else []
    if not isinstance(prices, list) or not prices:
        return pd.DataFrame()
    rows = []
    for item in prices:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            ts_ms = int(item[0])
            px_usd = float(item[1])
        except Exception:
            continue
        rows.append({"dt": pd.to_datetime(ts_ms, unit="ms", utc=True), "eth_usd": px_usd})
    return pd.DataFrame(rows).sort_values("dt")


def _hourly_heatmap(df: pd.DataFrame, value_col: str, title: str) -> go.Figure:
    work = df.copy()
    work["weekday"] = work["dt"].dt.weekday
    work["hour"] = work["dt"].dt.hour
    pivot = work.pivot_table(index="weekday", columns="hour", values=value_col, aggfunc="median")
    pivot = pivot.reindex(index=list(range(7)), columns=list(range(24)))
    y_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[f"{h:02d}" for h in pivot.columns],
            y=[y_labels[i] for i in pivot.index],
            colorscale="Turbo",
            colorbar={"title": value_col},
        )
    )
    fig.update_layout(title=title, xaxis_title="Hour (UTC)", yaxis_title="Weekday")
    return fig


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
    llama_rank = _read_csv(artifacts / "llama_weth_spike_rankings.csv")
    eth_px = _eth_price_series(artifacts, days=7)

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
        if not llama_rank.empty:
            hr = llama_rank.copy()
            hr["dt"] = pd.to_datetime(hr["hourStartUnix"], unit="s", utc=True)
            hr["pair_label"] = hr.get("token0Symbol", "").astype(str) + "/" + hr.get("token1Symbol", "").astype(str)
            top_pairs = (
                hr.groupby("pair", as_index=False)["usd_per_1000_liquidity_hourly"]
                .median()
                .sort_values("usd_per_1000_liquidity_hourly", ascending=False)
                .head(20)["pair"]
                .tolist()
            )
            hmap_df = hr[hr["pair"].isin(top_pairs)].copy()
            if not hmap_df.empty:
                hmap_df["hour"] = hmap_df["dt"].dt.strftime("%m-%d %H:00")
                heat = hmap_df.pivot_table(
                    index="pair_label",
                    columns="hour",
                    values="usd_per_1000_liquidity_hourly",
                    aggfunc="max",
                )
                fig_spike = go.Figure(
                    data=go.Heatmap(
                        z=heat.values,
                        x=list(heat.columns),
                        y=list(heat.index),
                        colorscale="Turbo",
                        colorbar={"title": "USD/$1k/hr"},
                    )
                )
                fig_spike.update_layout(title="Llama Spike Explorer Heatmap (Top Pairs)")
                st.plotly_chart(fig_spike, use_container_width=True)

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
    pool_df["usd_per_1000_liquidity_hourly"] = pool_df.get("fees_usd_per_1000_liquidity_hourly", 0.0)
    pool_df["volatility_proxy"] = pool_df.get("hourly_yield", 0.0).astype(float).rolling(24, min_periods=3).std().fillna(0.0)
    pool_df["net_proxy_usd_per_1000"] = pool_df["usd_per_1000_liquidity_hourly"] - (pool_df["volatility_proxy"] * 1000.0)

    fig_yield = px.line(
        pool_df,
        x="dt",
        y=["usd_per_1000_liquidity_hourly", "net_proxy_usd_per_1000"],
        title="Yield and Net Proxy (USD per $1k per hour)",
    )
    if not selected.empty and "pool_id" in selected.columns:
        plan_rows = selected[selected["pool_id"].astype(str) == str(selected_pool)]
        for _, r in plan_rows.iterrows():
            try:
                t_add = pd.to_datetime(r["next_add_cst"])
                t_remove = pd.to_datetime(r["next_remove_cst"])
            except Exception:
                continue
            fig_yield.add_vrect(x0=t_add, x1=t_remove, fillcolor="green", opacity=0.12, line_width=0)
    fig_swap = px.line(pool_df, x="dt", y=["volume_usd", "tvl_usd"], title="Swap Volume and TVL")
    fig_heat = _hourly_heatmap(
        pool_df,
        value_col="usd_per_1000_liquidity_hourly",
        title="Hour-of-Week Median Yield Heatmap",
    )
    st.plotly_chart(fig_yield, use_container_width=True)
    st.plotly_chart(fig_swap, use_container_width=True)
    st.plotly_chart(fig_heat, use_container_width=True)

    st.subheader("Market Context")
    if eth_px.empty:
        st.info("ETH price context unavailable (CoinGecko fetch/cache).")
    else:
        fig_eth = px.line(eth_px, x="dt", y="eth_usd", title="ETH/USD (CoinGecko, 7d)")
        st.plotly_chart(fig_eth, use_container_width=True)


if __name__ == "__main__":
    main()
