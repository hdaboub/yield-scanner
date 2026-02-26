#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path
import zipfile

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


DEFAULT_ARTIFACTS = Path("output_from_droplet/multichain_3w")
DEFAULT_REMOTE_HOST = os.getenv("YIELD_SCANNER_REMOTE_HOST", "")
DEFAULT_REMOTE_APP_DIR = os.getenv("YIELD_SCANNER_REMOTE_APP_DIR", "/opt/uniswap-yield-scanner")
DEFAULT_REMOTE_SERVICE = os.getenv("YIELD_SCANNER_REMOTE_SERVICE", "uniswap-jobs.service")
DEFAULT_REMOTE_CONFIG = os.getenv(
    "YIELD_SCANNER_REMOTE_CONFIG",
    "config/sources.uniswap-official.multichain.json",
)
DEFAULT_REMOTE_OUTPUT_DIR = os.getenv("YIELD_SCANNER_REMOTE_OUTPUT_DIR", "output/multichain_3w")


def _artifacts_dir() -> Path:
    raw = os.getenv("YIELD_SCANNER_ARTIFACTS", "").strip()
    if raw:
        candidate = Path(raw).expanduser().resolve()
        if candidate.is_file() and candidate.suffix.lower() == ".zip":
            stamp = f"{candidate.stem}_{int(candidate.stat().st_mtime)}_{candidate.stat().st_size}"
            extract_root = Path(tempfile.gettempdir()) / "yield_scanner_artifacts" / stamp
            if not (extract_root / "dashboard_state.json").exists():
                if extract_root.exists():
                    shutil.rmtree(extract_root, ignore_errors=True)
                extract_root.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(candidate) as zf:
                    zf.extractall(extract_root)
            return extract_root
        return candidate
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


def _add_hourly_datetime(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    work = df.copy()
    timestamp_candidates = [
        "ts",
        "timestamp",
        "hourStartUnix",
        "hour_start_unix",
        "hour_start_ts",
        "time",
    ]
    ts_col = next((c for c in timestamp_candidates if c in work.columns), None)
    if ts_col is not None:
        series = pd.to_numeric(work[ts_col], errors="coerce")
        if series.notna().any():
            # Heuristic: values > 1e12 are likely milliseconds.
            if float(series.dropna().median()) > 1e12:
                work["dt"] = pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
            else:
                work["dt"] = pd.to_datetime(series, unit="s", utc=True, errors="coerce")
            if work["dt"].notna().any():
                return work, None
    string_candidates = [
        "hour_utc",
        "hourStartUTC",
        "hour_start_utc",
        "datetime_utc",
    ]
    str_col = next((c for c in string_candidates if c in work.columns), None)
    if str_col is not None:
        work["dt"] = pd.to_datetime(work[str_col], utc=True, errors="coerce")
        if work["dt"].notna().any():
            return work, None
    return work, "No usable timestamp column found (expected one of ts/timestamp/hourStartUnix/hour_utc)."


def _ssh_run(host: str, command: str, timeout: int = 30) -> tuple[int, str, str]:
    if not host.strip():
        return 2, "", "Remote host is not configured. Set YIELD_SCANNER_REMOTE_HOST or use the sidebar field."
    try:
        proc = subprocess.run(
            ["ssh", host, command],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except Exception as err:  # noqa: BLE001
        return 1, "", str(err)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _service_status(host: str, service: str) -> str:
    quoted = shlex.quote(service)
    cmd = (
        f"systemctl is-active {quoted}; "
        f"systemctl show -p ActiveEnterTimestamp -p ActiveExitTimestamp -p ExecMainStatus {quoted}"
    )
    rc, out, err = _ssh_run(host, cmd)
    if rc != 0:
        return f"status_error: {err or out}"
    return out


def _service_log_tail(host: str, service: str, lines: int) -> str:
    quoted = shlex.quote(service)
    cmd = f"journalctl -u {quoted} -n {max(20, int(lines))} --no-pager -o short-iso"
    rc, out, err = _ssh_run(host, cmd)
    if rc != 0:
        return f"log_error: {err or out}"
    return out or "(no logs)"


def _manual_log_tail(host: str, lines: int) -> str:
    cmd = f"tail -n {max(20, int(lines))} /tmp/yield_scanner_dashboard_run.log 2>/dev/null || true"
    rc, out, err = _ssh_run(host, cmd)
    if rc != 0:
        return f"log_error: {err or out}"
    return out or "(no manual run log)"


def _restart_service(host: str, service: str) -> tuple[bool, str]:
    quoted = shlex.quote(service)
    cmd = f"systemctl restart {quoted} && systemctl is-active {quoted}"
    rc, out, err = _ssh_run(host, cmd, timeout=60)
    if rc != 0:
        return False, err or out or "restart failed"
    return True, out


def _start_remote_run(
    host: str,
    app_dir: str,
    config_path: str,
    output_dir: str,
    hours: int,
    move_cost: float,
    deploy_usd: float,
    workers: int,
    mode: str,
    force_include_sources: list[str] | None = None,
    force_exclude_sources: list[str] | None = None,
) -> tuple[bool, str]:
    args = [
        "./.venv/bin/python",
        "scanner.py",
        "--config",
        config_path,
        "--hours",
        str(max(1, int(hours))),
        "--workers",
        str(max(1, int(workers))),
        "--default-move-cost-usd",
        f"{max(0.0, float(move_cost))}",
        "--default-deploy-usd",
        f"{max(100.0, float(deploy_usd))}",
        "--output-dir",
        output_dir,
        "--no-open-report",
    ]
    if mode == "strict":
        args.append("--llama-strict-mode")
        args.extend(["--schedule-min-observed-days", "30"])
    else:
        args.extend(["--schedule-min-observed-days", "14", "--schedule-min-hit-rate", "0.50"])
    for key in (force_include_sources or []):
        args.extend(["--schedule-force-include-source", str(key)])
    for key in (force_exclude_sources or []):
        args.extend(["--schedule-force-exclude-source", str(key)])

    remote_cmd = (
        "cd "
        + shlex.quote(app_dir)
        + " && nohup "
        + " ".join(shlex.quote(x) for x in args)
        + " >/tmp/yield_scanner_dashboard_run.log 2>&1 & echo $!"
    )
    rc, out, err = _ssh_run(host, remote_cmd, timeout=60)
    if rc != 0:
        return False, err or out or "failed to start remote run"
    return True, f"Started run PID: {out}"


def _schedule_hour_slots(schedule_df: pd.DataFrame, pool_id: str) -> set[int]:
    slots: set[int] = set()
    if schedule_df.empty or "pool_id" not in schedule_df.columns:
        return slots
    rows = schedule_df[schedule_df["pool_id"].astype(str) == str(pool_id)]
    for _, row in rows.iterrows():
        try:
            add_ts = int(float(row.get("next_add_ts", 0)))
            block_hours = max(1, int(float(row.get("block_hours", 1))))
            add_dt = pd.to_datetime(add_ts, unit="s", utc=True)
        except Exception:
            continue
        start_how = int(add_dt.weekday()) * 24 + int(add_dt.hour)
        for i in range(block_hours):
            slots.add((start_how + i) % 168)
    return slots


def _render_pool_line_chart(pool_df: pd.DataFrame, scheduled_slots: set[int], title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=pool_df["dt"],
            y=pool_df["usd_per_1000_liquidity_hourly"],
            mode="lines",
            name="USD/$1k/hr",
            line={"color": "#2563eb", "width": 2},
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=pool_df["dt"],
            y=pool_df["tvl_usd"],
            mode="lines",
            name="TVL USD",
            line={"color": "#0f766e", "width": 1.6},
            yaxis="y2",
            opacity=0.8,
        )
    )
    if scheduled_slots:
        mask = pool_df["hour_of_week"].isin(sorted(scheduled_slots))
        marks = pool_df[mask]
        if not marks.empty:
            fig.add_trace(
                go.Scatter(
                    x=marks["dt"],
                    y=marks["usd_per_1000_liquidity_hourly"],
                    mode="markers",
                    marker={"color": "#dc2626", "size": 6, "symbol": "diamond"},
                    name="Scheduled Window",
                    yaxis="y1",
                )
            )
    fig.update_layout(
        title=title,
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "USD/$1k/hr"},
        yaxis2={"title": "TVL USD", "overlaying": "y", "side": "right", "showgrid": False},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        margin={"l": 50, "r": 55, "t": 45, "b": 40},
    )
    return fig


def _source_key(source_name: str, version: str, chain: str) -> str:
    return f"{source_name}|{version}|{chain}"


def _collect_excluded_source_keys(source_health_df: pd.DataFrame) -> set[str]:
    out: set[str] = set()
    if source_health_df.empty:
        return out
    for _, row in source_health_df.iterrows():
        try:
            excluded = str(row.get("excluded_from_schedule", "")).lower() in {"1", "true", "yes"}
            if not excluded:
                continue
            out.add(_source_key(str(row.get("source_name", "")), str(row.get("version", "")), str(row.get("chain", ""))))
        except Exception:
            continue
    return out


def _filter_schedule_with_source_overrides(
    schedule_df: pd.DataFrame,
    base_excluded: set[str],
    force_include: set[str],
    force_exclude: set[str],
) -> tuple[pd.DataFrame, set[str]]:
    if schedule_df.empty:
        return schedule_df.copy(), set()
    effective_excluded = (set(base_excluded) - set(force_include)) | set(force_exclude)
    work = schedule_df.copy()
    work["source_key"] = (
        work.get("source_name", "").astype(str)
        + "|"
        + work.get("version", "").astype(str)
        + "|"
        + work.get("chain", "").astype(str)
    )
    filtered = work[~work["source_key"].isin(effective_excluded)].copy()
    return filtered, effective_excluded


def _preview_selected_plan(
    schedule_df: pd.DataFrame,
    objective: str,
    deploy_usd: float,
    move_cost_usd: float,
    max_moves_per_day: int,
    min_hold_hours: int = 1,
) -> pd.DataFrame:
    if schedule_df.empty:
        return pd.DataFrame()
    required_cols = {"next_add_ts", "next_remove_ts", "max_deployable_usd_est", "block_hours"}
    if not required_cols.issubset(set(schedule_df.columns)):
        return pd.DataFrame()
    work = schedule_df.copy()
    objective_col = (
        "risk_adjusted_incremental_usd_per_1000"
        if objective == "risk_adjusted" and "risk_adjusted_incremental_usd_per_1000" in work.columns
        else "incremental_usd_per_1000"
    )
    if objective_col not in work.columns:
        return pd.DataFrame()

    work["next_add_ts"] = pd.to_numeric(work["next_add_ts"], errors="coerce").fillna(0).astype(int)
    work["next_remove_ts"] = pd.to_numeric(work["next_remove_ts"], errors="coerce").fillna(0).astype(int)
    work["block_hours"] = pd.to_numeric(work["block_hours"], errors="coerce").fillna(0).astype(float)
    work["max_deployable_usd_est"] = pd.to_numeric(work["max_deployable_usd_est"], errors="coerce").fillna(0.0)
    work[objective_col] = pd.to_numeric(work[objective_col], errors="coerce").fillna(0.0)
    work["effective_deploy_usd"] = work["max_deployable_usd_est"].clip(upper=max(0.0, float(deploy_usd)))
    work["expected_net_usd"] = (work["effective_deploy_usd"] / 1000.0) * work[objective_col] - max(0.0, float(move_cost_usd))
    work["net_per_hour"] = work["expected_net_usd"] / work["block_hours"].clip(lower=1.0)
    work["add_dt"] = pd.to_datetime(work["next_add_ts"], unit="s", utc=True, errors="coerce")
    work["day_key"] = work["add_dt"].dt.strftime("%Y-%m-%d")
    candidates = work[
        (work["block_hours"] >= max(1, int(min_hold_hours)))
        & (work["effective_deploy_usd"] > 0.0)
        & (work["expected_net_usd"] > 0.0)
    ].copy()
    if candidates.empty:
        return pd.DataFrame()
    candidates = candidates.sort_values(["net_per_hour", "expected_net_usd"], ascending=False)

    selected_idx: list[int] = []
    day_counts: dict[str, int] = {}
    intervals: list[tuple[int, int]] = []
    max_moves = max(1, int(max_moves_per_day))
    for idx, row in candidates.iterrows():
        s = int(row["next_add_ts"])
        e = int(row["next_remove_ts"])
        if s <= 0 or e <= s:
            continue
        day = str(row.get("day_key", ""))
        if day_counts.get(day, 0) >= max_moves:
            continue
        overlap = False
        for os, oe in intervals:
            if not (e <= os or s >= oe):
                overlap = True
                break
        if overlap:
            continue
        selected_idx.append(idx)
        intervals.append((s, e))
        day_counts[day] = day_counts.get(day, 0) + 1
    if not selected_idx:
        return pd.DataFrame()
    selected = candidates.loc[selected_idx].copy().sort_values("next_add_ts")
    return selected


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
    excluded_sources = pd.DataFrame(state.get("excluded_sources", []))
    schedule_enhanced = _read_csv(artifacts / "schedule_enhanced.csv")
    llama_rank = _read_csv(artifacts / "llama_weth_spike_rankings.csv")
    eth_px = _eth_price_series(artifacts, days=7)
    source_options: list[str] = []
    if not source_health.empty:
        source_options = sorted(
            {
                _source_key(str(r.get("source_name", "")), str(r.get("version", "")), str(r.get("chain", "")))
                for _, r in source_health.iterrows()
                if str(r.get("source_name", "")).strip()
            }
        )
    defaults = state.get("defaults", {}) if isinstance(state, dict) else {}
    default_force_include = [str(x) for x in (defaults.get("schedule_force_include_sources") or [])]
    default_force_exclude = [str(x) for x in (defaults.get("schedule_force_exclude_sources") or [])]
    base_excluded_keys = _collect_excluded_source_keys(source_health)

    with st.sidebar:
        st.header("Run Controls")
        remote_host = st.text_input("Remote host", value=DEFAULT_REMOTE_HOST)
        remote_service = st.text_input("Systemd service", value=DEFAULT_REMOTE_SERVICE)
        remote_app_dir = st.text_input("Remote app dir", value=DEFAULT_REMOTE_APP_DIR)
        remote_config = st.text_input("Config", value=DEFAULT_REMOTE_CONFIG)
        remote_output_dir = st.text_input("Output dir", value=DEFAULT_REMOTE_OUTPUT_DIR)

        hours = st.number_input("hours", min_value=24, max_value=24 * 365, value=504, step=24)
        move_cost = st.number_input("move_cost_usd", min_value=0.0, value=50.0, step=5.0)
        deploy_usd = st.number_input("deploy_usd", min_value=100.0, value=10000.0, step=500.0)
        max_moves = st.number_input(
            "max_moves_per_day",
            min_value=1,
            max_value=24,
            value=int(defaults.get("max_moves_per_day", 4) or 4),
            step=1,
        )
        objective = st.selectbox(
            "objective",
            options=["risk_adjusted", "raw"],
            index=0 if str(defaults.get("objective", "risk_adjusted")) == "risk_adjusted" else 1,
        )
        workers = st.number_input("workers", min_value=1, max_value=64, value=8, step=1)
        mode = st.selectbox("strict/relaxed", options=["relaxed", "strict"], index=0)
        st.caption("Source quarantine overrides")
        force_include_sel = st.multiselect(
            "Force include sources",
            options=source_options,
            default=[x for x in default_force_include if x in source_options],
            help="Temporarily include sources even if source-health excludes them.",
        )
        force_exclude_sel = st.multiselect(
            "Force exclude sources",
            options=source_options,
            default=[x for x in default_force_exclude if x in source_options],
            help="Temporarily exclude sources for what-if planning.",
        )

        if st.button("Restart service run", use_container_width=True):
            ok, msg = _restart_service(remote_host, remote_service)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

        if st.button("Run ad-hoc scan (custom knobs)", use_container_width=True):
            ok, msg = _start_remote_run(
                host=remote_host,
                app_dir=remote_app_dir,
                config_path=remote_config,
                output_dir=remote_output_dir,
                hours=int(hours),
                move_cost=float(move_cost),
                deploy_usd=float(deploy_usd),
                workers=int(workers),
                mode=mode,
                force_include_sources=list(force_include_sel),
                force_exclude_sources=list(force_exclude_sel),
            )
            if ok:
                st.success(msg)
            else:
                st.error(msg)

        st.divider()
        st.subheader("Service Status")
        lines = st.slider("log tail lines", min_value=40, max_value=500, value=120, step=20)
        status_text = _service_status(remote_host, remote_service)
        st.code(status_text or "(no status)", language="text")
        st.caption("journalctl tail")
        st.code(_service_log_tail(remote_host, remote_service, int(lines)), language="text")
        st.caption("manual ad-hoc log tail")
        st.code(_manual_log_tail(remote_host, int(lines)), language="text")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Spikes", len(spikes))
    c2.metric("Schedule Blocks", len(schedule))
    c3.metric("Selected Plan Rows", len(selected))
    c4.metric(
        "Excluded Sources",
        int(source_health.get("excluded_from_schedule", pd.Series(dtype=bool)).sum()) if not source_health.empty else 0,
    )

    st.subheader("Excluded Sources + Reasons")
    if excluded_sources.empty:
        st.info("No sources currently excluded.")
    else:
        cols = [
            "source_name",
            "version",
            "chain",
            "bad_run_streak",
            "persistent_anomaly_excluded",
            "reason",
        ]
        shown = [c for c in cols if c in excluded_sources.columns]
        st.dataframe(excluded_sources[shown], use_container_width=True)

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

    st.subheader("Schedule Impact Preview (Source Overrides)")
    if schedule_enhanced.empty:
        st.info("schedule_enhanced.csv not available for preview.")
    else:
        force_include_set = set(force_include_sel)
        force_exclude_set = set(force_exclude_sel)
        filtered_schedule, effective_excluded = _filter_schedule_with_source_overrides(
            schedule_df=schedule_enhanced,
            base_excluded=base_excluded_keys,
            force_include=force_include_set,
            force_exclude=force_exclude_set,
        )
        baseline_preview = _preview_selected_plan(
            schedule_df=schedule_enhanced[~(
                (
                    schedule_enhanced.get("source_name", "").astype(str)
                    + "|"
                    + schedule_enhanced.get("version", "").astype(str)
                    + "|"
                    + schedule_enhanced.get("chain", "").astype(str)
                ).isin(base_excluded_keys)
            )].copy() if not schedule_enhanced.empty else schedule_enhanced.copy(),
            objective=str(objective),
            deploy_usd=float(deploy_usd),
            move_cost_usd=float(move_cost),
            max_moves_per_day=int(max_moves),
            min_hold_hours=1,
        )
        override_preview = _preview_selected_plan(
            schedule_df=filtered_schedule,
            objective=str(objective),
            deploy_usd=float(deploy_usd),
            move_cost_usd=float(move_cost),
            max_moves_per_day=int(max_moves),
            min_hold_hours=1,
        )
        p1, p2, p3 = st.columns(3)
        p1.metric("Base excluded sources", len(base_excluded_keys))
        p2.metric("Effective excluded sources", len(effective_excluded))
        p3.metric("Schedule rows after overrides", len(filtered_schedule))

        q1, q2 = st.columns(2)
        q1.metric(
            "Baseline preview selected",
            len(baseline_preview),
            delta=f"net ${baseline_preview['expected_net_usd'].sum():,.2f}" if not baseline_preview.empty else "net $0.00",
        )
        q2.metric(
            "Override preview selected",
            len(override_preview),
            delta=f"net ${override_preview['expected_net_usd'].sum():,.2f}" if not override_preview.empty else "net $0.00",
        )
        st.caption(
            "Preview uses current artifacts with objective/deploy/move_cost/max_moves and overlap rules. "
            "Run ad-hoc scan to materialize full outputs."
        )
        if override_preview.empty:
            st.warning("No selected rows in override preview.")
        else:
            show_cols = [
                c
                for c in [
                    "source_name",
                    "version",
                    "chain",
                    "pair",
                    "next_add_ts",
                    "next_remove_ts",
                    "effective_deploy_usd",
                    "expected_net_usd",
                    "max_deployable_usd_est",
                    "capacity_flag",
                ]
                if c in override_preview.columns
            ]
            st.dataframe(override_preview[show_cols].head(40), use_container_width=True)

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

    st.subheader("Operator Decision Charts (Top Pools)")
    hourly = _read_csv(artifacts / "hourly_observations.csv")
    if hourly.empty:
        st.info("hourly_observations.csv not available.")
    else:
        hourly, dt_err = _add_hourly_datetime(hourly)
        if dt_err is not None:
            st.warning(dt_err)
            return
        hourly["pool_id"] = hourly.get("pool_id", "").astype(str)
        hourly["usd_per_1000_liquidity_hourly"] = pd.to_numeric(
            hourly.get("fees_usd_per_1000_liquidity_hourly", 0.0), errors="coerce"
        ).fillna(0.0)
        hourly["tvl_usd"] = pd.to_numeric(hourly.get("tvl_usd", 0.0), errors="coerce").fillna(0.0)
        hourly["hour_of_week"] = hourly["dt"].dt.weekday * 24 + hourly["dt"].dt.hour

        top_n = st.slider("Top pools to chart", min_value=1, max_value=20, value=10, step=1)
        pool_ids: list[str] = []
        if not schedule_enhanced.empty and "pool_id" in schedule_enhanced.columns:
            sort_col = (
                "risk_adjusted_incremental_usd_per_1000"
                if "risk_adjusted_incremental_usd_per_1000" in schedule_enhanced.columns
                else "incremental_usd_per_1000"
            )
            ranked = schedule_enhanced.sort_values(sort_col, ascending=False)
            pool_ids = ranked["pool_id"].astype(str).dropna().drop_duplicates().head(int(top_n)).tolist()
        if not pool_ids:
            pool_ids = hourly["pool_id"].dropna().drop_duplicates().head(int(top_n)).tolist()

        for pool_id in pool_ids:
            p = hourly[hourly["pool_id"] == str(pool_id)].copy().sort_values("dt")
            if p.empty:
                continue
            schedule_slots = _schedule_hour_slots(schedule_enhanced, str(pool_id))
            title = f"Pool {pool_id}"
            with st.expander(title, expanded=False):
                c_left, c_right = st.columns([2, 1])
                line_fig = _render_pool_line_chart(
                    p,
                    schedule_slots,
                    title="USD/$1k/hr + TVL with scheduled-window overlay",
                )
                heat_fig = _hourly_heatmap(
                    p,
                    value_col="usd_per_1000_liquidity_hourly",
                    title="Hour-of-week median USD/$1k/hr",
                )
                with c_left:
                    st.plotly_chart(line_fig, use_container_width=True)
                with c_right:
                    st.plotly_chart(heat_fig, use_container_width=True)

    st.subheader("Pool Drilldown")
    if hourly.empty:
        st.info("hourly_observations.csv not available.")
        return
    pool_ids_all = sorted(hourly["pool_id"].dropna().astype(str).unique().tolist())
    if not pool_ids_all:
        st.info("No pool IDs in hourly observations.")
        return
    selected_pool = st.selectbox("Pool ID", options=pool_ids_all, index=0)
    pool_df = hourly[hourly["pool_id"].astype(str) == str(selected_pool)].copy()
    if pool_df.empty:
        st.info("No rows for selected pool.")
        return
    pool_df = pool_df.sort_values("dt")
    hourly_yield_series = pd.to_numeric(pool_df.get("hourly_yield", pd.Series(dtype=float)), errors="coerce")
    if hourly_yield_series.empty or hourly_yield_series.notna().sum() == 0:
        # Backward/forward compatibility: some exports only include hourly_yield_pct.
        hourly_yield_series = pd.to_numeric(pool_df.get("hourly_yield_pct", 0.0), errors="coerce") / 100.0
    pool_df["volatility_proxy"] = (
        hourly_yield_series
        .fillna(0.0)
        .rolling(24, min_periods=3)
        .std()
        .fillna(0.0)
    )
    pool_df["net_proxy_usd_per_1000"] = pool_df["usd_per_1000_liquidity_hourly"] - (pool_df["volatility_proxy"] * 1000.0)

    fig_yield = px.line(
        pool_df,
        x="dt",
        y=["usd_per_1000_liquidity_hourly", "net_proxy_usd_per_1000"],
        title="Yield and Net Proxy (USD per $1k per hour)",
    )
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
