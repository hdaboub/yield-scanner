#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import math
import os
import re
import shlex
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


SECONDS_PER_HOUR = 3600
DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
ENV_TOKEN_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Common ERC-20 address aliases used when subgraphs omit token symbols.
TOKEN_SYMBOL_OVERRIDES: dict[str, str] = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2": "SUSHI",
    "0xdbdb4d16eda451d0503b854cf79d55697f90c8df": "ALCX",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": "MKR",
    "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b": "CVX",
    "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e": "YFI",
}


@dataclass(frozen=True)
class SourceConfig:
    name: str
    version: str
    chain: str
    endpoint: str
    hourly_query: str
    source_type: str = "hourly"
    weth_address: str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    page_size_override: int | None = None
    backfill_source: bool = False
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Observation:
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    ts: int
    volume_usd: float
    tvl_usd: float
    fees_usd: float | None
    hourly_yield: float | None


@dataclass(frozen=True)
class PoolRanking:
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    samples: int
    observed_hours: int
    observed_days: float
    total_fees_usd: float
    fee_period_start_ts: int
    fee_period_end_ts: int
    avg_hourly_yield_pct: float
    median_hourly_yield_pct: float
    trimmed_mean_hourly_yield_pct: float
    p90_hourly_yield_pct: float
    max_hourly_yield_pct: float
    outlier_hours: int
    avg_hourly_fees_usd: float
    avg_tvl_usd: float
    best_hour_utc: int
    best_day_utc: str
    best_window_utc: str
    best_window_start_ts: int
    best_window_end_ts: int
    score: float


@dataclass(frozen=True)
class PoolRankingDiagnostic:
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    samples_raw: int
    samples_after_tvl_floor: int
    samples_after_hard_cap: int
    ranking_tvl_floor_usd: float
    winsorize_percentile: float
    winsorize_cap_hourly_yield_pct: float
    capped_hours: int
    max_raw_hourly_yield_pct: float
    max_capped_hourly_yield_pct: float
    outlier_hours_raw: int
    rule_triggered: str


@dataclass(frozen=True)
class ScheduleRecommendation:
    pool_rank: int
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    reliability_hit_rate_pct: float
    reliable_occurrences: int
    threshold_hourly_yield_pct: float
    avg_block_hourly_yield_pct: float
    p90_block_hourly_yield_pct: float
    block_hours: int
    add_day_utc: str
    add_hour_utc: int
    remove_day_utc: str
    remove_hour_utc: int
    add_pattern_utc: str
    remove_pattern_utc: str
    next_add_ts: int
    next_remove_ts: int
    pool_score: float


@dataclass(frozen=True)
class SourceFailure:
    source_name: str
    version: str
    chain: str
    error: str


@dataclass(frozen=True)
class SourceCheckpoint:
    mode: str
    fetch_start_ts: int
    fetch_end_ts: int
    after_id: str
    skip: int
    cursor_end_ts: int | None
    pages_fetched: int
    rows_fetched: int


@dataclass(frozen=True)
class V2YieldSpikeRow:
    source_name: str
    chain: str
    pair_id: str
    token0: str
    token1: str
    ts: int
    ts_utc: str
    ts_chicago: str
    swap_count: int
    fee_weth: float
    reserve_weth: float
    score: float
    hourly_yield_pct: float
    usd_per_1000_liquidity_hourly: float
    rough_apr_pct: float


@dataclass(frozen=True)
class LlamaPairHourRow:
    source_name: str
    version: str
    chain: str
    endpoint: str
    hour_start_unix: int
    hour_start_utc: str
    hour_start_chicago: str
    pair: str
    token0: str
    token1: str
    token0_symbol: str
    token1_symbol: str
    token0_decimals: int | None
    token1_decimals: int | None
    swap_count: int
    fee0_raw: str
    fee1_raw: str
    reserve0_raw: str
    reserve1_raw: str
    fee0: float
    fee1: float
    reserve0: float
    reserve1: float
    weth_fee: float | None
    weth_reserve: float | None
    weth_fee_normalized: float | None
    weth_reserve_normalized: float | None
    score: float | None


@dataclass(frozen=True)
class LlamaAdaptiveThresholds:
    total_rows: int
    min_swap_count: int
    min_weth_liquidity: float
    band: str
    baseline_hours: int
    persistence_hours: int
    persistence_spike_multiplier: float
    persistence_min_hits: int
    fallback_engaged: bool
    fallback_trace: str


@dataclass(frozen=True)
class LlamaSpikeRankingRow:
    source_name: str
    chain: str
    hour_start_unix: int
    hour_start_utc: str
    hour_start_chicago: str
    pair: str
    token0: str
    token1: str
    token0_symbol: str
    token1_symbol: str
    swap_count: int
    weth_reserve: float
    weth_fee: float
    weth_reserve_normalized: float
    weth_fee_normalized: float
    score: float
    hourly_yield_pct: float
    usd_per_1000_liquidity_hourly: float
    rough_apr_pct: float
    baseline_median_score: float
    spike_multiplier: float
    spike_multiplier_capped: float
    persistence_hits: int
    notes_flags: str


@dataclass(frozen=True)
class LlamaDropoffCounters:
    fetched_raw_rows: int
    after_time_window_filter: int
    after_min_swaps_filter: int
    after_min_weth_filter: int
    after_baseline_ready_filter: int
    after_spike_multiplier_filter: int
    after_persistence_filter: int
    final_ranked_rows: int


@dataclass(frozen=True)
class LlamaRuntimeState:
    meta_block_number: int | None
    seed_next_index: int | None
    seed_total: int | None
    seed_last_block: int | None


@dataclass(frozen=True)
class LlamaRunDiagnostics:
    endpoint: str
    version: str
    source_name: str
    window_start_ts: int
    window_end_ts: int
    local_timezone: str
    thresholds_label: str
    min_swap_count: int | None
    min_weth_liquidity: float | None
    baseline_hours: int | None
    persistence_hours: int | None
    persistence_spike_multiplier: float | None
    persistence_min_hits: int | None
    fallback_trace: str
    counts: LlamaDropoffCounters
    meta_block_number: int | None
    seed_next_index: int | None
    seed_total: int | None
    seed_last_block: int | None
    empty_stage: str | None
    empty_message: str


def trimmed_mean(values: list[float], trim_ratio: float) -> float:
    if not values:
        return 0.0
    if len(values) < 5:
        return statistics.fmean(values)
    trim_ratio = max(0.0, min(0.4, trim_ratio))
    k = int(len(values) * trim_ratio)
    if k <= 0:
        return statistics.fmean(values)
    ordered = sorted(values)
    core = ordered[k : len(ordered) - k]
    if not core:
        return statistics.fmean(values)
    return statistics.fmean(core)


def robust_outlier_count(values: list[float], z_threshold: float = 10.0) -> int:
    if len(values) < 5:
        return 0
    median_v = statistics.median(values)
    deviations = [abs(v - median_v) for v in values]
    mad = statistics.median(deviations)
    if mad <= 0:
        return 0
    return sum(1 for v in values if abs(v - median_v) > (z_threshold * mad))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan Uniswap v3/v4 pool hourly data, estimate hourly yield, and rank pools by earning potential."
        )
    )
    parser.add_argument("--config", required=True, help="Path to JSON source config.")
    parser.add_argument(
        "--hours",
        type=int,
        default=24 * 7,
        help="Lookback window in hours from --end-ts (default: 168).",
    )
    parser.add_argument(
        "--end-ts",
        type=int,
        default=None,
        help="Window end timestamp (UTC, epoch seconds). Defaults to current hour.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="GraphQL page size for pagination (default: 1000).",
    )
    parser.add_argument(
        "--max-pages-per-source",
        type=int,
        default=None,
        help="Optional cap for pages fetched per source.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent source fetch workers (default: 4).",
    )
    parser.add_argument(
        "--parallel-window-hours",
        type=int,
        default=0,
        help=(
            "Split each non-backfill hourly source window into parallel time shards of this many hours. "
            "Set 0 to disable sharding (default: 0)."
        ),
    )
    parser.add_argument(
        "--include-backfill-sources",
        action="store_true",
        help="Include sources marked backfill_source=true in this run.",
    )
    parser.add_argument(
        "--only-backfill-sources",
        action="store_true",
        help="Run only sources marked backfill_source=true.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=24,
        help="Minimum hourly samples per pool to include in rankings (default: 24).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Top N pools to display in terminal and summary markdown (default: 20).",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for report artifacts (default: output).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=45,
        help="HTTP timeout in seconds for GraphQL calls (default: 45).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retries per GraphQL call (default: 3).",
    )
    parser.add_argument(
        "--strict-sources",
        action="store_true",
        help="Fail the run if any source request fails. Default is to continue with healthy sources.",
    )
    parser.add_argument(
        "--schedule-top-pools",
        type=int,
        default=25,
        help="Build liquidity schedules for top N ranked pools (default: 25).",
    )
    parser.add_argument(
        "--schedule-quantile",
        type=float,
        default=0.75,
        help=(
            "Quantile threshold for defining high-yield hours when building schedules "
            "when --schedule-min-usd-per-1000-hour is not set (default: 0.75)."
        ),
    )
    parser.add_argument(
        "--schedule-min-usd-per-1000-hour",
        type=float,
        default=None,
        help=(
            "Absolute threshold for high-yield hours, expressed as expected USD per hour "
            "for each $1,000 of liquidity. When set, this overrides --schedule-quantile."
        ),
    )
    parser.add_argument(
        "--schedule-min-hit-rate",
        type=float,
        default=0.60,
        help="Minimum hit rate for a recurring hour block to be considered reliable (default: 0.60).",
    )
    parser.add_argument(
        "--schedule-min-occurrences",
        type=int,
        default=2,
        help="Minimum number of historical occurrences required for a recurring schedule block (default: 2).",
    )
    parser.add_argument(
        "--schedule-max-blocks-per-pool",
        type=int,
        default=3,
        help="Maximum recommended add/remove blocks per pool (default: 3).",
    )
    parser.add_argument(
        "--no-open-report",
        action="store_true",
        help="Do not auto-open the generated HTML report in the default browser.",
    )
    parser.add_argument(
        "--cache-db",
        default=None,
        help=(
            "Optional SQLite cache path. When set, observations are stored locally and "
            "future runs fetch only new/overlap hours instead of full windows."
        ),
    )
    parser.add_argument(
        "--cache-overlap-hours",
        type=int,
        default=24,
        help=(
            "When --cache-db is enabled, re-fetch this many trailing hours from the latest "
            "cached timestamp per source to refresh edge data (default: 24)."
        ),
    )
    parser.add_argument(
        "--source-checkpoint-pages",
        type=int,
        default=20,
        help=(
            "When cache is enabled, commit source pagination checkpoints every N pages "
            "for resumable long-running backfill sources (default: 20)."
        ),
    )
    parser.add_argument(
        "--log-file",
        default="/tmp/uniswap-yield-scanner.log",
        help=(
            "Append verbose runtime logs to this file (default: /tmp/uniswap-yield-scanner.log). "
            "Set to empty string to disable file logging."
        ),
    )
    parser.add_argument(
        "--v2-spike-min-swap-count",
        type=int,
        default=10,
        help="Default minimum swapCount per hour for v2/llama spike rows (default: 10).",
    )
    parser.add_argument(
        "--v2-spike-min-reserve-weth",
        type=float,
        default=50.0,
        help="Default minimum reserveWETH for v2/llama spike rows (default: 50.0).",
    )
    parser.add_argument(
        "--v2-spike-top",
        type=int,
        default=100,
        help="Top N v2 spike rows to include in report sections (default: 100).",
    )
    parser.add_argument(
        "--llama-baseline-hours",
        type=int,
        default=72,
        help="Trailing hours used for per-pair baseline median score in llama spike ranking (default: 72).",
    )
    parser.add_argument(
        "--llama-persistence-hours",
        type=int,
        default=6,
        help="Trailing hours used for persistence_hits in llama spike ranking (default: 6).",
    )
    parser.add_argument(
        "--llama-persistence-spike-multiplier",
        type=float,
        default=3.0,
        help="Spike multiplier threshold used when counting persistence hits (default: 3.0).",
    )
    parser.add_argument(
        "--llama-persistence-min-hits",
        type=int,
        default=2,
        help="Minimum persistence hits required in llama spike ranking output (default: 2).",
    )
    parser.add_argument(
        "--llama-min-baseline-observations",
        type=int,
        default=12,
        help="Minimum trailing baseline observations required before a llama row is eligible (default: 12).",
    )
    parser.add_argument(
        "--llama-baseline-epsilon",
        type=float,
        default=1e-10,
        help="Minimum baseline median floor used for spike multiplier division guard (default: 1e-10).",
    )
    parser.add_argument(
        "--llama-spike-multiplier-cap",
        type=float,
        default=250.0,
        help="Cap applied to spike multiplier for ranking stability (default: 250.0).",
    )
    parser.add_argument(
        "--llama-strict-mode",
        action="store_true",
        help="Use strict llama thresholds first (100 WETH / 30 swaps), then fallback if sparse.",
    )
    parser.add_argument(
        "--llama-strict-min-swap-count",
        type=int,
        default=30,
        help="Strict mode minimum swapCount per hour for llama ranking (default: 30).",
    )
    parser.add_argument(
        "--llama-strict-min-reserve-weth",
        type=float,
        default=100.0,
        help="Strict mode minimum reserveWETH for llama ranking (default: 100.0).",
    )
    parser.add_argument(
        "--llama-min-ranked-target",
        type=int,
        default=20,
        help="Target minimum llama ranked rows before stopping fallback relaxation (default: 20).",
    )
    parser.add_argument(
        "--min-tvl-usd",
        type=float,
        default=10000.0,
        help="Minimum TVL/liquidity USD required per hourly row for ranking/schedule (default: 10000).",
    )
    parser.add_argument(
        "--max-hourly-yield-pct",
        type=float,
        default=100.0,
        help="Cap hourly yield percent per row to suppress extreme outliers (default: 100).",
    )
    parser.add_argument(
        "--yield-trim-ratio",
        type=float,
        default=0.10,
        help="Trim ratio used for robust trimmed mean yield stats (default: 0.10).",
    )
    parser.add_argument(
        "--ranking-stats-min-tvl-usd",
        type=float,
        default=25000.0,
        help=(
            "Additional TVL floor for non-llama ranking statistics (default: 25000). "
            "Effective floor is max(--min-tvl-usd, --ranking-stats-min-tvl-usd)."
        ),
    )
    parser.add_argument(
        "--ranking-yield-winsorize-percentile",
        type=float,
        default=0.99,
        help=(
            "Winsorization percentile applied to hourly yield values inside ranking stats "
            "to reduce extreme tails (default: 0.99)."
        ),
    )
    parser.add_argument(
        "--local-timezone",
        default="America/Chicago",
        help="IANA timezone name used alongside UTC in report timestamps (default: America/Chicago).",
    )
    return parser.parse_args()


def floor_to_hour(ts: int) -> int:
    return ts - (ts % SECONDS_PER_HOUR)


def split_time_windows(start_ts: int, end_ts: int, shard_hours: int) -> list[tuple[int, int]]:
    if shard_hours <= 0:
        return [(start_ts, end_ts)]
    shard_seconds = max(1, shard_hours) * SECONDS_PER_HOUR
    windows: list[tuple[int, int]] = []
    cursor = start_ts
    while cursor < end_ts:
        nxt = min(end_ts, cursor + shard_seconds)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def effective_page_size(source: SourceConfig, default_page_size: int) -> int:
    if source.page_size_override and source.page_size_override > 0:
        return source.page_size_override
    # Heavy source default override for faster pagination when config does not set one.
    if source.name == "uniswap-v4-base-official":
        return max(default_page_size, 1000)
    return default_page_size


def split_time_windows(start_ts: int, end_ts: int, window_hours: int) -> list[tuple[int, int]]:
    if window_hours <= 0:
        return [(start_ts, end_ts)]
    step = max(1, window_hours) * SECONDS_PER_HOUR
    out: list[tuple[int, int]] = []
    cur = start_ts
    while cur < end_ts:
        nxt = min(end_ts, cur + step)
        out.append((cur, nxt))
        cur = nxt
    return out


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    weight = pos - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


class TeeStream:
    def __init__(self, *streams: Any) -> None:
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()

    def isatty(self) -> bool:
        return any(getattr(stream, "isatty", lambda: False)() for stream in self.streams)

    @property
    def encoding(self) -> str:
        for stream in self.streams:
            value = getattr(stream, "encoding", None)
            if value:
                return str(value)
        return "utf-8"


class TimestampedFile:
    def __init__(self, stream: Any) -> None:
        self.stream = stream
        self._line_start = True

    def write(self, data: str) -> int:
        if not data:
            return 0
        written = 0
        for chunk in data.splitlines(keepends=True):
            if self._line_start:
                stamp = dt.datetime.now(dt.timezone.utc).strftime("[%Y-%m-%d %H:%M:%S UTC] ")
                self.stream.write(stamp)
            self.stream.write(chunk)
            written += len(chunk)
            self._line_start = chunk.endswith("\n")
        return written

    def flush(self) -> None:
        self.stream.flush()

    def close(self) -> None:
        self.stream.close()

    @property
    def encoding(self) -> str:
        value = getattr(self.stream, "encoding", None)
        if value:
            return str(value)
        return "utf-8"


def setup_run_logging(log_file: str | None, argv: list[str]) -> None:
    if not log_file:
        return

    path = Path(log_file).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_stream = TimestampedFile(path.open("a", encoding="utf-8"))

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    quoted_argv = " ".join(shlex.quote(part) for part in argv)
    file_stream.write(f"\n[{timestamp}] {quoted_argv}\n")
    file_stream.flush()

    sys.stdout = TeeStream(sys.stdout, file_stream)
    sys.stderr = TeeStream(sys.stderr, file_stream)


def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()


def resolve_endpoint(raw_endpoint: str, source_label: str) -> str:
    if "<" in raw_endpoint and ">" in raw_endpoint:
        raise ValueError(
            f"{source_label}: endpoint looks like a placeholder: {raw_endpoint!r}"
        )

    expanded = os.path.expandvars(raw_endpoint).strip()
    unresolved: list[str] = []
    for match in ENV_TOKEN_RE.finditer(expanded):
        unresolved.append(match.group(1) or match.group(2) or "")

    if unresolved:
        unresolved_vars = ", ".join(sorted(set(unresolved)))
        raise ValueError(
            f"{source_label}: endpoint has unresolved environment variables: {unresolved_vars}"
        )
    return expanded


def resolve_env_string(raw_value: str, label: str) -> str:
    expanded = os.path.expandvars(raw_value).strip()
    unresolved: list[str] = []
    for match in ENV_TOKEN_RE.finditer(expanded):
        unresolved.append(match.group(1) or match.group(2) or "")
    if unresolved:
        unresolved_vars = ", ".join(sorted(set(unresolved)))
        raise ValueError(f"{label} has unresolved environment variables: {unresolved_vars}")
    return expanded


def load_sources(config_path: Path) -> list[SourceConfig]:
    with config_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("Config must include a non-empty 'sources' list.")

    sources: list[SourceConfig] = []
    for idx, raw in enumerate(raw_sources, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"source[{idx}] must be an object.")

        try:
            enabled = to_bool(raw.get("enabled", True), default=True)
        except ValueError as err:
            raise ValueError(f"source[{idx}] invalid enabled flag: {err}") from err
        if not enabled:
            continue

        name = str(raw.get("name", "")).strip()
        version = str(raw.get("version", "")).strip()
        chain = str(raw.get("chain", "")).strip()
        endpoint_raw = str(raw.get("endpoint", "")).strip()
        source_type = str(raw.get("source_type", "hourly")).strip().lower()
        if not name or not version or not chain or not endpoint_raw:
            raise ValueError(
                f"source[{idx}] missing required fields: name/version/chain/endpoint"
            )
        endpoint = resolve_endpoint(endpoint_raw, f"source[{idx}] {name}")

        hourly_query = str(raw.get("hourly_query", "")).strip()
        hourly_query_file = str(raw.get("hourly_query_file", "")).strip()
        if hourly_query_file:
            query_path = Path(hourly_query_file)
            if not query_path.is_absolute():
                query_path = (config_path.parent / query_path).resolve()
            hourly_query = read_text(query_path)

        weth_address = str(
            raw.get("weth_address", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
        ).strip()
        if source_type not in {"hourly", "v2_spike"}:
            raise ValueError(
                f"source[{idx}] unsupported source_type {source_type!r}; expected 'hourly' or 'v2_spike'."
            )
        if source_type == "hourly" and not hourly_query:
            raise ValueError(
                f"source[{idx}] requires 'hourly_query' or 'hourly_query_file'."
            )
        page_size_override_raw = raw.get("page_size")
        page_size_override: int | None = None
        if page_size_override_raw is not None:
            page_size_override = to_int(page_size_override_raw, default=0)
            if page_size_override <= 0:
                raise ValueError(f"source[{idx}] page_size must be a positive integer when set.")
        try:
            backfill_source = to_bool(raw.get("backfill_source", False), default=False)
        except ValueError as err:
            raise ValueError(f"source[{idx}] invalid backfill_source flag: {err}") from err

        headers = raw.get("headers", {})
        if headers is None:
            headers = {}
        if not isinstance(headers, dict):
            raise ValueError(f"source[{idx}] headers must be an object.")

        parsed_headers: dict[str, str] = {}
        for k, v in headers.items():
            key = str(k).strip()
            value = resolve_env_string(str(v), f"source[{idx}] header {key!r}")
            parsed_headers[key] = value

        sources.append(
            SourceConfig(
                name=name,
                version=version,
                chain=chain,
                endpoint=endpoint,
                hourly_query=hourly_query,
                source_type=source_type,
                weth_address=weth_address.lower(),
                page_size_override=page_size_override,
                backfill_source=backfill_source,
                headers=parsed_headers,
            )
        )

    return sources


def graphql_query(
    source: SourceConfig,
    query: str,
    variables: dict[str, Any],
    timeout: int,
    retries: int,
) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "uniswap-yield-scanner/1.0",
    }
    headers.update(source.headers)

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(source.endpoint, data=payload, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
            parsed = json.loads(body)
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as err:
            last_err = err
            if attempt < retries:
                time.sleep(min(2**attempt, 8))
                continue
            hint = ""
            if isinstance(err, urllib.error.HTTPError) and err.code in {401, 403}:
                if "gateway.thegraph.com" in source.endpoint:
                    hint = (
                        " (Gateway auth failed: use a Graph Gateway Query API key, "
                        "not a Studio deploy key.)"
                    )
            raise RuntimeError(
                f"{source.name}: GraphQL request failed after {retries} attempts: {err}{hint}"
            ) from err

        if parsed.get("errors"):
            err_text = "; ".join(
                str(e.get("message", e)) for e in parsed.get("errors", [])
            )
            if "auth error: API key not found" in err_text and "gateway.thegraph.com" in source.endpoint:
                err_text += " (Use a Graph Gateway Query API key; Studio deploy keys do not work here.)"
            raise RuntimeError(f"{source.name}: GraphQL error: {err_text}")

        data = parsed.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"{source.name}: Missing JSON object at data.")
        return data

    if last_err:
        raise RuntimeError(f"{source.name}: GraphQL request failed: {last_err}")
    raise RuntimeError(f"{source.name}: GraphQL request failed.")


def extract_symbol(pool: dict[str, Any], primary: str, secondary: str) -> str:
    nested = pool.get(primary)
    if isinstance(nested, dict):
        symbol = nested.get("symbol")
        if symbol:
            return str(symbol)
    nested = pool.get(secondary)
    if isinstance(nested, dict):
        symbol = nested.get("symbol")
        if symbol:
            return str(symbol)
    # Some custom queries may flatten symbol fields.
    flat_primary = pool.get(f"{primary}Symbol")
    if flat_primary:
        return str(flat_primary)
    flat_secondary = pool.get(f"{secondary}Symbol")
    if flat_secondary:
        return str(flat_secondary)
    return "UNKNOWN"


def extract_token_id(pool: dict[str, Any], primary: str, secondary: str) -> str:
    nested = pool.get(primary)
    if isinstance(nested, dict):
        token_id = nested.get("id")
        if token_id:
            return str(token_id).lower()
    nested = pool.get(secondary)
    if isinstance(nested, dict):
        token_id = nested.get("id")
        if token_id:
            return str(token_id).lower()
    flat_primary = pool.get(f"{primary}Id")
    if flat_primary:
        return str(flat_primary).lower()
    flat_secondary = pool.get(f"{secondary}Id")
    if flat_secondary:
        return str(flat_secondary).lower()
    return ""


def _short_addr(addr: str) -> str:
    if isinstance(addr, str) and addr.startswith("0x") and len(addr) >= 12:
        return f"{addr[:6]}...{addr[-4:]}"
    return addr


def human_token_label(symbol: str, token_address: str = "") -> str:
    clean_symbol = (symbol or "").strip()
    if clean_symbol and clean_symbol.upper() != "UNKNOWN":
        return clean_symbol
    token_addr = (token_address or "").strip().lower()
    if token_addr:
        alias = TOKEN_SYMBOL_OVERRIDES.get(token_addr)
        if alias:
            return alias
        return _short_addr(token_addr)
    return "UNKNOWN"


def human_pool_label(
    token0_symbol: str,
    token1_symbol: str,
    token0_address: str = "",
    token1_address: str = "",
) -> str:
    token0 = human_token_label(token0_symbol, token0_address)
    token1 = human_token_label(token1_symbol, token1_address)
    return f"{token0}/{token1}"


def derive_hourly_fees_usd(volume_usd: float, fee_tier: int, source_type: str = "hourly") -> float | None:
    # Uniswap fee tiers are in hundredths of a bip for v3-style static tiers
    # and should be within 0..1_000_000 (0%..100%). Values above that are
    # protocol-specific encodings (for example dynamic-fee flags) and cannot
    # be converted to a direct percentage.
    # Guardrail: for regular hourly sources, only derive from well-known static
    # tiers. High/encoded values (for example near 1e6 on v4 dynamic paths) can
    # create unrealistic implied fee rates if treated as direct percentages.
    if source_type != "v2_spike" and (fee_tier < 0 or fee_tier > 100_000):
        return None
    if source_type == "v2_spike" and (fee_tier < 0 or fee_tier > 1_000_000):
        return None
    return volume_usd * (fee_tier / 1_000_000)


def sanitize_hourly_fees_usd(
    volume_usd: float,
    fee_tier: int,
    fees_usd: float | None,
    source_type: str = "hourly",
) -> float | None:
    if fees_usd is None:
        return None
    if volume_usd <= 0:
        return fees_usd
    implied_fee_rate = fees_usd / volume_usd
    # Fees above traded volume are impossible for LP fees and indicate bad scaling.
    if implied_fee_rate < 0 or implied_fee_rate > 1.0:
        return None

    if source_type != "v2_spike" and implied_fee_rate > 0.10:
        return None

    if fee_tier <= 1_000_000:
        expected_rate = fee_tier / 1_000_000
        # Allow some slippage in subgraph values, but reject extreme mismatches.
        if expected_rate > 0 and implied_fee_rate > max(0.10, expected_rate * 5.0):
            return None
    else:
        # Encoded/dynamic fee tiers often use non-percent integers; reject implausibly high rates.
        if implied_fee_rate > 0.10:
            return None
    return fees_usd


def to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def iso_hour_chicago(ts: int) -> str:
    if ZoneInfo is None:
        return iso_hour(ts)
    dt_local = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone(
        ZoneInfo("America/Chicago")
    )
    return dt_local.strftime("%Y-%m-%d %H:00:00 %Z")


def iso_hour_local(ts: int, tz_name: str) -> str:
    if ZoneInfo is None:
        return iso_hour(ts)
    try:
        tz_obj = ZoneInfo(tz_name)
    except Exception:
        return iso_hour(ts)
    dt_local = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone(tz_obj)
    return dt_local.strftime("%Y-%m-%d %H:00:00 %Z")


def normalize_row(source: SourceConfig, row: dict[str, Any]) -> Observation | None:
    pool = row.get("pool")
    if not isinstance(pool, dict):
        pool = {}

    ts = to_int(row.get("ts"))
    if ts <= 0:
        return None

    volume_usd = to_float(row.get("volumeUSD"), default=0.0)
    # Support either aliased tvlUSD or common raw field fallback.
    tvl_usd = to_float(
        row.get("tvlUSD", row.get("totalValueLockedUSD", row.get("liquidityUSD"))),
        default=0.0,
    )

    pool_id = str(pool.get("id", row.get("poolId", ""))).strip()
    if not pool_id:
        return None

    fee_tier = to_int(pool.get("feeTier", row.get("feeTier", 0)), default=0)
    fees_usd_raw = row.get("feesUSD")
    if fees_usd_raw is None:
        fees_usd = derive_hourly_fees_usd(volume_usd, fee_tier, source.source_type)
    else:
        fees_usd = to_float(fees_usd_raw)
    fees_usd = sanitize_hourly_fees_usd(
        volume_usd=volume_usd,
        fee_tier=fee_tier,
        fees_usd=fees_usd,
        source_type=source.source_type,
    )

    token0_address = extract_token_id(pool, "token0", "currency0")
    token1_address = extract_token_id(pool, "token1", "currency1")
    token0_symbol = extract_symbol(pool, "token0", "currency0")
    token1_symbol = extract_symbol(pool, "token1", "currency1")
    pair = human_pool_label(
        token0_symbol=token0_symbol,
        token1_symbol=token1_symbol,
        token0_address=token0_address,
        token1_address=token1_address,
    )

    hourly_yield = None
    if tvl_usd > 0 and fees_usd is not None:
        hourly_yield = fees_usd / tvl_usd

    return Observation(
        source_name=source.name,
        version=source.version,
        chain=source.chain,
        pool_id=pool_id,
        pair=pair,
        fee_tier=fee_tier,
        ts=ts,
        volume_usd=volume_usd,
        tvl_usd=tvl_usd,
        fees_usd=fees_usd,
        hourly_yield=hourly_yield,
    )


def classify_quality_rejection(
    obs: Observation,
    min_tvl_usd: float,
    max_hourly_yield_pct: float | None,
    v2_spike_sources: set[str] | None = None,
) -> str | None:
    is_v2_spike = v2_spike_sources is not None and obs.source_name in v2_spike_sources
    if obs.volume_usd < 0:
        return "negative_volume"
    if obs.tvl_usd < 0:
        return "negative_tvl"
    if obs.fees_usd is not None and obs.fees_usd < 0:
        return "negative_fees"
    if (not is_v2_spike) and obs.fees_usd is not None and obs.volume_usd > 0 and obs.fees_usd > obs.volume_usd:
        return "fees_gt_volume"
    if (
        not is_v2_spike
        and obs.fees_usd is not None
        and obs.volume_usd > 0
        and (obs.fees_usd / obs.volume_usd) > 0.10
    ):
        return "implied_fee_rate_gt_10pct"
    if (
        not is_v2_spike
        and
        obs.fees_usd is not None
        and obs.volume_usd > 0
        and obs.fee_tier > 1_000_000
        and (obs.fees_usd / obs.volume_usd) > 0.10
    ):
        return "dynamic_fee_rate_gt_10pct"
    if (not is_v2_spike) and obs.fees_usd is not None and obs.tvl_usd <= 0:
        return "fees_with_nonpositive_tvl"
    if obs.tvl_usd <= 0:
        return "nonpositive_tvl"
    if (not is_v2_spike) and obs.tvl_usd < max(0.0, min_tvl_usd):
        return "tvl_below_floor"
    if obs.hourly_yield is not None:
        if obs.hourly_yield < 0:
            return "negative_hourly_yield"
        if max_hourly_yield_pct is not None:
            cap_ratio = max(0.0, max_hourly_yield_pct) / 100.0
            if obs.hourly_yield > cap_ratio:
                return "hourly_yield_above_cap"
    return None


def filter_observations_with_quality_audit(
    observations: list[Observation],
    min_tvl_usd: float,
    max_hourly_yield_pct: float | None,
    v2_spike_sources: set[str] | None = None,
) -> tuple[list[Observation], dict[tuple[str, str, str, str], int]]:
    kept: list[Observation] = []
    rejected: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for obs in observations:
        reason = classify_quality_rejection(
            obs=obs,
            min_tvl_usd=min_tvl_usd,
            max_hourly_yield_pct=max_hourly_yield_pct,
            v2_spike_sources=v2_spike_sources,
        )
        if reason is None:
            kept.append(obs)
            continue
        key = (obs.source_name, obs.version, obs.chain, reason)
        rejected[key] += 1
    return kept, dict(rejected)


V2_PAIR_HOUR_QUERY = """
query PairHourPage($first: Int!, $start: Int!, $end: Int!) {
  pairHourDatas(
    first: $first
    orderBy: hourStartUnix
    orderDirection: desc
    where: { hourStartUnix_gte: $start, hourStartUnix_lte: $end }
  ) {
    id
    hourStartUnix
    fee0
    fee1
    reserve0
    reserve1
    swapCount
    pair {
      id
    }
  }
}
"""


V2_PAIR_META_QUERY = """
query PairMeta($ids: [String!]!) {
  pairs(where: { id_in: $ids }) {
    id
    token0
    token1
  }
}
"""


V2_TOKEN_META_QUERY = """
query TokenMeta($ids: [String!]!) {
  tokens(where: { id_in: $ids }) {
    id
    symbol
    name
    decimals
  }
}
"""


LLAMA_RUNTIME_QUERY = """
query LlamaRuntime {
  _meta {
    block {
      number
    }
  }
  v2SeedStates {
    id
    nextIndex
    total
    lastBlock
  }
}
"""


def _extract_pair_id(value: Any) -> str:
    if isinstance(value, dict):
        nested = value.get("id")
        if isinstance(nested, str):
            return nested.lower()
    if isinstance(value, str):
        return value.lower()
    return ""


def _extract_token_addr(value: Any) -> str:
    if isinstance(value, dict):
        nested = value.get("id")
        if isinstance(nested, str):
            return nested.lower()
    if isinstance(value, str):
        return value.lower()
    return ""


def _fetch_v2_pair_metadata(
    source: SourceConfig,
    pair_ids: Iterable[str],
    timeout: int,
    retries: int,
) -> dict[str, tuple[str, str]]:
    ids = sorted({pid.lower() for pid in pair_ids if pid})
    if not ids:
        return {}
    out: dict[str, tuple[str, str]] = {}
    chunk_size = 200
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        data = graphql_query(
            source=source,
            query=V2_PAIR_META_QUERY,
            variables={"ids": chunk},
            timeout=timeout,
            retries=retries,
        )
        rows = data.get("pairs")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            pair_id = str(row.get("id", "")).strip().lower()
            if not pair_id:
                continue
            token0 = _extract_token_addr(row.get("token0"))
            token1 = _extract_token_addr(row.get("token1"))
            out[pair_id] = (token0, token1)
    return out


def _fetch_v2_token_metadata(
    source: SourceConfig,
    token_ids: Iterable[str],
    timeout: int,
    retries: int,
) -> dict[str, tuple[str, str, str]]:
    ids = sorted({tid.lower() for tid in token_ids if tid})
    if not ids:
        return {}
    out: dict[str, tuple[str, str, str]] = {}
    chunk_size = 200
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        data = graphql_query(
            source=source,
            query=V2_TOKEN_META_QUERY,
            variables={"ids": chunk},
            timeout=timeout,
            retries=retries,
        )
        rows = data.get("tokens")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("id", "")).strip().lower()
            if not token_id:
                continue
            symbol = str(row.get("symbol", "") or "").strip()
            name = str(row.get("name", "") or "").strip()
            decimals = str(row.get("decimals", "") or "").strip()
            out[token_id] = (symbol, name, decimals)
    return out


def fetch_llama_runtime_state(
    source: SourceConfig,
    timeout: int,
    retries: int,
) -> LlamaRuntimeState:
    try:
        data = graphql_query(
            source=source,
            query=LLAMA_RUNTIME_QUERY,
            variables={},
            timeout=timeout,
            retries=retries,
        )
    except Exception:  # noqa: BLE001
        return LlamaRuntimeState(
            meta_block_number=None,
            seed_next_index=None,
            seed_total=None,
            seed_last_block=None,
        )

    meta_block: int | None = None
    meta = data.get("_meta")
    if isinstance(meta, dict):
        block = meta.get("block")
        if isinstance(block, dict):
            raw_number = block.get("number")
            if raw_number is not None:
                meta_block = to_int(raw_number, default=0) or None

    seed_next: int | None = None
    seed_total: int | None = None
    seed_last_block: int | None = None
    seed_states = data.get("v2SeedStates")
    if isinstance(seed_states, list) and seed_states:
        picked: dict[str, Any] | None = None
        for item in seed_states:
            if not isinstance(item, dict):
                continue
            if str(item.get("id", "")).lower() == "v2":
                picked = item
                break
            if picked is None:
                picked = item
        if picked is not None:
            seed_next = to_int(picked.get("nextIndex"), default=0) or None
            seed_total = to_int(picked.get("total"), default=0) or None
            seed_last_block = to_int(picked.get("lastBlock"), default=0) or None

    return LlamaRuntimeState(
        meta_block_number=meta_block,
        seed_next_index=seed_next,
        seed_total=seed_total,
        seed_last_block=seed_last_block,
    )


def summarize_llama_dropoff(counts: LlamaDropoffCounters) -> tuple[str | None, str]:
    stages = [
        ("fetched_raw_rows", counts.fetched_raw_rows),
        ("after_time_window_filter", counts.after_time_window_filter),
        ("after_min_swaps_filter", counts.after_min_swaps_filter),
        ("after_min_weth_filter", counts.after_min_weth_filter),
        ("after_baseline_ready_filter", counts.after_baseline_ready_filter),
        ("after_spike_multiplier_filter", counts.after_spike_multiplier_filter),
        ("after_persistence_filter", counts.after_persistence_filter),
        ("final_ranked_rows", counts.final_ranked_rows),
    ]
    if counts.fetched_raw_rows <= 0:
        return (
            "fetched_raw_rows",
            "No PairHourData rows returned from llama for the queried window (0 fetched).",
        )
    for idx in range(1, len(stages)):
        prev_name, prev_value = stages[idx - 1]
        cur_name, cur_value = stages[idx]
        if prev_value > 0 and cur_value == 0:
            return (
                cur_name,
                (
                    "Rows fetched from llama but all were filtered out "
                    f"(drop to 0 at {cur_name} after {prev_name}={prev_value})."
                ),
            )
    if counts.final_ranked_rows <= 0:
        return (
            "final_ranked_rows",
            "Rows fetched from llama but all were filtered out before final ranking.",
        )
    return (None, "Llama pipeline produced ranked rows.")


def build_llama_diagnostics(
    *,
    source: SourceConfig | None,
    runtime: LlamaRuntimeState | None,
    window_start_ts: int,
    window_end_ts: int,
    local_timezone: str,
    thresholds: LlamaAdaptiveThresholds | None,
    counts: LlamaDropoffCounters,
) -> LlamaRunDiagnostics | None:
    if source is None:
        return None
    empty_stage, empty_message = summarize_llama_dropoff(counts)
    return LlamaRunDiagnostics(
        endpoint=source.endpoint,
        version=source.version,
        source_name=source.name,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        local_timezone=local_timezone,
        thresholds_label=(thresholds.band if thresholds is not None else "unavailable"),
        min_swap_count=(thresholds.min_swap_count if thresholds is not None else None),
        min_weth_liquidity=(thresholds.min_weth_liquidity if thresholds is not None else None),
        baseline_hours=(thresholds.baseline_hours if thresholds is not None else None),
        persistence_hours=(thresholds.persistence_hours if thresholds is not None else None),
        persistence_spike_multiplier=(
            thresholds.persistence_spike_multiplier if thresholds is not None else None
        ),
        persistence_min_hits=(thresholds.persistence_min_hits if thresholds is not None else None),
        fallback_trace=(thresholds.fallback_trace if thresholds is not None else ""),
        counts=counts,
        meta_block_number=(runtime.meta_block_number if runtime is not None else None),
        seed_next_index=(runtime.seed_next_index if runtime is not None else None),
        seed_total=(runtime.seed_total if runtime is not None else None),
        seed_last_block=(runtime.seed_last_block if runtime is not None else None),
        empty_stage=empty_stage,
        empty_message=empty_message,
    )


def choose_llama_adaptive_thresholds(
    total_rows: int,
    min_swap_count_floor: int,
    min_weth_liquidity_floor: float,
    baseline_hours: int,
    persistence_hours: int,
    persistence_spike_multiplier: float,
    persistence_min_hits: int,
    *,
    band: str = "default",
    fallback_engaged: bool = False,
    fallback_trace: str = "",
) -> LlamaAdaptiveThresholds:
    rows = max(0, total_rows)
    return LlamaAdaptiveThresholds(
        total_rows=rows,
        min_swap_count=max(0, int(min_swap_count_floor)),
        min_weth_liquidity=max(0.0, float(min_weth_liquidity_floor)),
        band=band,
        baseline_hours=max(1, int(baseline_hours)),
        persistence_hours=max(1, int(persistence_hours)),
        persistence_spike_multiplier=max(0.0, float(persistence_spike_multiplier)),
        persistence_min_hits=max(1, int(persistence_min_hits)),
        fallback_engaged=fallback_engaged,
        fallback_trace=fallback_trace,
    )


def fetch_llama_pair_hour_rows(
    source: SourceConfig,
    start_ts: int,
    end_ts: int,
    page_size: int,
    max_pages: int | None,
    timeout: int,
    retries: int,
) -> tuple[list[LlamaPairHourRow], int]:
    cursor_end = end_ts - 1
    pages = 0
    raw_rows: list[dict[str, Any]] = []
    pair_ids: set[str] = set()
    while cursor_end >= start_ts:
        data = graphql_query(
            source=source,
            query=V2_PAIR_HOUR_QUERY,
            variables={"first": page_size, "start": start_ts, "end": cursor_end},
            timeout=timeout,
            retries=retries,
        )
        rows = data.get("pairHourDatas")
        if not isinstance(rows, list):
            raise RuntimeError(
                f"{source.name}: Expected 'pairHourDatas' list in llama query response."
            )
        if not rows:
            break
        min_hour_seen: int | None = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            pair_id = _extract_pair_id(row.get("pair"))
            if not pair_id:
                continue
            hour_ts = to_int(row.get("hourStartUnix"))
            if hour_ts <= 0:
                continue
            item = dict(row)
            item["__pair_id"] = pair_id
            raw_rows.append(item)
            pair_ids.add(pair_id)
            if min_hour_seen is None or hour_ts < min_hour_seen:
                min_hour_seen = hour_ts
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        if min_hour_seen is None:
            break
        next_cursor = min_hour_seen - 1
        if next_cursor >= cursor_end:
            next_cursor = cursor_end - 1
        cursor_end = next_cursor

    pair_meta = _fetch_v2_pair_metadata(source, pair_ids, timeout=timeout, retries=retries)
    token_ids: set[str] = set()
    for token0, token1 in pair_meta.values():
        if token0:
            token_ids.add(token0.lower())
        if token1:
            token_ids.add(token1.lower())
    token_meta = _fetch_v2_token_metadata(
        source=source,
        token_ids=token_ids,
        timeout=timeout,
        retries=retries,
    )
    weth = source.weth_address.lower()

    output: list[LlamaPairHourRow] = []
    for row in raw_rows:
        pair_id = str(row.get("__pair_id", "")).lower()
        token0, token1 = pair_meta.get(pair_id, ("", ""))
        fee0_dec = to_decimal(row.get("fee0"))
        fee1_dec = to_decimal(row.get("fee1"))
        reserve0_dec = to_decimal(row.get("reserve0"))
        reserve1_dec = to_decimal(row.get("reserve1"))
        hour_ts = to_int(row.get("hourStartUnix"))
        swap_count = to_int(row.get("swapCount"))
        token0_decimals = to_optional_int(token_meta.get(token0.lower(), ("", "", ""))[2]) if token0 else None
        token1_decimals = to_optional_int(token_meta.get(token1.lower(), ("", "", ""))[2]) if token1 else None
        weth_fee: float | None = None
        weth_reserve: float | None = None
        weth_fee_normalized: float | None = None
        weth_reserve_normalized: float | None = None
        score: float | None = None
        if token0 == weth and reserve0_dec > 0:
            weth_decimals = token0_decimals if token0_decimals is not None else 18
            weth_fee = float(fee0_dec)
            weth_reserve = float(reserve0_dec)
            scale = Decimal(10) ** max(0, weth_decimals)
            fee_norm = fee0_dec / scale
            reserve_norm = reserve0_dec / scale
            weth_fee_normalized = float(fee_norm)
            weth_reserve_normalized = float(reserve_norm)
            if reserve_norm > 0:
                score = float(fee_norm / reserve_norm)
        elif token1 == weth and reserve1_dec > 0:
            weth_decimals = token1_decimals if token1_decimals is not None else 18
            weth_fee = float(fee1_dec)
            weth_reserve = float(reserve1_dec)
            scale = Decimal(10) ** max(0, weth_decimals)
            fee_norm = fee1_dec / scale
            reserve_norm = reserve1_dec / scale
            weth_fee_normalized = float(fee_norm)
            weth_reserve_normalized = float(reserve_norm)
            if reserve_norm > 0:
                score = float(fee_norm / reserve_norm)

        token0_symbol_raw = token_meta.get(token0.lower(), ("", "", ""))[0] if token0 else ""
        token1_symbol_raw = token_meta.get(token1.lower(), ("", "", ""))[0] if token1 else ""
        token0_symbol = human_token_label(token0_symbol_raw, token0)
        token1_symbol = human_token_label(token1_symbol_raw, token1)
        output.append(
            LlamaPairHourRow(
                source_name=source.name,
                version=source.version,
                chain=source.chain,
                endpoint=source.endpoint,
                hour_start_unix=hour_ts,
                hour_start_utc=iso_hour(hour_ts),
                hour_start_chicago=iso_hour_chicago(hour_ts),
                pair=pair_id,
                token0=token0,
                token1=token1,
                token0_symbol=token0_symbol,
                token1_symbol=token1_symbol,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                swap_count=swap_count,
                fee0_raw=str(row.get("fee0", "")),
                fee1_raw=str(row.get("fee1", "")),
                reserve0_raw=str(row.get("reserve0", "")),
                reserve1_raw=str(row.get("reserve1", "")),
                fee0=float(fee0_dec),
                fee1=float(fee1_dec),
                reserve0=float(reserve0_dec),
                reserve1=float(reserve1_dec),
                weth_fee=weth_fee,
                weth_reserve=weth_reserve,
                weth_fee_normalized=weth_fee_normalized,
                weth_reserve_normalized=weth_reserve_normalized,
                score=score,
            )
        )
    output.sort(key=lambda r: (r.hour_start_unix, r.pair))
    return output, len(raw_rows)


def build_llama_spike_rankings(
    rows: Iterable[LlamaPairHourRow],
    thresholds: LlamaAdaptiveThresholds,
    *,
    min_baseline_observations: int,
    baseline_epsilon: float,
    spike_multiplier_cap: float,
) -> tuple[list[LlamaSpikeRankingRow], LlamaDropoffCounters]:
    grouped: dict[tuple[str, str], list[LlamaPairHourRow]] = defaultdict(list)
    fetched_rows = list(rows)
    after_time_window = len(fetched_rows)
    after_min_swaps = 0
    after_min_weth = 0
    after_baseline_ready = 0
    after_spike_multiplier = 0
    after_persistence = 0
    for row in fetched_rows:
        if row.score is None or row.weth_reserve is None or row.weth_fee is None:
            continue
        grouped[(row.source_name, row.pair)].append(row)

    ranked: list[LlamaSpikeRankingRow] = []
    baseline_seconds = thresholds.baseline_hours * SECONDS_PER_HOUR
    persistence_seconds = thresholds.persistence_hours * SECONDS_PER_HOUR
    spike_threshold = thresholds.persistence_spike_multiplier
    min_baseline_obs = max(1, int(min_baseline_observations))
    epsilon = max(0.0, float(baseline_epsilon))
    spike_cap = max(0.0, float(spike_multiplier_cap))

    for _key, pair_rows in grouped.items():
        pair_rows.sort(key=lambda r: r.hour_start_unix)
        baseline_window: list[LlamaPairHourRow] = []
        persistence_window: list[tuple[int, bool]] = []

        for row in pair_rows:
            row_ts = row.hour_start_unix
            baseline_window = [
                item for item in baseline_window if item.hour_start_unix >= row_ts - baseline_seconds
            ]
            baseline_scores = [item.score for item in baseline_window if item.score is not None]
            baseline_median = statistics.median(baseline_scores) if baseline_scores else 0.0
            score = row.score if row.score is not None else 0.0
            baseline_ready = len(baseline_scores) >= min_baseline_obs and baseline_median >= epsilon
            if baseline_ready and baseline_median > 0:
                spike_multiplier = score / baseline_median
            else:
                spike_multiplier = 0.0
            spike_multiplier_capped = (
                min(spike_multiplier, spike_cap) if spike_cap > 0 else spike_multiplier
            )
            is_spike = (
                spike_multiplier_capped >= spike_threshold if spike_threshold > 0 else True
            )
            persistence_window = [
                item for item in persistence_window if item[0] >= row_ts - persistence_seconds
            ]
            persistence_window.append((row_ts, is_spike))
            persistence_hits = sum(1 for _ts, hit in persistence_window if hit)

            flags: list[str] = []
            if row.reserve0 <= 0 or row.reserve1 <= 0:
                flags.append("zero_reserve_snapshot")
            if baseline_median <= 0:
                flags.append("new_pair_or_no_baseline")
            if row.swap_count < thresholds.min_swap_count:
                flags.append("low_swaps")
            normalized_reserve = row.weth_reserve_normalized or 0.0
            if normalized_reserve < thresholds.min_weth_liquidity:
                flags.append("low_liquidity")

            qualifies = (
                row.swap_count >= thresholds.min_swap_count
                and normalized_reserve >= thresholds.min_weth_liquidity
                and row.reserve0 > 0
                and row.reserve1 > 0
            )
            if row.swap_count >= thresholds.min_swap_count:
                after_min_swaps += 1
            if row.swap_count >= thresholds.min_swap_count and normalized_reserve >= thresholds.min_weth_liquidity:
                after_min_weth += 1
            if qualifies and baseline_ready:
                after_baseline_ready += 1
            if qualifies and baseline_ready and is_spike:
                after_spike_multiplier += 1
            if qualifies and baseline_ready and is_spike and persistence_hits >= thresholds.persistence_min_hits:
                after_persistence += 1
                ranked.append(
                    LlamaSpikeRankingRow(
                        source_name=row.source_name,
                        chain=row.chain,
                        hour_start_unix=row.hour_start_unix,
                        hour_start_utc=row.hour_start_utc,
                        hour_start_chicago=row.hour_start_chicago,
                        pair=row.pair,
                        token0=row.token0,
                        token1=row.token1,
                        token0_symbol=row.token0_symbol,
                        token1_symbol=row.token1_symbol,
                        swap_count=row.swap_count,
                        weth_reserve=(row.weth_reserve or 0.0),
                        weth_fee=(row.weth_fee or 0.0),
                        weth_reserve_normalized=(row.weth_reserve_normalized or 0.0),
                        weth_fee_normalized=(row.weth_fee_normalized or 0.0),
                        score=score,
                        hourly_yield_pct=score * 100.0,
                        usd_per_1000_liquidity_hourly=score * 1000.0,
                        rough_apr_pct=score * 24.0 * 365.0 * 100.0,
                        baseline_median_score=baseline_median,
                        spike_multiplier=spike_multiplier,
                        spike_multiplier_capped=spike_multiplier_capped,
                        persistence_hits=persistence_hits,
                        notes_flags=(", ".join(flags + (["spike_multiplier_capped"] if spike_cap > 0 and spike_multiplier > spike_cap else []))),
                    )
                )
            baseline_window.append(row)

    ranked.sort(
        key=lambda r: (math.log1p(max(0.0, r.spike_multiplier_capped)), r.score, r.persistence_hits, r.weth_reserve_normalized),
        reverse=True,
    )
    counters = LlamaDropoffCounters(
        fetched_raw_rows=after_time_window,
        after_time_window_filter=after_time_window,
        after_min_swaps_filter=after_min_swaps,
        after_min_weth_filter=after_min_weth,
        after_baseline_ready_filter=after_baseline_ready,
        after_spike_multiplier_filter=after_spike_multiplier,
        after_persistence_filter=after_persistence,
        final_ranked_rows=len(ranked),
    )
    return ranked, counters


def build_llama_rankings_with_fallback(
    rows: list[LlamaPairHourRow],
    *,
    baseline_hours: int,
    persistence_hours: int,
    persistence_spike_multiplier: float,
    persistence_min_hits: int,
    min_baseline_observations: int,
    baseline_epsilon: float,
    spike_multiplier_cap: float,
    strict_mode: bool,
    default_min_swap_count: int,
    default_min_weth_liquidity: float,
    strict_min_swap_count: int,
    strict_min_weth_liquidity: float,
    min_ranked_target: int,
) -> tuple[list[LlamaSpikeRankingRow], LlamaAdaptiveThresholds, LlamaDropoffCounters]:
    target = max(1, int(min_ranked_target))
    tiers: list[tuple[int, float, str]] = []
    if strict_mode:
        tiers.append((strict_min_swap_count, strict_min_weth_liquidity, "strict"))
    tiers.append((default_min_swap_count, default_min_weth_liquidity, "default"))
    tiers.extend(
        [
            (5, 25.0, "fallback-25/5"),
            (3, 10.0, "fallback-10/3"),
        ]
    )
    # De-duplicate while preserving order.
    seen: set[tuple[int, float]] = set()
    uniq_tiers: list[tuple[int, float, str]] = []
    for min_swaps, min_weth, label in tiers:
        key = (max(0, int(min_swaps)), max(0.0, float(min_weth)))
        if key in seen:
            continue
        seen.add(key)
        uniq_tiers.append((key[0], key[1], label))

    trace: list[str] = []
    final_ranked: list[LlamaSpikeRankingRow] = []
    final_counters = LlamaDropoffCounters(0, 0, 0, 0, 0, 0, 0, 0)
    selected_thresholds: LlamaAdaptiveThresholds | None = None
    for idx, (min_swaps, min_weth, label) in enumerate(uniq_tiers):
        thresholds = choose_llama_adaptive_thresholds(
            total_rows=len(rows),
            min_swap_count_floor=min_swaps,
            min_weth_liquidity_floor=min_weth,
            baseline_hours=baseline_hours,
            persistence_hours=persistence_hours,
            persistence_spike_multiplier=persistence_spike_multiplier,
            persistence_min_hits=persistence_min_hits,
            band=label,
            fallback_engaged=idx > 0,
            fallback_trace="",
        )
        ranked, counters = build_llama_spike_rankings(
            rows,
            thresholds,
            min_baseline_observations=min_baseline_observations,
            baseline_epsilon=baseline_epsilon,
            spike_multiplier_cap=spike_multiplier_cap,
        )
        trace.append(f"{label}:{min_weth:.0f}/{min_swaps}->{len(ranked)}")
        final_ranked = ranked
        final_counters = counters
        selected_thresholds = thresholds
        if len(ranked) >= target:
            break
    assert selected_thresholds is not None
    fallback_trace = " -> ".join(trace)
    selected_thresholds = choose_llama_adaptive_thresholds(
        total_rows=selected_thresholds.total_rows,
        min_swap_count_floor=selected_thresholds.min_swap_count,
        min_weth_liquidity_floor=selected_thresholds.min_weth_liquidity,
        baseline_hours=selected_thresholds.baseline_hours,
        persistence_hours=selected_thresholds.persistence_hours,
        persistence_spike_multiplier=selected_thresholds.persistence_spike_multiplier,
        persistence_min_hits=selected_thresholds.persistence_min_hits,
        band=selected_thresholds.band,
        fallback_engaged=("fallback" in selected_thresholds.band) or strict_mode,
        fallback_trace=fallback_trace,
    )
    return final_ranked, selected_thresholds, final_counters


def fetch_v2_spike_observations(
    source: SourceConfig,
    start_ts: int,
    end_ts: int,
    page_size: int,
    max_pages: int | None,
    timeout: int,
    retries: int,
    min_swap_count: int,
    min_reserve_weth: float,
) -> list[Observation]:
    print(
        f"{source.name}: fetching v2 spike window {iso_hour(start_ts)} -> {iso_hour(end_ts)}",
        file=sys.stderr,
    )
    cursor_end = end_ts - 1
    pages = 0
    raw_rows: list[dict[str, Any]] = []
    pair_ids: set[str] = set()

    while cursor_end >= start_ts:
        data = graphql_query(
            source=source,
            query=V2_PAIR_HOUR_QUERY,
            variables={
                "first": page_size,
                "start": start_ts,
                "end": cursor_end,
            },
            timeout=timeout,
            retries=retries,
        )
        rows = data.get("pairHourDatas")
        if not isinstance(rows, list):
            raise RuntimeError(
                f"{source.name}: Expected 'pairHourDatas' list in v2 spike query response."
            )
        if not rows:
            break

        min_hour_seen: int | None = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            pair_id = _extract_pair_id(row.get("pair"))
            if not pair_id:
                continue
            hour_ts = to_int(row.get("hourStartUnix"))
            if hour_ts <= 0:
                continue
            row["__pair_id"] = pair_id
            raw_rows.append(row)
            pair_ids.add(pair_id)
            if min_hour_seen is None or hour_ts < min_hour_seen:
                min_hour_seen = hour_ts

        pages += 1
        if pages == 1 or pages % 10 == 0:
            print(
                f"{source.name}: progress pages={pages} rows={len(raw_rows):,} (hour<= {cursor_end})",
                file=sys.stderr,
            )

        if max_pages is not None and pages >= max_pages:
            break

        if min_hour_seen is None:
            break
        next_cursor = min_hour_seen - 1
        if next_cursor >= cursor_end:
            next_cursor = cursor_end - 1
        cursor_end = next_cursor

    pair_meta = _fetch_v2_pair_metadata(source, pair_ids, timeout=timeout, retries=retries)
    weth = source.weth_address.lower()
    reserve_weth_min_dec = Decimal(str(min_reserve_weth))
    observations: list[Observation] = []
    missing_meta = 0

    for row in raw_rows:
        pair_id = str(row.get("__pair_id", "")).lower()
        token0, token1 = pair_meta.get(pair_id, ("", ""))
        if not token0 or not token1:
            missing_meta += 1
            continue
        if token0 != weth and token1 != weth:
            continue

        reserve0 = to_decimal(row.get("reserve0"))
        reserve1 = to_decimal(row.get("reserve1"))
        if reserve0 <= 0 or reserve1 <= 0:
            continue
        fee0 = to_decimal(row.get("fee0"))
        fee1 = to_decimal(row.get("fee1"))
        hour_ts = to_int(row.get("hourStartUnix"))
        if hour_ts <= 0:
            continue
        swap_count = to_int(row.get("swapCount"))
        if swap_count < max(0, min_swap_count):
            continue

        if token0 == weth:
            fee_weth = fee0
            reserve_weth = reserve0
        else:
            fee_weth = fee1
            reserve_weth = reserve1

        if reserve_weth <= 0 or reserve_weth < reserve_weth_min_dec:
            continue

        score = fee_weth / reserve_weth
        observations.append(
            Observation(
                source_name=source.name,
                version=source.version,
                chain=source.chain,
                pool_id=pair_id,
                pair=f"{token0}/{token1}",
                fee_tier=3000,
                ts=hour_ts,
                volume_usd=float(max(0, swap_count)),
                tvl_usd=float(reserve_weth),
                fees_usd=float(fee_weth),
                hourly_yield=float(score),
            )
        )

    if missing_meta:
        print(
            f"{source.name}: skipped {missing_meta:,} rows missing pair metadata",
            file=sys.stderr,
        )
    return observations


def fetch_source_observations(
    source: SourceConfig,
    start_ts: int,
    end_ts: int,
    page_size: int,
    max_pages: int | None,
    timeout: int,
    retries: int,
    v2_spike_min_swap_count: int,
    v2_spike_min_reserve_weth: float,
) -> list[Observation]:
    if source.source_type == "v2_spike":
        return fetch_v2_spike_observations(
            source=source,
            start_ts=start_ts,
            end_ts=end_ts,
            page_size=page_size,
            max_pages=max_pages,
            timeout=timeout,
            retries=retries,
            min_swap_count=v2_spike_min_swap_count,
            min_reserve_weth=v2_spike_min_reserve_weth,
        )
    print(
        f"{source.name}: fetching window {iso_hour(start_ts)} -> {iso_hour(end_ts)}",
        file=sys.stderr,
    )
    observations: list[Observation] = []
    skip = 0
    after_id = ""
    pages = 0
    use_skip_pagination = "$skip" in source.hourly_query or "skip:" in source.hourly_query

    while True:
        variables: dict[str, Any] = {
            "first": page_size,
            "start": start_ts,
            "end": end_ts,
        }
        if use_skip_pagination:
            variables["skip"] = skip
        else:
            variables["afterId"] = after_id
        data = graphql_query(
            source=source,
            query=source.hourly_query,
            variables=variables,
            timeout=timeout,
            retries=retries,
        )

        raw_rows = data.get("hourly")
        if not isinstance(raw_rows, list):
            raise RuntimeError(
                (
                    f"{source.name}: Expected query result field 'hourly' as a list. "
                    "Use an alias in the GraphQL query: hourly: <pool-hour-entity>(...)"
                )
            )

        if not raw_rows:
            break

        last_row_id: str | None = None
        for raw in raw_rows:
            if not isinstance(raw, dict):
                continue
            row_id = raw.get("id")
            if isinstance(row_id, str) and row_id:
                last_row_id = row_id
            obs = normalize_row(source, raw)
            if obs is not None:
                observations.append(obs)

        pages += 1
        if pages == 1 or pages % 10 == 0:
            cursor_info = f"skip={skip}" if use_skip_pagination else f"afterId={after_id or '<start>'}"
            print(
                f"{source.name}: progress pages={pages} rows={len(observations):,} ({cursor_info})",
                file=sys.stderr,
            )
        if max_pages is not None and pages >= max_pages:
            break

        if len(raw_rows) < page_size:
            break

        if use_skip_pagination:
            skip += page_size
        else:
            if not last_row_id:
                raise RuntimeError(
                    (
                        f"{source.name}: Cursor pagination requires each 'hourly' row to include "
                        "a string 'id' field."
                    )
                )
            after_id = last_row_id

    return observations


def fetch_all_observations(
    sources: list[SourceConfig],
    start_ts: int,
    end_ts: int,
    page_size: int,
    max_pages: int | None,
    workers: int,
    parallel_window_hours: int,
    timeout: int,
    retries: int,
    strict_sources: bool,
    v2_spike_min_swap_count: int,
    v2_spike_min_reserve_weth: float,
) -> tuple[list[Observation], list[SourceFailure]]:
    all_obs: list[Observation] = []
    failures: list[SourceFailure] = []
    if not sources:
        return all_obs, failures

    tasks: list[tuple[SourceConfig, int, int]] = []
    for source in sources:
        if source.source_type == "hourly" and not source.backfill_source:
            for s, e in split_time_windows(start_ts, end_ts, parallel_window_hours):
                tasks.append((source, s, e))
        else:
            tasks.append((source, start_ts, end_ts))

    max_workers = max(1, min(workers, len(tasks)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                fetch_source_observations,
                source,
                shard_start,
                shard_end,
                effective_page_size(source, page_size),
                max_pages,
                timeout,
                retries,
                v2_spike_min_swap_count,
                v2_spike_min_reserve_weth,
            ): (source, shard_start, shard_end)
            for source, shard_start, shard_end in tasks
        }

        for future in as_completed(future_map):
            source, shard_start, shard_end = future_map[future]
            try:
                result = future.result()
            except Exception as err:  # noqa: BLE001
                failures.append(
                    SourceFailure(
                        source_name=source.name,
                        version=source.version,
                        chain=source.chain,
                        error=str(err),
                    )
                )
                if strict_sources:
                    raise
                print(
                    f"Source failed: {source.name} ({source.version}/{source.chain}) -> {err}",
                    file=sys.stderr,
                )
                continue
            print(
                (
                    f"Fetched {len(result):,} rows from {source.name} ({source.version}/{source.chain}) "
                    f"[{iso_hour(shard_start)} -> {iso_hour(shard_end)}]"
                ),
                file=sys.stderr,
            )
            all_obs.extend(result)

    return all_obs, failures


def ensure_cache_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
            source_name TEXT NOT NULL,
            version TEXT NOT NULL,
            chain TEXT NOT NULL,
            pool_id TEXT NOT NULL,
            pair TEXT NOT NULL,
            fee_tier INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            volume_usd REAL NOT NULL,
            tvl_usd REAL NOT NULL,
            fees_usd REAL NULL,
            hourly_yield REAL NULL,
            PRIMARY KEY (source_name, version, chain, pool_id, fee_tier, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_observations_time
        ON observations (ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_observations_source_time
        ON observations (source_name, version, chain, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_state (
            source_name TEXT NOT NULL,
            version TEXT NOT NULL,
            chain TEXT NOT NULL,
            last_checked_end_ts INTEGER NOT NULL,
            updated_at_ts INTEGER NOT NULL,
            PRIMARY KEY (source_name, version, chain)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_checkpoint (
            source_name TEXT NOT NULL,
            version TEXT NOT NULL,
            chain TEXT NOT NULL,
            mode TEXT NOT NULL,
            fetch_start_ts INTEGER NOT NULL,
            fetch_end_ts INTEGER NOT NULL,
            after_id TEXT NOT NULL DEFAULT '',
            skip INTEGER NOT NULL DEFAULT 0,
            cursor_end_ts INTEGER NULL,
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            rows_fetched INTEGER NOT NULL DEFAULT 0,
            updated_at_ts INTEGER NOT NULL,
            PRIMARY KEY (source_name, version, chain)
        )
        """
    )
    conn.commit()


def get_source_latest_ts(conn: sqlite3.Connection, source: SourceConfig) -> int | None:
    row = conn.execute(
        """
        SELECT MAX(ts) FROM observations
        WHERE source_name = ? AND version = ? AND chain = ?
        """,
        (source.name, source.version, source.chain),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def get_source_last_checked_end_ts(conn: sqlite3.Connection, source: SourceConfig) -> int | None:
    row = conn.execute(
        """
        SELECT last_checked_end_ts FROM source_state
        WHERE source_name = ? AND version = ? AND chain = ?
        """,
        (source.name, source.version, source.chain),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def upsert_source_last_checked_end_ts(
    conn: sqlite3.Connection, source: SourceConfig, end_ts: int
) -> None:
    now_ts = int(time.time())
    conn.execute(
        """
        INSERT INTO source_state (
            source_name, version, chain, last_checked_end_ts, updated_at_ts
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_name, version, chain)
        DO UPDATE SET
            last_checked_end_ts = excluded.last_checked_end_ts,
            updated_at_ts = excluded.updated_at_ts
        """,
        (source.name, source.version, source.chain, int(end_ts), now_ts),
    )
    conn.commit()


def get_source_checkpoint(
    conn: sqlite3.Connection,
    source: SourceConfig,
    mode: str,
    fetch_start_ts: int,
    fetch_end_ts: int,
) -> SourceCheckpoint | None:
    row = conn.execute(
        """
        SELECT
            mode, fetch_start_ts, fetch_end_ts, after_id, skip,
            cursor_end_ts, pages_fetched, rows_fetched
        FROM source_checkpoint
        WHERE source_name = ? AND version = ? AND chain = ?
        """,
        (source.name, source.version, source.chain),
    ).fetchone()
    if not row:
        return None
    if (
        str(row[0]) != mode
        or int(row[1]) != int(fetch_start_ts)
        or int(row[2]) != int(fetch_end_ts)
    ):
        return None
    return SourceCheckpoint(
        mode=str(row[0]),
        fetch_start_ts=int(row[1]),
        fetch_end_ts=int(row[2]),
        after_id=str(row[3] or ""),
        skip=int(row[4] or 0),
        cursor_end_ts=(int(row[5]) if row[5] is not None else None),
        pages_fetched=int(row[6] or 0),
        rows_fetched=int(row[7] or 0),
    )


def upsert_source_checkpoint(
    conn: sqlite3.Connection,
    source: SourceConfig,
    mode: str,
    fetch_start_ts: int,
    fetch_end_ts: int,
    after_id: str,
    skip: int,
    cursor_end_ts: int | None,
    pages_fetched: int,
    rows_fetched: int,
) -> None:
    now_ts = int(time.time())
    conn.execute(
        """
        INSERT INTO source_checkpoint (
            source_name, version, chain, mode, fetch_start_ts, fetch_end_ts,
            after_id, skip, cursor_end_ts, pages_fetched, rows_fetched, updated_at_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, version, chain)
        DO UPDATE SET
            mode = excluded.mode,
            fetch_start_ts = excluded.fetch_start_ts,
            fetch_end_ts = excluded.fetch_end_ts,
            after_id = excluded.after_id,
            skip = excluded.skip,
            cursor_end_ts = excluded.cursor_end_ts,
            pages_fetched = excluded.pages_fetched,
            rows_fetched = excluded.rows_fetched,
            updated_at_ts = excluded.updated_at_ts
        """,
        (
            source.name,
            source.version,
            source.chain,
            mode,
            int(fetch_start_ts),
            int(fetch_end_ts),
            after_id,
            int(skip),
            (int(cursor_end_ts) if cursor_end_ts is not None else None),
            int(pages_fetched),
            int(rows_fetched),
            now_ts,
        ),
    )
    conn.commit()


def clear_source_checkpoint(conn: sqlite3.Connection, source: SourceConfig) -> None:
    conn.execute(
        """
        DELETE FROM source_checkpoint
        WHERE source_name = ? AND version = ? AND chain = ?
        """,
        (source.name, source.version, source.chain),
    )
    conn.commit()


def write_observations_cache(conn: sqlite3.Connection, observations: Iterable[Observation]) -> int:
    rows = [
        (
            o.source_name,
            o.version,
            o.chain,
            o.pool_id,
            o.pair,
            o.fee_tier,
            o.ts,
            o.volume_usd,
            o.tvl_usd,
            o.fees_usd,
            o.hourly_yield,
        )
        for o in observations
    ]
    if not rows:
        return 0

    conn.executemany(
        """
        INSERT INTO observations (
            source_name, version, chain, pool_id, pair, fee_tier, ts,
            volume_usd, tvl_usd, fees_usd, hourly_yield
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, version, chain, pool_id, fee_tier, ts)
        DO UPDATE SET
            pair = excluded.pair,
            volume_usd = excluded.volume_usd,
            tvl_usd = excluded.tvl_usd,
            fees_usd = excluded.fees_usd,
            hourly_yield = excluded.hourly_yield
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def load_observations_cache(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> list[Observation]:
    rows = conn.execute(
        """
        SELECT
            source_name, version, chain, pool_id, pair, fee_tier, ts,
            volume_usd, tvl_usd, fees_usd, hourly_yield
        FROM observations
        WHERE ts >= ? AND ts < ?
        ORDER BY ts ASC
        """,
        (start_ts, end_ts),
    ).fetchall()

    observations: list[Observation] = []
    for row in rows:
        observations.append(
            Observation(
                source_name=str(row[0]),
                version=str(row[1]),
                chain=str(row[2]),
                pool_id=str(row[3]),
                pair=str(row[4]),
                fee_tier=int(row[5]),
                ts=int(row[6]),
                volume_usd=float(row[7]),
                tvl_usd=float(row[8]),
                fees_usd=(float(row[9]) if row[9] is not None else None),
                hourly_yield=(float(row[10]) if row[10] is not None else None),
            )
        )
    return observations


def fetch_source_observations_checkpointed(
    conn: sqlite3.Connection,
    source: SourceConfig,
    fetch_start: int,
    fetch_end: int,
    page_size: int,
    max_pages: int | None,
    timeout: int,
    retries: int,
    checkpoint_pages: int,
) -> int:
    # This checkpoint loop is intended for large backfill-style hourly sources.
    if source.source_type != "hourly":
        result = fetch_source_observations(
            source=source,
            start_ts=fetch_start,
            end_ts=fetch_end,
            page_size=page_size,
            max_pages=max_pages,
            timeout=timeout,
            retries=retries,
            v2_spike_min_swap_count=0,
            v2_spike_min_reserve_weth=0.0,
        )
        written = write_observations_cache(conn, result)
        upsert_source_last_checked_end_ts(conn, source, fetch_end)
        clear_source_checkpoint(conn, source)
        return written

    mode = "hourly"
    checkpoint = get_source_checkpoint(
        conn=conn,
        source=source,
        mode=mode,
        fetch_start_ts=fetch_start,
        fetch_end_ts=fetch_end,
    )

    use_skip_pagination = "$skip" in source.hourly_query or "skip:" in source.hourly_query
    skip = checkpoint.skip if (checkpoint and use_skip_pagination) else 0
    after_id = checkpoint.after_id if (checkpoint and not use_skip_pagination) else ""
    pages = checkpoint.pages_fetched if checkpoint else 0
    rows_total = checkpoint.rows_fetched if checkpoint else 0
    buffered: list[Observation] = []

    print(
        f"{source.name}: fetching window {iso_hour(fetch_start)} -> {iso_hour(fetch_end)}"
        + (f" [resume pages={pages} rows={rows_total:,}]" if checkpoint else ""),
        file=sys.stderr,
    )

    while True:
        if max_pages is not None and pages >= max_pages:
            break

        variables: dict[str, Any] = {
            "first": page_size,
            "start": fetch_start,
            "end": fetch_end,
        }
        if use_skip_pagination:
            variables["skip"] = skip
        else:
            variables["afterId"] = after_id

        data = graphql_query(
            source=source,
            query=source.hourly_query,
            variables=variables,
            timeout=timeout,
            retries=retries,
        )

        raw_rows = data.get("hourly")
        if not isinstance(raw_rows, list):
            raise RuntimeError(
                (
                    f"{source.name}: Expected query result field 'hourly' as a list. "
                    "Use an alias in the GraphQL query: hourly: <pool-hour-entity>(...)"
                )
            )
        if not raw_rows:
            break

        page_obs: list[Observation] = []
        last_row_id: str | None = None
        for raw in raw_rows:
            if not isinstance(raw, dict):
                continue
            row_id = raw.get("id")
            if isinstance(row_id, str) and row_id:
                last_row_id = row_id
            obs = normalize_row(source, raw)
            if obs is not None:
                page_obs.append(obs)

        buffered.extend(page_obs)
        rows_total += len(page_obs)
        pages += 1

        if pages == 1 or pages % 10 == 0:
            cursor_info = f"skip={skip}" if use_skip_pagination else f"afterId={after_id or '<start>'}"
            print(
                f"{source.name}: progress pages={pages} rows={rows_total:,} ({cursor_info})",
                file=sys.stderr,
            )

        has_more = len(raw_rows) >= page_size
        next_skip = skip
        next_after_id = after_id
        if has_more:
            if use_skip_pagination:
                next_skip = skip + page_size
            else:
                if not last_row_id:
                    raise RuntimeError(
                        (
                            f"{source.name}: Cursor pagination requires each 'hourly' row to include "
                            "a string 'id' field."
                        )
                    )
                next_after_id = last_row_id

        should_checkpoint = checkpoint_pages > 0 and (pages % checkpoint_pages == 0)
        if should_checkpoint or not has_more:
            if buffered:
                write_observations_cache(conn, buffered)
                buffered.clear()
            upsert_source_checkpoint(
                conn=conn,
                source=source,
                mode=mode,
                fetch_start_ts=fetch_start,
                fetch_end_ts=fetch_end,
                after_id=next_after_id if not use_skip_pagination else "",
                skip=next_skip if use_skip_pagination else 0,
                cursor_end_ts=None,
                pages_fetched=pages,
                rows_fetched=rows_total,
            )
        if not has_more:
            break

        skip = next_skip
        after_id = next_after_id

    if buffered:
        write_observations_cache(conn, buffered)

    upsert_source_last_checked_end_ts(conn, source, fetch_end)
    clear_source_checkpoint(conn, source)
    return rows_total


def fetch_with_cache(
    sources: list[SourceConfig],
    start_ts: int,
    end_ts: int,
    page_size: int,
    max_pages: int | None,
    workers: int,
    parallel_window_hours: int,
    timeout: int,
    retries: int,
    strict_sources: bool,
    conn: sqlite3.Connection,
    overlap_hours: int,
    checkpoint_pages: int,
    v2_spike_min_swap_count: int,
    v2_spike_min_reserve_weth: float,
) -> tuple[list[Observation], list[SourceFailure], int]:
    overlap_seconds = max(0, overlap_hours) * SECONDS_PER_HOUR

    fetch_plan: list[tuple[SourceConfig, int, int]] = []
    for source in sources:
        if source.source_type == "v2_spike":
            # V2 spike sources are sparse and "no-row" windows are common.
            # Always scan the full requested window so historical spikes remain visible.
            fetch_start = start_ts
            fetch_end = end_ts
            if fetch_start < fetch_end:
                fetch_plan.append((source, fetch_start, fetch_end))
            continue
        latest_ts = get_source_latest_ts(conn, source)
        last_checked_end_ts = get_source_last_checked_end_ts(conn, source)
        if latest_ts is None:
            # If we have never cached rows for this source but we have already
            # checked it before, avoid re-querying the entire historical window.
            if last_checked_end_ts is not None:
                fetch_start = max(start_ts, last_checked_end_ts - overlap_seconds)
            else:
                fetch_start = start_ts
        else:
            fetch_start = max(start_ts, latest_ts - overlap_seconds)
        fetch_end = end_ts
        if fetch_start >= fetch_end:
            continue
        fetch_plan.append((source, fetch_start, fetch_end))

    fetched: list[Observation] = []
    failures: list[SourceFailure] = []
    written_checkpointed = 0
    checkpoint_plan = [
        entry for entry in fetch_plan if entry[0].backfill_source and entry[0].source_type == "hourly"
    ]
    parallel_plan = [entry for entry in fetch_plan if entry not in checkpoint_plan]
    parallel_tasks: list[tuple[SourceConfig, int, int]] = []
    for source, fetch_start, fetch_end in parallel_plan:
        if source.source_type == "hourly" and not source.backfill_source:
            for s, e in split_time_windows(fetch_start, fetch_end, parallel_window_hours):
                parallel_tasks.append((source, s, e))
        else:
            parallel_tasks.append((source, fetch_start, fetch_end))

    if parallel_tasks:
        max_workers = max(1, min(workers, len(parallel_tasks)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    fetch_source_observations,
                    source,
                    shard_start,
                    shard_end,
                    effective_page_size(source, page_size),
                    max_pages,
                    timeout,
                    retries,
                    v2_spike_min_swap_count,
                    v2_spike_min_reserve_weth,
                ): (source, shard_start, shard_end)
                for source, shard_start, shard_end in parallel_tasks
            }
            for future in as_completed(future_map):
                source, shard_start, shard_end = future_map[future]
                try:
                    result = future.result()
                except Exception as err:  # noqa: BLE001
                    failures.append(
                        SourceFailure(
                            source_name=source.name,
                            version=source.version,
                            chain=source.chain,
                            error=str(err),
                        )
                    )
                    if strict_sources:
                        raise
                    print(
                        f"Source failed: {source.name} ({source.version}/{source.chain}) -> {err}",
                        file=sys.stderr,
                    )
                    continue
                # Mark source as checked even when no rows were returned.
                upsert_source_last_checked_end_ts(conn, source, end_ts)
                print(
                    (
                        f"Fetched {len(result):,} rows from {source.name} ({source.version}/{source.chain}) "
                        f"[cache mode {iso_hour(shard_start)} -> {iso_hour(shard_end)}]"
                    ),
                    file=sys.stderr,
                )
                fetched.extend(result)

    for source, fetch_start, fetch_end in checkpoint_plan:
        try:
            written = fetch_source_observations_checkpointed(
                conn=conn,
                source=source,
                fetch_start=fetch_start,
                fetch_end=fetch_end,
                page_size=effective_page_size(source, page_size),
                max_pages=max_pages,
                timeout=timeout,
                retries=retries,
                checkpoint_pages=max(1, checkpoint_pages),
            )
        except Exception as err:  # noqa: BLE001
            failures.append(
                SourceFailure(
                    source_name=source.name,
                    version=source.version,
                    chain=source.chain,
                    error=str(err),
                )
            )
            if strict_sources:
                raise
            print(
                f"Source failed: {source.name} ({source.version}/{source.chain}) -> {err}",
                file=sys.stderr,
            )
            continue
        print(
            f"Fetched {written:,} rows from {source.name} ({source.version}/{source.chain}) [cache mode + checkpoint]",
            file=sys.stderr,
        )
        written_checkpointed += written

    written = write_observations_cache(conn, fetched) + written_checkpointed
    cached_window = load_observations_cache(conn, start_ts, end_ts)
    return cached_window, failures, written


def rank_pools(
    observations: Iterable[Observation],
    min_samples: int,
    min_tvl_usd: float = 0.0,
    max_hourly_yield_pct: float | None = None,
    trim_ratio: float = 0.10,
    ranking_stats_min_tvl_usd: float = 0.0,
    winsorize_percentile: float = 0.99,
    diagnostics_out: list[PoolRankingDiagnostic] | None = None,
) -> list[PoolRanking]:
    grouped: dict[tuple[str, str, str, str, str, int], list[Observation]] = {}
    for obs in observations:
        key = (
            obs.source_name,
            obs.version,
            obs.chain,
            obs.pool_id,
            obs.pair,
            obs.fee_tier,
        )
        grouped.setdefault(key, []).append(obs)

    rankings: list[PoolRanking] = []
    if diagnostics_out is not None:
        diagnostics_out.clear()
    for key, rows in grouped.items():
        raw_yield_rows = [r for r in rows if r.hourly_yield is not None and r.hourly_yield >= 0]
        samples_raw = len(raw_yield_rows)
        effective_tvl_floor = max(0.0, min_tvl_usd, ranking_stats_min_tvl_usd)
        yield_rows = raw_yield_rows
        if effective_tvl_floor > 0:
            yield_rows = [r for r in yield_rows if r.tvl_usd >= effective_tvl_floor]
        samples_after_tvl_floor = len(yield_rows)
        if max_hourly_yield_pct is not None:
            max_yield_ratio = max(0.0, max_hourly_yield_pct) / 100.0
            yield_rows = [r for r in yield_rows if (r.hourly_yield or 0.0) <= max_yield_ratio]
        samples_after_hard_cap = len(yield_rows)
        if len(yield_rows) < min_samples:
            continue

        yield_values_raw = [r.hourly_yield for r in yield_rows if r.hourly_yield is not None]
        fees_values = [r.fees_usd for r in yield_rows if r.fees_usd is not None]
        tvl_values = [r.tvl_usd for r in yield_rows if r.tvl_usd > 0]

        if not yield_values_raw or not tvl_values or not fees_values:
            continue
        p = max(0.0, min(1.0, winsorize_percentile))
        winsor_cap = percentile(yield_values_raw, p)
        yield_values = [min(v, winsor_cap) for v in yield_values_raw]
        capped_hours = sum(1 for v in yield_values_raw if v > winsor_cap)

        observed_hours = len(yield_rows)
        observed_days = observed_hours / 24
        total_fees_usd = sum(fees_values)
        fee_period_start_ts = min(r.ts for r in yield_rows)
        fee_period_end_ts = max(r.ts for r in yield_rows) + SECONDS_PER_HOUR

        hour_buckets: dict[int, list[float]] = {}
        day_buckets: dict[int, list[float]] = {}
        window_buckets: dict[tuple[int, int], list[float]] = {}

        for r in yield_rows:
            if r.hourly_yield is None:
                continue
            dt_utc = dt.datetime.fromtimestamp(r.ts, tz=dt.timezone.utc)
            hour_buckets.setdefault(dt_utc.hour, []).append(r.hourly_yield)
            day_buckets.setdefault(dt_utc.weekday(), []).append(r.hourly_yield)
            window_buckets.setdefault((dt_utc.weekday(), dt_utc.hour), []).append(r.hourly_yield)

        best_hour = max(hour_buckets, key=lambda h: statistics.fmean(hour_buckets[h]))
        best_day_idx = max(day_buckets, key=lambda d: statistics.fmean(day_buckets[d]))
        best_window_key = max(
            window_buckets,
            key=lambda wh: statistics.fmean(window_buckets[wh]),
        )
        best_window = f"{DAY_NAMES[best_window_key[0]]} {best_window_key[1]:02d}:00 UTC"
        best_window_rows = []
        for r in yield_rows:
            if r.hourly_yield is None:
                continue
            dt_utc = dt.datetime.fromtimestamp(r.ts, tz=dt.timezone.utc)
            if dt_utc.weekday() == best_window_key[0] and dt_utc.hour == best_window_key[1]:
                best_window_rows.append(r)
        if best_window_rows:
            # Pick the strongest observed occurrence for this best recurring window.
            best_window_observation = max(
                best_window_rows,
                key=lambda r: (r.hourly_yield if r.hourly_yield is not None else -1.0, r.ts),
            )
        else:
            best_window_observation = max(
                yield_rows,
                key=lambda r: (r.hourly_yield if r.hourly_yield is not None else -1.0, r.ts),
            )
        best_window_start_ts = best_window_observation.ts
        best_window_end_ts = best_window_start_ts + SECONDS_PER_HOUR

        avg_y = statistics.fmean(yield_values)
        median_y = statistics.median(yield_values)
        trimmed_y = trimmed_mean(yield_values, trim_ratio)
        p90_y = percentile(yield_values, 0.90)
        max_y = max(yield_values)
        outlier_hours = robust_outlier_count(yield_values_raw)

        # Composite score favors robust central tendency and durability over spikes.
        score = (p90_y * 0.45) + (trimmed_y * 0.35) + (median_y * 0.20)

        rankings.append(
            PoolRanking(
                source_name=key[0],
                version=key[1],
                chain=key[2],
                pool_id=key[3],
                pair=key[4],
                fee_tier=key[5],
                samples=len(yield_rows),
                observed_hours=observed_hours,
                observed_days=observed_days,
                total_fees_usd=total_fees_usd,
                fee_period_start_ts=fee_period_start_ts,
                fee_period_end_ts=fee_period_end_ts,
                avg_hourly_yield_pct=avg_y * 100,
                median_hourly_yield_pct=median_y * 100,
                trimmed_mean_hourly_yield_pct=trimmed_y * 100,
                p90_hourly_yield_pct=p90_y * 100,
                max_hourly_yield_pct=max_y * 100,
                outlier_hours=outlier_hours,
                avg_hourly_fees_usd=statistics.fmean(fees_values),
                avg_tvl_usd=statistics.fmean(tvl_values),
                best_hour_utc=best_hour,
                best_day_utc=DAY_NAMES[best_day_idx],
                best_window_utc=best_window,
                best_window_start_ts=best_window_start_ts,
                best_window_end_ts=best_window_end_ts,
                score=score,
            )
        )
        if diagnostics_out is not None:
            rules: list[str] = []
            if effective_tvl_floor > min_tvl_usd:
                rules.append("ranking_tvl_floor")
            if capped_hours > 0:
                rules.append("winsorized")
            if max_hourly_yield_pct is not None and samples_after_hard_cap < samples_after_tvl_floor:
                rules.append("hard_cap")
            diagnostics_out.append(
                PoolRankingDiagnostic(
                    source_name=key[0],
                    version=key[1],
                    chain=key[2],
                    pool_id=key[3],
                    pair=key[4],
                    fee_tier=key[5],
                    samples_raw=samples_raw,
                    samples_after_tvl_floor=samples_after_tvl_floor,
                    samples_after_hard_cap=samples_after_hard_cap,
                    ranking_tvl_floor_usd=effective_tvl_floor,
                    winsorize_percentile=p,
                    winsorize_cap_hourly_yield_pct=winsor_cap * 100.0,
                    capped_hours=capped_hours,
                    max_raw_hourly_yield_pct=max(yield_values_raw) * 100.0,
                    max_capped_hourly_yield_pct=max_y * 100.0,
                    outlier_hours_raw=outlier_hours,
                    rule_triggered=("+".join(rules) if rules else "none"),
                )
            )

    rankings.sort(key=lambda r: r.score, reverse=True)
    return rankings


def iso_hour(ts: int) -> str:
    dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:00:00 UTC")


def usd_per_1000_from_yield_pct(yield_pct: float) -> float:
    return (yield_pct / 100.0) * 1000.0


def infer_exchange_name(source_name: str) -> str:
    lowered = source_name.lower()
    if "sushi" in lowered:
        return "Sushi"
    if "uniswap" in lowered:
        return "Uniswap"
    if "pancake" in lowered:
        return "PancakeSwap"
    if "aerodrome" in lowered:
        return "Aerodrome"
    return "Unknown"


def _weekly_hour_index(ts: int) -> int:
    dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    return (dt_utc.weekday() * 24) + dt_utc.hour


def _schedule_start_weekly_hour(schedule: ScheduleRecommendation) -> int:
    return (DAY_NAMES.index(schedule.add_day_utc) * 24) + schedule.add_hour_utc


def is_schedule_active_at(schedule: ScheduleRecommendation, ts: int) -> bool:
    start = _schedule_start_weekly_hour(schedule)
    end = start + schedule.block_hours
    hour_idx = _weekly_hour_index(ts)
    if end <= 24 * 7:
        return start <= hour_idx < end
    wrapped_end = end - (24 * 7)
    return hour_idx >= start or hour_idx < wrapped_end


def select_jump_now_schedules(
    schedules: list[ScheduleRecommendation],
    now_ts: int,
    soon_hours: int,
    top_n: int,
) -> list[tuple[ScheduleRecommendation, str, int]]:
    soon_seconds = max(0, soon_hours) * SECONDS_PER_HOUR
    candidates: list[tuple[ScheduleRecommendation, str, int]] = []

    for schedule in schedules:
        if is_schedule_active_at(schedule, now_ts):
            candidates.append((schedule, "ACTIVE NOW", 0))
            continue

        eta_seconds = schedule.next_add_ts - now_ts
        if 0 <= eta_seconds <= soon_seconds:
            candidates.append((schedule, "STARTING SOON", eta_seconds))

    candidates.sort(
        key=lambda item: (
            0 if item[1] == "ACTIVE NOW" else 1,
            item[2],
            -item[0].avg_block_hourly_yield_pct,
            -item[0].reliability_hit_rate_pct,
        )
    )
    return candidates[: max(0, top_n)]


def format_eta(seconds: int) -> str:
    if seconds <= 0:
        return "now"
    hours = seconds // SECONDS_PER_HOUR
    minutes = (seconds % SECONDS_PER_HOUR) // 60
    if hours <= 0:
        return f"{minutes}m"
    return f"{hours}h {minutes}m"


def write_data_quality_audit_csv(
    path: Path,
    input_rows: int,
    output_rows: int,
    rejected_counts: dict[tuple[str, str, str, str], int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_reason: dict[str, int] = defaultdict(int)
    for (_, _, _, reason), count in rejected_counts.items():
        by_reason[reason] += count

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "scope",
                "source_name",
                "version",
                "chain",
                "reason",
                "count",
                "pct_of_input_rows",
                "input_rows",
                "output_rows",
            ]
        )
        writer.writerow(
            [
                "summary",
                "ALL",
                "ALL",
                "ALL",
                "input_rows",
                input_rows,
                "100.000000",
                input_rows,
                output_rows,
            ]
        )
        writer.writerow(
            [
                "summary",
                "ALL",
                "ALL",
                "ALL",
                "output_rows",
                output_rows,
                f"{(100.0 * output_rows / input_rows) if input_rows else 0.0:.6f}",
                input_rows,
                output_rows,
            ]
        )
        for reason, count in sorted(by_reason.items(), key=lambda kv: (-kv[1], kv[0])):
            writer.writerow(
                [
                    "reason_total",
                    "ALL",
                    "ALL",
                    "ALL",
                    reason,
                    count,
                    f"{(100.0 * count / input_rows) if input_rows else 0.0:.6f}",
                    input_rows,
                    output_rows,
                ]
            )
        for (source, version, chain, reason), count in sorted(
            rejected_counts.items(),
            key=lambda kv: (-kv[1], kv[0][0], kv[0][1], kv[0][2], kv[0][3]),
        ):
            writer.writerow(
                [
                    "source_reason",
                    source,
                    version,
                    chain,
                    reason,
                    count,
                    f"{(100.0 * count / input_rows) if input_rows else 0.0:.6f}",
                    input_rows,
                    output_rows,
                ]
            )


def next_weekly_occurrence(after_ts: int, weekday: int, hour: int) -> int:
    base = dt.datetime.fromtimestamp(after_ts, tz=dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    days_ahead = (weekday - base.weekday()) % 7
    candidate = base + dt.timedelta(days=days_ahead)
    candidate = candidate.replace(hour=hour)
    if candidate <= base:
        candidate += dt.timedelta(days=7)
    return int(candidate.timestamp())


def build_liquidity_schedule(
    rankings: list[PoolRanking],
    observations: Iterable[Observation],
    end_ts: int,
    top_pools: int,
    quantile: float,
    min_usd_per_1000_hour: float | None,
    min_hit_rate: float,
    min_occurrences: int,
    max_blocks_per_pool: int,
) -> list[ScheduleRecommendation]:
    top_rankings = rankings[: max(0, top_pools)]
    if not top_rankings:
        return []

    quantile = max(0.0, min(1.0, quantile))
    if min_usd_per_1000_hour is not None:
        min_usd_per_1000_hour = max(0.0, min_usd_per_1000_hour)
    min_hit_rate = max(0.0, min(1.0, min_hit_rate))
    min_occurrences = max(1, min_occurrences)
    max_blocks_per_pool = max(1, max_blocks_per_pool)

    ranking_map: dict[tuple[str, str, str, str, str, int], tuple[int, PoolRanking]] = {}
    for idx, ranking in enumerate(top_rankings, start=1):
        key = (
            ranking.source_name,
            ranking.version,
            ranking.chain,
            ranking.pool_id,
            ranking.pair,
            ranking.fee_tier,
        )
        ranking_map[key] = (idx, ranking)

    grouped_rows: dict[tuple[str, str, str, str, str, int], list[Observation]] = {}
    for obs in observations:
        key = (
            obs.source_name,
            obs.version,
            obs.chain,
            obs.pool_id,
            obs.pair,
            obs.fee_tier,
        )
        if key in ranking_map:
            grouped_rows.setdefault(key, []).append(obs)

    recommendations: list[ScheduleRecommendation] = []
    for key, (pool_rank, ranking) in ranking_map.items():
        rows = grouped_rows.get(key, [])
        yield_rows = [r for r in rows if r.hourly_yield is not None and r.hourly_yield >= 0]
        if len(yield_rows) < min_occurrences:
            continue

        yield_values = [r.hourly_yield for r in yield_rows if r.hourly_yield is not None]
        if not yield_values:
            continue
        if min_usd_per_1000_hour is not None:
            threshold = min_usd_per_1000_hour / 1000.0
        else:
            threshold = percentile(yield_values, quantile)

        bucket_values: dict[tuple[int, int], list[float]] = {}
        for row in yield_rows:
            if row.hourly_yield is None:
                continue
            dt_utc = dt.datetime.fromtimestamp(row.ts, tz=dt.timezone.utc)
            bucket_values.setdefault((dt_utc.weekday(), dt_utc.hour), []).append(row.hourly_yield)

        reliable_hours: dict[int, list[int]] = {}
        for (weekday, hour), values in bucket_values.items():
            occurrences = len(values)
            if occurrences < min_occurrences:
                continue
            hit_rate = sum(1 for value in values if value >= threshold) / occurrences
            avg_value = statistics.fmean(values)
            if hit_rate >= min_hit_rate and avg_value >= threshold:
                reliable_hours.setdefault(weekday, []).append(hour)

        pool_blocks: list[ScheduleRecommendation] = []
        for weekday, hours in reliable_hours.items():
            if not hours:
                continue
            sorted_hours = sorted(set(hours))
            block_start = sorted_hours[0]
            prev = sorted_hours[0]

            def emit_block(start_hour: int, end_hour: int) -> None:
                block_values: list[float] = []
                bucket_occurrences: list[int] = []
                hits = 0
                total = 0
                for bucket_hour in range(start_hour, end_hour):
                    values = bucket_values.get((weekday, bucket_hour), [])
                    if not values:
                        return
                    block_values.extend(values)
                    bucket_occurrences.append(len(values))
                    hits += sum(1 for value in values if value >= threshold)
                    total += len(values)
                if not block_values or total == 0:
                    return
                reliable_occurrences = min(bucket_occurrences)
                if reliable_occurrences < min_occurrences:
                    return
                hit_rate = hits / total
                if hit_rate < min_hit_rate:
                    return

                block_hours = end_hour - start_hour
                remove_day_idx = (weekday + (1 if end_hour == 24 else 0)) % 7
                remove_hour = end_hour % 24
                add_pattern_utc = f"{DAY_NAMES[weekday]} {start_hour:02d}:00 UTC"
                remove_pattern_utc = f"{DAY_NAMES[remove_day_idx]} {remove_hour:02d}:00 UTC"
                next_add_ts = next_weekly_occurrence(end_ts, weekday, start_hour)
                next_remove_ts = next_add_ts + (block_hours * SECONDS_PER_HOUR)

                pool_blocks.append(
                    ScheduleRecommendation(
                        pool_rank=pool_rank,
                        source_name=ranking.source_name,
                        version=ranking.version,
                        chain=ranking.chain,
                        pool_id=ranking.pool_id,
                        pair=ranking.pair,
                        fee_tier=ranking.fee_tier,
                        reliability_hit_rate_pct=hit_rate * 100,
                        reliable_occurrences=reliable_occurrences,
                        threshold_hourly_yield_pct=threshold * 100,
                        avg_block_hourly_yield_pct=statistics.fmean(block_values) * 100,
                        p90_block_hourly_yield_pct=percentile(block_values, 0.90) * 100,
                        block_hours=block_hours,
                        add_day_utc=DAY_NAMES[weekday],
                        add_hour_utc=start_hour,
                        remove_day_utc=DAY_NAMES[remove_day_idx],
                        remove_hour_utc=remove_hour,
                        add_pattern_utc=add_pattern_utc,
                        remove_pattern_utc=remove_pattern_utc,
                        next_add_ts=next_add_ts,
                        next_remove_ts=next_remove_ts,
                        pool_score=ranking.score,
                    )
                )

            for hour in sorted_hours[1:]:
                if hour == prev + 1:
                    prev = hour
                    continue
                emit_block(block_start, prev + 1)
                block_start = hour
                prev = hour
            emit_block(block_start, prev + 1)

        pool_blocks.sort(
            key=lambda block: (
                block.reliability_hit_rate_pct,
                block.avg_block_hourly_yield_pct,
                block.reliable_occurrences,
                block.block_hours,
            ),
            reverse=True,
        )
        recommendations.extend(pool_blocks[:max_blocks_per_pool])

    recommendations.sort(
        key=lambda block: (
            block.pool_rank,
            -block.reliability_hit_rate_pct,
            -block.avg_block_hourly_yield_pct,
            -block.reliable_occurrences,
        )
    )
    return recommendations


def write_hourly_csv(path: Path, observations: Iterable[Observation]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "chain",
                "pool_id",
                "pair",
                "fee_tier",
                "timestamp",
                "hour_utc",
                "volume_usd",
                "tvl_usd",
                "fees_usd",
                "hourly_yield_pct",
                "fees_usd_per_1000_liquidity_hourly",
                "annualized_apr_pct",
            ]
        )
        for o in observations:
            hourly_yield_pct = o.hourly_yield * 100 if o.hourly_yield is not None else ""
            fees_per_1000 = (
                usd_per_1000_from_yield_pct(hourly_yield_pct)
                if hourly_yield_pct != ""
                else ""
            )
            apr_pct = o.hourly_yield * 24 * 365 * 100 if o.hourly_yield is not None else ""
            fees_usd = f"{o.fees_usd:.10f}" if o.fees_usd is not None else ""
            writer.writerow(
                [
                    o.source_name,
                    o.version,
                    o.chain,
                    o.pool_id,
                    o.pair,
                    o.fee_tier,
                    o.ts,
                    iso_hour(o.ts),
                    f"{o.volume_usd:.10f}",
                    f"{o.tvl_usd:.10f}",
                    fees_usd,
                    f"{hourly_yield_pct:.10f}" if hourly_yield_pct != "" else "",
                    f"{fees_per_1000:.10f}" if fees_per_1000 != "" else "",
                    f"{apr_pct:.10f}" if apr_pct != "" else "",
                ]
            )


def write_rankings_csv(path: Path, rankings: Iterable[PoolRanking]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "chain",
                "pool_id",
                "pair",
                "fee_tier",
                "samples",
                "observed_hours",
                "observed_days",
                "total_fees_usd",
                "fee_period_start_utc",
                "fee_period_end_utc",
                "avg_hourly_yield_pct",
                "median_hourly_yield_pct",
                "trimmed_mean_hourly_yield_pct",
                "p90_hourly_yield_pct",
                "max_hourly_yield_pct",
                "outlier_hours",
                "avg_hourly_usd_per_1000_liquidity",
                "median_hourly_usd_per_1000_liquidity",
                "trimmed_mean_hourly_usd_per_1000_liquidity",
                "p90_hourly_usd_per_1000_liquidity",
                "max_hourly_usd_per_1000_liquidity",
                "avg_hourly_fees_usd",
                "avg_tvl_usd",
                "best_hour_utc",
                "best_day_utc",
                "best_window_utc",
                "best_window_start_utc",
                "best_window_end_utc",
                "score",
            ]
        )
        for r in rankings:
            writer.writerow(
                [
                    r.source_name,
                    r.version,
                    r.chain,
                    r.pool_id,
                    r.pair,
                    r.fee_tier,
                    r.samples,
                    r.observed_hours,
                    f"{r.observed_days:.4f}",
                    f"{r.total_fees_usd:.10f}",
                    iso_hour(r.fee_period_start_ts),
                    iso_hour(r.fee_period_end_ts),
                    f"{r.avg_hourly_yield_pct:.10f}",
                    f"{r.median_hourly_yield_pct:.10f}",
                    f"{r.trimmed_mean_hourly_yield_pct:.10f}",
                    f"{r.p90_hourly_yield_pct:.10f}",
                    f"{r.max_hourly_yield_pct:.10f}",
                    r.outlier_hours,
                    f"{usd_per_1000_from_yield_pct(r.avg_hourly_yield_pct):.10f}",
                    f"{usd_per_1000_from_yield_pct(r.median_hourly_yield_pct):.10f}",
                    f"{usd_per_1000_from_yield_pct(r.trimmed_mean_hourly_yield_pct):.10f}",
                    f"{usd_per_1000_from_yield_pct(r.p90_hourly_yield_pct):.10f}",
                    f"{usd_per_1000_from_yield_pct(r.max_hourly_yield_pct):.10f}",
                    f"{r.avg_hourly_fees_usd:.10f}",
                    f"{r.avg_tvl_usd:.10f}",
                    r.best_hour_utc,
                    r.best_day_utc,
                    r.best_window_utc,
                    iso_hour(r.best_window_start_ts),
                    iso_hour(r.best_window_end_ts),
                    f"{r.score:.12f}",
                ]
            )


def write_pool_rankings_diagnostics_csv(
    path: Path, rows: Iterable[PoolRankingDiagnostic]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "chain",
                "pool_id",
                "pair",
                "fee_tier",
                "samples_raw",
                "samples_after_tvl_floor",
                "samples_after_hard_cap",
                "ranking_tvl_floor_usd",
                "winsorize_percentile",
                "winsorize_cap_hourly_yield_pct",
                "capped_hours",
                "max_raw_hourly_yield_pct",
                "max_capped_hourly_yield_pct",
                "outlier_hours_raw",
                "rule_triggered",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.source_name,
                    r.version,
                    r.chain,
                    r.pool_id,
                    r.pair,
                    r.fee_tier,
                    r.samples_raw,
                    r.samples_after_tvl_floor,
                    r.samples_after_hard_cap,
                    f"{r.ranking_tvl_floor_usd:.6f}",
                    f"{r.winsorize_percentile:.6f}",
                    f"{r.winsorize_cap_hourly_yield_pct:.10f}",
                    r.capped_hours,
                    f"{r.max_raw_hourly_yield_pct:.10f}",
                    f"{r.max_capped_hourly_yield_pct:.10f}",
                    r.outlier_hours_raw,
                    r.rule_triggered,
                ]
            )


def write_schedule_csv(path: Path, schedules: Iterable[ScheduleRecommendation]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "pool_rank",
                "source_name",
                "version",
                "chain",
                "pool_id",
                "pair",
                "fee_tier",
                "reliability_hit_rate_pct",
                "reliable_occurrences",
                "threshold_hourly_yield_pct",
                "threshold_hourly_usd_per_1000_liquidity",
                "avg_block_hourly_yield_pct",
                "p90_block_hourly_yield_pct",
                "avg_block_hourly_usd_per_1000_liquidity",
                "p90_block_hourly_usd_per_1000_liquidity",
                "block_hours",
                "add_day_utc",
                "add_hour_utc",
                "remove_day_utc",
                "remove_hour_utc",
                "add_pattern_utc",
                "remove_pattern_utc",
                "next_add_utc",
                "next_remove_utc",
                "pool_score",
            ]
        )
        for schedule in schedules:
            writer.writerow(
                [
                    schedule.pool_rank,
                    schedule.source_name,
                    schedule.version,
                    schedule.chain,
                    schedule.pool_id,
                    schedule.pair,
                    schedule.fee_tier,
                    f"{schedule.reliability_hit_rate_pct:.6f}",
                    schedule.reliable_occurrences,
                    f"{schedule.threshold_hourly_yield_pct:.6f}",
                    f"{usd_per_1000_from_yield_pct(schedule.threshold_hourly_yield_pct):.6f}",
                    f"{schedule.avg_block_hourly_yield_pct:.6f}",
                    f"{schedule.p90_block_hourly_yield_pct:.6f}",
                    f"{usd_per_1000_from_yield_pct(schedule.avg_block_hourly_yield_pct):.6f}",
                    f"{usd_per_1000_from_yield_pct(schedule.p90_block_hourly_yield_pct):.6f}",
                    schedule.block_hours,
                    schedule.add_day_utc,
                    schedule.add_hour_utc,
                    schedule.remove_day_utc,
                    schedule.remove_hour_utc,
                    schedule.add_pattern_utc,
                    schedule.remove_pattern_utc,
                    iso_hour(schedule.next_add_ts),
                    iso_hour(schedule.next_remove_ts),
                    f"{schedule.pool_score:.12f}",
                ]
            )


def write_schedule_md(
    path: Path,
    schedules: list[ScheduleRecommendation],
    end_ts: int,
    top_n: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Recommended Liquidity Schedule")
    lines.append("")
    lines.append(
        f"- Built from recurring high-yield windows observed before {iso_hour(end_ts)}"
    )
    lines.append("- Add liquidity at `next_add_utc`; remove at `next_remove_utc`.")
    lines.append("")

    if not schedules:
        lines.append("No reliable recurring high-yield windows found.")
    else:
        rows = schedules[:top_n]
        lines.append("## Top Schedule Blocks")
        lines.append("")
        lines.append(
            "| Pool Rank | Version | Pair | Add Pattern (UTC) | Remove Pattern (UTC) | Next Add (UTC) | Next Remove (UTC) | Hit Rate % | Avg Block Hourly Yield % | Avg Block USD per $1k/hr | Block Hours |"
        )
        lines.append(
            "|---:|---|---|---|---|---|---|---:|---:|---:|---:|"
        )
        for schedule in rows:
            lines.append(
                "| "
                f"{schedule.pool_rank} | {schedule.version} | {schedule.pair} | "
                f"{schedule.add_pattern_utc} | {schedule.remove_pattern_utc} | "
                f"{iso_hour(schedule.next_add_ts)} | {iso_hour(schedule.next_remove_ts)} | "
                f"{schedule.reliability_hit_rate_pct:.2f} | {schedule.avg_block_hourly_yield_pct:.6f} | "
                f"{usd_per_1000_from_yield_pct(schedule.avg_block_hourly_yield_pct):.6f} | "
                f"{schedule.block_hours} |"
            )

    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def build_v2_spike_rows(
    observations: Iterable[Observation],
    v2_spike_sources: set[str],
    min_swap_count: int,
    min_reserve_weth: float,
) -> list[V2YieldSpikeRow]:
    rows: list[V2YieldSpikeRow] = []
    for obs in observations:
        if obs.source_name not in v2_spike_sources:
            continue
        token0, token1 = "", ""
        if "/" in obs.pair:
            token0, token1 = obs.pair.split("/", 1)
        swap_count = int(max(0, round(obs.volume_usd)))
        reserve_weth = obs.tvl_usd
        fee_weth = obs.fees_usd if obs.fees_usd is not None else 0.0
        score = obs.hourly_yield if obs.hourly_yield is not None else 0.0
        if swap_count < min_swap_count:
            continue
        if reserve_weth < min_reserve_weth:
            continue
        if reserve_weth <= 0:
            continue
        rows.append(
            V2YieldSpikeRow(
                source_name=obs.source_name,
                chain=obs.chain,
                pair_id=obs.pool_id,
                token0=token0,
                token1=token1,
                ts=obs.ts,
                ts_utc=iso_hour(obs.ts),
                ts_chicago=iso_hour_chicago(obs.ts),
                swap_count=swap_count,
                fee_weth=fee_weth,
                reserve_weth=reserve_weth,
                score=score,
                hourly_yield_pct=score * 100,
                usd_per_1000_liquidity_hourly=score * 1000,
                rough_apr_pct=score * 24 * 365 * 100,
            )
        )
    rows.sort(key=lambda r: (r.score, r.reserve_weth, r.swap_count), reverse=True)
    return rows


def write_v2_spike_csv(path: Path, rows: Iterable[V2YieldSpikeRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "chain",
                "pair_id",
                "token0",
                "token1",
                "timestamp",
                "hour_utc",
                "hour_chicago",
                "swap_count",
                "fee_weth",
                "reserve_weth",
                "score_feeWETH_over_reserveWETH",
                "hourly_yield_pct",
                "usd_per_1000_liquidity_hourly",
                "rough_apr_pct",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.source_name,
                    row.chain,
                    row.pair_id,
                    row.token0,
                    row.token1,
                    row.ts,
                    row.ts_utc,
                    row.ts_chicago,
                    row.swap_count,
                    f"{row.fee_weth:.10f}",
                    f"{row.reserve_weth:.10f}",
                    f"{row.score:.12f}",
                    f"{row.hourly_yield_pct:.10f}",
                    f"{row.usd_per_1000_liquidity_hourly:.10f}",
                    f"{row.rough_apr_pct:.10f}",
                ]
            )


def write_llama_pair_hour_csv(
    path: Path,
    rows: Iterable[LlamaPairHourRow],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "chain",
                "hourStartUnix",
                "hourStartUTC",
                "hourStartChicago",
                "pair",
                "token0",
                "token1",
                "token0Symbol",
                "token1Symbol",
                "token0Decimals",
                "token1Decimals",
                "swapCount",
                "fee0",
                "fee1",
                "reserve0",
                "reserve1",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.source_name,
                    row.version,
                    row.chain,
                    row.hour_start_unix,
                    row.hour_start_utc,
                    row.hour_start_chicago,
                    row.pair,
                    row.token0,
                    row.token1,
                    row.token0_symbol,
                    row.token1_symbol,
                    (row.token0_decimals if row.token0_decimals is not None else ""),
                    (row.token1_decimals if row.token1_decimals is not None else ""),
                    row.swap_count,
                    row.fee0_raw,
                    row.fee1_raw,
                    row.reserve0_raw,
                    row.reserve1_raw,
                ]
            )


def write_llama_spike_csv(
    path: Path,
    rows: Iterable[LlamaSpikeRankingRow],
    thresholds: LlamaAdaptiveThresholds | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "chain",
                "hourStartUnix",
                "hourStartUTC",
                "hourStartChicago",
                "pair",
                "token0",
                "token1",
                "token0Symbol",
                "token1Symbol",
                "swapCount",
                "wethReserve",
                "wethFee",
                "wethReserveNormalized",
                "wethFeeNormalized",
                "score",
                "hourly_yield_pct",
                "usd_per_1000_liquidity_hourly",
                "rough_apr_pct",
                "baseline_median_score",
                "spike_multiplier",
                "spike_multiplier_capped",
                "persistence_hits",
                "adaptive_min_swap_count",
                "adaptive_min_weth_liquidity",
                "notes_flags",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.source_name,
                    row.chain,
                    row.hour_start_unix,
                    row.hour_start_utc,
                    row.hour_start_chicago,
                    row.pair,
                    row.token0,
                    row.token1,
                    row.token0_symbol,
                    row.token1_symbol,
                    row.swap_count,
                    f"{row.weth_reserve:.10f}",
                    f"{row.weth_fee:.10f}",
                    f"{row.weth_reserve_normalized:.10f}",
                    f"{row.weth_fee_normalized:.10f}",
                    f"{row.score:.12f}",
                    f"{row.hourly_yield_pct:.10f}",
                    f"{row.usd_per_1000_liquidity_hourly:.10f}",
                    f"{row.rough_apr_pct:.10f}",
                    f"{row.baseline_median_score:.12f}",
                    f"{row.spike_multiplier:.12f}",
                    f"{row.spike_multiplier_capped:.12f}",
                    row.persistence_hits,
                    (thresholds.min_swap_count if thresholds is not None else ""),
                    (
                        f"{thresholds.min_weth_liquidity:.6f}"
                        if thresholds is not None
                        else ""
                    ),
                    row.notes_flags,
                ]
            )


def write_llama_diagnostics_csv(path: Path, diagnostics: LlamaRunDiagnostics | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "endpoint",
                "meta_block_number",
                "seed_next_index",
                "seed_total",
                "seed_last_block",
                "window_start_utc",
                "window_end_utc",
                "window_start_local",
                "window_end_local",
                "thresholds_label",
                "min_swap_count",
                "min_weth_liquidity",
                "baseline_hours",
                "persistence_hours",
                "persistence_spike_multiplier",
                "persistence_min_hits",
                "fallback_trace",
                "fetched_raw_rows",
                "after_time_window_filter",
                "after_min_swaps_filter",
                "after_min_weth_filter",
                "after_baseline_ready_filter",
                "after_spike_multiplier_filter",
                "after_persistence_filter",
                "final_ranked_rows",
                "empty_stage",
                "empty_message",
            ]
        )
        if diagnostics is None:
            return
        writer.writerow(
            [
                diagnostics.source_name,
                diagnostics.version,
                diagnostics.endpoint,
                diagnostics.meta_block_number,
                diagnostics.seed_next_index,
                diagnostics.seed_total,
                diagnostics.seed_last_block,
                iso_hour(diagnostics.window_start_ts),
                iso_hour(diagnostics.window_end_ts),
                iso_hour_local(diagnostics.window_start_ts, diagnostics.local_timezone),
                iso_hour_local(diagnostics.window_end_ts, diagnostics.local_timezone),
                diagnostics.thresholds_label,
                diagnostics.min_swap_count,
                (
                    f"{diagnostics.min_weth_liquidity:.6f}"
                    if diagnostics.min_weth_liquidity is not None
                    else ""
                ),
                diagnostics.baseline_hours,
                diagnostics.persistence_hours,
                (
                    f"{diagnostics.persistence_spike_multiplier:.6f}"
                    if diagnostics.persistence_spike_multiplier is not None
                    else ""
                ),
                diagnostics.persistence_min_hits,
                diagnostics.fallback_trace,
                diagnostics.counts.fetched_raw_rows,
                diagnostics.counts.after_time_window_filter,
                diagnostics.counts.after_min_swaps_filter,
                diagnostics.counts.after_min_weth_filter,
                diagnostics.counts.after_baseline_ready_filter,
                diagnostics.counts.after_spike_multiplier_filter,
                diagnostics.counts.after_persistence_filter,
                diagnostics.counts.final_ranked_rows,
                diagnostics.empty_stage,
                diagnostics.empty_message,
            ]
        )


def write_report_html(
    path: Path,
    rankings: list[PoolRanking],
    schedules: list[ScheduleRecommendation],
    v2_spike_rows: list[V2YieldSpikeRow],
    llama_rows: list[LlamaSpikeRankingRow],
    llama_thresholds: LlamaAdaptiveThresholds | None,
    llama_sources: list[SourceConfig],
    llama_diagnostics: LlamaRunDiagnostics | None,
    v2_spike_top: int,
    top_n: int,
    start_ts: int,
    end_ts: int,
    source_count: int,
    local_timezone: str,
    min_tvl_usd: float,
    max_hourly_yield_pct: float | None,
    quality_input_rows: int,
    quality_output_rows: int,
    quality_rejected_rows: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top_rankings = rankings[:top_n]
    top_schedules = schedules[:top_n]
    now_ts = int(time.time())
    generated_ts = iso_hour(now_ts)
    generated_local_ts = iso_hour_local(now_ts, local_timezone)
    jump_now_rows = select_jump_now_schedules(
        schedules=schedules,
        now_ts=now_ts,
        soon_hours=6,
        top_n=top_n,
    )
    jump_now_scope_label = "active now or starting within 6 hours"
    if not jump_now_rows:
        jump_now_rows = select_jump_now_schedules(
            schedules=schedules,
            now_ts=now_ts,
            soon_hours=24,
            top_n=top_n,
        )
        jump_now_scope_label = "active now or starting within 24 hours"

    ranking_rows = []
    for idx, row in enumerate(top_rankings, start=1):
        exchange = infer_exchange_name(row.source_name)
        ranking_rows.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_hourly_yield_pct):.6f}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{row.fee_tier}</td>"
            f"<td>{row.avg_tvl_usd:.2f}</td>"
            f"<td>{row.outlier_hours}</td>"
            f"<td>{row.avg_hourly_fees_usd:.2f}</td>"
            f"<td>{row.total_fees_usd:.2f}</td>"
            f"<td>{row.observed_hours}</td>"
            f"<td>{html.escape(iso_hour(row.fee_period_start_ts))}</td>"
            f"<td>{html.escape(iso_hour(row.fee_period_end_ts))}</td>"
            f"<td>{row.avg_hourly_yield_pct:.6f}</td>"
            f"<td>{row.median_hourly_yield_pct:.6f}</td>"
            f"<td>{row.trimmed_mean_hourly_yield_pct:.6f}</td>"
            f"<td>{row.p90_hourly_yield_pct:.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.median_hourly_yield_pct):.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.trimmed_mean_hourly_yield_pct):.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.p90_hourly_yield_pct):.6f}</td>"
            f"<td>{html.escape(row.best_window_utc)}</td>"
            f"<td>{html.escape(iso_hour(row.best_window_start_ts))}</td>"
            f"<td>{html.escape(iso_hour(row.best_window_end_ts))}</td>"
            "</tr>"
        )

    schedule_rows = []
    for row in top_schedules:
        exchange = infer_exchange_name(row.source_name)
        schedule_rows.append(
            "<tr>"
            f"<td>{row.pool_rank}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_block_hourly_yield_pct):.6f}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(row.add_pattern_utc)}</td>"
            f"<td>{html.escape(row.remove_pattern_utc)}</td>"
            f"<td>{html.escape(iso_hour(row.next_add_ts))}</td>"
            f"<td>{html.escape(iso_hour_local(row.next_add_ts, local_timezone))}</td>"
            f"<td>{html.escape(iso_hour(row.next_remove_ts))}</td>"
            f"<td>{html.escape(iso_hour_local(row.next_remove_ts, local_timezone))}</td>"
            f"<td>{row.reliability_hit_rate_pct:.2f}</td>"
            f"<td>{row.avg_block_hourly_yield_pct:.6f}</td>"
            f"<td>{row.block_hours}</td>"
            "</tr>"
        )

    jump_rows = []
    for row, status, eta_seconds in jump_now_rows:
        exchange = infer_exchange_name(row.source_name)
        jump_rows.append(
            "<tr>"
            f"<td>{row.pool_rank}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_block_hourly_yield_pct):.6f}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td>{html.escape(format_eta(eta_seconds))}</td>"
            f"<td>{html.escape(iso_hour(row.next_add_ts))}</td>"
            f"<td>{html.escape(iso_hour_local(row.next_add_ts, local_timezone))}</td>"
            f"<td>{html.escape(iso_hour(row.next_remove_ts))}</td>"
            f"<td>{html.escape(iso_hour_local(row.next_remove_ts, local_timezone))}</td>"
            f"<td>{row.reliability_hit_rate_pct:.2f}</td>"
            f"<td>{row.avg_block_hourly_yield_pct:.6f}</td>"
            "</tr>"
        )

    rankings_table_html = "\n".join(ranking_rows) if ranking_rows else (
        "<tr><td colspan='25'>No ranked pools found.</td></tr>"
    )
    schedules_table_html = "\n".join(schedule_rows) if schedule_rows else (
        "<tr><td colspan='14'>No reliable recurring schedule blocks found.</td></tr>"
    )
    jump_table_html = "\n".join(jump_rows) if jump_rows else (
        "<tr><td colspan='14'>No urgent pool windows found in the near-term schedule horizon.</td></tr>"
    )
    top_llama_rows = llama_rows[: max(0, v2_spike_top)]
    llama_rows_html = []
    for idx, row in enumerate(top_llama_rows, start=1):
        exchange = infer_exchange_name(row.source_name)
        pool_name = human_pool_label(
            token0_symbol=row.token0_symbol,
            token1_symbol=row.token1_symbol,
            token0_address=row.token0,
            token1_address=row.token1,
        )
        llama_rows_html.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{row.usd_per_1000_liquidity_hourly:.6f}</td>"
            f"<td>{html.escape(pool_name)}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{html.escape(row.source_name)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(row.token0)}</td>"
            f"<td>{html.escape(row.token1)}</td>"
            f"<td>{html.escape(row.token0_symbol)}</td>"
            f"<td>{html.escape(row.token1_symbol)}</td>"
            f"<td>{html.escape(row.hour_start_utc)}</td>"
            f"<td>{html.escape(row.hour_start_chicago)}</td>"
            f"<td>{row.swap_count}</td>"
            f"<td>{row.weth_fee_normalized:.8f}</td>"
            f"<td>{row.weth_reserve_normalized:.8f}</td>"
            f"<td>{row.score:.10f}</td>"
            f"<td>{row.hourly_yield_pct:.6f}</td>"
            f"<td>{row.rough_apr_pct:.4f}</td>"
            f"<td>{row.baseline_median_score:.10f}</td>"
            f"<td>{row.spike_multiplier:.4f}</td>"
            f"<td>{row.spike_multiplier_capped:.4f}</td>"
            f"<td>{row.persistence_hits}</td>"
            f"<td>{html.escape(row.notes_flags)}</td>"
            "</tr>"
        )
    if llama_rows_html:
        llama_table_html = "\n".join(llama_rows_html)
    else:
        empty_text = (
            html.escape(llama_diagnostics.empty_message)
            if llama_diagnostics is not None
            else "No V2 fee-yield spike rows matched adaptive threshold and persistence filters."
        )
        llama_table_html = f"<tr><td colspan='24'>{empty_text}</td></tr>"
    llama_endpoint_note = "<br/>".join(
        f"{html.escape(src.name)}: <code>{html.escape(src.endpoint)}</code>" for src in llama_sources
    )
    if not llama_endpoint_note:
        llama_endpoint_note = "No llama sources configured."
    if llama_thresholds is None:
        llama_threshold_note = "Adaptive thresholds unavailable (no llama rows fetched)."
    else:
        llama_threshold_note = (
            f"Threshold selection ({html.escape(llama_thresholds.band)}): "
            f"minSwapCount >= {llama_thresholds.min_swap_count}, "
            f"minWETHLiquidity >= {llama_thresholds.min_weth_liquidity:.2f}, "
            f"baselineWindow={llama_thresholds.baseline_hours}h, "
            f"persistenceWindow={llama_thresholds.persistence_hours}h, "
            f"spikeMultiplier>={llama_thresholds.persistence_spike_multiplier:.2f}, "
            f"minPersistenceHits={llama_thresholds.persistence_min_hits}, "
            f"rowsFetched={llama_thresholds.total_rows:,}."
        )
        if llama_thresholds.fallback_trace:
            llama_threshold_note += f" Fallback path: {html.escape(llama_thresholds.fallback_trace)}."
    llama_diag_banner = ""
    if llama_diagnostics is not None:
        d = llama_diagnostics
        llama_diag_banner = (
            "<p class='note'>"
            "<strong>Llama Diagnostics:</strong> "
            f"meta.block={('n/a' if d.meta_block_number is None else d.meta_block_number)}, "
            f"v2SeedState next/total={('n/a' if d.seed_next_index is None else d.seed_next_index)}/"
            f"{('n/a' if d.seed_total is None else d.seed_total)}, "
            f"seedLastBlock={('n/a' if d.seed_last_block is None else d.seed_last_block)}.<br/>"
            f"Window UTC: {html.escape(iso_hour(d.window_start_ts))} -> {html.escape(iso_hour(d.window_end_ts))}; "
            f"Window {html.escape(d.local_timezone)}: "
            f"{html.escape(iso_hour_local(d.window_start_ts, d.local_timezone))} -> "
            f"{html.escape(iso_hour_local(d.window_end_ts, d.local_timezone))}.<br/>"
            "Dropoff counts: "
            f"fetched={d.counts.fetched_raw_rows}, "
            f"after_window={d.counts.after_time_window_filter}, "
            f"after_min_swaps={d.counts.after_min_swaps_filter}, "
            f"after_min_weth={d.counts.after_min_weth_filter}, "
            f"after_baseline={d.counts.after_baseline_ready_filter}, "
            f"after_spike={d.counts.after_spike_multiplier_filter}, "
            f"after_persistence={d.counts.after_persistence_filter}, "
            f"final={d.counts.final_ranked_rows}.<br/>"
            f"Status: {html.escape(d.empty_message)}"
            "</p>"
        )

    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Uniswap Yield Report</title>
  <style>
    :root {{
      --bg: #f6f7f9;
      --card: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --line: #d7dce2;
      --accent: #0052cc;
      --accent-soft: #e7f0ff;
    }}
    body {{
      margin: 0;
      background: linear-gradient(180deg, #f1f5f9 0%, #f8fafc 100%);
      color: var(--ink);
      font: 14px/1.45 "IBM Plex Sans", "Segoe UI", Arial, sans-serif;
    }}
    .container {{
      max-width: 1400px;
      margin: 24px auto 40px;
      padding: 0 16px;
    }}
    .header {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 16px 18px;
      margin-bottom: 16px;
      box-shadow: 0 4px 18px rgba(15, 23, 42, 0.06);
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 26px;
      letter-spacing: 0.2px;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 8px 12px;
      color: var(--muted);
      margin-top: 8px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      margin-bottom: 16px;
      box-shadow: 0 4px 18px rgba(15, 23, 42, 0.05);
    }}
    h2 {{
      margin: 0 0 10px 0;
      font-size: 18px;
    }}
    p.note {{
      margin: 0 0 10px 0;
      color: var(--muted);
      background: var(--accent-soft);
      border: 1px solid #d6e4ff;
      border-radius: 8px;
      padding: 8px 10px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #fff;
    }}
    th, td {{
      border: 1px solid var(--line);
      padding: 7px 8px;
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      background: #f8fafc;
      position: sticky;
      top: 0;
      z-index: 2;
      font-weight: 600;
    }}
    .table-wrap {{
      overflow: auto;
      border-radius: 8px;
      border: 1px solid var(--line);
    }}
    .links a {{
      color: var(--accent);
      text-decoration: none;
      margin-right: 14px;
      font-weight: 600;
    }}
    .links a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <section class="header">
      <h1>Uniswap Yield & Liquidity Schedule Report</h1>
      <div class="meta">
        <div><strong>Generated (UTC):</strong> {html.escape(generated_ts)}</div>
        <div><strong>Generated ({html.escape(local_timezone)}):</strong> {html.escape(generated_local_ts)}</div>
        <div><strong>Sources Scanned:</strong> {source_count}</div>
        <div><strong>Analysis Window Start:</strong> {html.escape(iso_hour(start_ts))}</div>
        <div><strong>Analysis Window End:</strong> {html.escape(iso_hour(end_ts))}</div>
        <div><strong>Pools Ranked:</strong> {len(rankings)}</div>
        <div><strong>Schedule Blocks:</strong> {len(schedules)}</div>
        <div><strong>Quality Input Rows:</strong> {quality_input_rows}</div>
        <div><strong>Quality Output Rows:</strong> {quality_output_rows}</div>
        <div><strong>Quality Rejected Rows:</strong> {quality_rejected_rows}</div>
      </div>
      <div class="links" style="margin-top:10px;">
        <a href="pool_rankings.csv">pool_rankings.csv</a>
        <a href="hourly_observations.csv">hourly_observations.csv</a>
        <a href="liquidity_schedule.csv">liquidity_schedule.csv</a>
        <a href="sushi_v2_yield_spikes.csv">sushi_v2_yield_spikes.csv</a>
        <a href="llama_pair_hour_data.csv">llama_pair_hour_data.csv</a>
        <a href="llama_weth_spike_rankings.csv">llama_weth_spike_rankings.csv</a>
        <a href="llama_run_diagnostics.csv">llama_run_diagnostics.csv</a>
        <a href="pool_rankings_diagnostics.csv">pool_rankings_diagnostics.csv</a>
        <a href="data_quality_audit.csv">data_quality_audit.csv</a>
      </div>
    </section>

    <section class="card">
      <h2>Pools You Need To Jump Into Right Now</h2>
      <p class="note">
        Prioritized by urgency and expected yield from recurring schedule blocks ({html.escape(jump_now_scope_label)}).
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pool Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Version</th><th>Chain</th><th>Status</th>
              <th>ETA</th><th>Next Add UTC</th><th>Next Add Local</th><th>Next Remove UTC</th><th>Next Remove Local</th>
              <th>Hit Rate %</th><th>Avg Block Hourly Yield %</th>
            </tr>
          </thead>
          <tbody>
            {jump_table_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>V2 Fee Yield Spikes (Ethereum / Sushi V2)</h2>
      <p class="note">
        Endpoint(s): {llama_endpoint_note}
      </p>
      <p class="note">
        {llama_threshold_note}
      </p>
      <p class="note">
        Ranked by spike-vs-baseline (primary) and score (secondary), where score = feeWETH / reserveWETH from PairHourData.
      </p>
      {llama_diag_banner}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Source</th><th>Chain</th><th>Pair Address</th><th>Token0</th><th>Token1</th>
              <th>Token0 Symbol</th><th>Token1 Symbol</th>
              <th>Hour UTC</th><th>Hour Chicago</th><th>Swap Count</th>
              <th>feeWETH (normalized)</th><th>reserveWETH (normalized)</th><th>Score</th><th>Hourly Yield %</th><th>Rough APR %</th>
              <th>Baseline Median Score</th><th>Spike Multiplier</th><th>Spike Multiplier (Capped)</th><th>Persistence Hits</th><th>Flags</th>
            </tr>
          </thead>
          <tbody>
            {llama_table_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Top Pools by Earning Potential</h2>
      <p class="note">
        Sanity filters applied before ranking: min TVL ${min_tvl_usd:,.0f} per hour
        and hourly yield cap {"none" if max_hourly_yield_pct is None else f"{max_hourly_yield_pct:.2f}%"}.
        Robust stats include median, trimmed mean, and outlier-hour count (MAD-based).
      </p>
      <p class="note">
        Fee tier legend: standard v3 tiers are typically 500, 3000, 10000 (0.05%, 0.30%, 1.00%).
        Larger encoded values may be protocol-specific and are shown as-is.
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Version</th><th>Chain</th><th>Pair</th><th>Fee Tier</th>
              <th>Avg TVL USD</th><th>Outlier Hours</th>
              <th>Avg Hourly Fee USD</th><th>Total Fees USD</th><th>Obs Hours</th>
              <th>Fee Period Start UTC</th><th>Fee Period End UTC</th>
              <th>Avg Hourly Yield %</th><th>Median Hourly Yield %</th><th>Trimmed Mean Hourly Yield %</th><th>P90 Hourly Yield %</th>
              <th>Median USD per $1k / hr</th><th>Trimmed Mean USD per $1k / hr</th><th>P90 USD per $1k / hr</th>
              <th>Best Window Pattern</th><th>Best Window Start UTC</th><th>Best Window End UTC</th>
            </tr>
          </thead>
          <tbody>
            {rankings_table_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Recommended Add/Remove Schedule</h2>
      <p class="note">
        Schedule blocks are recurring high-yield windows with reliability filters applied.
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pool Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Version</th>
              <th>Add Pattern UTC</th><th>Remove Pattern UTC</th>
              <th>Next Add UTC</th><th>Next Add Local</th><th>Next Remove UTC</th><th>Next Remove Local</th>
              <th>Hit Rate %</th><th>Avg Block Hourly Yield %</th><th>Block Hours</th>
            </tr>
          </thead>
          <tbody>
            {schedules_table_html}
          </tbody>
        </table>
      </div>
    </section>
  </div>
</body>
</html>
"""
    with path.open("w", encoding="utf-8") as f:
        f.write(doc)


def open_report_in_browser(path: Path) -> tuple[bool, str]:
    resolved = path.resolve()
    uri = resolved.as_uri()

    wslview = shutil.which("wslview")
    if wslview:
        try:
            completed = subprocess.run(
                [wslview, str(resolved)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if completed.returncode == 0:
                return True, "wslview"
        except Exception:  # noqa: BLE001
            pass

    try:
        if bool(webbrowser.open(uri, new=2)):
            return True, "webbrowser"
    except Exception:  # noqa: BLE001
        pass

    xdg_open = shutil.which("xdg-open")
    if xdg_open:
        try:
            completed = subprocess.run(
                [xdg_open, str(resolved)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if completed.returncode == 0:
                return True, "xdg-open"
        except Exception:  # noqa: BLE001
            pass

    return False, "none"


def write_summary_md(
    path: Path,
    rankings: list[PoolRanking],
    top_n: int,
    start_ts: int,
    end_ts: int,
    source_count: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top_rows = rankings[:top_n]

    lines: list[str] = []
    lines.append("# Uniswap Yield Potential Report")
    lines.append("")
    lines.append(f"- Sources scanned: {source_count}")
    lines.append(f"- Analysis window: {iso_hour(start_ts)} to {iso_hour(end_ts)}")
    lines.append(f"- Pools ranked: {len(rankings)}")
    lines.append("- Fee columns are based on hourly buckets (`poolHourDatas`):")
    lines.append("  - `avg_hourly_fees_usd` = mean fee per 1-hour bucket.")
    lines.append("  - `total_fees_usd` = sum of hourly fees across observed hours.")
    lines.append("")

    if not top_rows:
        lines.append("No pools met ranking criteria.")
    else:
        lines.append("## Top Pools by Earning Potential")
        lines.append("")
        lines.append(
            "| Rank | Version | Chain | Pair | Fee Tier | Avg TVL USD | Outlier Hours | Avg Hourly Fee USD | Total Fees USD | Fee Obs Hours | Fee Period (UTC) | Avg Hourly Yield % | Median Hourly Yield % | Trimmed Mean Hourly Yield % | P90 Hourly Yield % | Avg USD per $1k / hr | Median USD per $1k / hr | Trimmed Mean USD per $1k / hr | P90 USD per $1k / hr | Best Window Pattern | Best Window Start (UTC) | Best Window End (UTC) |"
        )
        lines.append(
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|"
        )
        for idx, row in enumerate(top_rows, start=1):
            lines.append(
                "| "
                f"{idx} | {row.version} | {row.chain} | {row.pair} | {row.fee_tier} | "
                f"{row.avg_tvl_usd:.2f} | {row.outlier_hours} | "
                f"{row.avg_hourly_fees_usd:.2f} | {row.total_fees_usd:.2f} | {row.observed_hours} | "
                f"{iso_hour(row.fee_period_start_ts)} to {iso_hour(row.fee_period_end_ts)} | "
                f"{row.avg_hourly_yield_pct:.6f} | {row.median_hourly_yield_pct:.6f} | "
                f"{row.trimmed_mean_hourly_yield_pct:.6f} | {row.p90_hourly_yield_pct:.6f} | "
                f"{usd_per_1000_from_yield_pct(row.avg_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.median_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.trimmed_mean_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.p90_hourly_yield_pct):.6f} | "
                f"{row.best_window_utc} | {iso_hour(row.best_window_start_ts)} | {iso_hour(row.best_window_end_ts)} |"
            )

    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def print_top(rankings: list[PoolRanking], top_n: int) -> None:
    top_rows = rankings[:top_n]
    if not top_rows:
        print("No ranked pools found for the selected period and sample threshold.")
        return

    print("Top pools by earning potential")
    print(
        "rank | version | chain | pair | fee | avg_hourly_fee_usd | total_fees_usd | obs_hours | avg_hourly_yield% | p90_hourly_yield% | avg_usd_per_1000_hr | p90_usd_per_1000_hr | best_window_start_utc | best_window_end_utc"
    )
    for idx, row in enumerate(top_rows, start=1):
        print(
            f"{idx:>4} | {row.version:>7} | {row.chain:>10} | {row.pair:>15} | "
            f"{row.fee_tier:>7} | {row.avg_hourly_fees_usd:>18.2f} | {row.total_fees_usd:>14.2f} | "
            f"{row.observed_hours:>9} | {row.avg_hourly_yield_pct:>17.6f} | {row.p90_hourly_yield_pct:>17.6f} | "
            f"{usd_per_1000_from_yield_pct(row.avg_hourly_yield_pct):>19.6f} | "
            f"{usd_per_1000_from_yield_pct(row.p90_hourly_yield_pct):>19.6f} | "
            f"{iso_hour(row.best_window_start_ts)} | {iso_hour(row.best_window_end_ts)}"
        )


def print_schedule_top(schedules: list[ScheduleRecommendation], top_n: int) -> None:
    top_rows = schedules[:top_n]
    if not top_rows:
        print("No reliable recurring high-yield schedule blocks found.")
        return

    print("")
    print("Recommended add/remove schedule blocks")
    print(
        "pool_rank | pair | add_pattern_utc | remove_pattern_utc | next_add_utc | next_remove_utc | hit_rate% | avg_block_hourly_yield% | avg_usd_per_1000_hr"
    )
    for row in top_rows:
        print(
            f"{row.pool_rank:>9} | {row.pair:>15} | {row.add_pattern_utc:>20} | "
            f"{row.remove_pattern_utc:>23} | {iso_hour(row.next_add_ts)} | {iso_hour(row.next_remove_ts)} | "
            f"{row.reliability_hit_rate_pct:>8.2f} | {row.avg_block_hourly_yield_pct:>24.6f} | "
            f"{usd_per_1000_from_yield_pct(row.avg_block_hourly_yield_pct):>19.6f}"
        )


def main() -> int:
    args = parse_args()
    if args.only_backfill_sources and args.include_backfill_sources:
        print(
            "Use either --only-backfill-sources or --include-backfill-sources, not both.",
            file=sys.stderr,
        )
        return 2
    setup_run_logging(args.log_file.strip(), sys.argv)
    config_path = Path(args.config).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 2

    end_ts = floor_to_hour(args.end_ts or int(time.time()))
    start_ts = end_ts - (args.hours * SECONDS_PER_HOUR)

    try:
        sources = load_sources(config_path)
    except Exception as err:
        print(f"Failed to load config: {err}", file=sys.stderr)
        return 2
    if args.only_backfill_sources:
        sources = [s for s in sources if s.backfill_source]
    elif not args.include_backfill_sources:
        sources = [s for s in sources if not s.backfill_source]

    if not sources:
        print("No enabled sources found in config.", file=sys.stderr)
        return 2

    print(
        f"Scanning {len(sources)} sources from {iso_hour(start_ts)} to {iso_hour(end_ts)}...",
        file=sys.stderr,
    )
    v2_spike_sources = {source.name for source in sources if source.source_type == "v2_spike"}

    cache_path: Path | None = None
    if args.cache_db:
        cache_path = Path(args.cache_db).resolve()
        cache_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if cache_path is None:
            observations, failures = fetch_all_observations(
                sources=sources,
                start_ts=start_ts,
                end_ts=end_ts,
                page_size=args.page_size,
                max_pages=args.max_pages_per_source,
                workers=args.workers,
                parallel_window_hours=args.parallel_window_hours,
                timeout=args.timeout,
                retries=args.retries,
                strict_sources=args.strict_sources,
                v2_spike_min_swap_count=args.v2_spike_min_swap_count,
                v2_spike_min_reserve_weth=args.v2_spike_min_reserve_weth,
            )
        else:
            with sqlite3.connect(cache_path, timeout=120) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=120000")
                ensure_cache_schema(conn)
                observations, failures, written = fetch_with_cache(
                    sources=sources,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    page_size=args.page_size,
                    max_pages=args.max_pages_per_source,
                    workers=args.workers,
                    parallel_window_hours=args.parallel_window_hours,
                    timeout=args.timeout,
                    retries=args.retries,
                    strict_sources=args.strict_sources,
                    conn=conn,
                    overlap_hours=args.cache_overlap_hours,
                    checkpoint_pages=args.source_checkpoint_pages,
                    v2_spike_min_swap_count=args.v2_spike_min_swap_count,
                    v2_spike_min_reserve_weth=args.v2_spike_min_reserve_weth,
                )
            print(
                f"Cache active: {cache_path} (upserted {written:,} rows, loaded {len(observations):,} rows for window)",
                file=sys.stderr,
            )
    except Exception as err:
        print(f"Scan failed: {err}", file=sys.stderr)
        return 1

    quality_input_rows = len(observations)
    observations, quality_rejected_counts = filter_observations_with_quality_audit(
        observations=observations,
        min_tvl_usd=args.min_tvl_usd,
        max_hourly_yield_pct=args.max_hourly_yield_pct,
        v2_spike_sources=v2_spike_sources,
    )
    quality_output_rows = len(observations)
    quality_rejected_rows = quality_input_rows - quality_output_rows
    if quality_rejected_rows > 0:
        print(
            (
                f"Data quality filters rejected {quality_rejected_rows:,} / {quality_input_rows:,} rows "
                f"({(100.0 * quality_rejected_rows / quality_input_rows):.2f}%)."
            ),
            file=sys.stderr,
        )

    if failures and not args.strict_sources:
        print("", file=sys.stderr)
        print(f"{len(failures)} source(s) failed and were skipped:", file=sys.stderr)
        for failure in failures:
            print(
                f"- {failure.source_name} ({failure.version}/{failure.chain}): {failure.error}",
                file=sys.stderr,
            )

    if not observations:
        print("No observations fetched. Check endpoints/query mappings.", file=sys.stderr)

    ranking_diagnostics: list[PoolRankingDiagnostic] = []
    ranking_observations = [
        row for row in observations if row.source_name not in v2_spike_sources
    ]
    rankings = rank_pools(
        observations=ranking_observations,
        min_samples=args.min_samples,
        min_tvl_usd=args.min_tvl_usd,
        max_hourly_yield_pct=args.max_hourly_yield_pct,
        trim_ratio=args.yield_trim_ratio,
        ranking_stats_min_tvl_usd=args.ranking_stats_min_tvl_usd,
        winsorize_percentile=args.ranking_yield_winsorize_percentile,
        diagnostics_out=ranking_diagnostics,
    )
    v2_spike_rows = build_v2_spike_rows(
        observations=observations,
        v2_spike_sources=v2_spike_sources,
        min_swap_count=args.v2_spike_min_swap_count,
        min_reserve_weth=args.v2_spike_min_reserve_weth,
    )
    llama_sources = [source for source in sources if source.source_type == "v2_spike"]
    llama_pair_hour_rows: list[LlamaPairHourRow] = []
    llama_fetched_raw_rows = 0
    llama_runtime_state: LlamaRuntimeState | None = None
    for source in llama_sources:
        try:
            source_rows, source_raw_count = fetch_llama_pair_hour_rows(
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                page_size=effective_page_size(source, args.page_size),
                max_pages=args.max_pages_per_source,
                timeout=args.timeout,
                retries=args.retries,
            )
            if llama_runtime_state is None:
                llama_runtime_state = fetch_llama_runtime_state(
                    source=source,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            llama_pair_hour_rows.extend(source_rows)
            llama_fetched_raw_rows += source_raw_count
            print(
                (
                    f"{source.name}: fetched {len(source_rows):,} llama PairHourData rows "
                    f"for report output ({source_raw_count:,} raw)."
                ),
                file=sys.stderr,
            )
        except Exception as err:  # noqa: BLE001
            print(
                f"{source.name}: failed to fetch llama pair-hour rows for report output: {err}",
                file=sys.stderr,
            )
    llama_thresholds: LlamaAdaptiveThresholds | None = None
    llama_rankings: list[LlamaSpikeRankingRow] = []
    llama_dropoff = LlamaDropoffCounters(0, 0, 0, 0, 0, 0, 0, 0)
    if llama_pair_hour_rows:
        llama_rankings, llama_thresholds, llama_dropoff = build_llama_rankings_with_fallback(
            llama_pair_hour_rows,
            baseline_hours=args.llama_baseline_hours,
            persistence_hours=args.llama_persistence_hours,
            persistence_spike_multiplier=args.llama_persistence_spike_multiplier,
            persistence_min_hits=args.llama_persistence_min_hits,
            min_baseline_observations=args.llama_min_baseline_observations,
            baseline_epsilon=args.llama_baseline_epsilon,
            spike_multiplier_cap=args.llama_spike_multiplier_cap,
            strict_mode=args.llama_strict_mode,
            default_min_swap_count=args.v2_spike_min_swap_count,
            default_min_weth_liquidity=args.v2_spike_min_reserve_weth,
            strict_min_swap_count=args.llama_strict_min_swap_count,
            strict_min_weth_liquidity=args.llama_strict_min_reserve_weth,
            min_ranked_target=args.llama_min_ranked_target,
        )
    elif llama_sources:
        llama_dropoff = LlamaDropoffCounters(
            fetched_raw_rows=llama_fetched_raw_rows,
            after_time_window_filter=llama_fetched_raw_rows,
            after_min_swaps_filter=0,
            after_min_weth_filter=0,
            after_baseline_ready_filter=0,
            after_spike_multiplier_filter=0,
            after_persistence_filter=0,
            final_ranked_rows=0,
        )
    if llama_dropoff.fetched_raw_rows < llama_fetched_raw_rows:
        llama_dropoff = LlamaDropoffCounters(
            fetched_raw_rows=llama_fetched_raw_rows,
            after_time_window_filter=llama_fetched_raw_rows,
            after_min_swaps_filter=llama_dropoff.after_min_swaps_filter,
            after_min_weth_filter=llama_dropoff.after_min_weth_filter,
            after_baseline_ready_filter=llama_dropoff.after_baseline_ready_filter,
            after_spike_multiplier_filter=llama_dropoff.after_spike_multiplier_filter,
            after_persistence_filter=llama_dropoff.after_persistence_filter,
            final_ranked_rows=llama_dropoff.final_ranked_rows,
        )
    if llama_thresholds is not None:
        print(
            (
                "Llama adaptive thresholds: "
                f"rows={llama_thresholds.total_rows:,}, band={llama_thresholds.band}, "
                f"minSwapCount={llama_thresholds.min_swap_count}, "
                f"minWETHLiquidity={llama_thresholds.min_weth_liquidity:.2f}, "
                f"baselineHours={llama_thresholds.baseline_hours}, "
                f"persistenceHours={llama_thresholds.persistence_hours}, "
                f"spikeMultiplier>={llama_thresholds.persistence_spike_multiplier:.2f}, "
                f"minPersistenceHits={llama_thresholds.persistence_min_hits}, "
                f"fallbackTrace={llama_thresholds.fallback_trace}"
            ),
            file=sys.stderr,
        )
        print(f"Llama ranked spikes: {len(llama_rankings):,}", file=sys.stderr)
    llama_diagnostics = build_llama_diagnostics(
        source=(llama_sources[0] if llama_sources else None),
        runtime=llama_runtime_state,
        window_start_ts=start_ts,
        window_end_ts=end_ts,
        local_timezone=args.local_timezone,
        thresholds=llama_thresholds,
        counts=llama_dropoff,
    )
    schedules = build_liquidity_schedule(
        rankings=rankings,
        observations=observations,
        end_ts=end_ts,
        top_pools=args.schedule_top_pools,
        quantile=args.schedule_quantile,
        min_usd_per_1000_hour=args.schedule_min_usd_per_1000_hour,
        min_hit_rate=args.schedule_min_hit_rate,
        min_occurrences=args.schedule_min_occurrences,
        max_blocks_per_pool=args.schedule_max_blocks_per_pool,
    )

    hourly_csv = output_dir / "hourly_observations.csv"
    ranking_csv = output_dir / "pool_rankings.csv"
    ranking_diag_csv = output_dir / "pool_rankings_diagnostics.csv"
    summary_md = output_dir / "summary.md"
    schedule_csv = output_dir / "liquidity_schedule.csv"
    schedule_md = output_dir / "liquidity_schedule.md"
    v2_spike_csv = output_dir / "sushi_v2_yield_spikes.csv"
    llama_pair_hour_csv = output_dir / "llama_pair_hour_data.csv"
    llama_rankings_csv = output_dir / "llama_weth_spike_rankings.csv"
    llama_diag_csv = output_dir / "llama_run_diagnostics.csv"
    quality_csv = output_dir / "data_quality_audit.csv"
    report_html = output_dir / "report.html"

    write_hourly_csv(hourly_csv, observations)
    write_rankings_csv(ranking_csv, rankings)
    write_pool_rankings_diagnostics_csv(ranking_diag_csv, ranking_diagnostics)
    write_schedule_csv(schedule_csv, schedules)
    write_v2_spike_csv(v2_spike_csv, v2_spike_rows)
    write_llama_pair_hour_csv(llama_pair_hour_csv, llama_pair_hour_rows)
    write_llama_spike_csv(llama_rankings_csv, llama_rankings, llama_thresholds)
    write_llama_diagnostics_csv(llama_diag_csv, llama_diagnostics)
    write_data_quality_audit_csv(
        quality_csv,
        input_rows=quality_input_rows,
        output_rows=quality_output_rows,
        rejected_counts=quality_rejected_counts,
    )
    write_summary_md(
        summary_md,
        rankings,
        top_n=args.top,
        start_ts=start_ts,
        end_ts=end_ts,
        source_count=len(sources),
    )
    write_schedule_md(
        schedule_md,
        schedules,
        end_ts=end_ts,
        top_n=args.top,
    )
    write_report_html(
        report_html,
        rankings,
        schedules,
        v2_spike_rows,
        llama_rankings,
        llama_thresholds,
        llama_sources,
        llama_diagnostics,
        v2_spike_top=args.v2_spike_top,
        top_n=args.top,
        start_ts=start_ts,
        end_ts=end_ts,
        source_count=len(sources),
        local_timezone=args.local_timezone,
        min_tvl_usd=args.min_tvl_usd,
        max_hourly_yield_pct=args.max_hourly_yield_pct,
        quality_input_rows=quality_input_rows,
        quality_output_rows=quality_output_rows,
        quality_rejected_rows=quality_rejected_rows,
    )

    print_top(rankings, args.top)
    print_schedule_top(schedules, args.top)

    if args.no_open_report:
        print(f"Report auto-open disabled: {report_html}", file=sys.stderr)
    else:
        opened, method = open_report_in_browser(report_html)
        if opened:
            print(
                f"Opened report in default browser via {method}: {report_html}",
                file=sys.stderr,
            )
        else:
            print(
                (
                    f"Could not auto-open browser (tried wslview/webbrowser/xdg-open). "
                    f"Open this file manually: {report_html}"
                ),
                file=sys.stderr,
            )

    print("", file=sys.stderr)
    print(f"Wrote: {hourly_csv}", file=sys.stderr)
    print(f"Wrote: {ranking_csv}", file=sys.stderr)
    print(f"Wrote: {ranking_diag_csv}", file=sys.stderr)
    print(f"Wrote: {summary_md}", file=sys.stderr)
    print(f"Wrote: {schedule_csv}", file=sys.stderr)
    print(f"Wrote: {schedule_md}", file=sys.stderr)
    print(f"Wrote: {v2_spike_csv}", file=sys.stderr)
    print(f"Wrote: {llama_pair_hour_csv}", file=sys.stderr)
    print(f"Wrote: {llama_rankings_csv}", file=sys.stderr)
    print(f"Wrote: {llama_diag_csv}", file=sys.stderr)
    print(f"Wrote: {quality_csv}", file=sys.stderr)
    print(f"Wrote: {report_html}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
