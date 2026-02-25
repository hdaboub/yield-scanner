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
    tvl_missing: bool = False


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
class SpikeRunStats:
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    spike_threshold_usd_per_1000_hr: float
    nonzero_hours: int
    observed_hours: int
    spike_hours: int
    hit_rate_pct: float
    history_quality: str
    runs_total: int
    run_length_p50: float
    run_length_p75: float
    run_length_p90: float
    avg_usd_per_1000_hr_when_spiking: float
    p90_usd_per_1000_hr_when_spiking: float


@dataclass(frozen=True)
class ScheduleEnhancedRow:
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
    threshold_hourly_usd_per_1000_liquidity: float
    avg_block_hourly_yield_pct: float
    p90_block_hourly_yield_pct: float
    avg_block_hourly_usd_per_1000_liquidity: float
    p90_block_hourly_usd_per_1000_liquidity: float
    block_hours: int
    next_add_ts: int
    next_remove_ts: int
    pool_score: float
    baseline_usd_per_1000_hr: float
    baseline_p75_usd_per_1000_hr: float
    gross_block_usd_per_1000: float
    gross_block_usd_per_1000_p90: float
    baseline_block_usd_per_1000: float
    baseline_block_p75_usd_per_1000: float
    incremental_usd_per_1000: float
    incremental_vs_baseline_p75_usd_per_1000: float
    incremental_range_usd_per_1000: str
    breakeven_move_cost_usd_per_1000: float
    risk_adjusted_incremental_usd_per_1000: float
    tvl_median_usd_est: float
    max_deployable_usd_est: float
    deploy_fraction_cap: float
    capacity_flag: str
    run_length_p50: float
    run_length_p90: float
    hit_rate_pct: float
    history_quality: str
    nonzero_hours: int
    confidence_score: float


@dataclass(frozen=True)
class MovesDayCurveRow:
    objective: str
    move_cost_usd_per_move: float
    deploy_usd: float
    max_moves_per_day: int
    min_hold_hours: int
    cooldown_hours_between_moves: int
    selected_blocks_count: int
    selected_moves_count: int
    total_net_usd: float
    total_gross_usd: float
    total_baseline_usd: float
    notes: str


@dataclass(frozen=True)
class ScheduleRunDiagnosticsRow:
    scenario_id: str
    objective: str
    deploy_usd: float
    move_cost_usd_per_move: float
    max_moves_per_day: int
    min_hold_hours: int
    total_schedule_rows: int
    excluded_by_source_health: int
    excluded_by_capacity: int
    excluded_by_abs_capacity_floor: int
    excluded_by_min_incremental: int
    excluded_by_history_quality: int
    excluded_by_invalid_fee_tier: int
    excluded_by_min_tvl: int
    excluded_by_hold_hours: int
    candidates_after_filters: int
    selected_blocks_count: int
    reason_if_zero: str


@dataclass(frozen=True)
class ScheduleSummaryStats:
    blocks_count: int
    breakeven_min: float
    breakeven_p25: float
    breakeven_p50: float
    breakeven_p75: float
    breakeven_p90: float
    breakeven_max: float
    incremental_min: float
    incremental_p25: float
    incremental_p50: float
    incremental_p75: float
    incremental_p90: float
    incremental_max: float
    confidence_p50: float
    confidence_p75: float
    confidence_p90: float
    history_ok_count: int
    history_insufficient_count: int
    history_ok_median_breakeven: float
    history_insufficient_median_breakeven: float


@dataclass(frozen=True)
class SelectedPlanRow:
    objective: str
    move_cost_usd_per_move: float
    deploy_usd: float
    pool_rank: int
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    next_add_ts: int
    next_remove_ts: int
    block_hours: int
    effective_deploy_usd: float
    expected_net_usd: float
    expected_net_usd_per_1000: float
    expected_gross_usd: float
    expected_baseline_usd: float
    breakeven_move_cost_usd_per_1000: float
    breakeven_move_cost_usd: float
    max_deployable_usd_est: float
    confidence_score: float
    capacity_flag: str


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
class SourceHealthRow:
    source_name: str
    version: str
    chain: str
    input_rows: int
    fees_with_nonpositive_tvl_input_count: int
    fees_with_nonpositive_tvl_rate: float
    tvl_below_floor_count: int
    tvl_below_floor_rate: float
    invalid_fee_tier_count: int
    invalid_fee_tier_rate: float
    excluded_from_schedule: bool
    exclusion_reason: str


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
    requested_window_end_ts: int | None = None
    effective_window_end_ts: int | None = None
    indexed_tip_block_ts: int | None = None
    indexed_tip_block_utc: str = ""
    indexed_tip_block_local: str = ""
    index_lag_blocks: int | None = None
    index_lag_hours: float | None = None
    index_window_clamped: bool = False
    pages_fetched: int = 0
    fetched_min_hour_start_unix: int | None = None
    fetched_max_hour_start_unix: int | None = None
    graphql_page_errors: int = 0
    graphql_last_error: str = ""


@dataclass(frozen=True)
class LlamaFetchDiagnostics:
    pages_fetched: int
    fetched_raw_rows: int
    min_hour_start_unix: int | None
    max_hour_start_unix: int | None
    graphql_page_errors: int = 0
    graphql_last_error: str = ""


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
        "--require-run-history-quality-ok",
        action="store_true",
        help=(
            "Exclude pools with insufficient nonzero hourly history from schedule_enhanced "
            "and moves/day frontier outputs."
        ),
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
        "--llama-eth-rpc-url",
        default=os.getenv("LLAMA_ETH_RPC_URL", os.getenv("ETH_RPC_URL", "https://cloudflare-eth.com")),
        help=(
            "Ethereum JSON-RPC endpoint used to map llama _meta.block.number to timestamp for lag detection "
            "(default: $LLAMA_ETH_RPC_URL, then $ETH_RPC_URL, then Cloudflare)."
        ),
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
    parser.add_argument(
        "--optimizer-objective",
        choices=["raw", "risk_adjusted"],
        default="risk_adjusted",
        help="Objective used for move planning/frontier selection (default: risk_adjusted).",
    )
    parser.add_argument(
        "--default-deploy-usd",
        type=float,
        default=10000.0,
        help="Default deploy size (USD) used for execution panel and break-even scaling (default: 10000).",
    )
    parser.add_argument(
        "--default-move-cost-usd",
        type=float,
        default=50.0,
        help="Default move cost (USD per move) used for execution panel (default: 50).",
    )
    parser.add_argument(
        "--default-max-moves-per-day",
        type=int,
        default=4,
        help="Default max moves/day used for execution panel and default plan export (default: 4).",
    )
    parser.add_argument(
        "--min-incremental-usd-per-1000",
        type=float,
        default=0.25,
        help="Minimum incremental edge required to consider a block in optimizer (default: 0.25).",
    )
    parser.add_argument(
        "--capacity-deploy-fraction-cap",
        type=float,
        default=0.02,
        help="Estimated max deploy fraction of pool TVL before capacity warning (default: 0.02).",
    )
    parser.add_argument(
        "--moves-min-hold-hours",
        type=int,
        default=1,
        help="Minimum block hold hours required in optimizer (default: 1).",
    )
    parser.add_argument(
        "--moves-cooldown-hours",
        type=int,
        default=0,
        help="Cooldown hours required between selected moves (default: 0).",
    )
    parser.add_argument(
        "--schedule-min-max-deployable-usd",
        type=float,
        default=10000.0,
        help="Minimum estimated max deployable USD gate value used by optimizer (default: 10000).",
    )
    parser.add_argument(
        "--schedule-min-max-deployable-mode",
        choices=["fixed", "auto_p50", "auto_p75"],
        default="auto_p50",
        help=(
            "How to resolve schedule max deployable gate: fixed uses --schedule-min-max-deployable-usd; "
            "auto_p50/auto_p75 derive it from schedule_enhanced max_deployable_usd_est percentiles."
        ),
    )
    parser.add_argument(
        "--schedule-absolute-min-max-deployable-usd",
        type=float,
        default=0.0,
        help="Absolute hard floor for max deployable USD per block across all scenarios (default: 0).",
    )
    parser.add_argument(
        "--schedule-min-tvl-usd",
        type=float,
        default=0.0,
        help="Minimum median TVL USD per pool for schedule optimizer eligibility (default: 0).",
    )
    parser.add_argument(
        "--max-fees-with-nonpositive-tvl-rate",
        type=float,
        default=0.10,
        help="Exclude schedule sources if fees_with_nonpositive_tvl_input / input_rows exceeds this rate (default: 0.10).",
    )
    parser.add_argument(
        "--max-invalid-fee-tier-rate",
        type=float,
        default=0.10,
        help="Exclude schedule sources if invalid_fee_tier_count / input_rows exceeds this rate (default: 0.10).",
    )
    parser.add_argument(
        "--charts-enable",
        dest="charts_enable",
        action="store_true",
        help="Generate lightweight pool yield visualizations (PNG) and embed in report.",
    )
    parser.add_argument(
        "--charts-disable",
        dest="charts_enable",
        action="store_false",
        help="Disable chart generation.",
    )
    parser.add_argument(
        "--charts-top-n",
        type=int,
        default=10,
        help="Top N enhanced schedule pools to generate charts for (default: 10).",
    )
    parser.add_argument(
        "--charts-window-days",
        type=int,
        default=30,
        help="Window days for charts based on hourly observations (default: 30).",
    )
    parser.add_argument(
        "--charts-output-dir",
        default="charts",
        help="Charts output subdirectory under output-dir (default: charts).",
    )
    parser.add_argument(
        "--price-source",
        choices=["coingecko", "none"],
        default="coingecko",
        help="Token price source for charts (default: coingecko).",
    )
    parser.set_defaults(charts_enable=True)
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


def format_ts_cst(ts: int) -> str:
    if ZoneInfo is None:
        dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        return dt_utc.strftime("%m/%d/%y %H:%M UTC")
    dt_local = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone(
        ZoneInfo("America/Chicago")
    )
    return dt_local.strftime("%m/%d/%y %H:%M %Z")


def format_pattern_cst(ts: int) -> str:
    if ZoneInfo is None:
        dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        return dt_utc.strftime("%A %m/%d/%y %H:%M UTC")
    dt_local = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone(
        ZoneInfo("America/Chicago")
    )
    return dt_local.strftime("%A %m/%d/%y %H:%M %Z")


def normalize_row(source: SourceConfig, row: dict[str, Any]) -> Observation | None:
    pool = row.get("pool")
    if not isinstance(pool, dict):
        pool = {}

    ts = to_int(row.get("ts"))
    if ts <= 0:
        return None

    volume_usd = to_float(row.get("volumeUSD"), default=0.0)
    # Support either aliased tvlUSD or common raw field fallback.
    tvl_raw = row.get("tvlUSD", row.get("totalValueLockedUSD", row.get("liquidityUSD")))
    tvl_missing = tvl_raw is None or (isinstance(tvl_raw, str) and not tvl_raw.strip())
    tvl_usd = to_float(tvl_raw, default=0.0)

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
        tvl_missing=tvl_missing,
    )


def classify_quality_rejection(
    obs: Observation,
    min_tvl_usd: float,
    max_hourly_yield_pct: float | None,
    v2_spike_sources: set[str] | None = None,
) -> str | None:
    is_v2_spike = v2_spike_sources is not None and obs.source_name in v2_spike_sources
    if (not is_v2_spike) and obs.fee_tier == 999_999:
        return "invalid_fee_tier"
    implied_fee_rate = (
        (obs.fees_usd / obs.volume_usd)
        if obs.fees_usd is not None and obs.volume_usd > 0
        else None
    )
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
        and implied_fee_rate is not None
        and implied_fee_rate > 0.10
        and obs.fee_tier >= 100_000
    ):
        return "invalid_fee_tier"
    if (
        not is_v2_spike
        and implied_fee_rate is not None
        and implied_fee_rate > 0.10
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
        if obs.tvl_missing:
            return "fees_with_missing_tvl"
        if obs.tvl_usd == 0:
            return "fees_with_zero_tvl"
        return "fees_with_nonpositive_tvl"
    if obs.tvl_usd <= 0:
        if obs.tvl_missing:
            return "missing_tvl"
        if obs.tvl_usd == 0:
            return "zero_tvl"
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


def compute_source_tvl_alias_sanity(
    observations: list[Observation],
    v2_spike_sources: set[str] | None = None,
) -> dict[tuple[str, str, str], int]:
    counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for obs in observations:
        is_v2_spike = v2_spike_sources is not None and obs.source_name in v2_spike_sources
        if is_v2_spike:
            continue
        if obs.fees_usd is not None and obs.tvl_usd <= 0:
            counts[(obs.source_name, obs.version, obs.chain)] += 1
    return dict(counts)


V2_PAIR_HOUR_QUERY = """
query PairHourPage($first: Int!, $start: Int!, $end: Int!) {
  hourly: pairHourDatas(
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


def _eth_rpc_call(
    rpc_url: str,
    method: str,
    params: list[Any],
    timeout: int,
    retries: int,
) -> Any:
    payload = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "uniswap-yield-scanner/1.0",
    }
    last_err: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        req = urllib.request.Request(rpc_url, data=payload, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
            parsed = json.loads(body)
            if "error" in parsed:
                raise RuntimeError(str(parsed["error"]))
            return parsed.get("result")
        except Exception as err:  # noqa: BLE001
            last_err = err
            if attempt < max(1, retries):
                time.sleep(min(2**attempt, 8))
                continue
            return None
    if last_err is not None:
        return None
    return None


def _parse_hex_int(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        text = raw.strip().lower()
        if text.startswith("0x"):
            try:
                return int(text, 16)
            except ValueError:
                return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def fetch_eth_block_timestamp(
    rpc_url: str,
    block_number: int,
    timeout: int,
    retries: int,
) -> int | None:
    if not rpc_url or block_number <= 0:
        return None
    block_hex = hex(block_number)
    result = _eth_rpc_call(
        rpc_url=rpc_url,
        method="eth_getBlockByNumber",
        params=[block_hex, False],
        timeout=timeout,
        retries=retries,
    )
    if not isinstance(result, dict):
        return None
    return _parse_hex_int(result.get("timestamp"))


def fetch_eth_latest_block_number(
    rpc_url: str,
    timeout: int,
    retries: int,
) -> int | None:
    if not rpc_url:
        return None
    result = _eth_rpc_call(
        rpc_url=rpc_url,
        method="eth_blockNumber",
        params=[],
        timeout=timeout,
        retries=retries,
    )
    return _parse_hex_int(result)


def resolve_llama_effective_window_end_ts(
    runtime: LlamaRuntimeState | None,
    requested_end_ts: int,
    rpc_url: str,
    timeout: int,
    retries: int,
) -> tuple[int, int | None, int | None, int | None, bool]:
    requested_end = max(0, requested_end_ts)
    if runtime is None or runtime.meta_block_number is None:
        return requested_end, None, None, None, False
    indexed_tip_ts = fetch_eth_block_timestamp(
        rpc_url=rpc_url,
        block_number=runtime.meta_block_number,
        timeout=timeout,
        retries=retries,
    )
    latest_block = fetch_eth_latest_block_number(
        rpc_url=rpc_url,
        timeout=timeout,
        retries=retries,
    )
    lag_blocks: int | None = None
    if latest_block is not None and runtime.meta_block_number is not None:
        lag_blocks = max(0, latest_block - runtime.meta_block_number)
    if indexed_tip_ts is None:
        return requested_end, None, latest_block, lag_blocks, False
    indexed_hour_ceiling = floor_to_hour(indexed_tip_ts) + SECONDS_PER_HOUR
    effective_end = min(requested_end, indexed_hour_ceiling)
    return effective_end, indexed_tip_ts, latest_block, lag_blocks, effective_end < requested_end


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
    requested_window_end_ts: int,
    local_timezone: str,
    thresholds: LlamaAdaptiveThresholds | None,
    counts: LlamaDropoffCounters,
    indexed_tip_block_ts: int | None,
    index_lag_blocks: int | None,
    index_window_clamped: bool,
    fetch_diag: LlamaFetchDiagnostics | None,
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
        requested_window_end_ts=requested_window_end_ts,
        effective_window_end_ts=window_end_ts,
        indexed_tip_block_ts=indexed_tip_block_ts,
        indexed_tip_block_utc=(iso_hour(indexed_tip_block_ts) if indexed_tip_block_ts is not None else ""),
        indexed_tip_block_local=(
            iso_hour_local(indexed_tip_block_ts, local_timezone)
            if indexed_tip_block_ts is not None
            else ""
        ),
        index_lag_blocks=index_lag_blocks,
        index_lag_hours=(
            (max(0.0, requested_window_end_ts - window_end_ts) / 3600.0)
            if requested_window_end_ts is not None
            else None
        ),
        index_window_clamped=index_window_clamped,
        pages_fetched=(fetch_diag.pages_fetched if fetch_diag is not None else 0),
        fetched_min_hour_start_unix=(
            fetch_diag.min_hour_start_unix if fetch_diag is not None else None
        ),
        fetched_max_hour_start_unix=(
            fetch_diag.max_hour_start_unix if fetch_diag is not None else None
        ),
        graphql_page_errors=(fetch_diag.graphql_page_errors if fetch_diag is not None else 0),
        graphql_last_error=(fetch_diag.graphql_last_error if fetch_diag is not None else ""),
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
) -> tuple[list[LlamaPairHourRow], int, LlamaFetchDiagnostics]:
    cursor_end = end_ts - 1
    pages = 0
    raw_rows: list[dict[str, Any]] = []
    pair_ids: set[str] = set()
    min_hour_returned: int | None = None
    max_hour_returned: int | None = None
    while cursor_end >= start_ts:
        data = graphql_query(
            source=source,
            query=V2_PAIR_HOUR_QUERY,
            variables={"first": page_size, "start": start_ts, "end": cursor_end},
            timeout=timeout,
            retries=retries,
        )
        rows = data.get("hourly")
        if rows is None:
            rows = data.get("pairHourDatas")
        if not isinstance(rows, list):
            raise RuntimeError(
                f"{source.name}: Expected 'hourly' (or 'pairHourDatas') list in llama query response."
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
            if min_hour_returned is None or hour_ts < min_hour_returned:
                min_hour_returned = hour_ts
            if max_hour_returned is None or hour_ts > max_hour_returned:
                max_hour_returned = hour_ts
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
    fetch_diag = LlamaFetchDiagnostics(
        pages_fetched=pages,
        fetched_raw_rows=len(raw_rows),
        min_hour_start_unix=min_hour_returned,
        max_hour_start_unix=max_hour_returned,
    )
    return output, len(raw_rows), fetch_diag


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


def looks_like_address(value: str) -> bool:
    value = (value or "").strip().lower()
    return value.startswith("0x") and len(value) == 42


def metadata_quality_from_pair_label(pair: str) -> str:
    if "/" not in pair:
        return "low"
    left, right = pair.split("/", 1)
    if not left.strip() or not right.strip():
        return "low"
    if looks_like_address(left) or looks_like_address(right):
        return "low"
    return "high"


def metadata_quality_from_token_symbols(
    token0_symbol: str,
    token1_symbol: str,
    token0_address: str,
    token1_address: str,
) -> str:
    s0 = (token0_symbol or "").strip()
    s1 = (token1_symbol or "").strip()
    if not s0 or not s1:
        return "medium"
    if looks_like_address(s0) or looks_like_address(s1):
        return "low"
    if s0.lower() == (token0_address or "").lower():
        return "low"
    if s1.lower() == (token1_address or "").lower():
        return "low"
    return "high"


def pool_explorer_url(chain: str, pool_id: str) -> str | None:
    if not looks_like_address(pool_id):
        return None
    base_by_chain = {
        "ethereum": "https://etherscan.io/address/",
        "mainnet": "https://etherscan.io/address/",
        "base": "https://basescan.org/address/",
        "arbitrum": "https://arbiscan.io/address/",
        "polygon": "https://polygonscan.com/address/",
        "bnb": "https://bscscan.com/address/",
    }
    base = base_by_chain.get((chain or "").lower())
    if base is None:
        return None
    return f"{base}{pool_id}"


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
    source_input_counts: dict[tuple[str, str, str], int] | None = None,
    tvl_alias_sanity_counts: dict[tuple[str, str, str], int] | None = None,
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
            source_total = (
                source_input_counts.get((source, version, chain), 0)
                if source_input_counts is not None
                else 0
            )
            denom = source_total if source_total > 0 else input_rows
            writer.writerow(
                [
                    "source_reason",
                    source,
                    version,
                    chain,
                    reason,
                    count,
                    f"{(100.0 * count / denom) if denom else 0.0:.6f}",
                    input_rows,
                    output_rows,
                ]
            )
        if source_input_counts:
            for (source, version, chain), source_total in sorted(source_input_counts.items()):
                source_rejected = sum(
                    count
                    for (s, v, c, _), count in rejected_counts.items()
                    if s == source and v == version and c == chain
                )
                writer.writerow(
                    [
                        "source_summary",
                        source,
                        version,
                        chain,
                        "rejected_rows",
                        source_rejected,
                        f"{(100.0 * source_rejected / source_total) if source_total else 0.0:.6f}",
                        source_total,
                        max(0, source_total - source_rejected),
                    ]
                )
        if tvl_alias_sanity_counts:
            for (source, version, chain), count in sorted(
                tvl_alias_sanity_counts.items(),
                key=lambda kv: (-kv[1], kv[0][0], kv[0][1], kv[0][2]),
            ):
                source_total = (
                    source_input_counts.get((source, version, chain), 0)
                    if source_input_counts is not None
                    else 0
                )
                denom = source_total if source_total > 0 else input_rows
                writer.writerow(
                    [
                        "source_tvl_alias_sanity",
                        source,
                        version,
                        chain,
                        "fees_with_nonpositive_tvl_input",
                        count,
                        f"{(100.0 * count / denom) if denom else 0.0:.6f}",
                        input_rows,
                        output_rows,
                    ]
                )


def compute_source_health_rows(
    source_input_counts: dict[tuple[str, str, str], int],
    rejected_counts: dict[tuple[str, str, str, str], int],
    tvl_alias_sanity_counts: dict[tuple[str, str, str], int] | None,
    max_fees_with_nonpositive_tvl_rate: float,
    max_invalid_fee_tier_rate: float,
) -> list[SourceHealthRow]:
    max_rate = max(0.0, max_fees_with_nonpositive_tvl_rate)
    max_invalid_rate = max(0.0, max_invalid_fee_tier_rate)
    tvl_alias_sanity_counts = tvl_alias_sanity_counts or {}
    rows: list[SourceHealthRow] = []
    for key, input_rows in sorted(source_input_counts.items()):
        source_name, version, chain = key
        fees_nonpos = tvl_alias_sanity_counts.get(key, 0)
        tvl_below_floor = rejected_counts.get((source_name, version, chain, "tvl_below_floor"), 0)
        invalid_fee_tier = rejected_counts.get((source_name, version, chain, "invalid_fee_tier"), 0)
        denom = float(input_rows) if input_rows > 0 else 1.0
        nonpos_rate = fees_nonpos / denom
        tvl_floor_rate = tvl_below_floor / denom
        invalid_fee_rate = invalid_fee_tier / denom
        reasons: list[str] = []
        if nonpos_rate > max_rate:
            reasons.append(f"fees_with_nonpositive_tvl_rate {nonpos_rate:.4f} > {max_rate:.4f}")
        if invalid_fee_rate > max_invalid_rate:
            reasons.append(f"invalid_fee_tier_rate {invalid_fee_rate:.4f} > {max_invalid_rate:.4f}")
        excluded = bool(reasons)
        reason = "; ".join(reasons)
        rows.append(
            SourceHealthRow(
                source_name=source_name,
                version=version,
                chain=chain,
                input_rows=input_rows,
                fees_with_nonpositive_tvl_input_count=fees_nonpos,
                fees_with_nonpositive_tvl_rate=nonpos_rate,
                tvl_below_floor_count=tvl_below_floor,
                tvl_below_floor_rate=tvl_floor_rate,
                invalid_fee_tier_count=invalid_fee_tier,
                invalid_fee_tier_rate=invalid_fee_rate,
                excluded_from_schedule=excluded,
                exclusion_reason=reason,
            )
        )
    return rows


def write_source_health_csv(path: Path, rows: Iterable[SourceHealthRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source_name",
                "version",
                "chain",
                "input_rows",
                "fees_with_nonpositive_tvl_input_count",
                "fees_with_nonpositive_tvl_rate",
                "tvl_below_floor_count",
                "tvl_below_floor_rate",
                "invalid_fee_tier_count",
                "invalid_fee_tier_rate",
                "excluded_from_schedule",
                "exclusion_reason",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.source_name,
                    row.version,
                    row.chain,
                    row.input_rows,
                    row.fees_with_nonpositive_tvl_input_count,
                    f"{row.fees_with_nonpositive_tvl_rate:.8f}",
                    row.tvl_below_floor_count,
                    f"{row.tvl_below_floor_rate:.8f}",
                    row.invalid_fee_tier_count,
                    f"{row.invalid_fee_tier_rate:.8f}",
                    ("true" if row.excluded_from_schedule else "false"),
                    row.exclusion_reason,
                ]
            )


def _png_write_rgb(path: Path, width: int, height: int, pixels: list[tuple[int, int, int]]) -> None:
    import struct
    import zlib

    width = max(1, int(width))
    height = max(1, int(height))
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = bytearray()
    for y in range(height):
        raw.append(0)  # filter type
        row_start = y * width
        for x in range(width):
            r, g, b = pixels[row_start + x]
            raw.extend([max(0, min(255, int(r))), max(0, min(255, int(g))), max(0, min(255, int(b)))])

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack("!I", len(data))
            + tag
            + data
            + struct.pack("!I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack("!IIBBBBB", width, height, 8, 2, 0, 0, 0)
    idat = zlib.compress(bytes(raw), level=6)
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")
    path.write_bytes(png)


def _draw_line_on_pixels(
    pixels: list[tuple[int, int, int]],
    width: int,
    height: int,
    xs: list[int],
    ys: list[int],
    color: tuple[int, int, int],
) -> None:
    if len(xs) < 2 or len(ys) < 2:
        return
    for i in range(1, min(len(xs), len(ys))):
        x0, y0 = xs[i - 1], ys[i - 1]
        x1, y1 = xs[i], ys[i]
        dx = abs(x1 - x0)
        sx = 1 if x0 < x1 else -1
        dy = -abs(y1 - y0)
        sy = 1 if y0 < y1 else -1
        err = dx + dy
        x, y = x0, y0
        while True:
            if 0 <= x < width and 0 <= y < height:
                idx = y * width + x
                pixels[idx] = color
            if x == x1 and y == y1:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                x += sx
            if e2 <= dx:
                err += dx
                y += sy


def generate_pool_charts(
    chart_dir: Path,
    observations: list[Observation],
    schedule_rows: list[ScheduleEnhancedRow],
    end_ts: int,
    window_days: int,
    top_n: int,
) -> dict[str, dict[str, str]]:
    window_start = end_ts - (max(1, window_days) * 24 * SECONDS_PER_HOUR)
    by_pool: dict[tuple[str, str, str, str, str, int], list[Observation]] = defaultdict(list)
    for obs in observations:
        if obs.ts < window_start or obs.ts > end_ts:
            continue
        key = (obs.source_name, obs.version, obs.chain, obs.pool_id, obs.pair, obs.fee_tier)
        by_pool[key].append(obs)

    out: dict[str, dict[str, str]] = {}
    for row in schedule_rows[: max(0, top_n)]:
        key = (row.source_name, row.version, row.chain, row.pool_id, row.pair, row.fee_tier)
        rows = sorted(by_pool.get(key, []), key=lambda x: x.ts)
        if not rows:
            continue
        values = [(_metric_usd_per_1000(r) or 0.0) for r in rows]
        if not values:
            continue
        width, height = 900, 260
        bg = (245, 248, 252)
        pixels = [bg] * (width * height)
        # grid lines
        for gy in range(0, height, 40):
            for x in range(width):
                pixels[gy * width + x] = (220, 226, 234)
        min_v, max_v = min(values), max(values)
        if abs(max_v - min_v) < 1e-12:
            max_v = min_v + 1.0
        xs = [int(i * (width - 1) / max(1, len(values) - 1)) for i in range(len(values))]
        ys = [int((height - 1) - ((v - min_v) / (max_v - min_v)) * (height - 1)) for v in values]
        _draw_line_on_pixels(pixels, width, height, xs, ys, (18, 87, 197))

        safe_key = f"{row.chain}_{row.pool_id.lower()}"
        ts_png = chart_dir / f"pool_{safe_key}.png"
        _png_write_rgb(ts_png, width, height, pixels)

        # heatmap (7x24)
        heat_w, heat_h = 24 * 20, 7 * 22
        heat_px = [(250, 250, 250)] * (heat_w * heat_h)
        buckets: dict[tuple[int, int], list[float]] = defaultdict(list)
        for obs in rows:
            dtu = dt.datetime.fromtimestamp(obs.ts, tz=dt.timezone.utc)
            buckets[(dtu.weekday(), dtu.hour)].append(_metric_usd_per_1000(obs) or 0.0)
        vals = [statistics.fmean(v) for v in buckets.values() if v]
        hmin, hmax = (min(vals), max(vals)) if vals else (0.0, 1.0)
        if abs(hmax - hmin) < 1e-12:
            hmax = hmin + 1.0
        for day in range(7):
            for hour in range(24):
                v = statistics.fmean(buckets.get((day, hour), [0.0]))
                t = (v - hmin) / (hmax - hmin)
                r = int(255 * min(1.0, max(0.0, t)))
                g = int(80 + 120 * (1.0 - min(1.0, max(0.0, t))))
                b = int(255 * (1.0 - min(1.0, max(0.0, t))))
                for yy in range(day * 22, min((day + 1) * 22, heat_h)):
                    row_start = yy * heat_w
                    for xx in range(hour * 20, min((hour + 1) * 20, heat_w)):
                        heat_px[row_start + xx] = (r, g, b)
        heat_png = chart_dir / f"heatmap_{safe_key}.png"
        _png_write_rgb(heat_png, heat_w, heat_h, heat_px)

        out[row.pool_id] = {
            "timeseries": str(ts_png.relative_to(chart_dir.parent)),
            "heatmap": str(heat_png.relative_to(chart_dir.parent)),
        }
    return out
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


def _pool_key(
    source_name: str,
    version: str,
    chain: str,
    pool_id: str,
    pair: str,
    fee_tier: int,
) -> tuple[str, str, str, str, str, int]:
    return (source_name, version, chain, pool_id, pair, fee_tier)


def _metric_usd_per_1000(obs: Observation) -> float | None:
    if obs.hourly_yield is None:
        return None
    return obs.hourly_yield * 1000.0


def compute_spike_run_stats(
    observations: Iterable[Observation],
    schedules: list[ScheduleRecommendation],
    end_ts: int,
    window_days: int = 30,
    global_min_threshold: float = 0.0,
    threshold_floor: float = 0.001,
    min_nonzero_hours: int = 24,
    nonzero_eps: float = 1e-12,
) -> list[SpikeRunStats]:
    schedule_thresholds: dict[tuple[str, str, str, str, str, int], float] = {}
    for row in schedules:
        key = _pool_key(
            row.source_name,
            row.version,
            row.chain,
            row.pool_id,
            row.pair,
            row.fee_tier,
        )
        threshold = usd_per_1000_from_yield_pct(row.threshold_hourly_yield_pct)
        prev = schedule_thresholds.get(key)
        if prev is None or threshold > prev:
            schedule_thresholds[key] = threshold

    window_start = end_ts - (max(1, window_days) * 24 * SECONDS_PER_HOUR)
    grouped: dict[tuple[str, str, str, str, str, int], list[Observation]] = defaultdict(list)
    for obs in observations:
        if obs.ts < window_start or obs.ts > end_ts:
            continue
        metric = _metric_usd_per_1000(obs)
        if metric is None:
            continue
        grouped[_pool_key(obs.source_name, obs.version, obs.chain, obs.pool_id, obs.pair, obs.fee_tier)].append(obs)

    stats_rows: list[SpikeRunStats] = []
    for key, rows in grouped.items():
        rows_sorted = sorted(rows, key=lambda x: x.ts)
        metrics = [_metric_usd_per_1000(r) for r in rows_sorted]
        metric_values = [m for m in metrics if m is not None]
        if not metric_values:
            continue
        nonzero_values = [m for m in metric_values if m > max(0.0, nonzero_eps)]
        nonzero_hours = len(nonzero_values)
        history_quality = (
            "ok"
            if nonzero_hours >= max(1, min_nonzero_hours)
            else "insufficient_nonzero_history"
        )

        scheduled_threshold = schedule_thresholds.get(key)
        if scheduled_threshold is not None:
            threshold = scheduled_threshold
        elif history_quality == "ok":
            threshold = percentile(nonzero_values, 0.90)
        else:
            threshold = max(0.0, global_min_threshold)
        threshold = max(
            threshold,
            max(0.0, global_min_threshold),
            max(0.0, threshold_floor),
        )

        spike_hours = 0
        spike_values: list[float] = []
        run_lengths: list[int] = []
        current_run = 0
        prev_ts: int | None = None

        for obs in rows_sorted:
            metric = _metric_usd_per_1000(obs)
            if metric is None:
                continue
            is_spike = metric >= threshold
            if is_spike:
                spike_hours += 1
                spike_values.append(metric)
            if history_quality == "ok":
                if is_spike and (
                    prev_ts is None or obs.ts == prev_ts + SECONDS_PER_HOUR
                ):
                    current_run += 1
                elif is_spike:
                    if current_run > 0:
                        run_lengths.append(current_run)
                    current_run = 1
                else:
                    if current_run > 0:
                        run_lengths.append(current_run)
                        current_run = 0
            prev_ts = obs.ts

        if history_quality == "ok" and current_run > 0:
            run_lengths.append(current_run)

        observed_hours = len(rows_sorted)
        runs_total = len(run_lengths) if history_quality == "ok" else 0
        run_p50 = (
            percentile([float(v) for v in run_lengths], 0.50)
            if history_quality == "ok" and run_lengths
            else 0.0
        )
        run_p75 = (
            percentile([float(v) for v in run_lengths], 0.75)
            if history_quality == "ok" and run_lengths
            else 0.0
        )
        run_p90 = (
            percentile([float(v) for v in run_lengths], 0.90)
            if history_quality == "ok" and run_lengths
            else 0.0
        )
        avg_spike = statistics.fmean(spike_values) if spike_values else 0.0
        p90_spike = percentile(spike_values, 0.90) if spike_values else 0.0

        stats_rows.append(
            SpikeRunStats(
                source_name=key[0],
                version=key[1],
                chain=key[2],
                pool_id=key[3],
                pair=key[4],
                fee_tier=key[5],
                spike_threshold_usd_per_1000_hr=threshold,
                nonzero_hours=nonzero_hours,
                observed_hours=observed_hours,
                spike_hours=spike_hours,
                hit_rate_pct=(100.0 * spike_hours / observed_hours) if observed_hours else 0.0,
                history_quality=history_quality,
                runs_total=runs_total,
                run_length_p50=run_p50,
                run_length_p75=run_p75,
                run_length_p90=run_p90,
                avg_usd_per_1000_hr_when_spiking=avg_spike,
                p90_usd_per_1000_hr_when_spiking=p90_spike,
            )
        )

    stats_rows.sort(
        key=lambda r: (
            r.chain,
            r.source_name,
            -r.hit_rate_pct,
            -r.run_length_p90,
            -r.avg_usd_per_1000_hr_when_spiking,
        )
    )
    return stats_rows


def compute_run_stats_sanity(
    spike_stats: Iterable[SpikeRunStats],
) -> tuple[float, float]:
    rows = list(spike_stats)
    if not rows:
        return 0.0, 0.0
    threshold_zero = sum(1 for row in rows if row.spike_threshold_usd_per_1000_hr <= 0.0)
    always_spike_zero_avg = sum(
        1
        for row in rows
        if row.hit_rate_pct >= 100.0 and row.avg_usd_per_1000_hr_when_spiking <= 0.0
    )
    total = float(len(rows))
    return (100.0 * threshold_zero / total, 100.0 * always_spike_zero_avg / total)


def get_baseline_rate_usd_per_1000_hr(
    observations: Iterable[Observation],
    end_ts: int,
    chain: str | None = None,
    version: str | None = None,
    window_days: int = 30,
    top_k_by_median_tvl: int = 200,
) -> float:
    median_v, _p75 = get_baseline_stats_usd_per_1000_hr(
        observations=observations,
        end_ts=end_ts,
        chain=chain,
        version=version,
        window_days=window_days,
        top_k_by_median_tvl=top_k_by_median_tvl,
    )
    return median_v


def get_baseline_stats_usd_per_1000_hr(
    observations: Iterable[Observation],
    end_ts: int,
    chain: str | None = None,
    version: str | None = None,
    window_days: int = 30,
    top_k_by_median_tvl: int = 200,
) -> tuple[float, float]:
    window_start = end_ts - (max(1, window_days) * 24 * SECONDS_PER_HOUR)
    eligible: list[Observation] = []
    for obs in observations:
        if obs.ts < window_start or obs.ts > end_ts:
            continue
        if obs.hourly_yield is None or obs.tvl_usd <= 0:
            continue
        if chain is not None and obs.chain != chain:
            continue
        if version is not None and obs.version != version:
            continue
        eligible.append(obs)

    if not eligible:
        return 0.0, 0.0

    pool_tvls: dict[tuple[str, str, str, str, str, int], list[float]] = defaultdict(list)
    for obs in eligible:
        pool_tvls[_pool_key(obs.source_name, obs.version, obs.chain, obs.pool_id, obs.pair, obs.fee_tier)].append(obs.tvl_usd)
    ranked_pools = sorted(
        ((k, statistics.median(v)) for k, v in pool_tvls.items() if v),
        key=lambda kv: kv[1],
        reverse=True,
    )
    keep = {k for k, _ in ranked_pools[: max(1, top_k_by_median_tvl)]}
    metrics = [
        _metric_usd_per_1000(obs)
        for obs in eligible
        if _pool_key(obs.source_name, obs.version, obs.chain, obs.pool_id, obs.pair, obs.fee_tier) in keep
    ]
    metric_values = [m for m in metrics if m is not None]
    if not metric_values:
        return 0.0, 0.0
    return statistics.median(metric_values), percentile(metric_values, 0.75)


def build_schedule_enhanced_rows(
    schedules: list[ScheduleRecommendation],
    observations: Iterable[Observation],
    rankings: list[PoolRanking],
    spike_stats: list[SpikeRunStats],
    end_ts: int,
    baseline_window_days: int = 30,
    baseline_top_k: int = 200,
    require_history_quality_ok: bool = False,
    min_incremental_usd_per_1000: float = 0.25,
    capacity_deploy_fraction_cap: float = 0.02,
    capacity_warning_usd: float = 10000.0,
) -> list[ScheduleEnhancedRow]:
    baseline_by_chain: dict[str, tuple[float, float]] = {}
    chains = sorted({s.chain for s in schedules})
    for chain in chains:
        baseline_by_chain[chain] = get_baseline_stats_usd_per_1000_hr(
            observations=observations,
            end_ts=end_ts,
            chain=chain,
            version=None,
            window_days=baseline_window_days,
            top_k_by_median_tvl=baseline_top_k,
        )
    ranking_map = {
        _pool_key(
            row.source_name,
            row.version,
            row.chain,
            row.pool_id,
            row.pair,
            row.fee_tier,
        ): row
        for row in rankings
    }

    spike_map = {
        _pool_key(
            row.source_name,
            row.version,
            row.chain,
            row.pool_id,
            row.pair,
            row.fee_tier,
        ): row
        for row in spike_stats
    }

    rows: list[ScheduleEnhancedRow] = []
    for schedule in schedules:
        avg_block_hourly_usd = usd_per_1000_from_yield_pct(schedule.avg_block_hourly_yield_pct)
        p90_block_hourly_usd = usd_per_1000_from_yield_pct(schedule.p90_block_hourly_yield_pct)
        gross_block = avg_block_hourly_usd * schedule.block_hours
        gross_block_p90 = p90_block_hourly_usd * schedule.block_hours
        baseline_hr, baseline_p75_hr = baseline_by_chain.get(schedule.chain, (0.0, 0.0))
        baseline_block = baseline_hr * schedule.block_hours
        baseline_block_p75 = baseline_p75_hr * schedule.block_hours
        incremental = gross_block - baseline_block
        incremental_p75 = gross_block - baseline_block_p75
        incremental_range = f"{incremental:.6f}..{incremental_p75:.6f}"
        breakeven = max(incremental, 0.0)
        key = _pool_key(
            schedule.source_name,
            schedule.version,
            schedule.chain,
            schedule.pool_id,
            schedule.pair,
            schedule.fee_tier,
        )
        run_stats = spike_map.get(key)
        run_p50 = run_stats.run_length_p50 if run_stats is not None else 0.0
        run_p90 = run_stats.run_length_p90 if run_stats is not None else 0.0
        hit_rate = run_stats.hit_rate_pct if run_stats is not None else 0.0
        history_quality = run_stats.history_quality if run_stats is not None else "insufficient_nonzero_history"
        nonzero_hours = run_stats.nonzero_hours if run_stats is not None else 0
        if require_history_quality_ok and history_quality != "ok":
            continue
        block_hours_safe = max(1, schedule.block_hours)
        base = max(0.0, min(1.0, (hit_rate / 100.0) * (run_p50 / float(block_hours_safe))))
        rel = max(0.0, min(1.0, schedule.reliability_hit_rate_pct / 100.0))
        occ_target = max(2.0, float(schedule.block_hours * 2))
        occ = max(
            0.0,
            min(
                1.0,
                math.log1p(max(0.0, float(schedule.reliable_occurrences)))
                / math.log1p(occ_target),
            ),
        )
        hist_penalty = 1.0 if history_quality == "ok" else 0.25
        nz_penalty = max(0.0, min(1.0, nonzero_hours / 168.0))
        confidence_score = max(0.0, min(1.0, base * rel * occ * hist_penalty * nz_penalty))
        if incremental < min_incremental_usd_per_1000:
            continue
        ranking = ranking_map.get(key)
        avg_tvl_usd = ranking.avg_tvl_usd if ranking is not None else 0.0
        max_deployable_usd_est = max(0.0, avg_tvl_usd * max(0.0, capacity_deploy_fraction_cap))
        capacity_flag = "LOW_CAPACITY" if max_deployable_usd_est < max(0.0, capacity_warning_usd) else "OK"
        risk_adjusted_incremental = incremental * confidence_score
        if history_quality != "ok":
            risk_adjusted_incremental *= 0.25
        rows.append(
            ScheduleEnhancedRow(
                pool_rank=schedule.pool_rank,
                source_name=schedule.source_name,
                version=schedule.version,
                chain=schedule.chain,
                pool_id=schedule.pool_id,
                pair=schedule.pair,
                fee_tier=schedule.fee_tier,
                reliability_hit_rate_pct=schedule.reliability_hit_rate_pct,
                reliable_occurrences=schedule.reliable_occurrences,
                threshold_hourly_yield_pct=schedule.threshold_hourly_yield_pct,
                threshold_hourly_usd_per_1000_liquidity=usd_per_1000_from_yield_pct(
                    schedule.threshold_hourly_yield_pct
                ),
                avg_block_hourly_yield_pct=schedule.avg_block_hourly_yield_pct,
                p90_block_hourly_yield_pct=schedule.p90_block_hourly_yield_pct,
                avg_block_hourly_usd_per_1000_liquidity=avg_block_hourly_usd,
                p90_block_hourly_usd_per_1000_liquidity=p90_block_hourly_usd,
                block_hours=schedule.block_hours,
                next_add_ts=schedule.next_add_ts,
                next_remove_ts=schedule.next_remove_ts,
                pool_score=schedule.pool_score,
                baseline_usd_per_1000_hr=baseline_hr,
                baseline_p75_usd_per_1000_hr=baseline_p75_hr,
                gross_block_usd_per_1000=gross_block,
                gross_block_usd_per_1000_p90=gross_block_p90,
                baseline_block_usd_per_1000=baseline_block,
                baseline_block_p75_usd_per_1000=baseline_block_p75,
                incremental_usd_per_1000=incremental,
                incremental_vs_baseline_p75_usd_per_1000=incremental_p75,
                incremental_range_usd_per_1000=incremental_range,
                breakeven_move_cost_usd_per_1000=breakeven,
                risk_adjusted_incremental_usd_per_1000=risk_adjusted_incremental,
                tvl_median_usd_est=avg_tvl_usd,
                max_deployable_usd_est=max_deployable_usd_est,
                deploy_fraction_cap=capacity_deploy_fraction_cap,
                capacity_flag=capacity_flag,
                run_length_p50=run_p50,
                run_length_p90=run_p90,
                hit_rate_pct=hit_rate,
                history_quality=history_quality,
                nonzero_hours=nonzero_hours,
                confidence_score=confidence_score,
            )
        )
    rows.sort(
        key=lambda r: (
            r.pool_rank,
            -r.breakeven_move_cost_usd_per_1000,
            -r.incremental_usd_per_1000,
            -r.confidence_score,
        )
    )
    return rows


def _ranges_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and start_b < end_a


def _ranges_overlap_with_cooldown(
    start_a: int,
    end_a: int,
    start_b: int,
    end_b: int,
    cooldown_hours: int,
) -> bool:
    cooldown_seconds = max(0, cooldown_hours) * SECONDS_PER_HOUR
    return (start_a < (end_b + cooldown_seconds)) and (start_b < (end_a + cooldown_seconds))


def _objective_incremental_per_1000(row: ScheduleEnhancedRow, objective: str) -> float:
    if objective == "risk_adjusted":
        return row.risk_adjusted_incremental_usd_per_1000
    return row.incremental_usd_per_1000


def select_schedule_plan(
    schedule_rows: list[ScheduleEnhancedRow],
    objective: str,
    move_cost_usd_per_move: float,
    deploy_usd: float,
    max_moves_per_day: int,
    min_hold_hours: int,
    cooldown_hours_between_moves: int,
    schedule_min_max_deployable_usd: float,
    schedule_absolute_min_max_deployable_usd: float = 0.0,
    schedule_min_tvl_usd: float = 0.0,
) -> list[SelectedPlanRow]:
    required_capacity = min(max(0.0, deploy_usd), max(0.0, schedule_min_max_deployable_usd))
    abs_min_deploy = max(0.0, schedule_absolute_min_max_deployable_usd)
    min_tvl = max(0.0, schedule_min_tvl_usd)
    candidates: list[tuple[ScheduleEnhancedRow, float, float, float, float]] = []
    for item in schedule_rows:
        if item.block_hours < max(1, min_hold_hours):
            continue
        if item.max_deployable_usd_est < required_capacity:
            continue
        if item.max_deployable_usd_est < abs_min_deploy:
            continue
        est_tvl = (
            item.max_deployable_usd_est / item.deploy_fraction_cap
            if item.deploy_fraction_cap > 0
            else 0.0
        )
        if est_tvl < min_tvl:
            continue
        effective_deploy_usd = min(max(0.0, deploy_usd), max(0.0, item.max_deployable_usd_est))
        if effective_deploy_usd <= 0:
            continue
        effective_scale = effective_deploy_usd / 1000.0
        obj_incremental = _objective_incremental_per_1000(item, objective)
        net_usd = (effective_scale * obj_incremental) - move_cost_usd_per_move
        if net_usd <= 0:
            continue
        gross_usd = effective_scale * item.gross_block_usd_per_1000
        baseline_usd = effective_scale * item.baseline_block_usd_per_1000
        candidates.append((item, net_usd, gross_usd, baseline_usd, effective_deploy_usd))
    candidates.sort(
        key=lambda item: (
            item[1] / float(max(1, item[0].block_hours)),
            item[1],
            item[0].confidence_score,
        ),
        reverse=True,
    )
    selected: list[tuple[ScheduleEnhancedRow, float, float, float, float]] = []
    day_move_counts: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        row, net_usd, gross_usd, baseline_usd, effective_deploy_usd = candidate
        day_key = dt.datetime.fromtimestamp(
            row.next_add_ts, tz=dt.timezone.utc
        ).strftime("%Y-%m-%d")
        if day_move_counts[day_key] >= max_moves_per_day:
            continue
        overlap = False
        for picked, *_ in selected:
            if _ranges_overlap_with_cooldown(
                row.next_add_ts,
                row.next_remove_ts,
                picked.next_add_ts,
                picked.next_remove_ts,
                cooldown_hours_between_moves,
            ):
                overlap = True
                break
        if overlap:
            continue
        selected.append(candidate)
        day_move_counts[day_key] += 1

    plan_rows: list[SelectedPlanRow] = []
    for row, net_usd, gross_usd, baseline_usd, effective_deploy_usd in selected:
        max_deploy = row.max_deployable_usd_est
        capacity_flag = row.capacity_flag
        if effective_deploy_usd < deploy_usd and max_deploy > 0:
            capacity_flag = "LOW_CAPACITY"
        effective_scale = max(1e-9, effective_deploy_usd / 1000.0)
        plan_rows.append(
            SelectedPlanRow(
                objective=objective,
                move_cost_usd_per_move=move_cost_usd_per_move,
                deploy_usd=deploy_usd,
                pool_rank=row.pool_rank,
                source_name=row.source_name,
                version=row.version,
                chain=row.chain,
                pool_id=row.pool_id,
                pair=row.pair,
                next_add_ts=row.next_add_ts,
                next_remove_ts=row.next_remove_ts,
                block_hours=row.block_hours,
                effective_deploy_usd=effective_deploy_usd,
                expected_net_usd=net_usd,
                expected_net_usd_per_1000=(net_usd / effective_scale),
                expected_gross_usd=gross_usd,
                expected_baseline_usd=baseline_usd,
                breakeven_move_cost_usd_per_1000=row.breakeven_move_cost_usd_per_1000,
                breakeven_move_cost_usd=(row.breakeven_move_cost_usd_per_1000 * effective_scale),
                max_deployable_usd_est=max_deploy,
                confidence_score=row.confidence_score,
                capacity_flag=capacity_flag,
            )
        )
    return plan_rows


def build_moves_day_curve(
    schedule_rows: list[ScheduleEnhancedRow],
    move_cost_scenarios: list[float],
    deploy_scenarios: list[float],
    max_moves_per_day_scenarios: list[int],
    min_hold_hours: int,
    cooldown_hours_between_moves: int,
    objective: str,
    schedule_min_max_deployable_usd: float,
    schedule_absolute_min_max_deployable_usd: float = 0.0,
    schedule_min_tvl_usd: float = 0.0,
) -> list[MovesDayCurveRow]:
    rows: list[MovesDayCurveRow] = []
    for move_cost in move_cost_scenarios:
        for deploy_usd in deploy_scenarios:
            for max_moves in max_moves_per_day_scenarios:
                selected_plan = select_schedule_plan(
                    schedule_rows=schedule_rows,
                    objective=objective,
                    move_cost_usd_per_move=move_cost,
                    deploy_usd=deploy_usd,
                    max_moves_per_day=max_moves,
                    min_hold_hours=min_hold_hours,
                    cooldown_hours_between_moves=cooldown_hours_between_moves,
                    schedule_min_max_deployable_usd=schedule_min_max_deployable_usd,
                    schedule_absolute_min_max_deployable_usd=schedule_absolute_min_max_deployable_usd,
                    schedule_min_tvl_usd=schedule_min_tvl_usd,
                )
                total_net = sum(row.expected_net_usd for row in selected_plan)
                total_gross = sum(row.expected_gross_usd for row in selected_plan)
                total_baseline = sum(row.expected_baseline_usd for row in selected_plan)
                notes = "ok" if selected_plan else "0 candidates"
                rows.append(
                    MovesDayCurveRow(
                        objective=objective,
                        move_cost_usd_per_move=move_cost,
                        deploy_usd=deploy_usd,
                        max_moves_per_day=max_moves,
                        min_hold_hours=max(1, min_hold_hours),
                        cooldown_hours_between_moves=max(0, cooldown_hours_between_moves),
                        selected_blocks_count=len(selected_plan),
                        selected_moves_count=len(selected_plan),
                        total_net_usd=total_net,
                        total_gross_usd=total_gross,
                        total_baseline_usd=total_baseline,
                        notes=notes,
                    )
                )
    rows.sort(
        key=lambda r: (
            r.objective,
            r.move_cost_usd_per_move,
            r.deploy_usd,
            r.max_moves_per_day,
        )
    )
    return rows


def build_schedule_run_diagnostics(
    schedule_rows: list[ScheduleEnhancedRow],
    objective: str,
    move_cost_scenarios: list[float],
    deploy_scenarios: list[float],
    max_moves_per_day_scenarios: list[int],
    min_hold_hours: int,
    cooldown_hours_between_moves: int,
    schedule_min_max_deployable_usd: float,
    schedule_absolute_min_max_deployable_usd: float,
    schedule_min_tvl_usd: float,
    excluded_by_source_health: int,
) -> list[ScheduleRunDiagnosticsRow]:
    rows: list[ScheduleRunDiagnosticsRow] = []
    for move_cost in move_cost_scenarios:
        for deploy_usd in deploy_scenarios:
            for max_moves in max_moves_per_day_scenarios:
                required_capacity = min(max(0.0, deploy_usd), max(0.0, schedule_min_max_deployable_usd))
                abs_min_deploy = max(0.0, schedule_absolute_min_max_deployable_usd)
                min_tvl = max(0.0, schedule_min_tvl_usd)
                hold_floor = max(1, min_hold_hours)

                total = len(schedule_rows)
                excluded_hold = 0
                excluded_capacity = 0
                excluded_abs_capacity = 0
                excluded_min_tvl = 0
                excluded_min_incremental = 0
                excluded_history = 0
                excluded_invalid_fee = 0
                passed_filter_rows: list[ScheduleEnhancedRow] = []

                for item in schedule_rows:
                    if item.block_hours < hold_floor:
                        excluded_hold += 1
                        continue
                    if item.max_deployable_usd_est < required_capacity:
                        excluded_capacity += 1
                        continue
                    if item.max_deployable_usd_est < abs_min_deploy:
                        excluded_abs_capacity += 1
                        continue
                    est_tvl = (
                        item.max_deployable_usd_est / item.deploy_fraction_cap
                        if item.deploy_fraction_cap > 0
                        else 0.0
                    )
                    if est_tvl < min_tvl:
                        excluded_min_tvl += 1
                        continue
                    if _objective_incremental_per_1000(item, objective) <= 0:
                        excluded_min_incremental += 1
                        continue
                    if item.history_quality != "ok":
                        excluded_history += 1
                        continue
                    passed_filter_rows.append(item)

                selected = select_schedule_plan(
                    schedule_rows=schedule_rows,
                    objective=objective,
                    move_cost_usd_per_move=move_cost,
                    deploy_usd=deploy_usd,
                    max_moves_per_day=max_moves,
                    min_hold_hours=min_hold_hours,
                    cooldown_hours_between_moves=cooldown_hours_between_moves,
                    schedule_min_max_deployable_usd=schedule_min_max_deployable_usd,
                    schedule_absolute_min_max_deployable_usd=schedule_absolute_min_max_deployable_usd,
                    schedule_min_tvl_usd=schedule_min_tvl_usd,
                )
                candidates_after_filters = len(passed_filter_rows)
                reason_if_zero = ""
                if not selected:
                    if total == 0:
                        if excluded_by_source_health > 0:
                            reason_if_zero = "excluded_by_source_health"
                        else:
                            reason_if_zero = "no_schedule_rows"
                    elif candidates_after_filters == 0:
                        for reason, value in [
                            ("excluded_by_hold_hours", excluded_hold),
                            ("excluded_by_capacity", excluded_capacity),
                            ("excluded_by_abs_capacity_floor", excluded_abs_capacity),
                            ("excluded_by_min_tvl", excluded_min_tvl),
                            ("excluded_by_min_incremental", excluded_min_incremental),
                            ("excluded_by_history_quality", excluded_history),
                        ]:
                            if value > 0:
                                reason_if_zero = reason
                                break
                        if not reason_if_zero:
                            reason_if_zero = "filtered_out"
                    else:
                        reason_if_zero = "net_after_move_cost_or_overlap"

                scenario_id = (
                    f"{objective}|deploy={deploy_usd:.0f}|cost={move_cost:.0f}|moves={max_moves}|hold={hold_floor}"
                )
                rows.append(
                    ScheduleRunDiagnosticsRow(
                        scenario_id=scenario_id,
                        objective=objective,
                        deploy_usd=deploy_usd,
                        move_cost_usd_per_move=move_cost,
                        max_moves_per_day=max_moves,
                        min_hold_hours=hold_floor,
                        total_schedule_rows=total,
                        excluded_by_source_health=excluded_by_source_health,
                        excluded_by_capacity=excluded_capacity,
                        excluded_by_abs_capacity_floor=excluded_abs_capacity,
                        excluded_by_min_incremental=excluded_min_incremental,
                        excluded_by_history_quality=excluded_history,
                        excluded_by_invalid_fee_tier=excluded_invalid_fee,
                        excluded_by_min_tvl=excluded_min_tvl,
                        excluded_by_hold_hours=excluded_hold,
                        candidates_after_filters=candidates_after_filters,
                        selected_blocks_count=len(selected),
                        reason_if_zero=reason_if_zero,
                    )
                )

    rows.sort(
        key=lambda r: (r.objective, r.move_cost_usd_per_move, r.deploy_usd, r.max_moves_per_day)
    )
    return rows


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


def write_spike_run_stats_csv(path: Path, rows: Iterable[SpikeRunStats]) -> None:
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
                "spike_threshold_usd_per_1000_hr",
                "nonzero_hours",
                "observed_hours",
                "spike_hours",
                "hit_rate_pct",
                "history_quality",
                "runs_total",
                "run_length_p50",
                "run_length_p75",
                "run_length_p90",
                "avg_usd_per_1000_hr_when_spiking",
                "p90_usd_per_1000_hr_when_spiking",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.source_name,
                    row.version,
                    row.chain,
                    row.pool_id,
                    row.pair,
                    row.fee_tier,
                    f"{row.spike_threshold_usd_per_1000_hr:.10f}",
                    row.nonzero_hours,
                    row.observed_hours,
                    row.spike_hours,
                    f"{row.hit_rate_pct:.6f}",
                    row.history_quality,
                    row.runs_total,
                    f"{row.run_length_p50:.6f}",
                    f"{row.run_length_p75:.6f}",
                    f"{row.run_length_p90:.6f}",
                    f"{row.avg_usd_per_1000_hr_when_spiking:.10f}",
                    f"{row.p90_usd_per_1000_hr_when_spiking:.10f}",
                ]
            )


def write_schedule_enhanced_csv(path: Path, rows: Iterable[ScheduleEnhancedRow]) -> None:
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
                "next_add_cst",
                "next_remove_cst",
                "baseline_usd_per_1000_hr",
                "baseline_p75_usd_per_1000_hr",
                "gross_block_usd_per_1000",
                "gross_block_usd_per_1000_p90",
                "baseline_block_usd_per_1000",
                "baseline_block_p75_usd_per_1000",
                "incremental_usd_per_1000",
                "incremental_vs_baseline_p75_usd_per_1000",
                "incremental_range_usd_per_1000",
                "breakeven_move_cost_usd_per_1000",
                "risk_adjusted_incremental_usd_per_1000",
                "tvl_median_usd_est",
                "max_deployable_usd_est",
                "deploy_fraction_cap",
                "capacity_flag",
                "run_length_p50",
                "run_length_p90",
                "hit_rate_pct",
                "history_quality",
                "nonzero_hours",
                "confidence_score",
                "pool_score",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.pool_rank,
                    row.source_name,
                    row.version,
                    row.chain,
                    row.pool_id,
                    row.pair,
                    row.fee_tier,
                    f"{row.reliability_hit_rate_pct:.6f}",
                    row.reliable_occurrences,
                    f"{row.threshold_hourly_yield_pct:.6f}",
                    f"{row.threshold_hourly_usd_per_1000_liquidity:.6f}",
                    f"{row.avg_block_hourly_yield_pct:.6f}",
                    f"{row.p90_block_hourly_yield_pct:.6f}",
                    f"{row.avg_block_hourly_usd_per_1000_liquidity:.6f}",
                    f"{row.p90_block_hourly_usd_per_1000_liquidity:.6f}",
                    row.block_hours,
                    format_ts_cst(row.next_add_ts),
                    format_ts_cst(row.next_remove_ts),
                    f"{row.baseline_usd_per_1000_hr:.6f}",
                    f"{row.baseline_p75_usd_per_1000_hr:.6f}",
                    f"{row.gross_block_usd_per_1000:.6f}",
                    f"{row.gross_block_usd_per_1000_p90:.6f}",
                    f"{row.baseline_block_usd_per_1000:.6f}",
                    f"{row.baseline_block_p75_usd_per_1000:.6f}",
                    f"{row.incremental_usd_per_1000:.6f}",
                    f"{row.incremental_vs_baseline_p75_usd_per_1000:.6f}",
                    row.incremental_range_usd_per_1000,
                    f"{row.breakeven_move_cost_usd_per_1000:.6f}",
                    f"{row.risk_adjusted_incremental_usd_per_1000:.6f}",
                    f"{row.tvl_median_usd_est:.6f}",
                    f"{row.max_deployable_usd_est:.6f}",
                    f"{row.deploy_fraction_cap:.6f}",
                    row.capacity_flag,
                    f"{row.run_length_p50:.6f}",
                    f"{row.run_length_p90:.6f}",
                    f"{row.hit_rate_pct:.6f}",
                    row.history_quality,
                    row.nonzero_hours,
                    f"{row.confidence_score:.6f}",
                    f"{row.pool_score:.12f}",
                ]
            )


def write_moves_day_curve_csv(path: Path, rows: Iterable[MovesDayCurveRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "objective",
                "move_cost_usd_per_move",
                "deploy_usd",
                "max_moves_per_day",
                "min_hold_hours",
                "cooldown_hours_between_moves",
                "selected_blocks_count",
                "selected_moves_count",
                "total_net_usd",
                "total_gross_usd",
                "total_baseline_usd",
                "notes",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.objective,
                    f"{row.move_cost_usd_per_move:.6f}",
                    f"{row.deploy_usd:.2f}",
                    row.max_moves_per_day,
                    row.min_hold_hours,
                    row.cooldown_hours_between_moves,
                    row.selected_blocks_count,
                    row.selected_moves_count,
                    f"{row.total_net_usd:.6f}",
                    f"{row.total_gross_usd:.6f}",
                    f"{row.total_baseline_usd:.6f}",
                    row.notes,
                ]
            )


def write_schedule_run_diagnostics_csv(path: Path, rows: Iterable[ScheduleRunDiagnosticsRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "scenario_id",
                "objective",
                "deploy_usd",
                "move_cost_usd_per_move",
                "max_moves_per_day",
                "min_hold_hours",
                "total_schedule_rows",
                "excluded_by_source_health",
                "excluded_by_capacity",
                "excluded_by_abs_capacity_floor",
                "excluded_by_min_incremental",
                "excluded_by_history_quality",
                "excluded_by_invalid_fee_tier",
                "excluded_by_min_tvl",
                "excluded_by_hold_hours",
                "candidates_after_filters",
                "selected_blocks_count",
                "reason_if_zero",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.scenario_id,
                    row.objective,
                    f"{row.deploy_usd:.2f}",
                    f"{row.move_cost_usd_per_move:.6f}",
                    row.max_moves_per_day,
                    row.min_hold_hours,
                    row.total_schedule_rows,
                    row.excluded_by_source_health,
                    row.excluded_by_capacity,
                    row.excluded_by_abs_capacity_floor,
                    row.excluded_by_min_incremental,
                    row.excluded_by_history_quality,
                    row.excluded_by_invalid_fee_tier,
                    row.excluded_by_min_tvl,
                    row.excluded_by_hold_hours,
                    row.candidates_after_filters,
                    row.selected_blocks_count,
                    row.reason_if_zero,
                ]
            )


def summarize_schedule_enhanced(rows: list[ScheduleEnhancedRow]) -> ScheduleSummaryStats:
    if not rows:
        return ScheduleSummaryStats(
            blocks_count=0,
            breakeven_min=0.0,
            breakeven_p25=0.0,
            breakeven_p50=0.0,
            breakeven_p75=0.0,
            breakeven_p90=0.0,
            breakeven_max=0.0,
            incremental_min=0.0,
            incremental_p25=0.0,
            incremental_p50=0.0,
            incremental_p75=0.0,
            incremental_p90=0.0,
            incremental_max=0.0,
            confidence_p50=0.0,
            confidence_p75=0.0,
            confidence_p90=0.0,
            history_ok_count=0,
            history_insufficient_count=0,
            history_ok_median_breakeven=0.0,
            history_insufficient_median_breakeven=0.0,
        )
    breakeven_values = [r.breakeven_move_cost_usd_per_1000 for r in rows]
    incremental_values = [r.incremental_usd_per_1000 for r in rows]
    confidence_values = [r.confidence_score for r in rows]
    ok_rows = [r for r in rows if r.history_quality == "ok"]
    insuf_rows = [r for r in rows if r.history_quality != "ok"]
    return ScheduleSummaryStats(
        blocks_count=len(rows),
        breakeven_min=min(breakeven_values),
        breakeven_p25=percentile(breakeven_values, 0.25),
        breakeven_p50=percentile(breakeven_values, 0.50),
        breakeven_p75=percentile(breakeven_values, 0.75),
        breakeven_p90=percentile(breakeven_values, 0.90),
        breakeven_max=max(breakeven_values),
        incremental_min=min(incremental_values),
        incremental_p25=percentile(incremental_values, 0.25),
        incremental_p50=percentile(incremental_values, 0.50),
        incremental_p75=percentile(incremental_values, 0.75),
        incremental_p90=percentile(incremental_values, 0.90),
        incremental_max=max(incremental_values),
        confidence_p50=percentile(confidence_values, 0.50),
        confidence_p75=percentile(confidence_values, 0.75),
        confidence_p90=percentile(confidence_values, 0.90),
        history_ok_count=len(ok_rows),
        history_insufficient_count=len(insuf_rows),
        history_ok_median_breakeven=statistics.median(
            [r.breakeven_move_cost_usd_per_1000 for r in ok_rows]
        ) if ok_rows else 0.0,
        history_insufficient_median_breakeven=statistics.median(
            [r.breakeven_move_cost_usd_per_1000 for r in insuf_rows]
        ) if insuf_rows else 0.0,
    )


def resolve_schedule_min_max_deployable_usd(
    rows: list[ScheduleEnhancedRow],
    mode: str,
    fixed_value: float,
) -> float:
    fixed = max(0.0, float(fixed_value))
    if mode == "fixed" or not rows:
        return fixed
    values = [max(0.0, r.max_deployable_usd_est) for r in rows if r.max_deployable_usd_est > 0]
    if not values:
        return fixed
    auto_floor = 100.0
    if mode == "auto_p75":
        auto_value = percentile(values, 0.75)
    else:
        auto_value = percentile(values, 0.50)
    resolved = max(auto_floor, auto_value)
    if fixed > 0:
        resolved = min(resolved, fixed)
    return resolved


def write_schedule_summary_stats_csv(path: Path, stats: ScheduleSummaryStats) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerow(["blocks_count", stats.blocks_count])
        writer.writerow(["breakeven_min", f"{stats.breakeven_min:.6f}"])
        writer.writerow(["breakeven_p25", f"{stats.breakeven_p25:.6f}"])
        writer.writerow(["breakeven_p50", f"{stats.breakeven_p50:.6f}"])
        writer.writerow(["breakeven_p75", f"{stats.breakeven_p75:.6f}"])
        writer.writerow(["breakeven_p90", f"{stats.breakeven_p90:.6f}"])
        writer.writerow(["breakeven_max", f"{stats.breakeven_max:.6f}"])
        writer.writerow(["incremental_min", f"{stats.incremental_min:.6f}"])
        writer.writerow(["incremental_p25", f"{stats.incremental_p25:.6f}"])
        writer.writerow(["incremental_p50", f"{stats.incremental_p50:.6f}"])
        writer.writerow(["incremental_p75", f"{stats.incremental_p75:.6f}"])
        writer.writerow(["incremental_p90", f"{stats.incremental_p90:.6f}"])
        writer.writerow(["incremental_max", f"{stats.incremental_max:.6f}"])
        writer.writerow(["confidence_p50", f"{stats.confidence_p50:.6f}"])
        writer.writerow(["confidence_p75", f"{stats.confidence_p75:.6f}"])
        writer.writerow(["confidence_p90", f"{stats.confidence_p90:.6f}"])
        writer.writerow(["history_ok_count", stats.history_ok_count])
        writer.writerow(["history_insufficient_count", stats.history_insufficient_count])
        writer.writerow(["history_ok_median_breakeven", f"{stats.history_ok_median_breakeven:.6f}"])
        writer.writerow(["history_insufficient_median_breakeven", f"{stats.history_insufficient_median_breakeven:.6f}"])


def write_selected_plan_csv(path: Path, rows: list[SelectedPlanRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "objective",
                "move_cost_usd_per_move",
                "deploy_usd",
                "pool_rank",
                "source_name",
                "version",
                "chain",
                "pool_id",
                "pair",
                "next_add_cst",
                "next_remove_cst",
                "block_hours",
                "effective_deploy_usd",
                "expected_net_usd",
                "expected_net_usd_per_1000",
                "expected_gross_usd",
                "expected_baseline_usd",
                "breakeven_move_cost_usd_per_1000",
                "breakeven_move_cost_usd",
                "max_deployable_usd_est",
                "confidence_score",
                "capacity_flag",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.objective,
                    f"{row.move_cost_usd_per_move:.6f}",
                    f"{row.deploy_usd:.2f}",
                    row.pool_rank,
                    row.source_name,
                    row.version,
                    row.chain,
                    row.pool_id,
                    row.pair,
                    format_ts_cst(row.next_add_ts),
                    format_ts_cst(row.next_remove_ts),
                    row.block_hours,
                    f"{row.effective_deploy_usd:.2f}",
                    f"{row.expected_net_usd:.6f}",
                    f"{row.expected_net_usd_per_1000:.6f}",
                    f"{row.expected_gross_usd:.6f}",
                    f"{row.expected_baseline_usd:.6f}",
                    f"{row.breakeven_move_cost_usd_per_1000:.6f}",
                    f"{row.breakeven_move_cost_usd:.6f}",
                    f"{row.max_deployable_usd_est:.2f}",
                    f"{row.confidence_score:.6f}",
                    row.capacity_flag,
                ]
            )


def write_dashboard_state_json(
    path: Path,
    generated_ts: int,
    llama_diagnostics: LlamaRunDiagnostics | None,
    llama_thresholds: LlamaAdaptiveThresholds | None,
    llama_rows: list[LlamaSpikeRankingRow],
    schedule_rows: list[ScheduleEnhancedRow],
    selected_plan_rows: list[SelectedPlanRow],
    source_health_rows: list[SourceHealthRow],
    moves_day_curve: list[MovesDayCurveRow],
    schedule_run_diagnostics: list[ScheduleRunDiagnosticsRow],
    scenario_plan_filename: str,
    default_move_cost_usd: float,
    default_deploy_usd: float,
    default_max_moves_per_day: int,
    optimizer_objective: str,
    chart_assets: dict[str, dict[str, str]] | None,
    selected_plan_context: str = "active",
    selected_plan_context_scenario: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    chart_assets = chart_assets or {}
    chart_by_pool = chart_assets

    def with_chart(pool_id: str) -> dict[str, str | None]:
        entry = chart_by_pool.get(pool_id, {})
        return {
            "timeseries_png": entry.get("timeseries"),
            "heatmap_png": entry.get("heatmap"),
        }

    spikes_rows = [
        {
            "source_name": row.source_name,
            "chain": row.chain,
            "pair": row.pair,
            "pool_label": human_pool_label(
                token0_symbol=row.token0_symbol,
                token1_symbol=row.token1_symbol,
                token0_address=row.token0,
                token1_address=row.token1,
            ),
            "hour_start_ts": row.hour_start_unix,
            "hour_start_cst": format_ts_cst(row.hour_start_unix),
            "usd_per_1000_liquidity_hourly": row.usd_per_1000_liquidity_hourly,
            "score": row.score,
            "spike_multiplier": row.spike_multiplier,
            "persistence_hits": row.persistence_hits,
            "flags": row.notes_flags,
            "charts": with_chart(row.pair),
        }
        for row in llama_rows[:100]
    ]
    schedule_top_blocks = [
        {
            "pool_rank": row.pool_rank,
            "source_name": row.source_name,
            "chain": row.chain,
            "pool_id": row.pool_id,
            "pair": row.pair,
            "next_add_cst": format_ts_cst(row.next_add_ts),
            "next_remove_cst": format_ts_cst(row.next_remove_ts),
            "incremental_usd_per_1000": row.incremental_usd_per_1000,
            "risk_adjusted_incremental_usd_per_1000": row.risk_adjusted_incremental_usd_per_1000,
            "breakeven_move_cost_usd_per_1000": row.breakeven_move_cost_usd_per_1000,
            "max_deployable_usd_est": row.max_deployable_usd_est,
            "capacity_flag": row.capacity_flag,
            "confidence_score": row.confidence_score,
            "charts": with_chart(row.pool_id),
        }
        for row in schedule_rows[:200]
    ]
    selected_plan_state = [
        {
            "pool_rank": row.pool_rank,
            "source_name": row.source_name,
            "chain": row.chain,
            "pool_id": row.pool_id,
            "pair": row.pair,
            "next_add_cst": format_ts_cst(row.next_add_ts),
            "next_remove_cst": format_ts_cst(row.next_remove_ts),
            "effective_deploy_usd": row.effective_deploy_usd,
            "expected_net_usd": row.expected_net_usd,
            "expected_net_usd_per_1000": row.expected_net_usd_per_1000,
            "capacity_flag": row.capacity_flag,
            "confidence_score": row.confidence_score,
            "charts": with_chart(row.pool_id),
        }
        for row in selected_plan_rows
    ]
    curve_rows = [
        {
            "objective": row.objective,
            "move_cost_usd_per_move": row.move_cost_usd_per_move,
            "deploy_usd": row.deploy_usd,
            "max_moves_per_day": row.max_moves_per_day,
            "selected_blocks_count": row.selected_blocks_count,
            "selected_moves_count": row.selected_moves_count,
            "total_net_usd": row.total_net_usd,
            "notes": row.notes,
        }
        for row in moves_day_curve
    ]
    active_diag = next(
        (
            {
                "scenario_id": row.scenario_id,
                "objective": row.objective,
                "deploy_usd": row.deploy_usd,
                "move_cost_usd_per_move": row.move_cost_usd_per_move,
                "max_moves_per_day": row.max_moves_per_day,
                "min_hold_hours": row.min_hold_hours,
                "total_schedule_rows": row.total_schedule_rows,
                "excluded_by_source_health": row.excluded_by_source_health,
                "excluded_by_capacity": row.excluded_by_capacity,
                "excluded_by_abs_capacity_floor": row.excluded_by_abs_capacity_floor,
                "excluded_by_min_incremental": row.excluded_by_min_incremental,
                "excluded_by_history_quality": row.excluded_by_history_quality,
                "excluded_by_invalid_fee_tier": row.excluded_by_invalid_fee_tier,
                "excluded_by_min_tvl": row.excluded_by_min_tvl,
                "excluded_by_hold_hours": row.excluded_by_hold_hours,
                "candidates_after_filters": row.candidates_after_filters,
                "selected_blocks_count": row.selected_blocks_count,
                "reason_if_zero": row.reason_if_zero,
            }
            for row in schedule_run_diagnostics
            if row.objective == optimizer_objective
            and abs(row.deploy_usd - default_deploy_usd) < 1e-9
            and abs(row.move_cost_usd_per_move - default_move_cost_usd) < 1e-9
            and row.max_moves_per_day == default_max_moves_per_day
        ),
        None,
    )
    state = {
        "generated_utc": iso_hour(generated_ts),
        "defaults": {
            "objective": optimizer_objective,
            "move_cost_usd": default_move_cost_usd,
            "deploy_usd": default_deploy_usd,
            "max_moves_per_day": default_max_moves_per_day,
            "scenario_plan_filename": scenario_plan_filename,
            "selected_plan_context": selected_plan_context,
            "selected_plan_context_scenario": selected_plan_context_scenario,
        },
        "llama": {
            "endpoint": (llama_diagnostics.endpoint if llama_diagnostics is not None else ""),
            "meta_block_number": (llama_diagnostics.meta_block_number if llama_diagnostics is not None else None),
            "seed_next_index": (llama_diagnostics.seed_next_index if llama_diagnostics is not None else None),
            "seed_total": (llama_diagnostics.seed_total if llama_diagnostics is not None else None),
            "seed_last_block": (llama_diagnostics.seed_last_block if llama_diagnostics is not None else None),
            "window_start_ts": (llama_diagnostics.window_start_ts if llama_diagnostics is not None else None),
            "window_end_ts": (llama_diagnostics.window_end_ts if llama_diagnostics is not None else None),
            "requested_window_end_ts": (
                llama_diagnostics.requested_window_end_ts if llama_diagnostics is not None else None
            ),
            "effective_window_end_ts": (
                llama_diagnostics.effective_window_end_ts if llama_diagnostics is not None else None
            ),
            "indexed_tip_block_ts": (
                llama_diagnostics.indexed_tip_block_ts if llama_diagnostics is not None else None
            ),
            "index_lag_blocks": (
                llama_diagnostics.index_lag_blocks if llama_diagnostics is not None else None
            ),
            "index_lag_hours": (
                llama_diagnostics.index_lag_hours if llama_diagnostics is not None else None
            ),
            "index_window_clamped": (
                llama_diagnostics.index_window_clamped if llama_diagnostics is not None else False
            ),
            "thresholds": (
                {
                    "band": llama_thresholds.band,
                    "min_swap_count": llama_thresholds.min_swap_count,
                    "min_weth_liquidity": llama_thresholds.min_weth_liquidity,
                    "baseline_hours": llama_thresholds.baseline_hours,
                    "persistence_hours": llama_thresholds.persistence_hours,
                    "persistence_spike_multiplier": llama_thresholds.persistence_spike_multiplier,
                    "persistence_min_hits": llama_thresholds.persistence_min_hits,
                    "fallback_trace": llama_thresholds.fallback_trace,
                    "total_rows": llama_thresholds.total_rows,
                }
                if llama_thresholds is not None
                else None
            ),
            "dropoff": (
                {
                    "pages_fetched": llama_diagnostics.pages_fetched,
                    "fetched_min_hour_start_unix": llama_diagnostics.fetched_min_hour_start_unix,
                    "fetched_max_hour_start_unix": llama_diagnostics.fetched_max_hour_start_unix,
                    "graphql_page_errors": llama_diagnostics.graphql_page_errors,
                    "graphql_last_error": llama_diagnostics.graphql_last_error,
                    "fetched_raw_rows": llama_diagnostics.counts.fetched_raw_rows,
                    "after_time_window_filter": llama_diagnostics.counts.after_time_window_filter,
                    "after_min_swaps_filter": llama_diagnostics.counts.after_min_swaps_filter,
                    "after_min_weth_filter": llama_diagnostics.counts.after_min_weth_filter,
                    "after_baseline_ready_filter": llama_diagnostics.counts.after_baseline_ready_filter,
                    "after_spike_multiplier_filter": llama_diagnostics.counts.after_spike_multiplier_filter,
                    "after_persistence_filter": llama_diagnostics.counts.after_persistence_filter,
                    "final_ranked_rows": llama_diagnostics.counts.final_ranked_rows,
                }
                if llama_diagnostics is not None
                else None
            ),
            "top_spikes": spikes_rows,
        },
        "spikes": spikes_rows,
        "schedule": {
            "top_blocks": schedule_top_blocks,
            "selected_plan": selected_plan_state,
            "curve": curve_rows,
            "moves_day_curve": curve_rows,
            "diagnostics": {
                "active_scenario": active_diag,
                "all": [
                    {
                        "scenario_id": row.scenario_id,
                        "objective": row.objective,
                        "deploy_usd": row.deploy_usd,
                        "move_cost_usd_per_move": row.move_cost_usd_per_move,
                        "max_moves_per_day": row.max_moves_per_day,
                        "min_hold_hours": row.min_hold_hours,
                        "total_schedule_rows": row.total_schedule_rows,
                        "excluded_by_source_health": row.excluded_by_source_health,
                        "excluded_by_capacity": row.excluded_by_capacity,
                        "excluded_by_abs_capacity_floor": row.excluded_by_abs_capacity_floor,
                        "excluded_by_min_incremental": row.excluded_by_min_incremental,
                        "excluded_by_history_quality": row.excluded_by_history_quality,
                        "excluded_by_invalid_fee_tier": row.excluded_by_invalid_fee_tier,
                        "excluded_by_min_tvl": row.excluded_by_min_tvl,
                        "excluded_by_hold_hours": row.excluded_by_hold_hours,
                        "candidates_after_filters": row.candidates_after_filters,
                        "selected_blocks_count": row.selected_blocks_count,
                        "reason_if_zero": row.reason_if_zero,
                    }
                    for row in schedule_run_diagnostics
                ],
            },
        },
        "source_health": [
            {
                "source_name": row.source_name,
                "version": row.version,
                "chain": row.chain,
                "input_rows": row.input_rows,
                "fees_with_nonpositive_tvl_rate": row.fees_with_nonpositive_tvl_rate,
                "tvl_below_floor_rate": row.tvl_below_floor_rate,
                "invalid_fee_tier_rate": row.invalid_fee_tier_rate,
                "excluded_from_schedule": row.excluded_from_schedule,
                "exclusion_reason": row.exclusion_reason,
            }
            for row in source_health_rows
        ],
        "charts": chart_assets,
    }
    path.write_text(json.dumps(state, indent=2, sort_keys=False), encoding="utf-8")


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
        f"- Built from recurring high-yield windows observed before {format_ts_cst(end_ts)}"
    )
    lines.append("- Add liquidity at `next_add_cst`; remove at `next_remove_cst`.")
    lines.append("")

    if not schedules:
        lines.append("No reliable recurring high-yield windows found.")
    else:
        rows = schedules[:top_n]
        lines.append("## Top Schedule Blocks")
        lines.append("")
        lines.append(
            "| Pool Rank | Version | Pair | Add Pattern (CST) | Remove Pattern (CST) | Next Add (CST) | Next Remove (CST) | Hit Rate % | Avg Block Hourly Yield % | Avg Block USD per $1k/hr | Block Hours |"
        )
        lines.append(
            "|---:|---|---|---|---|---|---|---:|---:|---:|---:|"
        )
        for schedule in rows:
            lines.append(
                "| "
                f"{schedule.pool_rank} | {schedule.version} | {schedule.pair} | "
                f"{format_pattern_cst(schedule.next_add_ts)} | {format_pattern_cst(schedule.next_remove_ts)} | "
                f"{format_ts_cst(schedule.next_add_ts)} | {format_ts_cst(schedule.next_remove_ts)} | "
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
                "requested_window_end_utc",
                "requested_window_end_local",
                "effective_window_end_utc",
                "effective_window_end_local",
                "indexed_tip_block_utc",
                "indexed_tip_block_local",
                "index_lag_blocks",
                "index_lag_hours",
                "index_window_clamped",
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
                "pages_fetched",
                "fetched_min_hour_utc",
                "fetched_max_hour_utc",
                "graphql_page_errors",
                "graphql_last_error",
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
                (
                    iso_hour(diagnostics.requested_window_end_ts)
                    if diagnostics.requested_window_end_ts is not None
                    else ""
                ),
                (
                    iso_hour_local(diagnostics.requested_window_end_ts, diagnostics.local_timezone)
                    if diagnostics.requested_window_end_ts is not None
                    else ""
                ),
                (
                    iso_hour(diagnostics.effective_window_end_ts)
                    if diagnostics.effective_window_end_ts is not None
                    else ""
                ),
                (
                    iso_hour_local(diagnostics.effective_window_end_ts, diagnostics.local_timezone)
                    if diagnostics.effective_window_end_ts is not None
                    else ""
                ),
                diagnostics.indexed_tip_block_utc,
                diagnostics.indexed_tip_block_local,
                diagnostics.index_lag_blocks,
                (
                    f"{diagnostics.index_lag_hours:.6f}"
                    if diagnostics.index_lag_hours is not None
                    else ""
                ),
                diagnostics.index_window_clamped,
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
                diagnostics.pages_fetched,
                (
                    iso_hour(diagnostics.fetched_min_hour_start_unix)
                    if diagnostics.fetched_min_hour_start_unix is not None
                    else ""
                ),
                (
                    iso_hour(diagnostics.fetched_max_hour_start_unix)
                    if diagnostics.fetched_max_hour_start_unix is not None
                    else ""
                ),
                diagnostics.graphql_page_errors,
                diagnostics.graphql_last_error,
                diagnostics.empty_stage,
                diagnostics.empty_message,
            ]
        )


def write_report_html(
    path: Path,
    rankings: list[PoolRanking],
    schedules: list[ScheduleRecommendation],
    schedule_enhanced: list[ScheduleEnhancedRow] | None,
    spike_run_stats: list[SpikeRunStats] | None,
    moves_day_curve: list[MovesDayCurveRow] | None,
    schedule_run_diagnostics: list[ScheduleRunDiagnosticsRow] | None,
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
    run_stats_threshold_zero_pct: float = 0.0,
    run_stats_always_spike_zero_avg_pct: float = 0.0,
    require_run_history_quality_ok: bool = False,
    optimizer_objective: str = "risk_adjusted",
    default_deploy_usd: float = 10000.0,
    default_move_cost_usd: float = 50.0,
    default_max_moves_per_day: int = 4,
    schedule_summary_stats: ScheduleSummaryStats | None = None,
    selected_plan_rows: list[SelectedPlanRow] | None = None,
    source_health_rows: list[SourceHealthRow] | None = None,
    chart_assets: dict[str, dict[str, str]] | None = None,
    scenario_plan_filename: str = "selected_plan_default.csv",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    schedule_enhanced = schedule_enhanced or []
    spike_run_stats = spike_run_stats or []
    moves_day_curve = moves_day_curve or []
    schedule_run_diagnostics = schedule_run_diagnostics or []
    selected_plan_rows = selected_plan_rows or []
    source_health_rows = source_health_rows or []
    chart_assets = chart_assets or {}
    top_rankings = rankings[:top_n]
    top_schedules = schedules[:top_n]
    top_schedule_enhanced = schedule_enhanced[:top_n]
    now_ts = int(time.time())
    generated_ts = format_ts_cst(now_ts)
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
        metadata_quality = metadata_quality_from_pair_label(row.pair)
        explorer_url = pool_explorer_url(row.chain, row.pool_id)
        pool_display = html.escape(row.pair)
        if explorer_url:
            pool_display = (
                f"<a href=\"{html.escape(explorer_url)}\" target=\"_blank\" rel=\"noopener\">"
                f"{pool_display}</a>"
            )
        ranking_rows.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_hourly_yield_pct):.6f}</td>"
            f"<td>{pool_display}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{metadata_quality}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{row.fee_tier}</td>"
            f"<td>{row.avg_tvl_usd:.2f}</td>"
            f"<td>{row.outlier_hours}</td>"
            f"<td>{row.avg_hourly_fees_usd:.2f}</td>"
            f"<td>{row.total_fees_usd:.2f}</td>"
            f"<td>{row.observed_hours}</td>"
            f"<td>{html.escape(format_ts_cst(row.fee_period_start_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.fee_period_end_ts))}</td>"
            f"<td>{row.avg_hourly_yield_pct:.6f}</td>"
            f"<td>{row.median_hourly_yield_pct:.6f}</td>"
            f"<td>{row.trimmed_mean_hourly_yield_pct:.6f}</td>"
            f"<td>{row.p90_hourly_yield_pct:.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.median_hourly_yield_pct):.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.trimmed_mean_hourly_yield_pct):.6f}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.p90_hourly_yield_pct):.6f}</td>"
            f"<td>{html.escape(format_pattern_cst(row.best_window_start_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.best_window_start_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.best_window_end_ts))}</td>"
            "</tr>"
        )

    schedule_rows = []
    for row in top_schedules:
        exchange = infer_exchange_name(row.source_name)
        metadata_quality = metadata_quality_from_pair_label(row.pair)
        explorer_url = pool_explorer_url(row.chain, row.pool_id)
        pool_display = html.escape(row.pair)
        if explorer_url:
            pool_display = (
                f"<a href=\"{html.escape(explorer_url)}\" target=\"_blank\" rel=\"noopener\">"
                f"{pool_display}</a>"
            )
        schedule_rows.append(
            "<tr>"
            f"<td>{row.pool_rank}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_block_hourly_yield_pct):.6f}</td>"
            f"<td>{pool_display}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{metadata_quality}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(format_pattern_cst(row.next_add_ts))}</td>"
            f"<td>{html.escape(format_pattern_cst(row.next_remove_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_add_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_remove_ts))}</td>"
            f"<td>{row.reliability_hit_rate_pct:.2f}</td>"
            f"<td>{row.avg_block_hourly_yield_pct:.6f}</td>"
            f"<td>{row.block_hours}</td>"
            "</tr>"
        )

    jump_rows = []
    for row, status, eta_seconds in jump_now_rows:
        exchange = infer_exchange_name(row.source_name)
        metadata_quality = metadata_quality_from_pair_label(row.pair)
        explorer_url = pool_explorer_url(row.chain, row.pool_id)
        pool_display = html.escape(row.pair)
        if explorer_url:
            pool_display = (
                f"<a href=\"{html.escape(explorer_url)}\" target=\"_blank\" rel=\"noopener\">"
                f"{pool_display}</a>"
            )
        jump_rows.append(
            "<tr>"
            f"<td>{row.pool_rank}</td>"
            f"<td>{usd_per_1000_from_yield_pct(row.avg_block_hourly_yield_pct):.6f}</td>"
            f"<td>{pool_display}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{metadata_quality}</td>"
            f"<td>{html.escape(row.version)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td>{html.escape(format_eta(eta_seconds))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_add_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_remove_ts))}</td>"
            f"<td>{row.reliability_hit_rate_pct:.2f}</td>"
            f"<td>{row.avg_block_hourly_yield_pct:.6f}</td>"
            "</tr>"
        )

    rankings_table_html = "\n".join(ranking_rows) if ranking_rows else (
        "<tr><td colspan='26'>No ranked pools found.</td></tr>"
    )
    schedules_table_html = "\n".join(schedule_rows) if schedule_rows else (
        "<tr><td colspan='13'>No reliable recurring schedule blocks found.</td></tr>"
    )
    break_even_rows: list[str] = []
    for row in top_schedule_enhanced:
        exchange = infer_exchange_name(row.source_name)
        metadata_quality = metadata_quality_from_pair_label(row.pair)
        explorer_url = pool_explorer_url(row.chain, row.pool_id)
        pool_display = html.escape(row.pair)
        if explorer_url:
            pool_display = (
                f"<a href=\"{html.escape(explorer_url)}\" target=\"_blank\" rel=\"noopener\">"
                f"{pool_display}</a>"
            )
        break_even_rows.append(
            "<tr>"
            f"<td>{row.pool_rank}</td>"
            f"<td>{row.avg_block_hourly_usd_per_1000_liquidity:.6f}</td>"
            f"<td>{pool_display}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{metadata_quality}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{row.block_hours}</td>"
            f"<td>{row.gross_block_usd_per_1000:.6f}</td>"
            f"<td>{row.baseline_block_usd_per_1000:.6f}</td>"
            f"<td>{row.incremental_usd_per_1000:.6f}</td>"
            f"<td>{row.breakeven_move_cost_usd_per_1000:.6f}</td>"
            f"<td>{row.run_length_p50:.2f}</td>"
            f"<td>{row.run_length_p90:.2f}</td>"
            f"<td>{row.hit_rate_pct:.2f}</td>"
            f"<td>{row.confidence_score:.4f}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_add_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_remove_ts))}</td>"
            "</tr>"
        )
    break_even_table_html = "\n".join(break_even_rows) if break_even_rows else (
        "<tr><td colspan='17'>No enhanced schedule rows available.</td></tr>"
    )
    run_stats_warning = ""
    if run_stats_threshold_zero_pct > 2.0 or run_stats_always_spike_zero_avg_pct > 2.0:
        run_stats_warning = (
            "<p class='note'><strong>Warning:</strong> Spike run-stats sanity checks are high. "
            "Investigate thresholding/history filters before trusting confidence-driven scheduling.</p>"
        )

    frontier_rows: list[str] = []
    default_deploy = default_deploy_usd
    frontier_subset = [
        row
        for row in moves_day_curve
        if abs(row.deploy_usd - default_deploy) < 0.001 and row.objective == optimizer_objective
    ]
    for row in frontier_subset:
        frontier_rows.append(
            "<tr>"
            f"<td>{row.move_cost_usd_per_move:.2f}</td>"
            f"<td>{row.deploy_usd:.0f}</td>"
            f"<td>{row.max_moves_per_day}</td>"
            f"<td>{row.selected_blocks_count}</td>"
            f"<td>{row.selected_moves_count}</td>"
            f"<td>{row.total_net_usd:.2f}</td>"
            f"<td>{row.total_gross_usd:.2f}</td>"
            f"<td>{row.total_baseline_usd:.2f}</td>"
            f"<td>{html.escape(row.notes)}</td>"
            "</tr>"
        )
    frontier_table_html = "\n".join(frontier_rows) if frontier_rows else (
        "<tr><td colspan='9'>No frontier rows generated.</td></tr>"
    )
    frontier_by_cost_moves: dict[float, dict[int, float]] = defaultdict(dict)
    for row in frontier_subset:
        frontier_by_cost_moves[row.move_cost_usd_per_move][row.max_moves_per_day] = row.total_net_usd
    frontier_matrix_rows: list[str] = []
    for cost in sorted(frontier_by_cost_moves):
        vals = frontier_by_cost_moves[cost]
        frontier_matrix_rows.append(
            "<tr>"
            f"<td>{cost:.2f}</td>"
            f"<td>{vals.get(1, 0.0):.2f}</td>"
            f"<td>{vals.get(2, 0.0):.2f}</td>"
            f"<td>{vals.get(4, 0.0):.2f}</td>"
            f"<td>{vals.get(8, 0.0):.2f}</td>"
            "</tr>"
        )
    frontier_matrix_html = "\n".join(frontier_matrix_rows) if frontier_matrix_rows else (
        "<tr><td colspan='5'>No frontier matrix rows available.</td></tr>"
    )
    active_schedule_diag = next(
        (
            row
            for row in schedule_run_diagnostics
            if row.objective == optimizer_objective
            and abs(row.deploy_usd - default_deploy_usd) < 1e-9
            and abs(row.move_cost_usd_per_move - default_move_cost_usd) < 1e-9
            and row.max_moves_per_day == default_max_moves_per_day
        ),
        None,
    )
    schedule_diag_note = ""
    if active_schedule_diag is not None:
        schedule_diag_note = (
            "Schedule diagnostics: "
            f"total={active_schedule_diag.total_schedule_rows}, "
            f"src_health={active_schedule_diag.excluded_by_source_health}, "
            f"capacity={active_schedule_diag.excluded_by_capacity}, "
            f"abs_capacity={active_schedule_diag.excluded_by_abs_capacity_floor}, "
            f"min_tvl={active_schedule_diag.excluded_by_min_tvl}, "
            f"hold={active_schedule_diag.excluded_by_hold_hours}, "
            f"candidates={active_schedule_diag.candidates_after_filters}, "
            f"selected={active_schedule_diag.selected_blocks_count}, "
            f"reason_if_zero={active_schedule_diag.reason_if_zero or 'n/a'}."
        )
    plan_rows_html: list[str] = []
    for row in selected_plan_rows[:3]:
        plan_rows_html.append(
            "<tr>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_add_ts))}</td>"
            f"<td>{html.escape(format_ts_cst(row.next_remove_ts))}</td>"
            f"<td>{row.max_deployable_usd_est:.2f}</td>"
            f"<td>{row.effective_deploy_usd:.2f}</td>"
            f"<td>{row.expected_net_usd_per_1000:.4f}</td>"
            f"<td>{row.breakeven_move_cost_usd:.2f}</td>"
            f"<td>{row.confidence_score:.4f}</td>"
            f"<td>{html.escape(row.capacity_flag)}</td>"
            "</tr>"
        )
    today_plan_table_html = "\n".join(plan_rows_html) if plan_rows_html else (
        "<tr><td colspan='10'>No qualifying moves for current objective/cost scenario.</td></tr>"
    )
    summary_note = ""
    if schedule_summary_stats is not None:
        summary_note = (
            f"Most blocks break even below ${schedule_summary_stats.breakeven_p50:.2f} per $1k; "
            f"top quartile exceed ${schedule_summary_stats.breakeven_p75:.2f} per $1k. "
            f"Confidence p50/p75/p90 = {schedule_summary_stats.confidence_p50:.3f}/"
            f"{schedule_summary_stats.confidence_p75:.3f}/{schedule_summary_stats.confidence_p90:.3f}."
        )
    jump_table_html = "\n".join(jump_rows) if jump_rows else (
        "<tr><td colspan='13'>No urgent pool windows found in the near-term schedule horizon.</td></tr>"
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
        metadata_quality = metadata_quality_from_token_symbols(
            token0_symbol=row.token0_symbol,
            token1_symbol=row.token1_symbol,
            token0_address=row.token0,
            token1_address=row.token1,
        )
        explorer_url = pool_explorer_url(row.chain, row.pair)
        pool_display = html.escape(pool_name)
        if explorer_url:
            pool_display = (
                f"<a href=\"{html.escape(explorer_url)}\" target=\"_blank\" rel=\"noopener\">"
                f"{pool_display}</a>"
            )
        llama_rows_html.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{row.usd_per_1000_liquidity_hourly:.6f}</td>"
            f"<td>{pool_display}</td>"
            f"<td>{html.escape(exchange)}</td>"
            f"<td>{metadata_quality}</td>"
            f"<td>{html.escape(row.source_name)}</td>"
            f"<td>{html.escape(row.chain)}</td>"
            f"<td>{html.escape(row.pair)}</td>"
            f"<td>{html.escape(row.token0)}</td>"
            f"<td>{html.escape(row.token1)}</td>"
            f"<td>{html.escape(row.token0_symbol)}</td>"
            f"<td>{html.escape(row.token1_symbol)}</td>"
            f"<td>{html.escape(format_ts_cst(row.hour_start_unix))}</td>"
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
        clamp_note = ""
        if d.index_window_clamped:
            clamp_note = (
                "<br/><strong>Index lag:</strong> "
                f"llama indexed tip {html.escape(d.indexed_tip_block_utc or 'n/a')} "
                f"({html.escape(d.indexed_tip_block_local or 'n/a')}), "
                f"lag={('n/a' if d.index_lag_hours is None else f'{d.index_lag_hours:.2f}h')} / "
                f"{('n/a' if d.index_lag_blocks is None else str(d.index_lag_blocks) + ' blocks')}. "
                "Llama analysis window end was truncated to indexed coverage."
            )
        zero_fetch_hint = ""
        if d.counts.fetched_raw_rows == 0:
            zero_fetch_hint = (
                "<br/><strong>Most likely causes:</strong> "
                "1) query alias missing (`hourly: pairHourDatas(...)`), "
                "2) window does not overlap indexed data/startBlock, "
                "3) subgraph not fully synced.<br/>"
                "<code>curl -s -X POST -H 'Content-Type: application/json' "
                f"-d '{{\"query\":\"{{ _meta {{ block {{ number }} }} pairHourDatas(first:1, orderBy:hourStartUnix, orderDirection:desc) {{ id hourStartUnix }} }}\"}}' "
                f"'{html.escape(d.endpoint)}'</code>"
            )
        llama_diag_banner = (
            "<p class='note'>"
            "<strong>Llama Diagnostics:</strong> "
            f"meta.block={('n/a' if d.meta_block_number is None else d.meta_block_number)}, "
            f"v2SeedState next/total={('n/a' if d.seed_next_index is None else d.seed_next_index)}/"
            f"{('n/a' if d.seed_total is None else d.seed_total)}, "
            f"seedLastBlock={('n/a' if d.seed_last_block is None else d.seed_last_block)}.<br/>"
            f"Window (CST): {html.escape(format_ts_cst(d.window_start_ts))} -> {html.escape(format_ts_cst(d.window_end_ts))}.<br/>"
            f"Requested window end (CST): {html.escape(format_ts_cst(d.requested_window_end_ts or d.window_end_ts))}; "
            f"effective llama window (CST): {html.escape(format_ts_cst(d.window_start_ts))} -> {html.escape(format_ts_cst(d.window_end_ts))}.{clamp_note}<br/>"
            "Fetch health: "
            f"pages={d.pages_fetched}, "
            f"returned_range_utc={html.escape(iso_hour(d.fetched_min_hour_start_unix) if d.fetched_min_hour_start_unix is not None else 'n/a')} "
            f"-> {html.escape(iso_hour(d.fetched_max_hour_start_unix) if d.fetched_max_hour_start_unix is not None else 'n/a')}, "
            f"graphql_page_errors={d.graphql_page_errors}"
            f"{(' (' + html.escape(d.graphql_last_error) + ')') if d.graphql_last_error else ''}.<br/>"
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
            f"{zero_fetch_hint}"
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
        <div><strong>Generated (CST):</strong> {html.escape(generated_ts)}</div>
        <div><strong>Sources Scanned:</strong> {source_count}</div>
        <div><strong>Analysis Window Start (CST):</strong> {html.escape(format_ts_cst(start_ts))}</div>
        <div><strong>Analysis Window End (CST):</strong> {html.escape(format_ts_cst(end_ts))}</div>
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
        <a href="schedule_enhanced.csv">schedule_enhanced.csv</a>
        <a href="spike_run_stats.csv">spike_run_stats.csv</a>
        <a href="moves_day_curve.csv">moves_day_curve.csv</a>
        <a href="schedule_run_diagnostics.csv">schedule_run_diagnostics.csv</a>
        <a href="schedule_summary_stats.csv">schedule_summary_stats.csv</a>
        <a href="selected_plan_default.csv">selected_plan_default.csv</a>
        <a href="selected_plan_active.csv">selected_plan_active.csv</a>
        <a href="{html.escape(scenario_plan_filename)}">{html.escape(scenario_plan_filename)}</a>
        <a href="dashboard_state.json">dashboard_state.json</a>
        <a href="source_health.csv">source_health.csv</a>
        <a href="sushi_v2_yield_spikes.csv">sushi_v2_yield_spikes.csv</a>
        <a href="llama_pair_hour_data.csv">llama_pair_hour_data.csv</a>
        <a href="llama_weth_spike_rankings.csv">llama_weth_spike_rankings.csv</a>
        <a href="llama_run_diagnostics.csv">llama_run_diagnostics.csv</a>
        <a href="pool_rankings_diagnostics.csv">pool_rankings_diagnostics.csv</a>
        <a href="data_quality_audit.csv">data_quality_audit.csv</a>
      </div>
    </section>

    <section class="card">
      <h2>Source Health</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Source</th><th>Version</th><th>Chain</th><th>Input Rows</th><th>fees_with_nonpositive_tvl_rate</th>
              <th>tvl_below_floor_rate</th><th>invalid_fee_tier_rate</th><th>Excluded</th><th>Reason</th>
            </tr>
          </thead>
          <tbody>
            {"".join([
              "<tr>"
              f"<td>{html.escape(r.source_name)}</td><td>{html.escape(r.version)}</td><td>{html.escape(r.chain)}</td>"
              f"<td>{r.input_rows}</td><td>{r.fees_with_nonpositive_tvl_rate:.4%}</td>"
              f"<td>{r.tvl_below_floor_rate:.4%}</td><td>{r.invalid_fee_tier_rate:.4%}</td>"
              f"<td>{'yes' if r.excluded_from_schedule else 'no'}</td><td>{html.escape(r.exclusion_reason)}</td>"
              "</tr>"
              for r in source_health_rows
            ]) if source_health_rows else "<tr><td colspan='9'>No source health rows.</td></tr>"}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Today's Plan</h2>
      <p class="note">
        Scenario: objective={html.escape(optimizer_objective)}, deploy=${default_deploy_usd:,.0f}, move cost=${default_move_cost_usd:.2f}/move,
        max_moves/day={default_max_moves_per_day}. Export file: <code>{html.escape(scenario_plan_filename)}</code>.
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pool</th><th>Chain</th><th>Next Add (CST)</th><th>Next Remove (CST)</th>
              <th>Max Deployable USD</th><th>Effective Deploy USD</th><th>Expected Net USD/$1k</th><th>Breakeven Move Cost USD</th><th>Confidence</th><th>Capacity Flag</th>
            </tr>
          </thead>
          <tbody>
            {today_plan_table_html}
          </tbody>
        </table>
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
              <th>Pool Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Metadata Quality</th><th>Version</th><th>Chain</th><th>Status</th>
              <th>ETA</th><th>Next Add (CST)</th><th>Next Remove (CST)</th>
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
              <th>Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Metadata Quality</th><th>Source</th><th>Chain</th><th>Pair Address</th><th>Token0</th><th>Token1</th>
              <th>Token0 Symbol</th><th>Token1 Symbol</th>
              <th>Hour (CST)</th><th>Swap Count</th>
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
              <th>Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Metadata Quality</th><th>Version</th><th>Chain</th><th>Pair</th><th>Fee Tier</th>
              <th>Avg TVL USD</th><th>Outlier Hours</th>
              <th>Avg Hourly Fee USD</th><th>Total Fees USD</th><th>Obs Hours</th>
              <th>Fee Period Start (CST)</th><th>Fee Period End (CST)</th>
              <th>Avg Hourly Yield %</th><th>Median Hourly Yield %</th><th>Trimmed Mean Hourly Yield %</th><th>P90 Hourly Yield %</th>
              <th>Median USD per $1k / hr</th><th>Trimmed Mean USD per $1k / hr</th><th>P90 USD per $1k / hr</th>
              <th>Best Window Pattern (CST)</th><th>Best Window Start (CST)</th><th>Best Window End (CST)</th>
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
              <th>Pool Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Metadata Quality</th><th>Version</th>
              <th>Add Pattern (CST)</th><th>Remove Pattern (CST)</th>
              <th>Next Add (CST)</th><th>Next Remove (CST)</th>
              <th>Hit Rate %</th><th>Avg Block Hourly Yield %</th><th>Block Hours</th>
            </tr>
          </thead>
          <tbody>
            {schedules_table_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Break-even Move Cost Thresholds</h2>
      <p class="note">
        Baseline mode: per-chain median USD per $1k/hr over last 30 days using top 200 pools by median TVL.
        Break-even move cost is the max incremental advantage per $1k for each schedule block.
        Spike run stats rows available: {len(spike_run_stats)}.
        History-quality strict filter: {"enabled" if require_run_history_quality_ok else "disabled"}.
      </p>
      <p class="note">
        RunStats sanity: threshold==0: {run_stats_threshold_zero_pct:.2f}%, always-spike-with-zero-avg: {run_stats_always_spike_zero_avg_pct:.2f}%.
      </p>
      <p class="note">
        {html.escape(summary_note)}
      </p>
      {run_stats_warning}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pool Rank</th><th>Avg USD per $1k / hr</th><th>Pool</th><th>Exchange</th><th>Metadata Quality</th><th>Chain</th><th>Block Hours</th>
              <th>Gross Block USD/$1k</th><th>Baseline Block USD/$1k</th><th>Incremental USD/$1k</th><th>Breakeven Move Cost USD/$1k</th>
              <th>Run P50 (hrs)</th><th>Run P90 (hrs)</th><th>Hit Rate %</th><th>Confidence</th><th>Next Add (CST)</th><th>Next Remove (CST)</th>
            </tr>
          </thead>
          <tbody>
            {break_even_table_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Moves/Day Frontier</h2>
      <p class="note">
        Scenario grid uses move_cost_usd_per_move=[0,25,50,100,250], deploy_usd=[1000,10000], max_moves_per_day=[1,2,4,8], min_hold_hours=1.
        Table below shows deploy_usd={default_deploy_usd:.0f} under objective={html.escape(optimizer_objective)}; use moves_day_curve.csv for full grid.
      </p>
      <p class="note">{html.escape(schedule_diag_note) if schedule_diag_note else "Schedule diagnostics unavailable."}</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Move Cost USD</th><th>Deploy USD</th><th>Max Moves/Day</th><th>Selected Blocks</th><th>Selected Moves</th>
              <th>Total Net USD</th><th>Total Gross USD</th><th>Total Baseline USD</th><th>Notes</th>
            </tr>
          </thead>
          <tbody>
            {frontier_table_html}
          </tbody>
        </table>
      </div>
      <div class="table-wrap" style="margin-top:10px;">
        <table>
          <thead>
            <tr>
              <th>Move Cost USD</th><th>Net @ 1 move/day</th><th>Net @ 2 moves/day</th><th>Net @ 4 moves/day</th><th>Net @ 8 moves/day</th>
            </tr>
          </thead>
          <tbody>
            {frontier_matrix_html}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Pool Visualizations</h2>
      <p class="note">
        Charts show yield trend and hour-of-week heatmap for top schedule pools.
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pool</th><th>Time Series</th><th>Heatmap</th>
            </tr>
          </thead>
          <tbody>
            {"".join([
              "<tr>"
              f"<td>{html.escape(pool_id)}</td>"
              f"<td><a href='{html.escape(paths.get('timeseries',''))}' target='_blank' rel='noopener'>open</a><br/><img src='{html.escape(paths.get('timeseries',''))}' style='max-width:280px;max-height:100px;'/></td>"
              f"<td><a href='{html.escape(paths.get('heatmap',''))}' target='_blank' rel='noopener'>open</a><br/><img src='{html.escape(paths.get('heatmap',''))}' style='max-width:280px;max-height:100px;'/></td>"
              "</tr>"
              for pool_id, paths in list(chart_assets.items())[:20]
            ]) if chart_assets else "<tr><td colspan='3'>Charts unavailable.</td></tr>"}
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
    lines.append(f"- Analysis window (CST): {format_ts_cst(start_ts)} to {format_ts_cst(end_ts)}")
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
            "| Rank | Version | Chain | Pair | Fee Tier | Avg TVL USD | Outlier Hours | Avg Hourly Fee USD | Total Fees USD | Fee Obs Hours | Fee Period (CST) | Avg Hourly Yield % | Median Hourly Yield % | Trimmed Mean Hourly Yield % | P90 Hourly Yield % | Avg USD per $1k / hr | Median USD per $1k / hr | Trimmed Mean USD per $1k / hr | P90 USD per $1k / hr | Best Window Pattern (CST) | Best Window Start (CST) | Best Window End (CST) |"
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
                f"{format_ts_cst(row.fee_period_start_ts)} to {format_ts_cst(row.fee_period_end_ts)} | "
                f"{row.avg_hourly_yield_pct:.6f} | {row.median_hourly_yield_pct:.6f} | "
                f"{row.trimmed_mean_hourly_yield_pct:.6f} | {row.p90_hourly_yield_pct:.6f} | "
                f"{usd_per_1000_from_yield_pct(row.avg_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.median_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.trimmed_mean_hourly_yield_pct):.6f} | "
                f"{usd_per_1000_from_yield_pct(row.p90_hourly_yield_pct):.6f} | "
                f"{format_pattern_cst(row.best_window_start_ts)} | {format_ts_cst(row.best_window_start_ts)} | {format_ts_cst(row.best_window_end_ts)} |"
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
    source_input_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in observations:
        source_input_counts[(row.source_name, row.version, row.chain)] += 1
    tvl_alias_sanity_counts = compute_source_tvl_alias_sanity(
        observations=observations,
        v2_spike_sources=v2_spike_sources,
    )
    if tvl_alias_sanity_counts:
        worst = sorted(
            tvl_alias_sanity_counts.items(),
            key=lambda kv: kv[1],
            reverse=True,
        )[:5]
        print(
            "TVL alias sanity: rows with fees but non-positive TVL detected. "
            "Check GraphQL aliases for tvlUSD/totalValueLockedUSD/liquidityUSD.",
            file=sys.stderr,
        )
        for (source, version, chain), count in worst:
            print(
                f"- {source} ({version}/{chain}): {count:,} rows (fees_with_nonpositive_tvl_input)",
                file=sys.stderr,
            )
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
    source_health_rows = compute_source_health_rows(
        source_input_counts=source_input_counts,
        rejected_counts=quality_rejected_counts,
        tvl_alias_sanity_counts=tvl_alias_sanity_counts,
        max_fees_with_nonpositive_tvl_rate=args.max_fees_with_nonpositive_tvl_rate,
        max_invalid_fee_tier_rate=args.max_invalid_fee_tier_rate,
    )
    excluded_schedule_sources = {
        (row.source_name, row.version, row.chain)
        for row in source_health_rows
        if row.excluded_from_schedule
    }
    if excluded_schedule_sources:
        print(
            f"Excluding {len(excluded_schedule_sources)} source(s) from schedule optimization due to source-health gating.",
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
    llama_requested_end_ts = end_ts
    llama_effective_end_ts = end_ts
    llama_indexed_tip_ts: int | None = None
    llama_index_lag_blocks: int | None = None
    llama_index_window_clamped = False
    llama_fetch_diag = LlamaFetchDiagnostics(
        pages_fetched=0,
        fetched_raw_rows=0,
        min_hour_start_unix=None,
        max_hour_start_unix=None,
        graphql_page_errors=0,
        graphql_last_error="",
    )
    for source in llama_sources:
        try:
            if llama_runtime_state is None:
                llama_runtime_state = fetch_llama_runtime_state(
                    source=source,
                    timeout=args.timeout,
                    retries=args.retries,
                )
                (
                    llama_effective_end_ts,
                    llama_indexed_tip_ts,
                    _llama_latest_block,
                    llama_index_lag_blocks,
                    llama_index_window_clamped,
                ) = resolve_llama_effective_window_end_ts(
                    runtime=llama_runtime_state,
                    requested_end_ts=end_ts,
                    rpc_url=args.llama_eth_rpc_url,
                    timeout=args.timeout,
                    retries=args.retries,
                )
                if llama_index_window_clamped:
                    lag_hours = max(0.0, (end_ts - llama_effective_end_ts) / 3600.0)
                    print(
                        (
                            "Llama index lag detected: "
                            f"requested_end={iso_hour(end_ts)}, "
                            f"indexed_tip={iso_hour(llama_indexed_tip_ts or 0)}, "
                            f"effective_end={iso_hour(llama_effective_end_ts)}, "
                            f"lag_hours={lag_hours:.2f}, "
                            f"lag_blocks={llama_index_lag_blocks if llama_index_lag_blocks is not None else 'n/a'}"
                        ),
                        file=sys.stderr,
                    )
            source_rows, source_raw_count, source_fetch_diag = fetch_llama_pair_hour_rows(
                source=source,
                start_ts=start_ts,
                end_ts=llama_effective_end_ts,
                page_size=effective_page_size(source, args.page_size),
                max_pages=args.max_pages_per_source,
                timeout=args.timeout,
                retries=args.retries,
            )
            llama_pair_hour_rows.extend(source_rows)
            llama_fetched_raw_rows += source_raw_count
            if source_fetch_diag.pages_fetched > llama_fetch_diag.pages_fetched:
                llama_fetch_diag = source_fetch_diag
            print(
                (
                    f"{source.name}: fetched {len(source_rows):,} llama PairHourData rows "
                    f"for report output ({source_raw_count:,} raw)."
                ),
                file=sys.stderr,
            )
        except Exception as err:  # noqa: BLE001
            llama_fetch_diag = LlamaFetchDiagnostics(
                pages_fetched=llama_fetch_diag.pages_fetched,
                fetched_raw_rows=llama_fetch_diag.fetched_raw_rows,
                min_hour_start_unix=llama_fetch_diag.min_hour_start_unix,
                max_hour_start_unix=llama_fetch_diag.max_hour_start_unix,
                graphql_page_errors=llama_fetch_diag.graphql_page_errors + 1,
                graphql_last_error=str(err),
            )
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
        window_end_ts=llama_effective_end_ts,
        requested_window_end_ts=llama_requested_end_ts,
        local_timezone=args.local_timezone,
        thresholds=llama_thresholds,
        counts=llama_dropoff,
        indexed_tip_block_ts=llama_indexed_tip_ts,
        index_lag_blocks=llama_index_lag_blocks,
        index_window_clamped=llama_index_window_clamped,
        fetch_diag=llama_fetch_diag,
    )
    schedules_all = build_liquidity_schedule(
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
    schedules = [
        row
        for row in schedules_all
        if (row.source_name, row.version, row.chain) not in excluded_schedule_sources
    ]
    excluded_schedule_rows_by_source_health = max(0, len(schedules_all) - len(schedules))
    spike_run_stats = compute_spike_run_stats(
        observations=ranking_observations,
        schedules=schedules,
        end_ts=end_ts,
        window_days=30,
        global_min_threshold=0.0,
        threshold_floor=0.001,
        min_nonzero_hours=24,
        nonzero_eps=1e-12,
    )
    run_stats_threshold_zero_pct, run_stats_always_spike_zero_avg_pct = compute_run_stats_sanity(
        spike_run_stats
    )
    print(
        (
            "RunStats sanity: "
            f"threshold==0: {run_stats_threshold_zero_pct:.2f}%, "
            f"always-spike-with-zero-avg: {run_stats_always_spike_zero_avg_pct:.2f}%"
        ),
        file=sys.stderr,
    )
    if run_stats_threshold_zero_pct > 2.0 or run_stats_always_spike_zero_avg_pct > 2.0:
        print(
            "WARNING: run stats sanity exceeded 2%; investigate thresholding/history quality.",
            file=sys.stderr,
        )
    schedule_enhanced = build_schedule_enhanced_rows(
        schedules=schedules,
        observations=ranking_observations,
        rankings=rankings,
        spike_stats=spike_run_stats,
        end_ts=end_ts,
        baseline_window_days=30,
        baseline_top_k=200,
        require_history_quality_ok=args.require_run_history_quality_ok,
        min_incremental_usd_per_1000=args.min_incremental_usd_per_1000,
        capacity_deploy_fraction_cap=args.capacity_deploy_fraction_cap,
        capacity_warning_usd=args.schedule_min_max_deployable_usd,
    )
    deploy_scenarios = sorted(
        {
            1000.0,
            10000.0,
            float(args.default_deploy_usd),
        }
    )
    max_moves_per_day_scenarios = sorted({1, 2, 4, 8, int(args.default_max_moves_per_day)})
    schedule_summary_stats = summarize_schedule_enhanced(schedule_enhanced)
    resolved_schedule_min_max_deployable_usd = resolve_schedule_min_max_deployable_usd(
        rows=schedule_enhanced,
        mode=args.schedule_min_max_deployable_mode,
        fixed_value=args.schedule_min_max_deployable_usd,
    )
    print(
        (
            "Resolved schedule max deployable gate: "
            f"mode={args.schedule_min_max_deployable_mode}, "
            f"value={resolved_schedule_min_max_deployable_usd:.2f} USD"
        ),
        file=sys.stderr,
    )
    moves_day_curve = build_moves_day_curve(
        schedule_rows=schedule_enhanced,
        move_cost_scenarios=[0.0, 25.0, 50.0, 100.0, 250.0],
        deploy_scenarios=deploy_scenarios,
        max_moves_per_day_scenarios=max_moves_per_day_scenarios,
        min_hold_hours=args.moves_min_hold_hours,
        cooldown_hours_between_moves=args.moves_cooldown_hours,
        objective=args.optimizer_objective,
        schedule_min_max_deployable_usd=resolved_schedule_min_max_deployable_usd,
        schedule_absolute_min_max_deployable_usd=args.schedule_absolute_min_max_deployable_usd,
        schedule_min_tvl_usd=args.schedule_min_tvl_usd,
    )
    schedule_run_diagnostics = build_schedule_run_diagnostics(
        schedule_rows=schedule_enhanced,
        objective=args.optimizer_objective,
        move_cost_scenarios=[0.0, 25.0, 50.0, 100.0, 250.0],
        deploy_scenarios=deploy_scenarios,
        max_moves_per_day_scenarios=max_moves_per_day_scenarios,
        min_hold_hours=args.moves_min_hold_hours,
        cooldown_hours_between_moves=args.moves_cooldown_hours,
        schedule_min_max_deployable_usd=resolved_schedule_min_max_deployable_usd,
        schedule_absolute_min_max_deployable_usd=args.schedule_absolute_min_max_deployable_usd,
        schedule_min_tvl_usd=args.schedule_min_tvl_usd,
        excluded_by_source_health=excluded_schedule_rows_by_source_health,
    )
    selected_plan_default = select_schedule_plan(
        schedule_rows=schedule_enhanced,
        objective=args.optimizer_objective,
        move_cost_usd_per_move=args.default_move_cost_usd,
        deploy_usd=args.default_deploy_usd,
        max_moves_per_day=args.default_max_moves_per_day,
        min_hold_hours=args.moves_min_hold_hours,
        cooldown_hours_between_moves=args.moves_cooldown_hours,
        schedule_min_max_deployable_usd=resolved_schedule_min_max_deployable_usd,
        schedule_absolute_min_max_deployable_usd=args.schedule_absolute_min_max_deployable_usd,
        schedule_min_tvl_usd=args.schedule_min_tvl_usd,
    )
    selected_plan_for_state = selected_plan_default
    selected_plan_context = "active"
    selected_plan_context_scenario = (
        f"{args.optimizer_objective}|deploy={args.default_deploy_usd:.0f}|"
        f"cost={args.default_move_cost_usd:.0f}|moves={args.default_max_moves_per_day}|"
        f"hold={max(1, args.moves_min_hold_hours)}"
    )
    if not selected_plan_for_state:
        fallback_curve = sorted(
            [row for row in moves_day_curve if row.selected_blocks_count > 0 and row.objective == args.optimizer_objective],
            key=lambda r: (r.total_net_usd, r.selected_blocks_count),
            reverse=True,
        )
        if fallback_curve:
            fb = fallback_curve[0]
            selected_plan_for_state = select_schedule_plan(
                schedule_rows=schedule_enhanced,
                objective=fb.objective,
                move_cost_usd_per_move=fb.move_cost_usd_per_move,
                deploy_usd=fb.deploy_usd,
                max_moves_per_day=fb.max_moves_per_day,
                min_hold_hours=fb.min_hold_hours,
                cooldown_hours_between_moves=fb.cooldown_hours_between_moves,
                schedule_min_max_deployable_usd=resolved_schedule_min_max_deployable_usd,
                schedule_absolute_min_max_deployable_usd=args.schedule_absolute_min_max_deployable_usd,
                schedule_min_tvl_usd=args.schedule_min_tvl_usd,
            )
            if selected_plan_for_state:
                selected_plan_context = "fallback"
                selected_plan_context_scenario = (
                    f"{fb.objective}|deploy={fb.deploy_usd:.0f}|"
                    f"cost={fb.move_cost_usd_per_move:.0f}|moves={fb.max_moves_per_day}|"
                    f"hold={max(1, fb.min_hold_hours)}"
                )
    active_plan_move_cost = args.default_move_cost_usd
    active_plan_deploy_usd = args.default_deploy_usd
    active_plan_max_moves = args.default_max_moves_per_day
    if selected_plan_context == "fallback" and selected_plan_context_scenario:
        fb_diag = next(
            (row for row in schedule_run_diagnostics if row.scenario_id == selected_plan_context_scenario),
            None,
        )
        if fb_diag is not None:
            active_plan_move_cost = fb_diag.move_cost_usd_per_move
            active_plan_deploy_usd = fb_diag.deploy_usd
            active_plan_max_moves = fb_diag.max_moves_per_day
    scenario_plan_csv = output_dir / (
        f"selected_plan_{args.optimizer_objective}_cost{int(active_plan_move_cost)}_"
        f"deploy{int(active_plan_deploy_usd)}_moves{int(active_plan_max_moves)}.csv"
    )
    matching_curve = [
        row
        for row in moves_day_curve
        if row.objective == args.optimizer_objective
        and abs(row.move_cost_usd_per_move - args.default_move_cost_usd) < 1e-9
        and abs(row.deploy_usd - args.default_deploy_usd) < 1e-9
        and row.max_moves_per_day == args.default_max_moves_per_day
    ]
    if matching_curve:
        expected_blocks = matching_curve[0].selected_blocks_count
        if expected_blocks != len(selected_plan_default):
            raise RuntimeError(
                "selected plan export mismatch: "
                f"curve selected_blocks_count={expected_blocks}, "
                f"plan_rows={len(selected_plan_default)}"
            )

    hourly_csv = output_dir / "hourly_observations.csv"
    ranking_csv = output_dir / "pool_rankings.csv"
    ranking_diag_csv = output_dir / "pool_rankings_diagnostics.csv"
    summary_md = output_dir / "summary.md"
    schedule_csv = output_dir / "liquidity_schedule.csv"
    schedule_enhanced_csv = output_dir / "schedule_enhanced.csv"
    spike_run_stats_csv = output_dir / "spike_run_stats.csv"
    moves_day_curve_csv = output_dir / "moves_day_curve.csv"
    schedule_summary_stats_csv = output_dir / "schedule_summary_stats.csv"
    schedule_run_diagnostics_csv = output_dir / "schedule_run_diagnostics.csv"
    selected_plan_default_csv = output_dir / "selected_plan_default.csv"
    selected_plan_active_csv = output_dir / "selected_plan_active.csv"
    source_health_csv = output_dir / "source_health.csv"
    dashboard_state_json = output_dir / "dashboard_state.json"
    schedule_md = output_dir / "liquidity_schedule.md"
    v2_spike_csv = output_dir / "sushi_v2_yield_spikes.csv"
    llama_pair_hour_csv = output_dir / "llama_pair_hour_data.csv"
    llama_rankings_csv = output_dir / "llama_weth_spike_rankings.csv"
    llama_diag_csv = output_dir / "llama_run_diagnostics.csv"
    quality_csv = output_dir / "data_quality_audit.csv"
    report_html = output_dir / "report.html"
    charts_dir = output_dir / args.charts_output_dir

    chart_assets: dict[str, dict[str, str]] = {}
    if args.charts_enable:
        chart_assets = generate_pool_charts(
            chart_dir=charts_dir,
            observations=ranking_observations,
            schedule_rows=schedule_enhanced,
            end_ts=end_ts,
            window_days=args.charts_window_days,
            top_n=args.charts_top_n,
        )
    generated_now_ts = int(time.time())

    write_hourly_csv(hourly_csv, observations)
    write_rankings_csv(ranking_csv, rankings)
    write_pool_rankings_diagnostics_csv(ranking_diag_csv, ranking_diagnostics)
    write_schedule_csv(schedule_csv, schedules)
    write_schedule_enhanced_csv(schedule_enhanced_csv, schedule_enhanced)
    write_spike_run_stats_csv(spike_run_stats_csv, spike_run_stats)
    write_moves_day_curve_csv(moves_day_curve_csv, moves_day_curve)
    write_schedule_run_diagnostics_csv(schedule_run_diagnostics_csv, schedule_run_diagnostics)
    write_schedule_summary_stats_csv(schedule_summary_stats_csv, schedule_summary_stats)
    write_selected_plan_csv(selected_plan_default_csv, selected_plan_for_state)
    write_selected_plan_csv(selected_plan_active_csv, selected_plan_for_state)
    write_selected_plan_csv(scenario_plan_csv, selected_plan_for_state)
    write_source_health_csv(source_health_csv, source_health_rows)
    write_dashboard_state_json(
        path=dashboard_state_json,
        generated_ts=generated_now_ts,
        llama_diagnostics=llama_diagnostics,
        llama_thresholds=llama_thresholds,
        llama_rows=llama_rankings,
        schedule_rows=schedule_enhanced,
        selected_plan_rows=selected_plan_for_state,
        source_health_rows=source_health_rows,
        moves_day_curve=moves_day_curve,
        schedule_run_diagnostics=schedule_run_diagnostics,
        scenario_plan_filename=scenario_plan_csv.name,
        default_move_cost_usd=args.default_move_cost_usd,
        default_deploy_usd=args.default_deploy_usd,
        default_max_moves_per_day=args.default_max_moves_per_day,
        optimizer_objective=args.optimizer_objective,
        chart_assets=chart_assets,
        selected_plan_context=selected_plan_context,
        selected_plan_context_scenario=selected_plan_context_scenario,
    )
    write_v2_spike_csv(v2_spike_csv, v2_spike_rows)
    write_llama_pair_hour_csv(llama_pair_hour_csv, llama_pair_hour_rows)
    write_llama_spike_csv(llama_rankings_csv, llama_rankings, llama_thresholds)
    write_llama_diagnostics_csv(llama_diag_csv, llama_diagnostics)
    write_data_quality_audit_csv(
        quality_csv,
        input_rows=quality_input_rows,
        output_rows=quality_output_rows,
        rejected_counts=quality_rejected_counts,
        source_input_counts=source_input_counts,
        tvl_alias_sanity_counts=tvl_alias_sanity_counts,
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
        schedule_enhanced,
        spike_run_stats,
        moves_day_curve,
        schedule_run_diagnostics,
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
        run_stats_threshold_zero_pct=run_stats_threshold_zero_pct,
        run_stats_always_spike_zero_avg_pct=run_stats_always_spike_zero_avg_pct,
        require_run_history_quality_ok=args.require_run_history_quality_ok,
        optimizer_objective=args.optimizer_objective,
        default_deploy_usd=args.default_deploy_usd,
        default_move_cost_usd=args.default_move_cost_usd,
        default_max_moves_per_day=args.default_max_moves_per_day,
        schedule_summary_stats=schedule_summary_stats,
        selected_plan_rows=selected_plan_for_state,
        source_health_rows=source_health_rows,
        chart_assets=chart_assets,
        scenario_plan_filename=scenario_plan_csv.name,
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
    print(f"Wrote: {schedule_enhanced_csv}", file=sys.stderr)
    print(f"Wrote: {spike_run_stats_csv}", file=sys.stderr)
    print(f"Wrote: {moves_day_curve_csv}", file=sys.stderr)
    print(f"Wrote: {schedule_run_diagnostics_csv}", file=sys.stderr)
    print(f"Wrote: {schedule_summary_stats_csv}", file=sys.stderr)
    print(f"Wrote: {selected_plan_default_csv}", file=sys.stderr)
    print(f"Wrote: {selected_plan_active_csv}", file=sys.stderr)
    print(f"Wrote: {scenario_plan_csv}", file=sys.stderr)
    print(f"Wrote: {source_health_csv}", file=sys.stderr)
    print(f"Wrote: {dashboard_state_json}", file=sys.stderr)
    if args.charts_enable:
        print(f"Wrote charts: {charts_dir}", file=sys.stderr)
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
