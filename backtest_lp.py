#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import scanner
from scanner import Observation, ScheduleRecommendation


SECONDS_PER_HOUR = 3600
HOURS_PER_WEEK = 24 * 7
PoolKey = tuple[str, str, str, str, int]


@dataclass(frozen=True)
class PoolBacktestResult:
    source_name: str
    version: str
    chain: str
    pool_id: str
    pair: str
    fee_tier: int
    blocks_used: int
    test_hours: int
    active_hours: int
    inactive_hours: int
    utilization_pct: float
    baseline_cum_yield_pct: float
    scheduled_cum_yield_pct: float
    baseline_avg_hourly_yield_pct: float
    scheduled_avg_hourly_yield_pct: float
    inactive_avg_hourly_yield_pct: float
    uplift_vs_baseline_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Walk-forward LP backtest: build schedule recommendations on an older train "
            "window and evaluate them on the following test window."
        )
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--config", help="Path to JSON source config to fetch fresh data.")
    source_group.add_argument(
        "--hourly-csv",
        help="Path to an existing scanner hourly_observations.csv (offline backtest mode).",
    )

    parser.add_argument(
        "--train-hours",
        type=int,
        default=24 * 14,
        help="Training lookback in hours (default: 336, about two weeks).",
    )
    parser.add_argument(
        "--test-hours",
        type=int,
        default=24 * 7,
        help="Holdout test window in hours (default: 168, one week).",
    )
    parser.add_argument(
        "--end-ts",
        type=int,
        default=None,
        help="Backtest end timestamp (UTC epoch seconds). Defaults to current hour.",
    )

    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages-per-source", type=int, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--parallel-window-hours",
        type=int,
        default=0,
        help="Split source windows into parallel time shards of this many hours (0 disables).",
    )
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--strict-sources", action="store_true")

    parser.add_argument("--min-samples", type=int, default=24)
    parser.add_argument(
        "--min-tvl-usd",
        type=float,
        default=10000.0,
        help="Minimum TVL/liquidity USD required per hourly row for ranking/schedule.",
    )
    parser.add_argument(
        "--max-hourly-yield-pct",
        type=float,
        default=100.0,
        help="Cap hourly yield percent per row to suppress extreme outliers.",
    )
    parser.add_argument(
        "--yield-trim-ratio",
        type=float,
        default=0.10,
        help="Trim ratio used for robust trimmed mean yield stats.",
    )
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--schedule-top-pools", type=int, default=25)
    parser.add_argument("--schedule-quantile", type=float, default=0.75)
    parser.add_argument(
        "--schedule-min-usd-per-1000-hour",
        type=float,
        default=None,
        help=(
            "Absolute threshold for high-yield hours in schedule generation, expressed as "
            "USD per hour for each $1,000 of liquidity. Overrides --schedule-quantile when set."
        ),
    )
    parser.add_argument("--schedule-min-hit-rate", type=float, default=0.60)
    parser.add_argument("--schedule-min-occurrences", type=int, default=2)
    parser.add_argument("--schedule-max-blocks-per-pool", type=int, default=3)
    parser.add_argument("--output-dir", default="output/backtest")
    parser.add_argument(
        "--log-file",
        default="/tmp/uniswap-yield-scanner.log",
        help=(
            "Append verbose runtime logs to this file (default: /tmp/uniswap-yield-scanner.log). "
            "Set to empty string to disable file logging."
        ),
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> str | None:
    if args.train_hours <= 0:
        return "--train-hours must be > 0"
    if args.test_hours <= 0:
        return "--test-hours must be > 0"
    if args.page_size <= 0:
        return "--page-size must be > 0"
    if args.max_pages_per_source is not None and args.max_pages_per_source <= 0:
        return "--max-pages-per-source must be > 0"
    if args.workers <= 0:
        return "--workers must be > 0"
    if args.parallel_window_hours < 0:
        return "--parallel-window-hours must be >= 0"
    if args.timeout <= 0:
        return "--timeout must be > 0"
    if args.retries <= 0:
        return "--retries must be > 0"
    if args.min_samples <= 0:
        return "--min-samples must be > 0"
    if args.min_tvl_usd < 0:
        return "--min-tvl-usd must be >= 0"
    if args.max_hourly_yield_pct < 0:
        return "--max-hourly-yield-pct must be >= 0"
    if not (0.0 <= args.yield_trim_ratio <= 0.4):
        return "--yield-trim-ratio must be in [0, 0.4]"
    if args.top <= 0:
        return "--top must be > 0"
    if args.schedule_top_pools <= 0:
        return "--schedule-top-pools must be > 0"
    if args.schedule_min_occurrences <= 0:
        return "--schedule-min-occurrences must be > 0"
    if args.schedule_max_blocks_per_pool <= 0:
        return "--schedule-max-blocks-per-pool must be > 0"
    if not (0.0 <= args.schedule_quantile <= 1.0):
        return "--schedule-quantile must be in [0, 1]"
    if args.schedule_min_usd_per_1000_hour is not None and args.schedule_min_usd_per_1000_hour < 0:
        return "--schedule-min-usd-per-1000-hour must be >= 0"
    if not (0.0 <= args.schedule_min_hit_rate <= 1.0):
        return "--schedule-min-hit-rate must be in [0, 1]"
    return None


def pool_key_for_observation(obs: Observation) -> PoolKey:
    return (obs.source_name, obs.version, obs.chain, obs.pool_id, obs.fee_tier)


def pool_key_for_schedule(schedule: ScheduleRecommendation) -> PoolKey:
    return (
        schedule.source_name,
        schedule.version,
        schedule.chain,
        schedule.pool_id,
        schedule.fee_tier,
    )


def hour_of_week(ts: int) -> int:
    dt_utc = time.gmtime(ts)
    return (dt_utc.tm_wday * 24) + dt_utc.tm_hour


def is_active_for_block(ts: int, schedule: ScheduleRecommendation) -> bool:
    start = (scanner.DAY_NAMES.index(schedule.add_day_utc) * 24) + schedule.add_hour_utc
    end = start + schedule.block_hours
    h = hour_of_week(ts)
    if end <= HOURS_PER_WEEK:
        return start <= h < end
    wrapped_end = end - HOURS_PER_WEEK
    return h >= start or h < wrapped_end


def load_observations_csv(path: Path) -> list[Observation]:
    observations: list[Observation] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = int(row["timestamp"])
                volume = float(row["volume_usd"])
                tvl = float(row["tvl_usd"])
                fee_tier = int(row["fee_tier"])
                fees_raw = row.get("fees_usd", "")
                hourly_raw = row.get("hourly_yield_pct", "")
                fees = float(fees_raw) if fees_raw else None
                hourly_yield = (float(hourly_raw) / 100.0) if hourly_raw else None
            except (TypeError, ValueError, KeyError):
                continue

            observations.append(
                Observation(
                    source_name=row.get("source_name", ""),
                    version=row.get("version", ""),
                    chain=row.get("chain", ""),
                    pool_id=row.get("pool_id", ""),
                    pair=row.get("pair", "UNKNOWN/UNKNOWN"),
                    fee_tier=fee_tier,
                    ts=ts,
                    volume_usd=volume,
                    tvl_usd=tvl,
                    fees_usd=fees,
                    hourly_yield=hourly_yield,
                )
            )
    return observations


def evaluate_schedules(
    schedules: list[ScheduleRecommendation],
    test_observations: Iterable[Observation],
) -> list[PoolBacktestResult]:
    schedules_by_pool: dict[PoolKey, list[ScheduleRecommendation]] = {}
    for item in schedules:
        schedules_by_pool.setdefault(pool_key_for_schedule(item), []).append(item)

    rows_by_pool: dict[PoolKey, list[Observation]] = {}
    for obs in test_observations:
        if obs.hourly_yield is None or obs.hourly_yield < 0:
            continue
        key = pool_key_for_observation(obs)
        if key in schedules_by_pool:
            rows_by_pool.setdefault(key, []).append(obs)

    results: list[PoolBacktestResult] = []
    for key, rows in rows_by_pool.items():
        blocks = schedules_by_pool[key]
        active_yields: list[float] = []
        inactive_yields: list[float] = []
        latest_pair = max(rows, key=lambda r: r.ts).pair

        for row in rows:
            if any(is_active_for_block(row.ts, block) for block in blocks):
                active_yields.append(row.hourly_yield or 0.0)
            else:
                inactive_yields.append(row.hourly_yield or 0.0)

        all_yields = active_yields + inactive_yields
        if not all_yields or not active_yields:
            continue

        baseline_avg = statistics.fmean(all_yields)
        scheduled_avg = statistics.fmean(active_yields) if active_yields else 0.0
        inactive_avg = statistics.fmean(inactive_yields) if inactive_yields else 0.0
        uplift = 0.0
        if baseline_avg > 0:
            uplift = ((scheduled_avg / baseline_avg) - 1.0) * 100

        results.append(
            PoolBacktestResult(
                source_name=key[0],
                version=key[1],
                chain=key[2],
                pool_id=key[3],
                pair=latest_pair,
                fee_tier=key[4],
                blocks_used=len(blocks),
                test_hours=len(all_yields),
                active_hours=len(active_yields),
                inactive_hours=len(inactive_yields),
                utilization_pct=(len(active_yields) / len(all_yields)) * 100,
                baseline_cum_yield_pct=sum(all_yields) * 100,
                scheduled_cum_yield_pct=sum(active_yields) * 100,
                baseline_avg_hourly_yield_pct=baseline_avg * 100,
                scheduled_avg_hourly_yield_pct=scheduled_avg * 100,
                inactive_avg_hourly_yield_pct=inactive_avg * 100,
                uplift_vs_baseline_pct=uplift,
            )
        )

    results.sort(
        key=lambda row: (
            row.uplift_vs_baseline_pct,
            row.scheduled_avg_hourly_yield_pct,
            row.active_hours,
        ),
        reverse=True,
    )
    return results


def write_backtest_csv(path: Path, results: list[PoolBacktestResult]) -> None:
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
                "blocks_used",
                "test_hours",
                "active_hours",
                "inactive_hours",
                "utilization_pct",
                "baseline_cum_yield_pct",
                "scheduled_cum_yield_pct",
                "baseline_avg_hourly_yield_pct",
                "scheduled_avg_hourly_yield_pct",
                "inactive_avg_hourly_yield_pct",
                "uplift_vs_baseline_pct",
            ]
        )
        for row in results:
            writer.writerow(
                [
                    row.source_name,
                    row.version,
                    row.chain,
                    row.pool_id,
                    row.pair,
                    row.fee_tier,
                    row.blocks_used,
                    row.test_hours,
                    row.active_hours,
                    row.inactive_hours,
                    f"{row.utilization_pct:.6f}",
                    f"{row.baseline_cum_yield_pct:.10f}",
                    f"{row.scheduled_cum_yield_pct:.10f}",
                    f"{row.baseline_avg_hourly_yield_pct:.10f}",
                    f"{row.scheduled_avg_hourly_yield_pct:.10f}",
                    f"{row.inactive_avg_hourly_yield_pct:.10f}",
                    f"{row.uplift_vs_baseline_pct:.6f}",
                ]
            )


def write_backtest_summary(
    path: Path,
    results: list[PoolBacktestResult],
    train_start_ts: int,
    train_end_ts: int,
    test_start_ts: int,
    test_end_ts: int,
    schedules_count: int,
    top_n: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# LP Schedule Walk-Forward Backtest")
    lines.append("")
    lines.append(f"- Train window: {scanner.iso_hour(train_start_ts)} to {scanner.iso_hour(train_end_ts)}")
    lines.append(f"- Test window: {scanner.iso_hour(test_start_ts)} to {scanner.iso_hour(test_end_ts)}")
    lines.append(f"- Schedule blocks generated (train): {schedules_count}")
    lines.append(f"- Pools evaluated (test): {len(results)}")
    lines.append("")

    if not results:
        lines.append("No pools were eligible for schedule backtest in the holdout window.")
    else:
        total_active_hours = sum(result.active_hours for result in results)
        total_test_hours = sum(result.test_hours for result in results)
        weighted_scheduled = sum(
            result.scheduled_avg_hourly_yield_pct * result.active_hours for result in results
        )
        weighted_baseline = sum(
            result.baseline_avg_hourly_yield_pct * result.active_hours for result in results
        )
        weighted_uplift = 0.0
        if weighted_baseline > 0:
            weighted_uplift = ((weighted_scheduled / weighted_baseline) - 1.0) * 100

        utilization = (total_active_hours / total_test_hours * 100) if total_test_hours else 0.0
        lines.append(f"- Aggregate utilization: {utilization:.2f}%")
        lines.append(f"- Weighted avg uplift vs baseline: {weighted_uplift:.2f}%")
        lines.append("")
        lines.append("## Top Backtest Results")
        lines.append("")
        lines.append(
            "| Rank | Version | Chain | Pair | Blocks | Active/Test Hours | Utilization % | Scheduled Avg Hourly Yield % | Baseline Avg Hourly Yield % | Uplift vs Baseline % |"
        )
        lines.append("|---:|---|---|---|---:|---|---:|---:|---:|---:|")
        for idx, row in enumerate(results[:top_n], start=1):
            lines.append(
                "| "
                f"{idx} | {row.version} | {row.chain} | {row.pair} | {row.blocks_used} | "
                f"{row.active_hours}/{row.test_hours} | {row.utilization_pct:.2f} | "
                f"{row.scheduled_avg_hourly_yield_pct:.6f} | {row.baseline_avg_hourly_yield_pct:.6f} | "
                f"{row.uplift_vs_baseline_pct:.2f} |"
            )

    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    scanner.setup_run_logging(args.log_file.strip(), sys.argv)
    invalid = validate_args(args)
    if invalid:
        print(f"Invalid arguments: {invalid}", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir).resolve()
    end_ts = scanner.floor_to_hour(args.end_ts or int(time.time()))

    test_start_ts = end_ts - (args.test_hours * SECONDS_PER_HOUR)
    train_start_ts = test_start_ts - (args.train_hours * SECONDS_PER_HOUR)

    if args.hourly_csv:
        hourly_csv_path = Path(args.hourly_csv).resolve()
        if not hourly_csv_path.exists():
            print(f"Hourly CSV not found: {hourly_csv_path}", file=sys.stderr)
            return 2
        observations = load_observations_csv(hourly_csv_path)
        if not observations:
            print("No observations loaded from hourly CSV.", file=sys.stderr)
            return 1
        if args.end_ts is None:
            end_ts = max(obs.ts for obs in observations) + SECONDS_PER_HOUR
            test_start_ts = end_ts - (args.test_hours * SECONDS_PER_HOUR)
            train_start_ts = test_start_ts - (args.train_hours * SECONDS_PER_HOUR)
    else:
        config_path = Path(args.config).resolve()
        if not config_path.exists():
            print(f"Config not found: {config_path}", file=sys.stderr)
            return 2

        try:
            sources = scanner.load_sources(config_path)
        except Exception as err:
            print(f"Failed to load config: {err}", file=sys.stderr)
            return 2

        if not sources:
            print("No enabled sources found in config.", file=sys.stderr)
            return 2

        print(
            (
                f"Backtest scan: {len(sources)} sources | "
                f"train={scanner.iso_hour(train_start_ts)}..{scanner.iso_hour(test_start_ts)} | "
                f"test={scanner.iso_hour(test_start_ts)}..{scanner.iso_hour(end_ts)}"
            ),
            file=sys.stderr,
        )

        try:
            observations, failures = scanner.fetch_all_observations(
                sources=sources,
                start_ts=train_start_ts,
                end_ts=end_ts,
                page_size=args.page_size,
                max_pages=args.max_pages_per_source,
                workers=args.workers,
                parallel_window_hours=args.parallel_window_hours,
                timeout=args.timeout,
                retries=args.retries,
                strict_sources=args.strict_sources,
            )
        except Exception as err:
            print(f"Scan failed: {err}", file=sys.stderr)
            return 1

        if failures and not args.strict_sources:
            print("", file=sys.stderr)
            print(f"{len(failures)} source(s) failed and were skipped:", file=sys.stderr)
            for failure in failures:
                print(
                    f"- {failure.source_name} ({failure.version}/{failure.chain}): {failure.error}",
                    file=sys.stderr,
                )

    train_observations = [obs for obs in observations if train_start_ts <= obs.ts < test_start_ts]
    test_observations = [obs for obs in observations if test_start_ts <= obs.ts < end_ts]

    print(
        "Backtest phase: ranking train pools and building schedules...",
        file=sys.stderr,
    )
    rankings = scanner.rank_pools(
        observations=train_observations,
        min_samples=args.min_samples,
        min_tvl_usd=args.min_tvl_usd,
        max_hourly_yield_pct=args.max_hourly_yield_pct,
        trim_ratio=args.yield_trim_ratio,
    )
    schedules = scanner.build_liquidity_schedule(
        rankings=rankings,
        observations=train_observations,
        end_ts=test_start_ts,
        top_pools=args.schedule_top_pools,
        quantile=args.schedule_quantile,
        min_usd_per_1000_hour=args.schedule_min_usd_per_1000_hour,
        min_hit_rate=args.schedule_min_hit_rate,
        min_occurrences=args.schedule_min_occurrences,
        max_blocks_per_pool=args.schedule_max_blocks_per_pool,
    )
    print(
        "Backtest phase: evaluating schedules on holdout window...",
        file=sys.stderr,
    )
    backtest_results = evaluate_schedules(schedules, test_observations)

    output_dir.mkdir(parents=True, exist_ok=True)
    scanner.write_rankings_csv(output_dir / "train_pool_rankings.csv", rankings)
    scanner.write_schedule_csv(output_dir / "train_liquidity_schedule.csv", schedules)
    scanner.write_schedule_md(
        output_dir / "train_liquidity_schedule.md",
        schedules=schedules,
        end_ts=test_start_ts,
        top_n=args.top,
    )
    write_backtest_csv(output_dir / "backtest_results.csv", backtest_results)
    write_backtest_summary(
        output_dir / "backtest_summary.md",
        results=backtest_results,
        train_start_ts=train_start_ts,
        train_end_ts=test_start_ts,
        test_start_ts=test_start_ts,
        test_end_ts=end_ts,
        schedules_count=len(schedules),
        top_n=args.top,
    )

    print("", file=sys.stderr)
    print(f"Train observations: {len(train_observations):,}", file=sys.stderr)
    print(f"Test observations: {len(test_observations):,}", file=sys.stderr)
    print(f"Train pools ranked: {len(rankings):,}", file=sys.stderr)
    print(f"Train schedule blocks: {len(schedules):,}", file=sys.stderr)
    print(f"Backtest pools evaluated: {len(backtest_results):,}", file=sys.stderr)
    if backtest_results:
        best = backtest_results[0]
        print(
            (
                "Top holdout uplift: "
                f"{best.version}/{best.chain} {best.pair} "
                f"uplift={best.uplift_vs_baseline_pct:.2f}% "
                f"active_hours={best.active_hours}/{best.test_hours}"
            ),
            file=sys.stderr,
        )
    print(f"Wrote: {output_dir / 'train_pool_rankings.csv'}", file=sys.stderr)
    print(f"Wrote: {output_dir / 'train_liquidity_schedule.csv'}", file=sys.stderr)
    print(f"Wrote: {output_dir / 'train_liquidity_schedule.md'}", file=sys.stderr)
    print(f"Wrote: {output_dir / 'backtest_results.csv'}", file=sys.stderr)
    print(f"Wrote: {output_dir / 'backtest_summary.md'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
