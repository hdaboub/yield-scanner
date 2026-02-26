#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
import zipfile
from pathlib import Path


REQUIRED_FILES = [
    "report.html",
    "dashboard_state.json",
    "run_manifest.json",
    "hourly_observations.csv",
    "pool_rankings.csv",
    "liquidity_schedule.csv",
    "schedule_enhanced.csv",
    "schedule_run_diagnostics.csv",
    "moves_day_curve.csv",
    "selected_plan_default.csv",
    "selected_plan_active.csv",
    "llama_pair_hour_data.csv",
    "llama_weth_spike_rankings.csv",
    "llama_run_diagnostics.csv",
    "source_health.csv",
    "source_tvl_sanity_samples.csv",
    "data_quality_audit.csv",
]


def csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return 0
        return sum(1 for _ in reader)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unpack artifacts zip and run consistency checks."
    )
    parser.add_argument("zip_path", help="Path to artifacts zip file")
    parser.add_argument(
        "--out-dir",
        default="output/unpacked_latest",
        help="Destination directory (default: output/unpacked_latest)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on warnings (schema/count mismatches).",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip_path).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    if not zip_path.exists():
        print(f"zip not found: {zip_path}", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)

    missing = [name for name in REQUIRED_FILES if not (out_dir / name).exists()]
    if missing:
        print("missing required artifact files:", file=sys.stderr)
        for name in missing:
            print(f"  - {name}", file=sys.stderr)
        return 3

    warnings: list[str] = []

    schedule_rows = csv_row_count(out_dir / "liquidity_schedule.csv")
    schedule_enhanced_rows = csv_row_count(out_dir / "schedule_enhanced.csv")
    if schedule_rows == 0 and schedule_enhanced_rows > 0:
        warnings.append(
            "liquidity_schedule.csv has 0 rows while schedule_enhanced.csv has rows."
        )

    with (out_dir / "dashboard_state.json").open("r", encoding="utf-8") as f:
        state = json.load(f)
    state_plan_rows = len((state.get("schedule") or {}).get("selected_plan") or [])
    plan_rows = csv_row_count(out_dir / "selected_plan_active.csv")
    if state_plan_rows != plan_rows:
        warnings.append(
            "dashboard_state.schedule.selected_plan row count does not match selected_plan_active.csv"
        )

    default_scenario = (
        (state.get("defaults") or {}).get("scenario_plan_filename") or ""
    )
    if default_scenario and not (out_dir / default_scenario).exists():
        warnings.append(
            f"default scenario CSV referenced by dashboard_state not found: {default_scenario}"
        )

    print(f"unpacked: {zip_path} -> {out_dir}")
    print(f"schedule rows: {schedule_rows}")
    print(f"schedule_enhanced rows: {schedule_enhanced_rows}")
    print(f"selected plan rows (csv/json): {plan_rows}/{state_plan_rows}")
    print(f"llama rankings rows: {csv_row_count(out_dir / 'llama_weth_spike_rankings.csv')}")
    print(f"moves/day rows: {csv_row_count(out_dir / 'moves_day_curve.csv')}")
    if warnings:
        print("warnings:")
        for w in warnings:
            print(f"  - {w}")
        if args.strict:
            return 4
    else:
        print("consistency checks: ok")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
