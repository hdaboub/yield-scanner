# Latest Run Analysis

Updated on February 25, 2026 (post-fix run).

## Inputs used

- `output_from_droplet/multichain_3w/llama_run_diagnostics.csv`
- `output_from_droplet/multichain_3w/llama_weth_spike_rankings.csv`
- `output_from_droplet/multichain_3w/schedule_enhanced.csv`
- `output_from_droplet/multichain_3w/schedule_run_diagnostics.csv`
- `output_from_droplet/multichain_3w/moves_day_curve.csv`
- `output_from_droplet/multichain_3w/data_quality_audit.csv`
- `output_from_droplet/multichain_3w/source_health.csv`

## Why schedule was empty for some scenarios

- `schedule_enhanced.csv` still shows small `max_deployable_usd_est` values (roughly `$0.5k–$3.1k`).
- `moves_day_curve.csv` now has non-empty scenarios (for low move-cost assumptions), and selected plan export is populated.
- Empty scenarios are now explainable via `schedule_run_diagnostics.csv` (capacity and/or net-after-cost filters).

This is not a silent failure: with current data and assumptions, expected block edge does not exceed move costs.

## Llama pipeline status

- Llama is healthy in this run:
  - non-zero fetched rows,
  - non-zero ranked rows,
  - threshold/persistence dropoff telemetry present.
- Diagnostics now include:
  - requested vs effective window,
  - index lag fields,
  - fetch paging range and GraphQL page errors.

## Data quality findings

- Rejections are dominated by TVL-related issues and TVL floor gating.
- The audit now splits TVL failures into explicit buckets:
  - `fees_with_zero_tvl`, `fees_with_missing_tvl`,
  - `zero_tvl`, `missing_tvl`, `negative_tvl`.
- Source health remains essential to exclude pathological feeds from schedule optimization.

## Changes implemented in this iteration

1. Added `run_manifest.json` output with commit, args, window, source endpoints.
2. Added `scripts/package_artifacts.sh` to reliably include key files, including:
   - `schedule_run_diagnostics.csv`
   - `run_manifest.json`
   - `selected_plan_active.csv`
3. Added optional llama schedule generation mode:
   - `--llama-schedule-mode off|merge|llama_only`
   - recurring buckets from llama ranked spikes.
4. Added `selected_plan_active.csv` as first-class output and report link.
5. Extended diagnostics/reporting for llama fetch/index state.
6. Added static `dashboard.html` generation from `dashboard_state.json` and linked it in `report.html`.
7. Added artifact workflow scripts:
   - `scripts/pull_llama_subgraph.sh`
   - `scripts/unpack_latest_artifacts.py`
   - `scripts/generate_dashboard_html.py`
8. Updated artifact packaging to include `dashboard.html` and all scenario `selected_plan_*.csv` files.

## Repro

```bash
python3 scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --hours 504 \
  --cache-db output/cache/observations.sqlite \
  --cache-overlap-hours 24 \
  --output-dir output/multichain_3w

./scripts/package_artifacts.sh output/multichain_3w artifacts.zip
```
