# Latest Run Analysis

Updated on February 26, 2026.

## Inputs used

- `output_from_droplet/multichain_3w/llama_run_diagnostics.csv`
- `output_from_droplet/multichain_3w/llama_weth_spike_rankings.csv`
- `output_from_droplet/multichain_3w/schedule_enhanced.csv`
- `output_from_droplet/multichain_3w/schedule_run_diagnostics.csv`
- `output_from_droplet/multichain_3w/moves_day_curve.csv`
- `output_from_droplet/multichain_3w/data_quality_audit.csv`
- `output_from_droplet/multichain_3w/source_health.csv`

## Current state

- `schedule_enhanced.csv` has 52 rows.
- Capacity distribution (`max_deployable_usd_est`) is wide:
  - p50: `$5,195`
  - p75: `$14,525`
  - p90: `$65,087`
  - max: `$1,314,291`
- 30/52 rows are still flagged `LOW_CAPACITY`.

## Why default schedule can still be empty

- `moves_day_curve.csv` has non-zero scenarios, but the active default scenario can still have 0 selected rows.
- In current artifacts:
  - active scenario (`risk_adjusted`, deploy=10000, move_cost=50, max_moves=4) has:
    - candidates_after_filters=28
    - selected_blocks_count=0
    - reason=`net_after_move_cost_or_overlap`
- This means emptiness is mostly economics/overlap under move-cost assumptions, not a silent data failure.

## Llama pipeline status

- Healthy:
  - fetched raw rows: `40,083`
  - final ranked rows: `301`
  - status: “Llama pipeline produced ranked rows.”
- Spike multipliers remain heavy-tailed:
  - p99: `1635.334`
  - max: `6656.558`
- Recommendation: keep persistence + baseline sufficiency gating visible and strict.

## Source health and data quality

- `source_health.csv`: 13 sources total, 4 excluded.
- Top exclusions remain TVL/fee anomalies:
  - `uniswap-v4-base-official`: `fees_with_nonpositive_tvl_rate=0.7493`
  - `uniswap-v3-bnb-official`: `0.1169`
  - `uniswap-v3-unichain-official`: `0.1095`
  - `uniswap-v4-bnb-official`: `invalid_fee_tier_rate=0.1430`
- Data-quality rejects still dominated by TVL-linked reasons and floor gating.

## Next iteration priorities

1. Keep auto-quarantine strict, but add targeted source patching (TVL alias/schema fixes) for top offenders.
2. Improve default scenario selection:
   - if default selects 0 rows, auto-select highest-net non-empty scenario for `selected_plan_active.csv`.
3. Keep capacity-aware gate transparent:
   - always show gate mode/base/effective + max_deployable percentiles in report/dashboard.
4. Continue separating predictable schedule candidates from ad-hoc spikes:
   - require stronger baseline sufficiency for multiplier-heavy rows.

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
