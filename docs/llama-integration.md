# Llama Integration Runbook

## Purpose
Operational reference for how `yield-scanner` uses the llama subgraph for V2 fee-yield spike detection and report output.

## Source Of Truth
- Subgraph code in this repo: `subgraphs/llama/`
- Authoritative droplet path: `/root/llama`
- Studio endpoint (current):  
  `https://api.studio.thegraph.com/query/1742316/llama/v0.2.9`

## Scanner Wiring
Key queries in `scanner.py`:
- `LLAMA_RUNTIME_QUERY`
  - fetches `_meta.block.number`
  - fetches `v2SeedStates { nextIndex total lastBlock }`
- `V2_PAIR_HOUR_QUERY`
  - queries PairHourData with alias:
  - `hourly: pairHourDatas(...)`
- `V2_PAIR_META_QUERY`
- `V2_TOKEN_META_QUERY`

The fetcher accepts both:
- `data.hourly`
- fallback `data.pairHourDatas`

## Why Alias Matters
The scanner’s hourly ingestion path expects a top-level list field named `hourly`.  
If a query returns `pairHourDatas` without aliasing to `hourly`, ingestion can fail or return empty unexpectedly.

## Ranking Math (Llama Section)
For WETH pairs:
- `score = feeWETH / reserveWETH`
- `hourly_yield_pct = score * 100`
- `usd_per_1000_liquidity_hourly = score * 1000`
- `spike_multiplier = score / baseline_median_score`
- `persistence_hits` counted over trailing persistence window

## Empty Llama Output: Fast Triage
If report shows 0 llama rows:
1. Confirm endpoint/version is reachable.
2. Verify `_meta.block.number` and `v2SeedStates`.
3. Verify query window overlaps indexed hours.
4. Verify PairHourData query shape and alias (`hourly:`).

Quick check:
```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _meta { block { number } } v2SeedStates { id nextIndex total lastBlock } pairHourDatas(first:1, orderBy:hourStartUnix, orderDirection:desc){ id hourStartUnix } }"}' \
  'https://api.studio.thegraph.com/query/1742316/llama/v0.2.9'
```

## Quality Diagnostics Added
`data_quality_audit.csv` now includes:
- `invalid_fee_tier` rejection reason
  - non-v2-spike rows where implied fee rate > 10% and fee tier is suspiciously high
- `source_tvl_alias_sanity` rows
  - per-source counts for `fees_with_nonpositive_tvl_input`
  - use this to diagnose missing/incorrect TVL aliases in source GraphQL queries

## Subgraph Sync Command
Refresh local subgraph source from droplet:
```bash
rsync -az --delete \
  --exclude '.git/' --exclude 'node_modules/' --exclude 'build/' --exclude 'generated/' --exclude '.yarn/cache/' \
  root@134.199.135.19:/root/llama/ ./subgraphs/llama/
```

