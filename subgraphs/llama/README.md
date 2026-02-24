# Llama Subgraph (The Graph Studio)

## Purpose
This subgraph indexes Sushi/Uniswap-style DEX activity on Ethereum mainnet and produces hourly V2 fee-yield primitives used by the scanner/report app for spike detection.

## Source Of Truth
- Droplet project path: `/root/llama`
- Repo mirror path: `subgraphs/llama/`

Sync command used:
```bash
rsync -az --delete \
  --exclude '.git/' --exclude 'node_modules/' --exclude 'build/' --exclude 'generated/' --exclude '.yarn/cache/' \
  root@134.199.135.19:/root/llama/ ./subgraphs/llama/
```

## Network + Endpoint
- Network: Ethereum mainnet
- Studio endpoint (current):
  - `https://api.studio.thegraph.com/query/1742316/llama/v0.2.9`

## Key Entities
- `PairHourData`: `pair`, `hourStartUnix`, `swapCount`, `fee0`, `fee1`, `reserve0`, `reserve1`
- `Pair`: `id`, `token0`, `token1`, `reserve0`, `reserve1`, sync metadata
- `Token`: `id`, `symbol`, `name`, `decimals`
- `V2Swap`, `V3Swap`
- `v2SeedStates`: `nextIndex`, `total`, `lastBlock`

## Deploy / Build (Subgraph Project)
From `subgraphs/llama`:
```bash
yarn install
yarn codegen
yarn build
# deploy command depends on your graph-cli auth/setup
```

## Runtime Queries Used By Scanner
Defined in `scanner.py`:
- `LLAMA_RUNTIME_QUERY`: `_meta.block.number` + `v2SeedStates`
- `V2_PAIR_HOUR_QUERY`: paged PairHourData query
- `V2_PAIR_META_QUERY`, `V2_TOKEN_META_QUERY`: metadata enrichment

Important: scanner now queries PairHourData with alias:
```graphql
hourly: pairHourDatas(...)
```
and accepts fallback to `pairHourDatas` for compatibility.

## Empty-Result Troubleshooting
If report diagnostics show `fetched_raw_rows=0`:
1. Verify query alias and payload shape (`hourly` list for scanner expectations where relevant)
2. Verify requested time window overlaps indexed data/startBlock
3. Verify sync status via `_meta.block.number` and `v2SeedStates`

Quick check:
```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _meta { block { number } } v2SeedStates { id nextIndex total lastBlock } pairHourDatas(first:1, orderBy:hourStartUnix, orderDirection:desc){ id hourStartUnix } }"}' \
  'https://api.studio.thegraph.com/query/1742316/llama/v0.2.9'
```

## Yield Spike Scoring Used In Reports
For WETH pairs:
- `score = feeWETH / reserveWETH`
- `hourly_yield_pct = score * 100`
- `usd_per_1000_liquidity_hourly = score * 1000`
- `spike_multiplier = score / baseline_median_score`
- `persistence_hits` over trailing persistence window
