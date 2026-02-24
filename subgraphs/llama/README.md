# Llama Subgraph (The Graph Studio)

## Purpose
This subgraph indexes Sushi/Uniswap-style DEX activity on Ethereum mainnet and produces hourly fee-yield primitives for yield-spike detection.

## Network
- Ethereum mainnet

## Studio Endpoint (GraphQL)
- `https://api.studio.thegraph.com/query/1742316/llama/v0.2.9`

Use the latest deployed version label when querying.

## Key Entities
- `PairHourData`:
  - `pair` (address)
  - `hourStartUnix` (hour bucket start)
  - `swapCount`
  - `fee0`, `fee1` (estimated LP fees in token units; `3/1000` of input-side swap amount)
  - `reserve0`, `reserve1` (hourly reserve snapshot; v0.2.9 writes reserves during swaps from latest `Pair` reserves so hour buckets do not strictly depend on `Sync` in that hour)
- `Pair`:
  - `token0`, `token1`
  - `reserve0`, `reserve1`
  - `lastSyncTimestamp`, `lastSyncBlockNumber`
- `Token`:
  - `symbol`, `name`, `decimals` (best-effort via ERC20 `try_` calls)
- `V2Swap` (raw event rows)
- `V3Swap` (raw Sushi V3 pool swaps; no hourly rollup entity yet)
- `V2SeedState`:
  - progress state for pair seeding (`nextIndex`, `total`, `lastBlock`)

## Indexing Strategy
- Current `startBlock` in this checked-in manifest: `24136052` (late-2025 / early-2026 range).
- V2 factory uses a block handler `seedV2Pairs` to enumerate `allPairs(i)` from the factory and create templates for pairs that existed before `startBlock`.
- Pair seeding progress is persisted in `V2SeedState`.
- Swap/Sync indexing still starts from `startBlock` forward.

## Yield Spike Scoring
For pairs containing WETH (`0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2`), define per-hour score:

- `score = feeWETH / reserveWETH`

Derived metrics:
- `hourly_yield_pct = score * 100`
- `usd_per_1000_liquidity_hourly ~= score * 1000`
- `rough_apr_pct = score * 24 * 365 * 100`

Recommended filters:
- Start with `min reserveWETH >= 10`, then raise toward `50+`
- Start with `min swapCount >= 3`, then raise toward `10–30`
- Optional persistence rule: top-K appears in at least `2` of last `6` hours

## App Integration
The reporting app includes a dedicated client at:
- `src/thegraph/llama_client.py`

Functions:
- `fetchRecentPairHourData(...)`
- `fetchPairsMeta(...)`
- `computeWethScore(...)`

Generated outputs in report runs:
- `llama_pair_hour_data.csv`
- `llama_weth_spike_rankings.csv`
