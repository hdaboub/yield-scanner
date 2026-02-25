# Llama Subgraph (Ethereum Mainnet)

This subgraph indexes Sushi/Uniswap-style activity on Ethereum mainnet and exposes V2 pair-hour fee/reserve primitives used by the yield scanner report.

## Endpoint

- Studio GraphQL endpoint (current): `https://api.studio.thegraph.com/query/1742316/llama/v0.2.9`

## What It Indexes

- V2 factory + pair templates
  - `PairHourData` (hourly aggregates)
  - `Pair` / `Token` metadata
  - raw `V2Swap` events
- V3 factory + pool templates
  - raw `V3Swap` events (no hourly rollup yet)

## Key Entities Used by Scanner

- `pairHourDatas`
  - `pair`, `hourStartUnix`, `swapCount`, `fee0`, `fee1`, `reserve0`, `reserve1`
- `pairs`
  - `id`, `token0`, `token1`
- `tokens` (best effort metadata)
  - `id`, `symbol`, `name`, `decimals`
- runtime health checks:
  - `_meta { block { number } }`
  - `v2SeedStates { nextIndex total lastBlock }`

## Build and Deploy

```bash
yarn
yarn codegen
yarn build
graph deploy llama -l v0.2.9
```

Use a newer label when releasing an updated deployment.

## Scanner Integration Notes

- The scanner treats this as a `source_type: v2_spike` source.
- Llama hourly query responses must expose rows under top-level alias `hourly` for scanner compatibility.
- Core ranking signal for WETH pairs:
  - `score = feeWETH / reserveWETH`
- Ranking uses:
  - baseline median score,
  - spike multiplier,
  - persistence hits.

## Example Queries

Runtime health:

```graphql
{
  _meta { block { number } }
  v2SeedStates(where: { id: "v2" }) { nextIndex total lastBlock }
}
```

Pair-hour window:

```graphql
query PairHours($start: Int!, $end: Int!, $first: Int!, $afterId: String) {
  hourly: pairHourDatas(
    first: $first
    orderBy: hourStartUnix
    orderDirection: asc
    where: { hourStartUnix_gte: $start, hourStartUnix_lte: $end, id_gt: $afterId }
  ) {
    id
    pair { id }
    hourStartUnix
    fee0
    fee1
    reserve0
    reserve1
    swapCount
  }
}
```
