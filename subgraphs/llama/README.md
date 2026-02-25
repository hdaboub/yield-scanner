# Llama Subgraph (The Graph Studio)

This directory tracks the deployed `llama` subgraph source pulled from the droplet (`/root/llama`).

## Purpose

Indexes Ethereum Sushi/Uniswap-style activity for scanner/reporting:

- V2 pair-hour aggregates (`pairHourDatas`) for fee-yield spike detection
- Pair and token metadata (`pairs`, `tokens`)
- Runtime seeding state (`v2SeedStates`)

Current Studio endpoint:

- `https://api.studio.thegraph.com/query/1742316/llama/v0.2.9`

## Build

```bash
yarn
yarn codegen
yarn build
```

## Deploy (Studio)

```bash
graph deploy llama -l v0.2.9
```

Use a new label (`v0.2.10`, etc.) when promoting changes.

## Runtime checks

Query indexing tip + seed progress:

```graphql
{
  _meta { block { number } }
  v2SeedStates(where: { id: "v2" }) {
    id
    nextIndex
    total
    lastBlock
  }
}
```

Query pair-hour rows (scanner expects alias `hourly`):

```graphql
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
    pair { id }
  }
}
```

## Sync helper

To refresh this directory from droplet:

```bash
./scripts/pull_llama_subgraph.sh 134.199.135.19
```
