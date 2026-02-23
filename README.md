# Uniswap Hourly Yield Scanner

CLI application to scan Uniswap v3 and v4 pool hourly data from GraphQL sources, estimate fee yield per hour, and rank pools by earning potential with best hour/day windows.

## What It Produces

- `hourly_observations.csv`: raw hourly observations per pool
- `pool_rankings.csv`: per-pool ranking metrics
- `summary.md`: top pools with best hour/day windows
- `liquidity_schedule.csv`: recommended add/remove schedule blocks from reliable recurring high-yield windows
- `liquidity_schedule.md`: human-readable schedule summary with next UTC add/remove times
- `sushi_v2_yield_spikes.csv`: ranked V2 hourly fee-yield spikes using `feeWETH / reserveWETH`
- `report.html`: consolidated interactive report (rankings + schedule)
- Fee-time columns in rankings:
  - `observed_hours`, `observed_days`
  - `fee_period_start_utc`, `fee_period_end_utc`
  - `total_fees_usd`, `avg_hourly_fees_usd`

## Yield Model

Hourly yield is computed as:

- `hourly_yield = feesUSD / tvlUSD`
- If `feesUSD` is absent, the scanner derives fees as `volumeUSD * feeTier / 1_000_000` only when `feeTier` is within `0..1_000_000`.
- For non-standard fee-tier encodings (for example dynamic-fee flags above `1_000_000`), fees are not derived.

Ranking score favors stable upside over one-off spikes:

- `score = (p90_yield * 0.45) + (avg_yield * 0.40) + (max_yield * 0.15)`

## Quick Start

1. Edit `config/sources.example.json` with valid v3/v4 endpoints.
2. Set `enabled: true` for every source you want scanned.
3. Ensure each query file matches your subgraph schema.
4. Run:

```bash
cd /root/uniswap-yield-scanner
python3 scanner.py \
  --config config/sources.example.json \
  --hours 336 \
  --top 25 \
  --schedule-top-pools 25 \
  --schedule-quantile 0.75 \
  --schedule-min-hit-rate 0.60 \
  --schedule-min-occurrences 2 \
  --output-dir output
```

By default, the scanner attempts to open `report.html` in your default browser at the end.
Use `--no-open-report` to disable that behavior.
In WSL, if `wslview` is installed, the scanner uses it first to open the report in your Windows default browser.

To avoid re-querying the same historical hours on every run, enable local cache mode:

```bash
python3 scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --hours 504 \
  --cache-db output/cache/observations.sqlite \
  --cache-overlap-hours 24 \
  --output-dir output/multichain_3w
```

In cache mode, fetched rows are upserted into the SQLite DB and reports are generated from cached rows in the requested window.

Optional V2 spike filters:

- `--v2-spike-min-swap-count` (default `10`)
- `--v2-spike-min-reserve-weth` (default `10`)
- `--v2-spike-top` (default `100`)

## Official Uniswap Mainnet Sources (Gateway)

Use `config/sources.uniswap-official.mainnet.json` for official mainnet v3/v4 subgraphs by ID.

Important:
- `THE_GRAPH_QUERY_API_KEY` must be a Graph Gateway Query API key.
- A Studio deploy key from `graph auth` will not work for gateway queries.

Example:

```bash
export THE_GRAPH_QUERY_API_KEY='<gateway-query-key>'
python3 scanner.py \
  --config config/sources.uniswap-official.mainnet.json \
  --hours 336 \
  --top 25 \
  --output-dir output
```

## Official Multichain Sources (Gateway)

Use `config/sources.uniswap-official.multichain.json` to scan:

- Ethereum
- Arbitrum
- Base
- Polygon
- Unichain
- BNB Chain

This config includes v3 and v4 sources where available.
`solana` is included as a disabled placeholder because this scanner's current source model expects Uniswap v3/v4-style pool subgraphs.

```bash
export THE_GRAPH_QUERY_API_KEY='<gateway-query-key>'
python3 scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --hours 336 \
  --top 25 \
  --output-dir output
```

## Query Contract

Each source query must alias pool-hour rows as `hourly` and return these fields (or aliases):

- `ts` (hour timestamp, unix)
- `volumeUSD`
- `tvlUSD` (or compatible TVL field aliased to `tvlUSD`)
- `feesUSD` (optional)
- `pool.id`
- `pool.feeTier` (optional but needed for fee derivation fallback)
- pool token symbols under `token0/token1` or `currency0/currency1`

The scanner paginates using variables: `first`, `start`, `end`, `afterId` (cursor mode).
Legacy `skip` pagination is still supported for custom queries, but may fail on large windows due to indexer limits.

For Sushi/Uniswap-style V2 fee-yield spikes, add a source with:

- `"source_type": "v2_spike"`
- mainnet Studio endpoint
- optional `"weth_address"` override

Example source:

```json
{
  "name": "sushi-v2-fee-spikes-mainnet",
  "enabled": true,
  "source_type": "v2_spike",
  "version": "v2",
  "chain": "ethereum",
  "endpoint": "https://api.studio.thegraph.com/query/1742316/llama/v0.2.1",
  "weth_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
}
```

## Schedule Logic

The recommended schedule is built per pool from recurring weekly hour buckets:

- A bucket is considered high-yield when its hourly yield is at/above the pool's `--schedule-quantile` threshold.
  - Alternatively, set `--schedule-min-usd-per-1000-hour` to use an absolute threshold in USD/hour per $1,000 liquidity (this overrides quantile mode).
- A recurring bucket is considered reliable when:
  - it appears at least `--schedule-min-occurrences` times
  - its high-yield hit rate is at least `--schedule-min-hit-rate`
- Adjacent reliable hours on the same day are merged into one add/remove block.
- Each block includes:
  - recurring patterns (`add_pattern_utc`, `remove_pattern_utc`)
  - concrete upcoming actions (`next_add_utc`, `next_remove_utc`)

## Walk-Forward LP Backtest

Use `backtest_lp.py` to train schedule recommendations on an older window and evaluate them on the following holdout week.

Example (offline mode from an existing scan output):

```bash
python3 backtest_lp.py \
  --hourly-csv output/multichain_run/hourly_observations.csv \
  --train-hours 336 \
  --test-hours 168 \
  --schedule-top-pools 25 \
  --schedule-quantile 0.75 \
  --schedule-min-hit-rate 0.60 \
  --schedule-min-occurrences 2 \
  --output-dir output/backtest_multichain
```

Example (fresh fetch mode):

```bash
export THE_GRAPH_QUERY_API_KEY='<gateway-query-key>'
python3 backtest_lp.py \
  --config config/sources.uniswap-official.multichain.json \
  --train-hours 336 \
  --test-hours 168 \
  --schedule-top-pools 25 \
  --output-dir output/backtest_multichain
```

Backtest artifacts:

- `train_pool_rankings.csv`: rankings learned on the train window
- `train_liquidity_schedule.csv`: schedule blocks learned on the train window
- `backtest_results.csv`: per-pool holdout metrics (scheduled vs baseline)
- `backtest_summary.md`: holdout summary and top backtested pools

Important: if `--schedule-min-occurrences` is `2`, you generally need at least 3 weeks of total data (`2 weeks train + 1 week test`) to produce recurring weekly schedule blocks.

## Notes

- Use multiple sources to cover all chains and both protocol versions.
- Set `UNISWAP_V4_ENDPOINT` (or your own env var names) if your endpoint is env-based.
- Source endpoints can use shell-style env references such as `${UNISWAP_V4_ENDPOINT}`.
- Source headers can also use env references (for example `Authorization: Bearer ${TOKEN}`).
- By default, a failed source is skipped and the scan continues. Add `--strict-sources` to fail the full run on any source error.
- For large historical windows, reduce API throttling risk with smaller `--page-size` and fewer `--workers`.
- Timestamps are reported in UTC for hour/day seasonality.

## Cloud Deployment

- DigitalOcean deploy artifacts are in `deploy/digitalocean/`:
  - `deploy/digitalocean/cloud-init.yaml`
  - `deploy/digitalocean/install_runtime.sh`
  - `deploy/digitalocean/setup_lpscan.sh`
  - `deploy/digitalocean/deploy_update_and_run.sh`
  - `deploy/digitalocean/enable_ops_hardening.sh`
  - `deploy/digitalocean/README.md`
