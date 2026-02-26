# Yield Scanner Dashboard (Streamlit)

This is a lightweight interactive dashboard for local exploration of scanner artifacts.

## Run

```bash
pip install streamlit pandas plotly
streamlit run dashboard/app.py
```

Optional artifact path override:

```bash
YIELD_SCANNER_ARTIFACTS=output_from_droplet/multichain_3w streamlit run dashboard/app.py
```

You can also point to a packaged zip bundle; it will auto-extract to a temp directory:

```bash
YIELD_SCANNER_ARTIFACTS=artifacts.zip streamlit run dashboard/app.py
```

Default artifacts path:

- `output_from_droplet/multichain_3w`

Optional remote-control environment variables (for one-click reruns + live service logs):

- `YIELD_SCANNER_REMOTE_HOST` (example: `root@134.199.135.19`)
- `YIELD_SCANNER_REMOTE_APP_DIR` (default: `/opt/uniswap-yield-scanner`)
- `YIELD_SCANNER_REMOTE_SERVICE` (default: `uniswap-jobs.service`)
- `YIELD_SCANNER_REMOTE_CONFIG` (default: `config/sources.uniswap-official.multichain.json`)
- `YIELD_SCANNER_REMOTE_OUTPUT_DIR` (default: `output/multichain_3w`)

## What it shows

- Llama spike leaderboard (sortable/filterable)
- Compact excluded-source panel with quarantine reasons
- Source quarantine override controls (force-include / force-exclude by source key)
- Schedule blocks and active selected plan
- Moves/day frontier table
- Operator decision charts per top pool:
  - line chart (USD/$1k/hr + TVL with schedule-window overlay)
  - heatmap (hour-of-week median USD/$1k/hr)
- Schedule impact preview (before rerun) under selected objective/deploy/move-cost
- One-click service restart and ad-hoc rerun controls (hours, move cost, deploy, workers, strict/relaxed)
- Live service status + log tails
- Pool-level drilldown with yield and swapCount time series
