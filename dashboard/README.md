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

## What it shows

- Llama spike leaderboard (sortable/filterable)
- Schedule blocks and active selected plan
- Moves/day frontier table
- Source health exclusions
- Pool-level drilldown with yield and swapCount time series
