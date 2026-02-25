#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${1:-output/multichain_3w}"
OUT_ZIP="${2:-artifacts.zip}"

if [[ ! -d "$ARTIFACT_DIR" ]]; then
  echo "Artifact directory not found: $ARTIFACT_DIR" >&2
  exit 2
fi

tmp_list="$(mktemp)"
trap 'rm -f "$tmp_list"' EXIT

(
  cd "$ARTIFACT_DIR"
  for f in \
    report.html \
    summary.md \
    hourly_observations.csv \
    pool_rankings.csv \
    pool_rankings_diagnostics.csv \
    liquidity_schedule.csv \
    liquidity_schedule.md \
    schedule_enhanced.csv \
    schedule_summary_stats.csv \
    schedule_run_diagnostics.csv \
    moves_day_curve.csv \
    selected_plan_default.csv \
    selected_plan_active.csv \
    source_health.csv \
    data_quality_audit.csv \
    run_manifest.json \
    dashboard.html \
    llama_pair_hour_data.csv \
    llama_weth_spike_rankings.csv \
    llama_run_diagnostics.csv \
    sushi_v2_yield_spikes.csv \
    dashboard_state.json; do
    [[ -f "$f" ]] && echo "$f"
  done
  find . -maxdepth 1 -type f -name 'selected_plan_*.csv' -printf '%f\n' | sort
  if [[ -d charts ]]; then
    find charts -type f | sort
  fi
) | sort -u > "$tmp_list"

if command -v zip >/dev/null 2>&1; then
  rm -f "$OUT_ZIP"
  (
    cd "$ARTIFACT_DIR"
    zip -q -@ "$OLDPWD/$OUT_ZIP" < "$tmp_list"
  )
else
  python3 - "$ARTIFACT_DIR" "$OUT_ZIP" "$tmp_list" <<'PY'
import sys
import zipfile
from pathlib import Path
art = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2]).resolve()
lst = Path(sys.argv[3]).read_text(encoding="utf-8").splitlines()
if out.exists():
    out.unlink()
with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for rel in lst:
        p = (art / rel).resolve()
        if p.exists() and p.is_file():
            zf.write(p, rel)
PY
fi

echo "Wrote $OUT_ZIP"
