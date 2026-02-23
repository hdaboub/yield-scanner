#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <repo_url> <graph_gateway_query_api_key> [branch]" >&2
  exit 1
fi

REPO_URL="$1"
GRAPH_KEY="$2"
BRANCH="${3:-main}"
APP_DIR="/opt/uniswap-yield-scanner"
RUNNER_USER="${SUDO_USER:-$USER}"

sudo mkdir -p "$APP_DIR"
sudo chown -R "$RUNNER_USER":"$RUNNER_USER" "$APP_DIR"

if [[ ! -d "$APP_DIR/.git" ]]; then
  git clone --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" fetch origin
  git -C "$APP_DIR" checkout "$BRANCH"
  git -C "$APP_DIR" pull --ff-only origin "$BRANCH"
fi

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/python" -m pip install --upgrade pip

sudo tee /etc/uniswap-yield-scanner.env >/dev/null <<EOF
THE_GRAPH_QUERY_API_KEY=${GRAPH_KEY}
PYTHONUNBUFFERED=1
EOF
sudo chmod 600 /etc/uniswap-yield-scanner.env

sudo tee /etc/systemd/system/uniswap-jobs.service >/dev/null <<'EOF'
[Unit]
Description=Uniswap scanner + backtest daily run
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/uniswap-yield-scanner.env
WorkingDirectory=/opt/uniswap-yield-scanner
ExecStart=/bin/bash -lc '/opt/uniswap-yield-scanner/.venv/bin/python scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --hours 504 \
  --page-size 500 \
  --workers 8 \
  --timeout 60 \
  --retries 6 \
  --cache-db output/cache/observations.sqlite \
  --cache-overlap-hours 24 \
  --top 25 \
  --schedule-top-pools 25 \
  --schedule-min-occurrences 2 \
  --output-dir output/multichain_3w \
  --no-open-report && \
/opt/uniswap-yield-scanner/.venv/bin/python backtest_lp.py \
  --hourly-csv output/multichain_3w/hourly_observations.csv \
  --train-hours 336 \
  --test-hours 168 \
  --schedule-top-pools 25 \
  --schedule-min-occurrences 2 \
  --output-dir output/backtest_multichain_3w'
EOF

sudo tee /etc/systemd/system/uniswap-jobs.timer >/dev/null <<'EOF'
[Unit]
Description=Run scanner and backtest every day at 01:30 UTC

[Timer]
OnCalendar=*-*-* 01:30:00 UTC
Persistent=true
Unit=uniswap-jobs.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable uniswap-jobs.timer
sudo systemctl start uniswap-jobs.service
sudo systemctl restart uniswap-jobs.timer

echo "Install complete."
echo "Check status:"
echo "  systemctl status uniswap-jobs.service"
echo "  systemctl list-timers | grep uniswap"
echo "Outputs: $APP_DIR/output/multichain_3w and $APP_DIR/output/backtest_multichain_3w"
