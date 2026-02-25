#!/usr/bin/env bash
set -euo pipefail

HOST="lpscan"
REMOTE_USER="root"
APP_DIR="/opt/uniswap-yield-scanner"
SOURCE_DIR=""
RUN_NOW="yes"

usage() {
  cat <<EOF
Usage: $0 [options]

Deploy latest local repo to droplet and run uniswap jobs.

Options:
  --host <hostname>       SSH host alias or IP (default: lpscan)
  --user <ssh_user>       SSH user (default: root)
  --source-dir <path>     Local repo path (default: auto-detect)
  --app-dir <path>        Remote app dir (default: /opt/uniswap-yield-scanner)
  --no-run-now            Deploy only; do not trigger immediate run
  -h, --help              Show help

Examples:
  $0
  $0 --host 165.245.143.206
  $0 --host lpscan --no-run-now
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --user)
      REMOTE_USER="$2"
      shift 2
      ;;
    --source-dir)
      SOURCE_DIR="$2"
      shift 2
      ;;
    --app-dir)
      APP_DIR="$2"
      shift 2
      ;;
    --no-run-now)
      RUN_NOW="no"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$SOURCE_DIR" ]]; then
  SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "Source directory does not exist: $SOURCE_DIR" >&2
  exit 2
fi

echo "[1/4] Checking SSH connectivity to ${REMOTE_USER}@${HOST}"
ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE_USER}@${HOST}" 'echo connected >/dev/null'

echo "[2/4] Uploading latest project to ${HOST}:${APP_DIR}"
tar \
  --exclude='.git' \
  --exclude='output' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  -C "$SOURCE_DIR" -czf - . \
  | ssh "${REMOTE_USER}@${HOST}" "APP_DIR='${APP_DIR}' bash -lc 'set -euo pipefail; TMP_DIR=\$(mktemp -d /tmp/uniswap-yield-scanner.deploy.XXXXXX); mkdir -p \"\$TMP_DIR\"; tar -xzf - -C \"\$TMP_DIR\"; if [[ -d \"\$APP_DIR/output/cache\" ]]; then mkdir -p \"\$TMP_DIR/output\"; cp -a \"\$APP_DIR/output/cache\" \"\$TMP_DIR/output/\"; fi; rm -rf \"\$APP_DIR\"; mv \"\$TMP_DIR\" \"\$APP_DIR\"'"

echo "[3/4] Refreshing runtime units on remote"
ssh "${REMOTE_USER}@${HOST}" "APP_DIR='${APP_DIR}' bash -s" <<'REMOTE'
set -euo pipefail

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/python" -m pip install --upgrade pip >/dev/null

# Ensure jobs unit exists (API key remains in /etc/uniswap-yield-scanner.env)
cat >/etc/systemd/system/uniswap-jobs.service <<'EOF'
[Unit]
Description=Uniswap scanner + backtest daily run
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
TimeoutStartSec=infinity
EnvironmentFile=/etc/uniswap-yield-scanner.env
WorkingDirectory=/opt/uniswap-yield-scanner
ExecStart=/bin/bash -lc '/opt/uniswap-yield-scanner/.venv/bin/python scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --hours 504 \
  --page-size 500 \
  --workers 12 \
  --parallel-window-hours 6 \
  --timeout 60 \
  --retries 6 \
  --cache-db output/cache/observations.sqlite \
  --cache-overlap-hours 24 \
  --source-checkpoint-pages 20 \
  --top 25 \
  --schedule-top-pools 25 \
  --schedule-min-occurrences 2 \
  --require-run-history-quality-ok \
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

cat >/etc/systemd/system/uniswap-backfill.service <<'EOF'
[Unit]
Description=Uniswap heavy-source backfill (checkpointed)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
TimeoutStartSec=infinity
ExecCondition=/bin/bash -lc '! systemctl is-active --quiet uniswap-jobs.service'
EnvironmentFile=/etc/uniswap-yield-scanner.env
WorkingDirectory=/opt/uniswap-yield-scanner
ExecStart=/bin/bash -lc '/opt/uniswap-yield-scanner/.venv/bin/python scanner.py \
  --config config/sources.uniswap-official.multichain.json \
  --only-backfill-sources \
  --hours 504 \
  --page-size 1000 \
  --workers 1 \
  --timeout 60 \
  --retries 6 \
  --cache-db output/cache/observations.sqlite \
  --cache-overlap-hours 24 \
  --source-checkpoint-pages 10 \
  --top 5 \
  --schedule-top-pools 5 \
  --schedule-min-occurrences 2 \
  --require-run-history-quality-ok \
  --output-dir output/backfill_multichain \
  --no-open-report'
EOF

cat >/etc/systemd/system/uniswap-jobs.timer <<'EOF'
[Unit]
Description=Run scanner and backtest every day at 01:30 UTC

[Timer]
OnCalendar=*-*-* 01:30:00 UTC
Persistent=true
Unit=uniswap-jobs.service

[Install]
WantedBy=timers.target
EOF

cat >/etc/systemd/system/uniswap-backfill.timer <<'EOF'
[Unit]
Description=Run heavy-source backfill every 2 hours

[Timer]
OnCalendar=*-*-* 0/2:05:00 UTC
Persistent=true
Unit=uniswap-backfill.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable uniswap-jobs.timer >/dev/null
systemctl enable uniswap-backfill.timer >/dev/null
systemctl restart uniswap-jobs.timer
systemctl restart uniswap-backfill.timer
REMOTE

if [[ "$RUN_NOW" == "yes" ]]; then
  echo "[4/4] Starting run now (non-blocking)"
  ssh "${REMOTE_USER}@${HOST}" 'systemctl start --no-block uniswap-jobs.service'
else
  echo "[4/4] Skipped immediate run (--no-run-now)"
fi

echo
echo "Deploy complete."
echo "Check status:"
echo "  ssh ${REMOTE_USER}@${HOST} 'systemctl status uniswap-jobs.service --no-pager'"
echo "  ssh ${REMOTE_USER}@${HOST} 'systemctl status uniswap-backfill.service --no-pager'"
echo "  ssh ${REMOTE_USER}@${HOST} 'journalctl -u uniswap-jobs.service -f'"
echo "  ssh ${REMOTE_USER}@${HOST} 'journalctl -u uniswap-backfill.service -f'"
echo "Output files:"
echo "  ${APP_DIR}/output/multichain_3w/report.html"
echo "  ${APP_DIR}/output/backtest_multichain_3w/backtest_summary.md"
