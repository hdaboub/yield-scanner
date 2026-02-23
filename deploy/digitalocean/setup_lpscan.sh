#!/usr/bin/env bash
set -euo pipefail

HOST="lpscan"
APP_DIR="/opt/uniswap-yield-scanner"
REMOTE_USER="root"
GRAPH_KEY=""
SOURCE_DIR=""

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --host <hostname>         SSH host alias or IP (default: lpscan)
  --user <ssh_user>         SSH user (default: root)
  --source-dir <path>       Local repo path (default: auto-detect from script location)
  --graph-key <key>         The Graph Gateway Query API key
  --prompt-key              Prompt securely for Graph API key
  -h, --help                Show this help

Examples:
  $0 --prompt-key
  $0 --host 165.245.143.206 --graph-key 'YOUR_KEY'
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
    --graph-key)
      GRAPH_KEY="$2"
      shift 2
      ;;
    --prompt-key)
      read -r -s -p "Graph Gateway Query API key: " GRAPH_KEY
      echo
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

if [[ -z "$GRAPH_KEY" ]]; then
  echo "Missing API key. Use --graph-key or --prompt-key." >&2
  exit 2
fi

if [[ "$GRAPH_KEY" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
  echo "The provided key looks like an Ethereum address, not a Graph Gateway API key." >&2
  exit 2
fi

echo "[1/5] Checking SSH connectivity to ${REMOTE_USER}@${HOST}"
ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE_USER}@${HOST}" 'echo connected >/dev/null'

echo "[2/5] Installing base packages and hardening (ufw/fail2ban)"
ssh "${REMOTE_USER}@${HOST}" 'bash -s' <<'REMOTE'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y git python3 python3-venv python3-pip ufw fail2ban
ufw default deny incoming || true
ufw default allow outgoing || true
ufw allow OpenSSH || true
ufw --force enable || true
mkdir -p /etc/fail2ban/jail.d
cat >/etc/fail2ban/jail.d/sshd.local <<EOF
[sshd]
enabled = true
maxretry = 5
bantime = 1h
findtime = 10m
EOF
systemctl enable fail2ban
systemctl restart fail2ban
timedatectl set-timezone UTC
mkdir -p /opt/uniswap-yield-scanner
REMOTE

echo "[3/5] Uploading project to ${HOST}:${APP_DIR}"
tar \
  --exclude='.git' \
  --exclude='output' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  -C "$SOURCE_DIR" -czf - . \
  | ssh "${REMOTE_USER}@${HOST}" "APP_DIR='${APP_DIR}' bash -lc 'set -euo pipefail; TMP_DIR=\$(mktemp -d /tmp/uniswap-yield-scanner.deploy.XXXXXX); mkdir -p \"\$TMP_DIR\"; tar -xzf - -C \"\$TMP_DIR\"; if [[ -d \"\$APP_DIR/output/cache\" ]]; then mkdir -p \"\$TMP_DIR/output\"; cp -a \"\$APP_DIR/output/cache\" \"\$TMP_DIR/output/\"; fi; rm -rf \"\$APP_DIR\"; mv \"\$TMP_DIR\" \"\$APP_DIR\"'"

echo "[4/5] Configuring runtime and systemd jobs"
GRAPH_KEY_B64="$(printf '%s' "$GRAPH_KEY" | base64 -w0)"
ssh "${REMOTE_USER}@${HOST}" "GRAPH_KEY_B64='${GRAPH_KEY_B64}' APP_DIR='${APP_DIR}' bash -s" <<'REMOTE'
set -euo pipefail
GRAPH_KEY="$(printf '%s' "$GRAPH_KEY_B64" | base64 -d)"

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/python" -m pip install --upgrade pip

cat >/etc/uniswap-yield-scanner.env <<EOF
THE_GRAPH_QUERY_API_KEY=${GRAPH_KEY}
PYTHONUNBUFFERED=1
EOF
chmod 600 /etc/uniswap-yield-scanner.env

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
systemctl enable uniswap-jobs.timer
systemctl enable uniswap-backfill.timer
systemctl restart uniswap-jobs.timer
systemctl restart uniswap-backfill.timer
REMOTE

echo "[5/5] Triggering first scan/backtest now (non-blocking)"
ssh "${REMOTE_USER}@${HOST}" 'systemctl start --no-block uniswap-jobs.service'

echo

echo "Setup complete. Check status with:"
echo "  ssh ${REMOTE_USER}@${HOST} 'systemctl status uniswap-jobs.service --no-pager'"
echo "  ssh ${REMOTE_USER}@${HOST} 'systemctl list-timers | grep uniswap'"
echo "  ssh ${REMOTE_USER}@${HOST} 'journalctl -u uniswap-jobs.service -n 120 --no-pager'"
echo "  ssh ${REMOTE_USER}@${HOST} 'journalctl -u uniswap-backfill.service -n 120 --no-pager'"
echo "Output directories:"
echo "  ${APP_DIR}/output/multichain_3w"
echo "  ${APP_DIR}/output/backtest_multichain_3w"
