#!/usr/bin/env bash
set -euo pipefail

# Configure all three operational hardening items:
# 1) Persistent journald logs
# 2) Daily backup sync to S3/Spaces
# 3) Failure webhook alerts for uniswap jobs

APP_DIR="/opt/uniswap-yield-scanner"
BACKUP_PREFIX_DEFAULT="uniswap-yield-scanner"
BACKUP_TIME_UTC="02:30:00"

WEBHOOK_URL=""
BACKUP_BUCKET=""
BACKUP_PREFIX="$BACKUP_PREFIX_DEFAULT"
AWS_ACCESS_KEY_ID_VAL=""
AWS_SECRET_ACCESS_KEY_VAL=""
AWS_REGION_VAL="us-east-1"
AWS_ENDPOINT_URL_VAL=""

usage() {
  cat <<EOF
Usage: sudo $0 [options]

Required:
  --webhook-url <url>          Webhook URL for failure alerts (Slack/Discord/etc)
  --backup-bucket <name>       S3/Spaces bucket name
  --aws-access-key-id <id>     Object storage access key
  --aws-secret-access-key <k>  Object storage secret key

Optional:
  --aws-region <region>        Default: us-east-1
  --aws-endpoint-url <url>     For Spaces/custom S3 endpoint (example: https://nyc3.digitaloceanspaces.com)
  --backup-prefix <prefix>     Default: uniswap-yield-scanner
  --backup-time-utc <HH:MM:SS> Default: 02:30:00
  -h, --help                   Show this help

Example (DigitalOcean Spaces):
  sudo $0 \
    --webhook-url 'https://hooks.slack.com/services/...' \
    --backup-bucket 'my-space-name' \
    --aws-access-key-id 'DO00...' \
    --aws-secret-access-key '...' \
    --aws-region 'us-east-1' \
    --aws-endpoint-url 'https://nyc3.digitaloceanspaces.com'
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --webhook-url)
      WEBHOOK_URL="$2"
      shift 2
      ;;
    --backup-bucket)
      BACKUP_BUCKET="$2"
      shift 2
      ;;
    --backup-prefix)
      BACKUP_PREFIX="$2"
      shift 2
      ;;
    --aws-access-key-id)
      AWS_ACCESS_KEY_ID_VAL="$2"
      shift 2
      ;;
    --aws-secret-access-key)
      AWS_SECRET_ACCESS_KEY_VAL="$2"
      shift 2
      ;;
    --aws-region)
      AWS_REGION_VAL="$2"
      shift 2
      ;;
    --aws-endpoint-url)
      AWS_ENDPOINT_URL_VAL="$2"
      shift 2
      ;;
    --backup-time-utc)
      BACKUP_TIME_UTC="$2"
      shift 2
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

if [[ "$EUID" -ne 0 ]]; then
  echo "Run as root (use sudo)." >&2
  exit 2
fi

if [[ -z "$WEBHOOK_URL" || -z "$BACKUP_BUCKET" || -z "$AWS_ACCESS_KEY_ID_VAL" || -z "$AWS_SECRET_ACCESS_KEY_VAL" ]]; then
  echo "Missing required arguments." >&2
  usage
  exit 2
fi

echo "[1/6] Installing required packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y awscli curl

echo "[2/6] Enabling persistent journald logs"
mkdir -p /etc/systemd/journald.conf.d /var/log/journal
cat >/etc/systemd/journald.conf.d/persistent.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=500M
EOF
systemctl restart systemd-journald

echo "[3/6] Writing backup environment"
cat >/etc/uniswap-backup.env <<EOF
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID_VAL}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY_VAL}
AWS_DEFAULT_REGION=${AWS_REGION_VAL}
AWS_REGION=${AWS_REGION_VAL}
AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL_VAL}
BACKUP_BUCKET=${BACKUP_BUCKET}
BACKUP_PREFIX=${BACKUP_PREFIX}
APP_DIR=${APP_DIR}
EOF
chmod 600 /etc/uniswap-backup.env

echo "[4/6] Installing backup + alert scripts"
cat >/usr/local/bin/uniswap_backup.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /etc/uniswap-backup.env

DATE_UTC="$(date -u +%Y-%m-%d)"
DST="s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${DATE_UTC}"
SRC="${APP_DIR}/output"

if [[ ! -d "$SRC" ]]; then
  echo "Output directory does not exist: $SRC" >&2
  exit 1
fi

AWS_ARGS=()
if [[ -n "${AWS_ENDPOINT_URL:-}" ]]; then
  AWS_ARGS+=(--endpoint-url "$AWS_ENDPOINT_URL")
fi

aws "${AWS_ARGS[@]}" s3 sync "$SRC" "$DST" --delete
EOF
chmod +x /usr/local/bin/uniswap_backup.sh

cat >/etc/uniswap-alert.env <<EOF
WEBHOOK_URL=${WEBHOOK_URL}
EOF
chmod 600 /etc/uniswap-alert.env

cat >/usr/local/bin/uniswap_alert.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /etc/uniswap-alert.env
UNIT_NAME="${1:-unknown-unit}"
HOSTNAME="$(hostname -f 2>/dev/null || hostname)"
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -z "${WEBHOOK_URL:-}" ]]; then
  echo "WEBHOOK_URL not configured" >&2
  exit 1
fi

PAYLOAD=$(cat <<JSON
{"text":"[ALERT] ${UNIT_NAME} failed on ${HOSTNAME} at ${TS} UTC"}
JSON
)

curl -fsS -X POST -H 'Content-Type: application/json' -d "$PAYLOAD" "$WEBHOOK_URL" >/dev/null
EOF
chmod +x /usr/local/bin/uniswap_alert.sh

echo "[5/6] Configuring systemd units and hooks"
mkdir -p /etc/systemd/system/uniswap-jobs.service.d
cat >/etc/systemd/system/uniswap-jobs.service.d/override.conf <<'EOF'
[Unit]
OnFailure=uniswap-alert@%n.service
EOF

cat >/etc/systemd/system/uniswap-backup.service <<'EOF'
[Unit]
Description=Backup Uniswap output to object storage
After=uniswap-jobs.service network-online.target
Wants=network-online.target
OnFailure=uniswap-alert@%n.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/uniswap_backup.sh
EOF

cat >/etc/systemd/system/uniswap-backup.timer <<EOF
[Unit]
Description=Run backup daily at ${BACKUP_TIME_UTC} UTC

[Timer]
OnCalendar=*-*-* ${BACKUP_TIME_UTC} UTC
Persistent=true
Unit=uniswap-backup.service

[Install]
WantedBy=timers.target
EOF

cat >/etc/systemd/system/uniswap-alert@.service <<'EOF'
[Unit]
Description=Send alert for failed unit %i

[Service]
Type=oneshot
ExecStart=/usr/local/bin/uniswap_alert.sh %i
EOF

echo "[6/6] Reloading systemd and running verification"
systemctl daemon-reload
systemctl enable uniswap-backup.timer
systemctl restart uniswap-jobs.timer
systemctl restart uniswap-backup.timer

# quick backup credential check and dry run
systemctl start uniswap-backup.service

echo

echo "Done. Verify with:"
echo "  systemctl status uniswap-jobs.timer --no-pager"
echo "  systemctl status uniswap-backup.timer --no-pager"
echo "  systemctl status uniswap-backup.service --no-pager"
echo "  journalctl -u uniswap-jobs.service -n 80 --no-pager"
echo "  journalctl -u uniswap-backup.service -n 80 --no-pager"
