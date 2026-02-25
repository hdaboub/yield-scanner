# DigitalOcean Deployment

This deploys the scanner and LP backtest on a single Ubuntu Droplet and schedules a daily run.

## 1. Create Droplet

Recommended baseline:

- Region: close to you/data consumers
- Image: Ubuntu LTS
- Size: Basic, at least 2 vCPU / 4 GB RAM (3-week multichain scans are heavy)
- Auth: SSH keys only (disable password login)

If using `doctl`:

```bash
doctl compute droplet create uniswap-yield-scanner \
  --region nyc3 \
  --image ubuntu-24-04-x64 \
  --size s-2vcpu-4gb \
  --ssh-keys <SSH_KEY_FINGERPRINT> \
  --user-data-file deploy/digitalocean/cloud-init.yaml \
  --wait
```

Get IP:

```bash
doctl compute droplet list --format Name,PublicIPv4,Status
```

## 2. Connect

```bash
ssh root@<DROPLET_IP>
```

## 3. Install Runtime + Jobs

On the server:

```bash
cd /tmp
curl -fsSL -o install_runtime.sh \
  https://raw.githubusercontent.com/<YOUR_GITHUB_USER>/<YOUR_REPO>/main/deploy/digitalocean/install_runtime.sh
chmod +x install_runtime.sh
./install_runtime.sh \
  https://github.com/<YOUR_GITHUB_USER>/<YOUR_REPO>.git \
  <THE_GRAPH_GATEWAY_QUERY_API_KEY> \
  main
```

## 4. Validate

```bash
systemctl status uniswap-jobs.service --no-pager
systemctl list-timers | grep uniswap
journalctl -u uniswap-jobs.service -n 120 --no-pager
```

Outputs:

- `/opt/uniswap-yield-scanner/output/multichain_3w`
- `/opt/uniswap-yield-scanner/output/backtest_multichain_3w`

## 5. Rotate API Key

Edit env file then rerun the service:

```bash
sudo nano /etc/uniswap-yield-scanner.env
sudo systemctl restart uniswap-jobs.service
```

## Notes

- If your repo is private, install with a deploy key or use a GitHub token URL.
- The Graph key must be a **Gateway Query API key**.
- To run immediately anytime:

```bash
sudo systemctl start uniswap-jobs.service
```

## Operator Dashboard (Local)

Run the local dashboard against pulled artifacts and your droplet:

```bash
python3 deploy/digitalocean/ops_dashboard.py \
  --bind 127.0.0.1 \
  --port 8787 \
  --host <DROPLET_IP> \
  --artifacts-dir output_from_droplet/multichain_3w
```

Key APIs:

- `GET /api/state` (reads `dashboard_state.json`)
- `GET /api/spikes`
- `GET /api/schedule`
- `GET /api/pool/<pool_id>`
- `GET /api/files/<relative_path>` (charts/csv/html from artifacts dir)

Dashboard pages:

- `http://127.0.0.1:8787/` (Ops controls + logs)
- `http://127.0.0.1:8787/pools` (Pool/strategy visual dashboard: top pools, live spikes, executable plan, frontier, source health)

Manual report service controls from dashboard:

- Start/stop an on-demand scanner run as a transient systemd service (`uniswap-manual-report.service`)
- Control run parameters from UI:
  - hours, workers, top, page-size
  - optimizer objective
  - deploy USD, move cost, max moves/day
  - output subdirectory
- APIs:
  - `POST /api/report/start`
  - `POST /api/report/stop`
  - `GET /api/report/status`
  - `GET /api/report/logs`
