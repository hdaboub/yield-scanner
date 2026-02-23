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
