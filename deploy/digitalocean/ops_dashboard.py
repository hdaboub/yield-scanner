#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import mimetypes
import re
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


HOST_RE = re.compile(r"^[A-Za-z0-9.-]+$")
DEFAULT_LPSCAN_HOST = "134.199.135.19"
MANUAL_REPORT_UNIT = "uniswap-manual-report.service"
DEPLOY_SCRIPT = Path(__file__).resolve().parent / "deploy_update_and_run.sh"
REPO_ROOT = DEPLOY_SCRIPT.parent.parent.parent
DEFAULT_ARTIFACTS_DIR = REPO_ROOT / "output_from_droplet" / "multichain_3w"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_host(host: str) -> str:
    host = host.strip()
    if not host or not HOST_RE.match(host):
        raise ValueError(f"Invalid host: {host!r}")
    return host


def run_ssh(host: str, remote_cmd: str, timeout: int = 20) -> dict[str, Any]:
    cmd = ["ssh", f"root@{host}", remote_cmd]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    return {
        "ok": proc.returncode == 0,
        "code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "cmd": " ".join(shlex.quote(x) for x in cmd),
    }


def run_remote_json(host: str, remote_cmd: str, timeout: int = 20) -> dict[str, Any]:
    out = run_ssh(host, remote_cmd, timeout=timeout)
    if not out["ok"]:
        return {"ok": False, "error": out["stderr"].strip() or out["stdout"].strip(), "raw": out}
    try:
        return {"ok": True, "data": json.loads(out["stdout"])}
    except json.JSONDecodeError as err:
        return {"ok": False, "error": f"JSON parse error: {err}", "raw": out}


@dataclass
class DeployState:
    running: bool = False
    started_at: str | None = None
    ended_at: str | None = None
    return_code: int | None = None
    host: str | None = None
    output: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "running": self.running,
                "started_at": self.started_at,
                "ended_at": self.ended_at,
                "return_code": self.return_code,
                "host": self.host,
                "output": self.output[-600:],
            }

    def append_line(self, line: str) -> None:
        with self.lock:
            self.output.append(f"[{utc_now_iso()}] {line.rstrip()}")
            if len(self.output) > 5000:
                self.output = self.output[-3000:]


DEPLOY = DeployState()


def start_deploy(host: str) -> tuple[bool, str]:
    host = validate_host(host)
    with DEPLOY.lock:
        if DEPLOY.running:
            return False, "Deploy already running."
        DEPLOY.running = True
        DEPLOY.started_at = utc_now_iso()
        DEPLOY.ended_at = None
        DEPLOY.return_code = None
        DEPLOY.host = host
        DEPLOY.output = [f"[{utc_now_iso()}] Starting deploy for host={host}"]

    def _runner() -> None:
        cmd = [str(DEPLOY_SCRIPT), "--host", host]
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            DEPLOY.append_line(line)
        code = proc.wait()
        with DEPLOY.lock:
            DEPLOY.running = False
            DEPLOY.return_code = code
            DEPLOY.ended_at = utc_now_iso()
            DEPLOY.output.append(f"[{utc_now_iso()}] Deploy finished with code={code}")

    threading.Thread(target=_runner, daemon=True).start()
    return True, "Deploy started."


def collect_summary(host: str) -> dict[str, Any]:
    host = validate_host(host)
    summary: dict[str, Any] = {
        "timestamp_utc": utc_now_iso(),
        "host": host,
    }

    service_payload = run_remote_json(
        host,
        (
            "python3 - <<'PY'\n"
            "import json, subprocess\n"
            "units=['uniswap-jobs.service','uniswap-backfill.service']\n"
            "out={}\n"
            "for u in units:\n"
            "  p=subprocess.run(['systemctl','show',u,'--property=ActiveState,SubState,MainPID,ExecMainStatus,ExecMainStartTimestamp'],capture_output=True,text=True)\n"
            "  d={}\n"
            "  for line in p.stdout.splitlines():\n"
            "    if '=' in line:\n"
            "      k,v=line.split('=',1)\n"
            "      d[k]=v\n"
            "  out[u]=d\n"
            "print(json.dumps(out))\n"
            "PY"
        ),
        timeout=25,
    )
    summary["services"] = service_payload

    timers = run_ssh(host, "systemctl list-timers --all --no-pager | grep -E 'uniswap-(jobs|backfill)' || true")
    summary["timers"] = timers

    outputs = run_ssh(
        host,
        (
            "bash -lc 'for d in "
            "/opt/uniswap-yield-scanner/output/multichain_3w "
            "/opt/uniswap-yield-scanner/output/backtest_multichain_3w "
            "/opt/uniswap-yield-scanner/output/backfill_multichain; "
            "do if [ -d \"$d\" ]; then echo \"## $d\"; ls -lh \"$d\"; fi; done'"
        ),
        timeout=25,
    )
    summary["outputs"] = outputs

    checkpoints = run_remote_json(
        host,
        (
            "python3 - <<'PY'\n"
            "import json, sqlite3\n"
            "db='/opt/uniswap-yield-scanner/output/cache/observations.sqlite'\n"
            "out={'exists':False,'rows':[]}\n"
            "try:\n"
            "  conn=sqlite3.connect(db)\n"
            "  out['exists']=True\n"
            "  cur=conn.execute('select source_name,mode,fetch_start_ts,fetch_end_ts,pages_fetched,rows_fetched,updated_at_ts from source_checkpoint order by updated_at_ts desc')\n"
            "  out['rows']=[list(r) for r in cur.fetchall()]\n"
            "except Exception as e:\n"
            "  out['error']=str(e)\n"
            "print(json.dumps(out))\n"
            "PY"
        ),
        timeout=25,
    )
    summary["checkpoints"] = checkpoints
    return summary


def tail_unit(host: str, unit: str, lines: int) -> dict[str, Any]:
    unit = unit.strip()
    if not unit.endswith(".service"):
        return {"ok": False, "error": "unit must end with .service"}
    lines = max(10, min(lines, 1000))
    return run_ssh(host, f"journalctl -u {shlex.quote(unit)} -n {lines} --no-pager", timeout=25)


def run_action(host: str, action: str) -> dict[str, Any]:
    host = validate_host(host)
    actions = {
        "restart_jobs": "systemctl restart uniswap-jobs.service",
        "start_jobs": "systemctl start uniswap-jobs.service",
        "stop_jobs": "systemctl stop uniswap-jobs.service",
        "restart_backfill": "systemctl restart uniswap-backfill.service",
        "start_backfill": "systemctl start uniswap-backfill.service",
        "stop_backfill": "systemctl stop uniswap-backfill.service",
    }
    cmd = actions.get(action)
    if not cmd:
        return {"ok": False, "error": f"Unknown action: {action}"}
    return run_ssh(host, cmd, timeout=40)


def _to_int(value: Any, default: int, min_v: int, max_v: int) -> int:
    try:
        n = int(value)
    except Exception:  # noqa: BLE001
        return default
    return max(min_v, min(max_v, n))


def _to_float(value: Any, default: float, min_v: float, max_v: float) -> float:
    try:
        n = float(value)
    except Exception:  # noqa: BLE001
        return default
    return max(min_v, min(max_v, n))


def _safe_slug(text: str, default: str = "manual_run") -> str:
    text = (text or "").strip()
    if not text:
        return default
    if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
        return default
    return text


def start_manual_report(host: str, payload: dict[str, Any]) -> dict[str, Any]:
    host = validate_host(host)
    hours = _to_int(payload.get("hours"), default=504, min_v=24, max_v=24 * 365)
    workers = _to_int(payload.get("workers"), default=4, min_v=1, max_v=64)
    top = _to_int(payload.get("top"), default=25, min_v=1, max_v=500)
    page_size = _to_int(payload.get("page_size"), default=500, min_v=100, max_v=5000)
    retries = _to_int(payload.get("retries"), default=6, min_v=1, max_v=20)
    timeout = _to_int(payload.get("timeout"), default=60, min_v=10, max_v=600)
    objective = str(payload.get("optimizer_objective", "risk_adjusted")).strip().lower()
    if objective not in {"raw", "risk_adjusted"}:
        objective = "risk_adjusted"
    deploy_usd = _to_float(payload.get("default_deploy_usd"), default=10000.0, min_v=100.0, max_v=10_000_000.0)
    move_cost = _to_float(payload.get("default_move_cost_usd"), default=50.0, min_v=0.0, max_v=100_000.0)
    max_moves = _to_int(payload.get("default_max_moves_per_day"), default=4, min_v=1, max_v=48)
    output_subdir = _safe_slug(str(payload.get("output_subdir", "manual_run")))
    config_file = _safe_slug(str(payload.get("config_file", "sources.uniswap-official.multichain.json")), default="sources.uniswap-official.multichain.json")
    charts_enable = bool(payload.get("charts_enable", True))
    include_backfill = bool(payload.get("include_backfill_sources", False))

    args = [
        "/opt/uniswap-yield-scanner/.venv/bin/python",
        "scanner.py",
        "--config",
        f"config/{config_file}",
        "--hours",
        str(hours),
        "--workers",
        str(workers),
        "--top",
        str(top),
        "--page-size",
        str(page_size),
        "--retries",
        str(retries),
        "--timeout",
        str(timeout),
        "--optimizer-objective",
        objective,
        "--default-deploy-usd",
        f"{deploy_usd}",
        "--default-move-cost-usd",
        f"{move_cost}",
        "--default-max-moves-per-day",
        str(max_moves),
        "--output-dir",
        f"output/{output_subdir}",
        "--no-open-report",
    ]
    if charts_enable:
        args.append("--charts-enable")
    else:
        args.append("--charts-disable")
    if include_backfill:
        args.append("--include-backfill-sources")

    cmd_text = " ".join(shlex.quote(x) for x in args)
    remote_cmd = (
        "bash -lc "
        + shlex.quote(
            "systemctl reset-failed {unit} >/dev/null 2>&1 || true; "
            "systemctl stop {unit} >/dev/null 2>&1 || true; "
            "systemd-run --unit {unit_base} --collect --same-dir "
            "--property=WorkingDirectory=/opt/uniswap-yield-scanner "
            "/bin/bash -lc {run_cmd}"
        ).format(
            unit=MANUAL_REPORT_UNIT,
            unit_base=MANUAL_REPORT_UNIT.removesuffix(".service"),
            run_cmd=shlex.quote(f"cd /opt/uniswap-yield-scanner && {cmd_text}"),
        )
    )
    out = run_ssh(host, remote_cmd, timeout=60)
    out["requested"] = {
        "hours": hours,
        "workers": workers,
        "top": top,
        "page_size": page_size,
        "retries": retries,
        "timeout": timeout,
        "optimizer_objective": objective,
        "default_deploy_usd": deploy_usd,
        "default_move_cost_usd": move_cost,
        "default_max_moves_per_day": max_moves,
        "output_subdir": output_subdir,
        "charts_enable": charts_enable,
        "include_backfill_sources": include_backfill,
    }
    return out


def manual_report_status(host: str) -> dict[str, Any]:
    return run_ssh(
        host,
        (
            "systemctl show "
            + shlex.quote(MANUAL_REPORT_UNIT)
            + " --property=ActiveState,SubState,Result,MainPID,ExecMainStatus,ExecMainStartTimestamp,ExecMainExitTimestamp,FragmentPath"
        ),
        timeout=20,
    )


def manual_report_logs(host: str, lines: int) -> dict[str, Any]:
    lines = max(20, min(2000, int(lines)))
    return run_ssh(host, f"journalctl -u {shlex.quote(MANUAL_REPORT_UNIT)} -n {lines} --no-pager", timeout=30)


def stop_manual_report(host: str) -> dict[str, Any]:
    return run_ssh(host, f"systemctl stop {shlex.quote(MANUAL_REPORT_UNIT)}", timeout=30)


def _safe_artifact_path(base_dir: Path, rel_path: str) -> Path | None:
    if not rel_path:
        return None
    candidate = (base_dir / rel_path).resolve()
    try:
        candidate.relative_to(base_dir.resolve())
    except Exception:
        return None
    return candidate


def load_dashboard_state(artifacts_dir: Path) -> dict[str, Any]:
    state_path = artifacts_dir / "dashboard_state.json"
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as err:  # noqa: BLE001
            return {"error": f"failed to parse dashboard_state.json: {err}"}
    return {"error": f"missing {state_path}"}


def _load_csv_dicts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "lpscan-dashboard/1.0"

    def _json(self, payload: Any, status: int = 200) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _html(self, body: str, status: int = 200) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        host = qs.get("host", [self.server.default_host])[0]
        artifacts_dir: Path = self.server.artifacts_dir

        if parsed.path == "/":
            self._html(DASHBOARD_HTML)
            return
        if parsed.path == "/pools":
            self._html(POOLS_DASHBOARD_HTML)
            return
        if parsed.path == "/api/state":
            self._json({"ok": True, "data": load_dashboard_state(artifacts_dir)})
            return
        if parsed.path == "/api/spikes":
            state = load_dashboard_state(artifacts_dir)
            spikes = state.get("spikes", []) if isinstance(state, dict) else []
            if not spikes and isinstance(state, dict):
                spikes = state.get("llama", {}).get("top_spikes", [])
            if not spikes:
                spikes = _load_csv_dicts(artifacts_dir / "llama_weth_spike_rankings.csv")
            offset = max(0, int(qs.get("offset", ["0"])[0]))
            limit = max(1, min(500, int(qs.get("limit", ["100"])[0])))
            self._json({"ok": True, "offset": offset, "limit": limit, "items": spikes[offset : offset + limit], "total": len(spikes)})
            return
        if parsed.path == "/api/schedule":
            state = load_dashboard_state(artifacts_dir)
            sched = state.get("schedule", {}).get("top_blocks", []) if isinstance(state, dict) else []
            if not sched:
                sched = _load_csv_dicts(artifacts_dir / "schedule_enhanced.csv")
            offset = max(0, int(qs.get("offset", ["0"])[0]))
            limit = max(1, min(500, int(qs.get("limit", ["100"])[0])))
            self._json({"ok": True, "offset": offset, "limit": limit, "items": sched[offset : offset + limit], "total": len(sched)})
            return
        if parsed.path.startswith("/api/pool/"):
            pool_id = unquote(parsed.path[len("/api/pool/") :]).strip().lower()
            state = load_dashboard_state(artifacts_dir)
            if isinstance(state, dict):
                spikes = state.get("spikes", [])
                if not spikes:
                    spikes = state.get("llama", {}).get("top_spikes", [])
                sched = state.get("schedule", {}).get("top_blocks", [])
                selected = state.get("schedule", {}).get("selected_plan", [])
            else:
                spikes, sched, selected = [], [], []
            payload = {
                "pool_id": pool_id,
                "spikes": [r for r in spikes if str(r.get("pair", "")).lower() == pool_id or str(r.get("pool_id", "")).lower() == pool_id],
                "schedule_blocks": [r for r in sched if str(r.get("pool_id", "")).lower() == pool_id],
                "selected_plan_rows": [r for r in selected if str(r.get("pool_id", "")).lower() == pool_id],
            }
            self._json({"ok": True, "data": payload})
            return
        if parsed.path.startswith("/api/files/"):
            rel = unquote(parsed.path[len("/api/files/") :]).lstrip("/")
            target = _safe_artifact_path(artifacts_dir, rel)
            if target is None or not target.exists() or not target.is_file():
                self._json({"ok": False, "error": "file not found"}, status=404)
                return
            data = target.read_bytes()
            ctype, _ = mimetypes.guess_type(str(target))
            self.send_response(200)
            self.send_header("Content-Type", ctype or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if parsed.path == "/api/summary":
            try:
                payload = collect_summary(host)
                payload["deploy"] = DEPLOY.snapshot()
                self._json(payload)
            except Exception as err:  # noqa: BLE001
                self._json({"ok": False, "error": str(err)}, status=500)
            return
        if parsed.path == "/api/logs":
            unit = qs.get("unit", ["uniswap-jobs.service"])[0]
            lines = int(qs.get("lines", ["120"])[0])
            self._json(tail_unit(host, unit, lines))
            return
        if parsed.path == "/api/deploy":
            self._json(DEPLOY.snapshot())
            return
        if parsed.path == "/api/report/status":
            self._json(manual_report_status(host))
            return
        if parsed.path == "/api/report/logs":
            lines = int(qs.get("lines", ["200"])[0])
            self._json(manual_report_logs(host, lines))
            return
        self._json({"ok": False, "error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {}

        host = payload.get("host", self.server.default_host)

        if parsed.path == "/api/deploy/start":
            ok, msg = start_deploy(str(host))
            self._json({"ok": ok, "message": msg, "deploy": DEPLOY.snapshot()}, status=(200 if ok else 409))
            return
        if parsed.path == "/api/action":
            action = str(payload.get("action", ""))
            self._json(run_action(str(host), action))
            return
        if parsed.path == "/api/report/start":
            self._json(start_manual_report(str(host), payload))
            return
        if parsed.path == "/api/report/stop":
            self._json(stop_manual_report(str(host)))
            return
        self._json({"ok": False, "error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{utc_now_iso()}] {self.address_string()} {fmt % args}")


class DashboardServer(ThreadingHTTPServer):
    def __init__(
        self,
        addr: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        default_host: str,
        artifacts_dir: Path,
    ) -> None:
        super().__init__(addr, handler)
        self.default_host = default_host
        self.artifacts_dir = artifacts_dir.resolve()


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LP Scan Ops Dashboard</title>
  <style>
    :root { --bg:#0b1020; --panel:#121a2f; --ink:#e5ecff; --muted:#96a7d4; --good:#29cc7a; --bad:#ff6b6b; --warn:#f5b642; --line:#27345a; --btn:#1f6feb; }
    body { margin:0; font:14px/1.4 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto; background:var(--bg); color:var(--ink);}
    .wrap { max-width:1400px; margin:18px auto; padding:0 14px; }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:12px; margin-bottom:12px; }
    .top { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    input,select,button { background:#0e1630; color:var(--ink); border:1px solid var(--line); border-radius:8px; padding:8px 10px; }
    button { background:var(--btn); border:none; cursor:pointer; }
    button.alt { background:#34456f; }
    h1 { margin:0 0 8px 0; font-size:20px; }
    h2 { margin:0 0 8px 0; font-size:16px; }
    pre { margin:0; max-height:360px; overflow:auto; white-space:pre-wrap; background:#0a1125; border:1px solid var(--line); border-radius:8px; padding:10px; color:#cdd9ff; }
    .muted { color:var(--muted); }
    .status-ok { color:var(--good); font-weight:700; }
    .status-bad { color:var(--bad); font-weight:700; }
    .status-warn { color:var(--warn); font-weight:700; }
    .kv { display:grid; grid-template-columns:180px 1fr; gap:4px 8px; }
    @media (max-width: 980px) { .row { grid-template-columns:1fr; } .kv { grid-template-columns:1fr; } }
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>LP Scan Ops Dashboard</h1>
    <div class="top">
      <label>Host</label>
      <input id="host" size="18" value="134.199.135.19" />
      <button onclick="refreshAll()">Refresh</button>
      <button onclick="startDeploy()">Deploy Update + Restart</button>
      <button class="alt" onclick="runAction('restart_jobs')">Restart Jobs</button>
      <button class="alt" onclick="runAction('start_backfill')">Start Backfill</button>
      <button class="alt" onclick="runAction('stop_backfill')">Stop Backfill</button>
      <a href="/pools" style="color:#9fc6ff;text-decoration:none;border:1px solid #27345a;padding:8px 10px;border-radius:8px;">Pool Dashboard</a>
      <span class="muted" id="clock"></span>
    </div>
  </div>

  <div class="card">
    <h2>Manual Report Service (On-Demand)</h2>
    <div class="top">
      <label>Hours</label><input id="rep-hours" size="5" value="504" />
      <label>Workers</label><input id="rep-workers" size="3" value="4" />
      <label>Top</label><input id="rep-top" size="3" value="25" />
      <label>Page Size</label><input id="rep-page-size" size="4" value="500" />
      <label>Objective</label>
      <select id="rep-objective"><option value="risk_adjusted" selected>risk_adjusted</option><option value="raw">raw</option></select>
      <label>Deploy USD</label><input id="rep-deploy" size="7" value="10000" />
      <label>Move Cost</label><input id="rep-move-cost" size="5" value="50" />
      <label>Max Moves/Day</label><input id="rep-max-moves" size="3" value="4" />
      <label>Output Subdir</label><input id="rep-output" size="12" value="manual_run" />
      <button onclick="startReport()">Start Report Service</button>
      <button class="alt" onclick="stopReport()">Stop Report Service</button>
      <button class="alt" onclick="refreshReportStatus()">Refresh Report Status</button>
    </div>
    <div class="kv" id="report-meta" style="margin-top:8px;"></div>
    <pre id="report-log"></pre>
  </div>

  <div class="row">
    <div class="card">
      <h2>Services</h2>
      <div class="kv" id="svc"></div>
    </div>
    <div class="card">
      <h2>Deploy Task</h2>
      <div class="kv" id="deploy-meta"></div>
      <pre id="deploy-log"></pre>
    </div>
  </div>

  <div class="row">
    <div class="card">
      <h2>Timers</h2>
      <pre id="timers"></pre>
    </div>
    <div class="card">
      <h2>Checkpoint Progress</h2>
      <pre id="checkpoints"></pre>
    </div>
  </div>

  <div class="row">
    <div class="card">
      <h2>Jobs Log</h2>
      <pre id="jobs-log"></pre>
    </div>
    <div class="card">
      <h2>Backfill Log</h2>
      <pre id="backfill-log"></pre>
    </div>
  </div>

  <div class="card">
    <h2>Output Files</h2>
    <pre id="outputs"></pre>
  </div>
</div>

<script>
function esc(s){ return (s ?? '').toString(); }
function statusClass(s){
  if(!s) return "status-warn";
  if(s.includes("active")) return "status-ok";
  if(s.includes("failed")) return "status-bad";
  return "status-warn";
}
async function jget(url){
  const r = await fetch(url);
  return await r.json();
}
async function jpost(url, body){
  const r = await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  return await r.json();
}
function host(){ return document.getElementById('host').value.trim(); }
function setText(id, txt){ document.getElementById(id).textContent = txt || ''; }
function setHTML(id, html){ document.getElementById(id).innerHTML = html; }

async function refreshAll(){
  const h = encodeURIComponent(host());
  document.getElementById('clock').textContent = "refreshing...";
  try{
    const s = await jget(`/api/summary?host=${h}`);
    const svc = s.services?.data || {};
    const jobs = svc["uniswap-jobs.service"] || {};
    const back = svc["uniswap-backfill.service"] || {};
    setHTML("svc",
      `<div>jobs</div><div class="${statusClass(jobs.ActiveState)}">${esc(jobs.ActiveState)} / ${esc(jobs.SubState)} (pid=${esc(jobs.MainPID)})</div>`+
      `<div>jobs started</div><div>${esc(jobs.ExecMainStartTimestamp)}</div>`+
      `<div>backfill</div><div class="${statusClass(back.ActiveState)}">${esc(back.ActiveState)} / ${esc(back.SubState)} (pid=${esc(back.MainPID)})</div>`+
      `<div>backfill started</div><div>${esc(back.ExecMainStartTimestamp)}</div>`
    );

    setText("timers", (s.timers?.stdout || s.timers?.stderr || "").trim());
    setText("outputs", (s.outputs?.stdout || s.outputs?.stderr || "").trim());

    const cp = s.checkpoints?.data;
    if(cp){
      setText("checkpoints", JSON.stringify(cp, null, 2));
    } else {
      setText("checkpoints", JSON.stringify(s.checkpoints, null, 2));
    }

    const dep = s.deploy || {};
    setHTML("deploy-meta",
      `<div>running</div><div class="${dep.running ? 'status-warn':'status-ok'}">${dep.running}</div>`+
      `<div>host</div><div>${esc(dep.host)}</div>`+
      `<div>started</div><div>${esc(dep.started_at)}</div>`+
      `<div>ended</div><div>${esc(dep.ended_at)}</div>`+
      `<div>return code</div><div>${esc(dep.return_code)}</div>`
    );
    setText("deploy-log", (dep.output || []).join("\\n"));

    const jl = await jget(`/api/logs?host=${h}&unit=uniswap-jobs.service&lines=140`);
    setText("jobs-log", (jl.stdout || jl.stderr || "").trim());
    const bl = await jget(`/api/logs?host=${h}&unit=uniswap-backfill.service&lines=140`);
    setText("backfill-log", (bl.stdout || bl.stderr || "").trim());
    await refreshReportStatus();
  }catch(err){
    setText("jobs-log", "Dashboard error: " + err);
  }
  document.getElementById('clock').textContent = "last refresh " + new Date().toLocaleTimeString();
}

async function startDeploy(){
  const res = await jpost("/api/deploy/start", {host: host()});
  if(!res.ok){ alert(res.message || res.error || "deploy failed to start"); }
  refreshAll();
}

async function runAction(action){
  const res = await jpost("/api/action", {host: host(), action});
  if(!res.ok){ alert(res.error || res.stderr || "action failed"); }
  setTimeout(refreshAll, 1200);
}

function reportPayload(){
  return {
    host: host(),
    hours: Number(document.getElementById('rep-hours').value || 504),
    workers: Number(document.getElementById('rep-workers').value || 4),
    top: Number(document.getElementById('rep-top').value || 25),
    page_size: Number(document.getElementById('rep-page-size').value || 500),
    optimizer_objective: document.getElementById('rep-objective').value || 'risk_adjusted',
    default_deploy_usd: Number(document.getElementById('rep-deploy').value || 10000),
    default_move_cost_usd: Number(document.getElementById('rep-move-cost').value || 50),
    default_max_moves_per_day: Number(document.getElementById('rep-max-moves').value || 4),
    output_subdir: document.getElementById('rep-output').value || 'manual_run',
    charts_enable: true
  };
}

async function startReport(){
  const res = await jpost('/api/report/start', reportPayload());
  if(!res.ok){ alert(res.error || res.stderr || res.message || 'failed to start report service'); }
  await refreshReportStatus();
}

async function stopReport(){
  const res = await jpost('/api/report/stop', {host: host()});
  if(!res.ok){ alert(res.error || res.stderr || 'failed to stop report service'); }
  await refreshReportStatus();
}

async function refreshReportStatus(){
  const h = encodeURIComponent(host());
  const st = await jget(`/api/report/status?host=${h}`);
  const kv = {};
  ((st.stdout || '').split('\\n')).forEach(line=>{
    const i = line.indexOf('=');
    if(i>0){ kv[line.slice(0,i)] = line.slice(i+1); }
  });
  setHTML('report-meta',
    `<div>ActiveState</div><div class="${statusClass(kv.ActiveState||'')}">${esc(kv.ActiveState)} / ${esc(kv.SubState)}</div>`+
    `<div>Result</div><div>${esc(kv.Result)}</div>`+
    `<div>MainPID</div><div>${esc(kv.MainPID)}</div>`+
    `<div>Start</div><div>${esc(kv.ExecMainStartTimestamp)}</div>`+
    `<div>Exit</div><div>${esc(kv.ExecMainExitTimestamp)}</div>`+
    `<div>Status</div><div>${esc(kv.ExecMainStatus)}</div>`
  );
  const lg = await jget(`/api/report/logs?host=${h}&lines=200`);
  setText('report-log', (lg.stdout || lg.stderr || '').trim());
}

refreshAll();
setInterval(refreshAll, 7000);
</script>
</body>
</html>
"""

POOLS_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LP Pool & Strategy Dashboard</title>
  <style>
    :root { --bg:#0b1020; --panel:#121a2f; --ink:#e5ecff; --muted:#96a7d4; --line:#27345a; --good:#29cc7a; --warn:#f5b642; --bad:#ff6b6b; --accent:#1f6feb; }
    body { margin:0; background:var(--bg); color:var(--ink); font:14px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto; }
    .wrap { max-width:1600px; margin:18px auto; padding:0 14px; }
    .card { background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }
    h1,h2 { margin:0 0 8px 0; }
    h1 { font-size:22px; }
    h2 { font-size:16px; }
    .top { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    input,button,select { background:#0e1630; color:var(--ink); border:1px solid var(--line); border-radius:8px; padding:8px 10px; }
    button { background:var(--accent); border:none; cursor:pointer; }
    .muted { color:var(--muted); }
    .grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:8px; }
    .kpi { background:#0d1530; border:1px solid var(--line); border-radius:10px; padding:10px; }
    .kpi .v { font-size:20px; font-weight:700; }
    .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:10px; }
    table { width:100%; border-collapse:collapse; }
    th,td { border:1px solid var(--line); padding:6px 8px; text-align:left; white-space:nowrap; }
    th { background:#0d1530; position:sticky; top:0; z-index:2; }
    .badge { padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; }
    .ok { background:rgba(41,204,122,0.15); color:var(--good); border:1px solid rgba(41,204,122,0.4); }
    .low { background:rgba(245,182,66,0.15); color:var(--warn); border:1px solid rgba(245,182,66,0.4); }
    .bad { background:rgba(255,107,107,0.15); color:var(--bad); border:1px solid rgba(255,107,107,0.4); }
    .bar { height:8px; border-radius:999px; background:#1b2748; overflow:hidden; }
    .bar > span { display:block; height:100%; background:linear-gradient(90deg,#2f80ff,#29cc7a); }
    .small { font-size:12px; color:var(--muted); }
    @media (max-width: 1100px) { .grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>Pool & Strategy Dashboard</h1>
    <div class="top">
      <label>Top N</label>
      <select id="topN">
        <option>5</option><option selected>10</option><option>20</option><option>50</option>
      </select>
      <button onclick="refresh()">Refresh</button>
      <a href="/" style="color:#9fc6ff;text-decoration:none;border:1px solid #27345a;padding:8px 10px;border-radius:8px;">Ops Dashboard</a>
      <span id="status" class="muted"></span>
    </div>
  </div>

  <div class="card">
    <h2>Run Snapshot</h2>
    <div class="kpis" id="kpis"></div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>Top Pools by Incremental Edge</h2>
      <div class="table-wrap"><table><thead>
        <tr><th>#</th><th>Pool</th><th>Chain</th><th>Inc USD/$1k</th><th>Risk-Adj USD/$1k</th><th>Breakeven USD/$1k</th><th>Confidence</th><th>Capacity</th><th>Next Add</th></tr>
      </thead><tbody id="topPools"></tbody></table></div>
    </div>
    <div class="card">
      <h2>Live Llama Spikes</h2>
      <div class="table-wrap"><table><thead>
        <tr><th>#</th><th>Pool</th><th>Chain</th><th>USD/$1k/hr</th><th>Spike Mult</th><th>Persistence</th><th>Hour (CST)</th><th>Flags</th></tr>
      </thead><tbody id="spikes"></tbody></table></div>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>Executable Plan (Default Scenario)</h2>
      <div class="small" id="scenarioMeta"></div>
      <div class="table-wrap"><table><thead>
        <tr><th>#</th><th>Pool</th><th>Chain</th><th>Add</th><th>Remove</th><th>Eff Deploy USD</th><th>Net USD</th><th>Net USD/$1k</th><th>Conf</th><th>Cap</th></tr>
      </thead><tbody id="plan"></tbody></table></div>
    </div>
    <div class="card">
      <h2>Moves/Day Frontier (Default Objective)</h2>
      <div class="small" id="schedDiag"></div>
      <div class="table-wrap"><table><thead>
        <tr><th>Move Cost</th><th>Deploy</th><th>Max Moves/Day</th><th>Blocks</th><th>Total Net USD</th><th>Notes</th></tr>
      </thead><tbody id="frontier"></tbody></table></div>
    </div>
  </div>

  <div class="card">
    <h2>Source Health</h2>
    <div class="table-wrap"><table><thead>
      <tr><th>Source</th><th>Version</th><th>Chain</th><th>Input</th><th>fees_with_nonpositive_tvl_rate</th><th>tvl_below_floor_rate</th><th>invalid_fee_tier_rate</th><th>Excluded</th></tr>
    </thead><tbody id="sourceHealth"></tbody></table></div>
  </div>
</div>

<script>
function esc(v){ return (v===null||v===undefined)?'':String(v); }
function num(v,d=2){ const n=Number(v); return Number.isFinite(n)?n.toFixed(d):''; }
function badge(flag){
  if(flag==='OK') return '<span class="badge ok">OK</span>';
  if(flag==='LOW_CAPACITY') return '<span class="badge low">LOW_CAPACITY</span>';
  return '<span class="badge bad">'+esc(flag||'n/a')+'</span>';
}
async function refresh(){
  const topN = Number(document.getElementById('topN').value || 10);
  const stEl = document.getElementById('status');
  stEl.textContent = 'loading...';
  let payload;
  try {
    const r = await fetch('/api/state');
    const j = await r.json();
    payload = j.data || {};
  } catch (e){
    stEl.textContent = 'state fetch failed: '+e;
    return;
  }
  const defaults = payload.defaults || {};
  const schedule = payload.schedule || {};
  const llama = payload.llama || {};
  const spikesState = payload.spikes || llama.top_spikes || [];
  const sourceHealth = payload.source_health || [];

  document.getElementById('scenarioMeta').textContent =
    `objective=${esc(defaults.objective)} deploy=${esc(defaults.deploy_usd)} move_cost=${esc(defaults.move_cost_usd)} max_moves/day=${esc(defaults.max_moves_per_day)} plan=${esc(defaults.scenario_plan_filename)}`;
  const activeDiag = (((schedule.diagnostics||{}).active_scenario)||null);
  if(activeDiag){
    document.getElementById('schedDiag').textContent =
      `diagnostics: total=${activeDiag.total_schedule_rows} src_health=${activeDiag.excluded_by_source_health} cap=${activeDiag.excluded_by_capacity} abs_cap=${activeDiag.excluded_by_abs_capacity_floor} min_tvl=${activeDiag.excluded_by_min_tvl} hold=${activeDiag.excluded_by_hold_hours} candidates=${activeDiag.candidates_after_filters} selected=${activeDiag.selected_blocks_count} reason_if_zero=${activeDiag.reason_if_zero||'n/a'}`;
  } else {
    document.getElementById('schedDiag').textContent = 'diagnostics unavailable';
  }

  const kpis = [
    ['Generated UTC', esc(payload.generated_utc)],
    ['Llama Meta Block', esc(llama.meta_block_number)],
    ['Llama Ranked Spikes', esc((spikesState||[]).length)],
    ['Schedule Blocks', esc((schedule.top_blocks||[]).length)],
    ['Selected Plan Rows', esc((schedule.selected_plan||[]).length)],
    ['Source Exclusions', esc(sourceHealth.filter(r=>r.excluded_from_schedule).length)],
  ];
  document.getElementById('kpis').innerHTML = kpis.map(([k,v])=>`<div class="kpi"><div class="small">${k}</div><div class="v">${v}</div></div>`).join('');

  const topPools = (schedule.top_blocks||[]).slice(0, topN).map((r,i)=>{
    const conf = Math.max(0, Math.min(1, Number(r.confidence_score||0)));
    return `<tr>
      <td>${i+1}</td><td>${esc(r.pair)}</td><td>${esc(r.chain)}</td>
      <td>${num(r.incremental_usd_per_1000,4)}</td>
      <td>${num(r.risk_adjusted_incremental_usd_per_1000,4)}</td>
      <td>${num(r.breakeven_move_cost_usd_per_1000,4)}</td>
      <td><div class="small">${num(conf,3)}</div><div class="bar"><span style="width:${(conf*100).toFixed(1)}%"></span></div></td>
      <td>${badge(r.capacity_flag)}</td>
      <td>${esc(r.next_add_cst)}</td>
    </tr>`;
  }).join('');
  document.getElementById('topPools').innerHTML = topPools || '<tr><td colspan="9">No rows</td></tr>';

  const spikes = (spikesState||[]).slice(0, topN).map((r,i)=>`<tr>
      <td>${i+1}</td><td>${esc(r.pool_label||r.pair)}</td><td>${esc(r.chain)}</td>
      <td>${num(r.usd_per_1000_liquidity_hourly,4)}</td>
      <td>${num(r.spike_multiplier,3)}</td>
      <td>${esc(r.persistence_hits)}</td>
      <td>${esc(r.hour_start_cst)}</td>
      <td>${esc(r.flags)}</td>
  </tr>`).join('');
  document.getElementById('spikes').innerHTML = spikes || '<tr><td colspan="8">No rows</td></tr>';

  const plan = (schedule.selected_plan||[]).slice(0, topN).map((r,i)=>`<tr>
      <td>${i+1}</td><td>${esc(r.pair)}</td><td>${esc(r.chain)}</td>
      <td>${esc(r.next_add_cst)}</td><td>${esc(r.next_remove_cst)}</td>
      <td>${num(r.effective_deploy_usd,2)}</td><td>${num(r.expected_net_usd,2)}</td>
      <td>${num(r.expected_net_usd_per_1000,4)}</td><td>${num(r.confidence_score,3)}</td><td>${badge(r.capacity_flag)}</td>
  </tr>`).join('');
  document.getElementById('plan').innerHTML = plan || '<tr><td colspan="10">No selected moves for default scenario</td></tr>';

  const frontier = ((schedule.curve||schedule.moves_day_curve||[]))
    .filter(r=>String(r.objective)===String(defaults.objective||''))
    .slice(0,200)
    .map(r=>`<tr>
      <td>${num(r.move_cost_usd_per_move,2)}</td><td>${num(r.deploy_usd,0)}</td><td>${esc(r.max_moves_per_day)}</td>
      <td>${esc(r.selected_blocks_count)}</td><td>${num(r.total_net_usd,2)}</td><td>${esc(r.notes)}</td>
    </tr>`).join('');
  document.getElementById('frontier').innerHTML = frontier || '<tr><td colspan="6">No rows</td></tr>';

  const sh = sourceHealth.slice(0,200).map(r=>`<tr>
    <td>${esc(r.source_name)}</td><td>${esc(r.version)}</td><td>${esc(r.chain)}</td><td>${esc(r.input_rows)}</td>
    <td>${num((Number(r.fees_with_nonpositive_tvl_rate||0)*100),2)}%</td>
    <td>${num((Number(r.tvl_below_floor_rate||0)*100),2)}%</td>
    <td>${num((Number(r.invalid_fee_tier_rate||0)*100),2)}%</td>
    <td>${r.excluded_from_schedule ? '<span class="badge bad">excluded</span>' : '<span class="badge ok">ok</span>'}</td>
  </tr>`).join('');
  document.getElementById('sourceHealth').innerHTML = sh || '<tr><td colspan="8">No rows</td></tr>';

  stEl.textContent = 'last refresh ' + new Date().toLocaleTimeString();
}
refresh();
setInterval(refresh, 15000);
</script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Local web dashboard for LP scan ops.")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default=DEFAULT_LPSCAN_HOST, help="Default remote droplet host/IP.")
    parser.add_argument(
        "--artifacts-dir",
        default=str(DEFAULT_ARTIFACTS_DIR),
        help="Directory containing latest pulled run artifacts (default: output_from_droplet/multichain_3w).",
    )
    args = parser.parse_args()

    default_host = validate_host(args.host)
    artifacts_dir = Path(args.artifacts_dir).expanduser().resolve()
    server = DashboardServer((args.bind, args.port), DashboardHandler, default_host, artifacts_dir)
    print(
        f"[{utc_now_iso()}] Dashboard listening on http://{args.bind}:{args.port} "
        f"(default host={default_host}, artifacts={artifacts_dir})"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
