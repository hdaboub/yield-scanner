#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


HOST_RE = re.compile(r"^[A-Za-z0-9.-]+$")
DEFAULT_LPSCAN_HOST = "134.199.135.19"
DEPLOY_SCRIPT = Path(__file__).resolve().parent / "deploy_update_and_run.sh"
REPO_ROOT = DEPLOY_SCRIPT.parent.parent.parent


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

        if parsed.path == "/":
            self._html(DASHBOARD_HTML)
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
        self._json({"ok": False, "error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{utc_now_iso()}] {self.address_string()} {fmt % args}")


class DashboardServer(ThreadingHTTPServer):
    def __init__(self, addr: tuple[str, int], handler: type[BaseHTTPRequestHandler], default_host: str) -> None:
        super().__init__(addr, handler)
        self.default_host = default_host


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
      <span class="muted" id="clock"></span>
    </div>
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

refreshAll();
setInterval(refreshAll, 7000);
</script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Local web dashboard for LP scan ops.")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default=DEFAULT_LPSCAN_HOST, help="Default remote droplet host/IP.")
    args = parser.parse_args()

    default_host = validate_host(args.host)
    server = DashboardServer((args.bind, args.port), DashboardHandler, default_host)
    print(f"[{utc_now_iso()}] Dashboard listening on http://{args.bind}:{args.port} (default host={default_host})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

