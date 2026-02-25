#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scanner  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate static dashboard.html from dashboard_state.json"
    )
    parser.add_argument(
        "--state",
        default="output/multichain_3w/dashboard_state.json",
        help="Path to dashboard_state.json",
    )
    parser.add_argument(
        "--out",
        default="output/multichain_3w/dashboard.html",
        help="Output dashboard HTML path",
    )
    args = parser.parse_args()

    state_path = Path(args.state).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    if not state_path.exists():
        raise FileNotFoundError(f"dashboard state file not found: {state_path}")

    with state_path.open("r", encoding="utf-8") as f:
        state = json.load(f)

    scanner.write_dashboard_html(out_path, state)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
