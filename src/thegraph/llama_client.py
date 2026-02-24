from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any

DEFAULT_LLAMA_ENDPOINT = "https://api.studio.thegraph.com/query/1742316/llama/v0.2.9"
DEFAULT_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()

PAIR_HOUR_QUERY = """
query PairHourPage($first: Int!, $start: Int!, $end: Int!) {
  pairHourDatas(
    first: $first
    orderBy: hourStartUnix
    orderDirection: desc
    where: { hourStartUnix_gte: $start, hourStartUnix_lte: $end }
  ) {
    id
    hourStartUnix
    fee0
    fee1
    reserve0
    reserve1
    swapCount
    pair {
      id
    }
  }
}
"""

PAIR_META_QUERY = """
query PairMeta($ids: [String!]!) {
  pairs(where: { id_in: $ids }) {
    id
    token0
    token1
  }
}
"""


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _post_graphql(
    endpoint: str,
    query: str,
    variables: dict[str, Any],
    timeout: int = 45,
    retries: int = 3,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "uniswap-yield-scanner/llama-client",
    }
    if headers:
        req_headers.update(headers)

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(endpoint, data=payload, headers=req_headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
            last_err = err
            if attempt < retries:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"GraphQL request failed after {retries} attempts: {err}") from err

        if parsed.get("errors"):
            err_text = "; ".join(str(e.get("message", e)) for e in parsed.get("errors", []))
            raise RuntimeError(f"GraphQL error: {err_text}")
        data = parsed.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("GraphQL response missing data object")
        return data

    if last_err:
        raise RuntimeError(f"GraphQL request failed: {last_err}")
    raise RuntimeError("GraphQL request failed")


def fetchRecentPairHourData(
    *,
    endpoint: str = DEFAULT_LLAMA_ENDPOINT,
    first: int = 1000,
    minSwapCount: int = 0,
    sinceHourStartUnix: int | None = None,
    endHourStartUnix: int | None = None,
    timeout: int = 45,
    retries: int = 3,
    headers: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    now_hour = int(time.time() // 3600 * 3600)
    start = sinceHourStartUnix if sinceHourStartUnix is not None else (now_hour - 24 * 3600)
    end = endHourStartUnix if endHourStartUnix is not None else now_hour
    data = _post_graphql(
        endpoint=endpoint,
        query=PAIR_HOUR_QUERY,
        variables={"first": max(1, min(1000, first)), "start": int(start), "end": int(end)},
        timeout=timeout,
        retries=retries,
        headers=headers,
    )
    rows = data.get("pairHourDatas")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        swap_count = int(row.get("swapCount") or 0)
        if swap_count < max(0, minSwapCount):
            continue
        out.append(row)
    return out


def fetchPairsMeta(
    pairIds: list[str],
    *,
    endpoint: str = DEFAULT_LLAMA_ENDPOINT,
    timeout: int = 45,
    retries: int = 3,
    headers: dict[str, str] | None = None,
) -> dict[str, dict[str, str]]:
    ids = sorted({str(p).lower() for p in pairIds if p})
    if not ids:
        return {}
    out: dict[str, dict[str, str]] = {}
    chunk = 200
    for i in range(0, len(ids), chunk):
        batch = ids[i : i + chunk]
        data = _post_graphql(
            endpoint=endpoint,
            query=PAIR_META_QUERY,
            variables={"ids": batch},
            timeout=timeout,
            retries=retries,
            headers=headers,
        )
        pairs = data.get("pairs")
        if not isinstance(pairs, list):
            continue
        for row in pairs:
            if not isinstance(row, dict):
                continue
            pid = str(row.get("id", "")).lower()
            if not pid:
                continue
            token0 = row.get("token0")
            token1 = row.get("token1")
            if isinstance(token0, dict):
                token0 = token0.get("id")
            if isinstance(token1, dict):
                token1 = token1.get("id")
            out[pid] = {
                "token0": str(token0 or "").lower(),
                "token1": str(token1 or "").lower(),
            }
    return out


def computeWethScore(
    pairHourDataRow: dict[str, Any],
    pairMeta: dict[str, str],
    *,
    wethAddress: str = DEFAULT_WETH,
) -> float | None:
    token0 = str(pairMeta.get("token0", "")).lower()
    token1 = str(pairMeta.get("token1", "")).lower()
    weth = wethAddress.lower()
    if token0 != weth and token1 != weth:
        return None

    reserve0 = _to_decimal(pairHourDataRow.get("reserve0"))
    reserve1 = _to_decimal(pairHourDataRow.get("reserve1"))
    fee0 = _to_decimal(pairHourDataRow.get("fee0"))
    fee1 = _to_decimal(pairHourDataRow.get("fee1"))

    if token0 == weth:
        reserve_weth = reserve0
        fee_weth = fee0
    else:
        reserve_weth = reserve1
        fee_weth = fee1

    if reserve_weth <= 0:
        return None
    return float(fee_weth / reserve_weth)
