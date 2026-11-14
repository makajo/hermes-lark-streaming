"""DeepSeek 官方账户余额查询 — 页脚 ``balance`` 字段的数据源。

只在"确实走 DeepSeek 官方 API"时供值（见 ``finalize._deepseek_official_route``）：

- ``GET https://api.deepseek.com/user/balance``（Bearer ``DEEPSEEK_API_KEY``）
- 返回形如 ``{"is_available": true, "balance_infos": [{"currency": "CNY",
  "total_balance": "25.42", ...}]}``
- 优先取 CNY 条目，格式化成 ``¥25.42``；任何异常/超时/无 key → 返回 ``""``，
  渲染层见空值自动跳过该字段（绝不打断定稿）。

取舍：余额查询是纯装饰，绝不能拖慢定稿或在上游抖动时反复打点——因此
① 3 s 超时；② 进程内 TTL 缓存（成功 30 s / 失败 60 s）；③ 每次定稿最多一次
串行查询（无并发打点）。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.request
from typing import Any

_BALANCE_URL = "https://api.deepseek.com/user/balance"
_HTTP_TIMEOUT_SEC = 3.0
_AWAIT_TIMEOUT_SEC = 3.5
_TTL_OK_SEC = 30.0
_TTL_FAIL_SEC = 60.0

_CURRENCY_SYMBOLS = {"CNY": "¥", "USD": "$"}

# {"ts": monotonic, "value": "¥25.42" | ""}
_cache: dict[str, Any] = {"ts": 0.0, "value": ""}


def _api_key() -> str:
    """``DEEPSEEK_API_KEY``：进程环境优先，其次 profile 的 ``.env``。"""
    key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if key:
        return key
    try:
        home = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
        with open(os.path.join(home, ".env"), encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("DEEPSEEK_API_KEY="):
                    return line.split("=", 1)[1].strip().strip("'\"")
    except OSError:
        pass
    return ""


def _format_balance(payload: dict[str, Any]) -> str:
    infos = payload.get("balance_infos")
    if not isinstance(infos, list) or not infos:
        return ""
    info = next(
        (i for i in infos if isinstance(i, dict) and str(i.get("currency", "")).upper() == "CNY"),
        next((i for i in infos if isinstance(i, dict)), None),
    )
    if info is None:
        return ""
    raw_amount = info.get("total_balance")
    if raw_amount is None:
        return ""
    try:
        amount = float(raw_amount)
    except (TypeError, ValueError):
        return ""
    currency = str(info.get("currency") or "").upper()
    symbol = _CURRENCY_SYMBOLS.get(currency, f"{currency} " if currency else "")
    return f"{symbol}{amount:,.2f}"


def _fetch_sync(key: str) -> str:
    req = urllib.request.Request(
        _BALANCE_URL,
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SEC) as resp:
        payload = json.loads(resp.read().decode("utf-8", "replace"))
    return _format_balance(payload) if isinstance(payload, dict) else ""


async def deepseek_balance_text() -> str:
    """返回 ``¥25.42`` 形态的余额文本；不可用时返回 ``""``（尽力而为）。"""
    now = time.monotonic()
    if _cache["value"] or _cache["ts"]:
        ttl = _TTL_OK_SEC if _cache["value"] else _TTL_FAIL_SEC
        if now - float(_cache["ts"]) < ttl:
            return str(_cache["value"])

    key = _api_key()
    if not key:
        _cache.update(ts=now, value="")
        return ""

    try:
        value = await asyncio.wait_for(
            asyncio.to_thread(_fetch_sync, key), timeout=_AWAIT_TIMEOUT_SEC,
        )
    except Exception:  # noqa: BLE001 — 装饰性数据，失败即跳过
        value = ""
    _cache.update(ts=now, value=value)
    return value
