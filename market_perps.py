"""Public Bitget USDT perpetual data; never substitute a regular stock quote.

MSTRUSDT is a USDT-settled stock perpetual. The public ticker pushes roughly
every 100 ms; the provider's smallest supported candle interval here is 1 min.
"""
from __future__ import annotations

import json
import math
import threading
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websocket


PERP_SYMBOLS = {"MSTR"}
PERP_PERIODS = {"1m", "15m", "1H", "4H", "1D"}
PERP_SOURCE = "Bitget永续"
_REST = "https://api.bitget.com"
_WS = "wss://ws.bitget.com/v2/ws/public"
_PERIOD_MS = {"1m": 60_000, "15m": 900_000, "1H": 3_600_000,
              "4H": 14_400_000, "1D": 86_400_000}
_METADATA_TTL = 60.0
_METADATA = {}
_METADATA_NEXT = {}
_METADATA_INFLIGHT = set()
_METADATA_LOCK = threading.Lock()
_STATUS_LABELS = {
    "normal": "正常交易", "listed": "已上架", "maintain": "暂停交易/维护",
    "limit_open": "限制开仓", "restrictedAPI": "API下单受限", "off": "已下架",
}


def _symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    if symbol not in PERP_SYMBOLS:
        raise ValueError(f"不支持的永续代码: {symbol}")
    return symbol


def _read_market(path: str, params: dict) -> dict:
    request = Request(_REST + path + "?" + urlencode(params), headers={
        "User-Agent": "MarketPeek/1.0.1", "Accept": "application/json",
    })
    with urlopen(request, timeout=6) as response:
        payload = json.load(response)
    if not isinstance(payload, dict) or payload.get("code") != "00000":
        raise ValueError(f"Bitget 永续接口错误: {payload}")
    if not isinstance(payload.get("data"), list) or not payload["data"]:
        raise ValueError("Bitget 永续行情为空")
    return payload


def _refresh_contract_metadata(symbol: str) -> None:
    """Refresh in a worker; failed reads retain the last verified status."""
    try:
        payload = _read_market("/api/v2/mix/market/contracts", {
            "symbol": symbol + "USDT", "productType": "USDT-FUTURES",
        })
        item = next((entry for entry in payload["data"]
                     if isinstance(entry, dict) and entry.get("symbol") == symbol + "USDT"), None)
        if (not item or item.get("symbolType") != "perpetual"
                or item.get("quoteCoin") != "USDT" or not item.get("symbolStatus")):
            raise ValueError("Bitget 永续合约状态、类型或计价币种无效")
        status = str(item["symbolStatus"]).strip()
        if not status:
            raise ValueError("Bitget 永续合约状态为空")
        state = {
            "market_status": status,
            "market_status_label": _STATUS_LABELS.get(status, "合约状态: " + status),
            "metadata_timestamp_ms": int(payload.get("requestTime") or time.time() * 1000),
            "contract_metadata": dict(item),
            "verified_at": time.monotonic(), "metadata_error": "",
        }
        with _METADATA_LOCK:
            _METADATA[symbol] = state
    except Exception as exc:
        with _METADATA_LOCK:
            state = dict(_METADATA.get(symbol, {}))
            state["metadata_error"] = "{}: {}".format(type(exc).__name__, exc)
            _METADATA[symbol] = state
    finally:
        with _METADATA_LOCK:
            # Also rate-limit failed attempts, including slow connections.
            _METADATA_NEXT[symbol] = time.monotonic() + _METADATA_TTL
            _METADATA_INFLIGHT.discard(symbol)


def _contract_status(symbol: str, wait: bool = False) -> dict:
    """REST workers may wait; WS callbacks only read cache/start a worker."""
    refresh = False
    with _METADATA_LOCK:
        now = time.monotonic()
        if now >= _METADATA_NEXT.get(symbol, 0) and symbol not in _METADATA_INFLIGHT:
            _METADATA_INFLIGHT.add(symbol)
            refresh = True
    if refresh:
        if wait:
            _refresh_contract_metadata(symbol)
        else:
            threading.Thread(target=_refresh_contract_metadata, args=(symbol,),
                             name="MSTR-contract-status", daemon=True).start()
    with _METADATA_LOCK:
        state = dict(_METADATA.get(symbol, {}))
    verified_at = state.pop("verified_at", None)
    stale = (verified_at is None or time.monotonic() - verified_at >= _METADATA_TTL
             or bool(state.get("metadata_error")))
    state.setdefault("market_status", "unknown")
    state.setdefault("market_status_label", "状态待确认")
    state.setdefault("metadata_timestamp_ms", None)
    state.setdefault("metadata_error", "")
    state["metadata_stale"] = stale
    state["contract_metadata"] = dict(state.get("contract_metadata", {}))
    return state


def _quote_with_status(symbol: str, quote: dict, wait: bool = False) -> dict:
    quote.update(_contract_status(symbol, wait=wait))
    return quote


def _parse_quote(item: dict, timestamp_ms: int | None = None) -> dict | None:
    try:
        price = float(item["lastPr"])
        change = float(item["change24h"]) * 100.0
        stamp = int(item.get("ts") or timestamp_ms or 0)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(price) or price <= 0 or not math.isfinite(change) or stamp <= 0:
        return None
    return {
        "price": price, "change": change, "unit": "USDT",
        "source": PERP_SOURCE, "timestamp": stamp // 1000,
        "timestamp_ms": stamp, "change_label": "24h", "market": "perpetual",
    }


def _parse_candle(item: list, period: str, now_ms: int) -> dict | None:
    if period not in _PERIOD_MS or not isinstance(item, (list, tuple)) or len(item) < 6:
        return None
    try:
        stamp = int(item[0])
        opening, high, low, close, volume = (float(value) for value in item[1:6])
    except (TypeError, ValueError, OverflowError):
        return None
    if (stamp <= 0 or not all(math.isfinite(value) for value in
                             (opening, high, low, close, volume))
            or min(opening, high, low, close) <= 0 or volume < 0
            or high < max(opening, close, low) or low > min(opening, close, high)):
        return None
    return {
        "ts": stamp, "open": opening, "high": high, "low": low,
        "close": close, "volume": volume,
        "closed": stamp + _PERIOD_MS[period] <= now_ms,
    }


def fetch_perp_quotes(symbols: list[str]) -> dict:
    quotes, errors = {}, {}
    for value in dict.fromkeys(symbols):
        try:
            symbol = _symbol(value)
            payload = _read_market("/api/v2/mix/market/ticker", {
                "symbol": symbol + "USDT", "productType": "USDT-FUTURES",
            })
            item = next((entry for entry in payload["data"]
                         if entry.get("symbol") == symbol + "USDT"), None)
            quote = _parse_quote(item or {}, payload.get("requestTime"))
            if quote is None:
                raise ValueError("Bitget 永续代码、价格或数据时间无效")
            quotes[symbol] = _quote_with_status(symbol, quote, wait=True)
        except Exception as exc:
            errors[str(value)] = f"{type(exc).__name__}: {exc}"
    return {"quotes": quotes, "errors": errors}


def fetch_perp_candles(symbol: str, period: str) -> dict:
    symbol = _symbol(symbol)
    if period not in _PERIOD_MS:
        raise ValueError("Bitget 永续 K线最小周期为 1 分钟")
    payload = _read_market("/api/v2/mix/market/candles", {
        "symbol": symbol + "USDT", "productType": "USDT-FUTURES",
        "granularity": period, "limit": 120,
    })
    now_ms = int(payload.get("requestTime") or time.time() * 1000)
    bars = {}
    for item in payload["data"]:
        bar = _parse_candle(item, period, now_ms)
        if bar:
            bars[bar["ts"]] = bar
    if not bars:
        raise ValueError("Bitget 永续 K线无效")
    return {"bars": sorted(bars.values(), key=lambda bar: bar["ts"]),
            "source": PERP_SOURCE, "unit": "USDT"}


class PerpStream:
    """One symbol, optional candles, automatic heartbeat and reconnection.

    Each instance is single use. stop() prevents further callbacks, including
    those from an old socket while a different chart is being subscribed.
    """

    def __init__(self, symbol: str, period: str | None, emit, proxy_options: dict | None = None):
        self.symbol = _symbol(symbol)
        if period is not None and period not in _PERIOD_MS:
            raise ValueError("Bitget 永续 K线最小周期为 1 分钟")
        self.period = period
        self._callback = emit
        self._proxy_options = dict(proxy_options or {})
        self._stop = threading.Event()
        self._emit_lock = threading.RLock()
        self._app_lock = threading.Lock()
        self._app = None
        self._thread = None

    def start(self) -> None:
        with self._app_lock:
            if self._thread is not None or self._stop.is_set():
                return
            self._thread = threading.Thread(target=self._run, name="MSTR-perpetual", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._emit_lock:
            self._stop.set()
        with self._app_lock:
            app = self._app
        if app is not None:
            app.close(timeout=0)

    def _emit(self, kind: str, payload) -> None:
        with self._emit_lock:
            if not self._stop.is_set():
                self._callback(kind, payload)

    def _on_message(self, _app, message: str) -> None:
        if message == "pong" or self._stop.is_set():
            return
        try:
            payload = json.loads(message)
        except (TypeError, ValueError):
            return
        if not isinstance(payload, dict):
            return
        if payload.get("event") == "error":
            _app.close(timeout=0)
            return
        arg = payload.get("arg", {})
        if (not isinstance(arg, dict) or arg.get("instType") != "USDT-FUTURES"
                or arg.get("instId") != self.symbol + "USDT"):
            return
        data = payload.get("data")
        if not isinstance(data, list):
            return
        if arg.get("channel") == "ticker":
            for item in data:
                if not isinstance(item, dict):
                    continue
                expected = self.symbol + "USDT"
                if any(item.get(field) not in (None, "", expected) for field in ("symbol", "instId")):
                    continue
                quote = _parse_quote(item, payload.get("ts"))
                if quote:
                    _quote_with_status(self.symbol, quote)
                    self._emit("perp_quote", (self.symbol, quote, quote["timestamp_ms"]))
        elif self.period and arg.get("channel") == "candle" + self.period:
            try:
                now_ms = int(payload.get("ts") or time.time() * 1000)
            except (TypeError, ValueError, OverflowError):
                return
            for item in data:
                bar = _parse_candle(item, self.period, now_ms)
                if bar:
                    self._emit("perp_candle", (self.symbol, self.period, bar))

    def _run(self) -> None:
        retry = 1.0
        while not self._stop.is_set():
            disconnected = threading.Event()
            last_received = [time.monotonic()]

            def heartbeat(app, done=disconnected, received=last_received):
                while not done.wait(20):
                    if self._stop.is_set():
                        break
                    try:
                        if time.monotonic() - received[0] > 45:
                            app.close(timeout=0)
                            break
                        app.send("ping")
                    except Exception:
                        app.close(timeout=0)
                        break

            def on_open(app):
                nonlocal retry
                if self._stop.is_set():
                    app.close(timeout=0)
                    return
                retry = 1.0
                last_received[0] = time.monotonic()
                args = [{"instType": "USDT-FUTURES", "channel": "ticker",
                         "instId": self.symbol + "USDT"}]
                if self.period:
                    args.append({"instType": "USDT-FUTURES", "channel": "candle" + self.period,
                                 "instId": self.symbol + "USDT"})
                app.send(json.dumps({"op": "subscribe", "args": args}))
                threading.Thread(target=heartbeat, args=(app,), daemon=True).start()

            def on_message(app, message):
                last_received[0] = time.monotonic()
                self._on_message(app, message)

            app = websocket.WebSocketApp(_WS, on_open=on_open, on_message=on_message)
            with self._app_lock:
                if self._stop.is_set():
                    return
                self._app = app
            try:
                app.run_forever(ping_interval=0, **self._proxy_options)
            except Exception:
                pass
            finally:
                disconnected.set()
                app.close(timeout=0)
                with self._app_lock:
                    if self._app is app:
                        self._app = None
            if self._stop.wait(retry):
                break
            retry = min(retry * 2.0, 15.0)
