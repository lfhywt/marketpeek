"""Public reference quotes and candles for MSTR / STRC (Yahoo chart v8).

Yahoo does not promise a feed delay in this response. All quotes are explicitly
marked as reference/delayed data, and retain the provider's data timestamp.
query1/query2 are alternate endpoints of the same provider, not independent feeds.
"""
from __future__ import annotations

import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import threading
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


STOCK_SYMBOLS = {"MSTR", "STRC"}
_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
_CACHE: dict[tuple, tuple[float, dict]] = {}
_LOCKS: dict[tuple, threading.Lock] = {}
_STATE_LOCK = threading.Lock()
_PREFERRED_HOST = _HOSTS[0]
_PERIODS = {
    "1m": ("1m", "1d", 60, 4.5),
    "15m": ("15m", "5d", 900, 15.0),
    "1H": ("1h", "1mo", 3600, 30.0),
    "4H": ("1h", "3mo", 14400, 30.0),
    "1D": ("1d", "1y", 86400, 60.0),
}


def _positive(value) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    if symbol not in STOCK_SYMBOLS:
        raise ValueError(f"不支持的美股代码: {symbol}")
    return symbol


def _read_chart(symbol: str, interval: str, span: str, ttl: float) -> dict:
    global _PREFERRED_HOST
    key = (symbol, interval, span)
    with _STATE_LOCK:
        lock = _LOCKS.setdefault(key, threading.Lock())
    with lock:
        cached = _CACHE.get(key)
        if cached and time.monotonic() - cached[0] < ttl:
            return cached[1]
        preferred = _PREFERRED_HOST
        hosts = (preferred,) + tuple(host for host in _HOSTS if host != preferred)
        errors = []
        for host in hosts:
            url = f"https://{host}/v8/finance/chart/{symbol}?" + urlencode({
                "interval": interval, "range": span, "includePrePost": "true",
            })
            try:
                request = Request(url, headers={
                    "User-Agent": "MarketPeek/1.0.1", "Accept": "application/json",
                })
                with urlopen(request, timeout=6) as response:
                    payload = json.load(response)
                chart = payload.get("chart", {})
                results = chart.get("result")
                if chart.get("error") or not results:
                    raise ValueError(str(chart.get("error") or "行情为空"))
                result = results[0]
                meta = result.get("meta", {})
                if meta.get("symbol", "").upper() != symbol or meta.get("currency") != "USD":
                    raise ValueError("代码或计价币种不匹配")
                if not isinstance(result.get("timestamp"), list):
                    raise ValueError("行情缺少数据时间")
                _CACHE[key] = (time.monotonic(), result)
                _PREFERRED_HOST = host
                return result
            except Exception as exc:
                errors.append(f"{host}: {type(exc).__name__}: {exc}")
        raise RuntimeError("; ".join(errors))


def _session_periods(meta: dict) -> list[tuple[int, int, str]]:
    """Historical session windows include holidays, early closes, and UTC DST offsets."""
    periods = set()

    def visit(value, kind):
        if isinstance(value, list):
            for item in value:
                visit(item, kind)
        elif isinstance(value, dict) and "start" in value and "end" in value:
            try:
                start, end = int(value["start"]), int(value["end"])
                if start < end:
                    periods.add((start, end, kind))
            except (TypeError, ValueError):
                pass

    for container in (meta.get("tradingPeriods", {}), meta.get("currentTradingPeriod", {})):
        if isinstance(container, dict):
            for kind in ("pre", "regular", "post"):
                visit(container.get(kind), kind)
        elif isinstance(container, list):
            visit(container, "regular")
    return sorted(periods)


def _session_at(stamp: int | float, periods: list[tuple[int, int, str]]):
    return next((item for item in periods if item[0] <= stamp < item[1]), None)


def _ny_offset(stamp: int | float) -> int:
    """US DST rules since 2007; avoids requiring tzdata on packaged Windows."""
    year = datetime.fromtimestamp(stamp, timezone.utc).year
    march_first = calendar.monthrange(year, 3)[0]
    november_first = calendar.monthrange(year, 11)[0]
    march_sunday = 1 + (6 - march_first) % 7 + 7
    november_sunday = 1 + (6 - november_first) % 7
    start = datetime(year, 3, march_sunday, 7, tzinfo=timezone.utc).timestamp()
    end = datetime(year, 11, november_sunday, 6, tzinfo=timezone.utc).timestamp()
    return -14400 if start <= stamp < end else -18000


def _market_day(stamp: int | float):
    return datetime.fromtimestamp(stamp + _ny_offset(stamp), timezone.utc).date()


def _candle_session(stamp: int, periods: list[tuple[int, int, str]]):
    session = _session_at(stamp, periods)
    if session is not None:
        return session
    # Historical responses can omit tradingPeriods. Keep the original 09:30
    # regular-market alignment before normalizing 1H candles or aggregating 4H.
    midnight = calendar.timegm(_market_day(stamp).timetuple()) - _ny_offset(stamp)
    fallback = (
        (midnight + 4 * 3600, midnight + 9 * 3600 + 1800, "pre"),
        (midnight + 9 * 3600 + 1800, midnight + 16 * 3600, "regular"),
        (midnight + 16 * 3600, midnight + 20 * 3600, "post"),
    )
    return _session_at(stamp, fallback)


def _session_label(meta: dict, stamp: int, now: float, regular_quote: bool = False) -> str:
    periods = _session_periods(meta)
    current = _session_at(now, periods)
    if current is None:
        return "休市"
    label = {"pre": "盘前", "regular": "盘中", "post": "盘后"}[current[2]]
    # Old Friday data must not be presented as a fresh Monday premarket trade.
    if stamp < current[0] - 1 or (regular_quote and current[2] != "regular"):
        return f"{label}·待更新"
    return label


def _latest_quote(chart: dict) -> dict:
    meta = chart.get("meta", {})
    stamps = chart.get("timestamp", [])
    quote = chart.get("indicators", {}).get("quote", [{}])[0]
    closes = quote.get("close", [])
    latest = None
    for index in range(len(stamps) - 1, -1, -1):
        price = _positive(closes[index]) if index < len(closes) else None
        if price is not None:
            latest = (int(stamps[index]), price)
            break
    regular_price = _positive(meta.get("regularMarketPrice"))
    regular_stamp = int(meta.get("regularMarketTime") or 0)
    regular_quote = False
    if regular_price is not None and regular_stamp and (latest is None or regular_stamp >= latest[0]):
        latest = (regular_stamp, regular_price)
        regular_quote = True
    if latest is None:
        raise ValueError("美股没有有效报价")
    stamp, price = latest
    # chartPreviousClose means the start of the requested RANGE, not always yesterday.
    # Quotes use range=1d; previousClose is preferred when supplied by Yahoo.
    previous = _positive(meta.get("previousClose")) or _positive(meta.get("chartPreviousClose"))
    if previous is None:
        raise ValueError("美股缺少前收盘价")
    return {"price": price, "change": (price / previous - 1.0) * 100.0,
            "unit": "USD", "source": "Yahoo", "timestamp": stamp,
            "session": _session_label(meta, stamp, time.time(), regular_quote), "delayed": True,
            "change_label": "日"}


def fetch_stock_quotes(symbols) -> dict:
    """Return partial success without discarding another symbol's valid quote."""
    requested = sorted({str(value).strip().upper() for value in symbols})
    quotes, errors = {}, {}

    def fetch(symbol):
        try:
            _symbol(symbol)
            return symbol, _latest_quote(_read_chart(symbol, "1m", "1d", 4.5)), None
        except Exception as exc:
            return symbol, None, str(exc)

    if requested:
        with ThreadPoolExecutor(max_workers=min(2, len(requested))) as pool:
            for symbol, quote, error in pool.map(fetch, requested):
                if error:
                    errors[symbol] = error
                else:
                    quotes[symbol] = quote
    return {"quotes": quotes, "errors": errors}


def _parse_bars(chart: dict, interval_seconds: int) -> list[dict]:
    meta = chart.get("meta", {})
    stamps = chart.get("timestamp", [])
    quotes = chart.get("indicators", {}).get("quote", [{}])[0]
    periods = _session_periods(meta)
    now = time.time()
    buckets = {}
    for index, raw_stamp in enumerate(stamps):
        values = {}
        for name in ("open", "high", "low", "close"):
            column = quotes.get(name, [])
            value = _positive(column[index]) if index < len(column) else None
            if value is None:
                break
            values[name] = value
        if len(values) != 4 or values["high"] < values["low"]:
            continue
        stamp = int(raw_stamp)
        session = _candle_session(stamp, periods)
        if interval_seconds >= 86400:
            # Yahoo daily candles already use the true exchange open as their stamp.
            bucket = stamp
            end = next((end for start, end, kind in periods
                        if kind == "regular" and _market_day(start) == _market_day(stamp)), None)
            if end is None:
                local_midnight = calendar.timegm(_market_day(stamp).timetuple())
                end = local_midnight + 16 * 3600 - _ny_offset(stamp)
        elif session:
            anchor, session_end, _ = session
            bucket = anchor + ((stamp - anchor) // interval_seconds) * interval_seconds
            end = min(bucket + interval_seconds, session_end)
        else:
            bucket = stamp // interval_seconds * interval_seconds
            end = bucket + interval_seconds
        column = quotes.get("volume", [])
        try:
            volume = max(0.0, float(column[index] or 0)) if index < len(column) else 0.0
            if not math.isfinite(volume):
                volume = 0.0
        except (ValueError, TypeError, OverflowError):
            volume = 0.0
        existing = buckets.get(bucket)
        if existing:
            # Yahoo appends a non-aligned last-trade snapshot. It updates the same
            # candle; its zero volume must not erase the actual minute's volume.
            existing["high"] = max(existing["high"], values["high"])
            existing["low"] = min(existing["low"], values["low"])
            existing["close"] = values["close"]
            existing["volume"] = max(existing["volume"], volume)
        else:
            buckets[bucket] = {"ts": bucket * 1000, **values, "volume": volume,
                               "closed": now >= end}
    return [buckets[key] for key in sorted(buckets)]


def _aggregate_4h(chart: dict) -> list[dict]:
    periods = _session_periods(chart.get("meta", {}))
    grouped = {}
    for bar in _parse_bars(chart, 3600):
        stamp = bar["ts"] // 1000
        session = _candle_session(stamp, periods)
        if session:
            anchor, session_end, kind = session
        else:
            anchor = calendar.timegm(_market_day(stamp).timetuple()) - _ny_offset(stamp)
            kind, session_end = "other", anchor + 86400
        bucket = anchor + ((stamp - anchor) // 14400) * 14400
        # Include session kind and exchange date so no candle spans sessions/days.
        key = (_market_day(stamp), kind, bucket)
        if key in grouped:
            current = grouped[key]
            current["high"] = max(current["high"], bar["high"])
            current["low"] = min(current["low"], bar["low"])
            current["close"] = bar["close"]
            current["volume"] += bar["volume"]
        else:
            grouped[key] = {**bar, "ts": bucket * 1000,
                            "closed": time.time() >= min(bucket + 14400, session_end)}
    return sorted(grouped.values(), key=lambda bar: bar["ts"])


def fetch_stock_candles(symbol: str, period: str) -> dict:
    symbol = _symbol(symbol)
    if period not in _PERIODS:
        raise ValueError("美股公开源最小为 1 分钟 K 线，不支持 1 秒")
    interval, span, seconds, ttl = _PERIODS[period]
    chart = _read_chart(symbol, interval, span, ttl)
    bars = _aggregate_4h(chart) if period == "4H" else _parse_bars(chart, seconds)
    if not bars:
        raise ValueError("美股没有有效 K 线")
    return {"bars": bars[-240:], "source": "Yahoo", "unit": "USD",
            "delayed": True, "regular_only": period == "1D"}
