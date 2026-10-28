"""A floating Windows crypto/equity market window with a tray and quick hide."""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from datetime import datetime
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time
import tkinter as tk
from tkinter import messagebox
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, getproxies, urlopen

from PIL import Image, ImageDraw
import pystray
import websocket
from market_stocks import fetch_stock_candles, fetch_stock_quotes
from market_perps import PerpStream, fetch_perp_candles, fetch_perp_quotes


APP_NAME = "MarketPeek"
APP_VERSION = "1.0.1"
APP_AUTHOR = "LIFANGHAO"
AUTHOR_CREDIT = f"作者:{APP_AUTHOR} V{APP_VERSION}"
APP_DIR = Path(os.environ.get("APPDATA", Path.home())) / APP_NAME
CONFIG_PATH = APP_DIR / "config.json"
WINDOW_WIDTH = 420
COMPACT_WINDOW_WIDTH = 336
ROW_HEIGHT = 55
MIN_BODY_ROWS = 3
LEFT_COLUMN_WIDTH = 81
RIGHT_COLUMN_WIDTH = 126
MAX_CHART_BARS = 120
MIN_VISIBLE_CANDLES = 12
MAX_VISIBLE_CANDLES = 80
DEFAULT_VISIBLE_CANDLES = 36
CHART_ZOOM_LEVELS = (12, 18, 24, 36, 48, 64, 80)
CHART_SMOOTH_MS = 140
CHART_FRAME_MS = 28
UI_QUEUE_MS = 40
REST_REFRESH_SECONDS = 1
MIN_MANUAL_REFRESH_SECONDS = 1
STOCK_REFRESH_SECONDS = 5
STOCK_CHART_REFRESH_SECONDS = 15
PERP_REST_REFRESH_SECONDS = 2
MAX_SELECTED_ASSETS = 8
# 币安公开数据专用域名: 国内可直连且无需 API key。
# 注意 api.binance.com / stream.binance.com 在国内不可达, 必须用 .vision 这组。
BINANCE_REST = "https://data-api.binance.vision"
BINANCE_WS = "wss://data-stream.binance.vision/ws"
# Gate.io 第二顺位: 国内同样可直连, 但没有 1 秒 K 线
GATE_REST = "https://api.gateio.ws/api/v4"
GATE_WS = "wss://api.gateio.ws/ws/v4/"
# OKX 第三顺位: 国内直连不稳, 开着代理/VPN 时可用; 它的周期写法与程序内部一致
OKX_REST = "https://www.okx.com"
CHART_PERIODS = (
    ("1秒", "1s"),
    ("1分", "1m"),
    ("15分", "15m"),
    ("1时", "1H"),
    ("4时", "4H"),
    ("1天", "1D"),
)
CHART_PERIOD_VALUES = {value for _, value in CHART_PERIODS}
CHART_PERIOD_MS = {"1s": 1_000, "1m": 60_000, "15m": 900_000, "1H": 3_600_000, "4H": 14_400_000, "1D": 86_400_000}
# 程序内部的周期写法 -> 各数据源的 interval 写法
BINANCE_INTERVALS = {"1s": "1s", "1m": "1m", "15m": "15m", "1H": "1h", "4H": "4h", "1D": "1d"}
GATE_INTERVALS = {"1m": "1m", "15m": "15m", "1H": "1h", "4H": "4h", "1D": "1d"}
OKX_INTERVALS = {"1s": "1s", "1m": "1m", "15m": "15m", "1H": "1H", "4H": "4H", "1D": "1D"}

COINS = {
    "BTC": ("bitcoin", "比特币"),
    "ETH": ("ethereum", "以太坊"),
    "SOL": ("solana", "Solana"),
    "XRP": ("ripple", "瑞波币"),
    "BNB": ("binancecoin", "BNB"),
    "DOGE": ("dogecoin", "狗狗币"),
    "ADA": ("cardano", "Cardano"),
    "AVAX": ("avalanche-2", "Avalanche"),
    "LINK": ("chainlink", "Chainlink"),
    "LTC": ("litecoin", "莱特币"),
    "DOT": ("polkadot", "Polkadot"),
    "TRX": ("tron", "波场"),
}
STOCKS = {
    "STRC": ("Stretch", "优先股"),
}
PERPS = {"MSTR": ("MSTRUSDT", "永续合约")}
ASSETS = {**COINS, **PERPS, **STOCKS}


def initial_source(symbol: str) -> str:
    return "Bitget永续" if symbol in PERPS else "Yahoo" if symbol in STOCKS else "币安"

BG = "#0C1219"
HEADER = "#121D27"
ROW_BG = "#111B24"
FOOTER = "#101923"
BORDER = "#263541"
WHITE = "#EAF2F7"
MUTED = "#8B9BA8"
GREEN = "#3FD5A1"
RED = "#FF7A83"
AMBER = "#F2BE68"
PRICE_LINE = "#A7C4D0"


def _read_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": f"MarketPeek/{APP_VERSION}", "Accept": "application/json"})
    with urlopen(request, timeout=8) as response:
        return json.load(response)


def _websocket_proxy_options() -> dict:
    proxies = getproxies()
    for key in ("https", "http"):
        proxy = urlsplit(proxies.get(key, ""))
        if proxy.hostname and proxy.scheme == "http":
            return {"http_proxy_host": proxy.hostname, "http_proxy_port": proxy.port or 80, "proxy_type": "http"}
    return {}


def _parse_binance_candle(item: list) -> dict | None:
    """币安K线: [openTime, 开, 高, 低, 收, 成交量, closeTime, ...]，没有收线标志列。"""
    if not isinstance(item, list) or len(item) < 7:
        return None
    try:
        stamp = int(item[0])
        opening, high, low, close = (float(item[index]) for index in range(1, 5))
        volume = max(0.0, float(item[5]))
        close_time = int(item[6])
    except (TypeError, ValueError):
        return None
    if min(opening, high, low, close) <= 0 or high < low:
        return None
    # 币安不给 confirm 列，用收盘时刻反推：还没到收盘时间的就是当前未收线的那根
    return {
        "ts": stamp,
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "closed": close_time <= int(time.time() * 1000),
    }


def _parse_gate_candle(item: list) -> dict | None:
    """Gate.ioK线列顺序特殊: [时间戳(秒), 计价量, 收, 高, 低, 开, 基础量, 是否收线]。"""
    if not isinstance(item, list) or len(item) < 8:
        return None
    try:
        stamp = int(item[0]) * 1000
        close = float(item[2])
        high = float(item[3])
        low = float(item[4])
        opening = float(item[5])
        volume = max(0.0, float(item[6]))
    except (TypeError, ValueError):
        return None
    if min(opening, high, low, close) <= 0 or high < low:
        return None
    return {
        "ts": stamp,
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "closed": str(item[7]).strip().lower() == "true",
    }


def _parse_okx_candle(item: list) -> dict | None:
    """OKXK线: [时间戳, 开, 高, 低, 收, 成交量, 成交额, 成交额币种, 是否收线]。"""
    if not isinstance(item, list) or len(item) < 9:
        return None
    try:
        stamp = int(item[0])
        opening, high, low, close = (float(item[index]) for index in range(1, 5))
        volume = max(0.0, float(item[5]))
    except (TypeError, ValueError):
        return None
    if min(opening, high, low, close) <= 0 or high < low:
        return None
    return {
        "ts": stamp,
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "closed": str(item[8]) == "1",
    }


def _parse_binance_ticker(payload: dict) -> tuple[str, dict, int] | None:
    """币安 24hrTicker 推送 -> (币种, 报价, 毫秒时间戳)。"""
    if payload.get("e") != "24hrTicker":
        return None
    pair = str(payload.get("s", "")).upper()
    if not pair.endswith("USDT"):
        return None
    price = float(payload["c"])
    if price <= 0:
        return None
    stamp = int(payload.get("E") or time.time() * 1000)
    return pair[: -len("USDT")], {"price": price, "change": float(payload.get("P", 0.0))}, stamp


def _parse_gate_ticker(payload: dict) -> tuple[str, dict, int] | None:
    """Gate.io spot.tickers 推送 -> (币种, 报价, 毫秒时间戳)。"""
    if payload.get("channel") != "spot.tickers" or payload.get("event") != "update":
        return None
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    pair = str(result.get("currency_pair", ""))
    if not pair.endswith("_USDT"):
        return None
    price = float(result["last"])
    if price <= 0:
        return None
    stamp = int(payload.get("time_ms") or time.time() * 1000)
    return pair[: -len("_USDT")], {"price": price, "change": float(result.get("change_percentage", 0.0))}, stamp


def _parse_binance_kline(payload: dict) -> dict | None:
    """币安 kline 推送 -> bar。"""
    if payload.get("e") != "kline":
        return None
    candle = payload.get("k")
    if not isinstance(candle, dict):
        return None
    bar = _parse_binance_candle([
        candle.get("t"), candle.get("o"), candle.get("h"), candle.get("l"),
        candle.get("c"), candle.get("v"), candle.get("T"),
    ])
    if bar:
        # WS 的 x 字段是权威的收线标志，比用收盘时刻推算准
        bar["closed"] = bool(candle.get("x", bar["closed"]))
    return bar


def _parse_gate_kline(payload: dict) -> dict | None:
    """Gate.io spot.candlesticks 推送 -> bar。量能取 a(基础量)，不是 v(计价量)。"""
    if payload.get("channel") != "spot.candlesticks" or payload.get("event") != "update":
        return None
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    try:
        stamp = int(result["t"]) * 1000
        opening = float(result["o"])
        high = float(result["h"])
        low = float(result["l"])
        close = float(result["c"])
        volume = max(0.0, float(result.get("a", 0.0)))
    except (KeyError, TypeError, ValueError):
        return None
    if min(opening, high, low, close) <= 0 or high < low:
        return None
    return {
        "ts": stamp,
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "closed": bool(result.get("w", False)),
    }


def _newer_candle(previous: dict | None, incoming: dict) -> dict:
    if previous is None:
        return incoming
    if previous["closed"] and not incoming["closed"]:
        return previous
    if incoming["closed"] and not previous["closed"]:
        return incoming
    if incoming.get("volume", 0.0) < previous.get("volume", 0.0):
        return previous
    return incoming


def fetch_binance(symbols: list[str]) -> dict:
    # 这个端点可以一次拿多个币种，不用像以前那样逐个请求。
    # 币安要求 symbols 是 JSON 数组，元素必须带引号
    pairs = ",".join(f'"{symbol}USDT"' for symbol in symbols)
    query = urlencode({"symbols": f"[{pairs}]"}, safe=",")
    payload = _read_json(f"{BINANCE_REST}/api/v3/ticker/24hr?" + query)
    if not isinstance(payload, list):
        payload = [payload]
    quotes = {}
    timestamps = []
    for item in payload:
        pair = str(item.get("symbol", ""))
        if not pair.endswith("USDT"):
            continue
        symbol = pair[: -len("USDT")]
        if symbol not in symbols:
            continue
        price = float(item["lastPrice"])
        if price <= 0:
            raise ValueError(f"币安返回了无效的 {symbol} 价格")
        quotes[symbol] = {"price": price, "change": float(item["priceChangePercent"])}
        timestamps.append(int(item.get("closeTime", 0)) // 1000)
    missing = [symbol for symbol in symbols if symbol not in quotes]
    if missing:
        raise ValueError(f"币安没有 {'/'.join(missing)} 的行情")
    return {"quotes": quotes, "source": "币安", "unit": "USDT", "timestamp": min(timestamps)}


def fetch_gate(symbols: list[str]) -> dict:
    pairs = ",".join(f"{symbol}_USDT" for symbol in symbols)
    query = urlencode({"currency_pairs": pairs, "timezone": "utc0"})
    payload = _read_json(f"{GATE_REST}/spot/tickers?" + query)
    if not isinstance(payload, list):
        payload = [payload]
    quotes = {}
    for item in payload:
        pair = str(item.get("currency_pair", ""))
        if not pair.endswith("_USDT"):
            continue
        symbol = pair[: -len("_USDT")]
        if symbol not in symbols:
            continue
        price = float(item["last"])
        if price <= 0:
            raise ValueError(f"Gate.io 返回了无效的 {symbol} 价格")
        quotes[symbol] = {"price": price, "change": float(item.get("change_percentage", 0.0))}
    missing = [symbol for symbol in symbols if symbol not in quotes]
    if missing:
        raise ValueError(f"Gate.io 没有 {'/'.join(missing)} 的行情")
    return {"quotes": quotes, "source": "Gate.io", "unit": "USDT", "timestamp": int(time.time())}


def fetch_binance_candles(symbol: str, period: str) -> list[dict]:
    query = urlencode({"symbol": f"{symbol}USDT", "interval": BINANCE_INTERVALS[period], "limit": MAX_CHART_BARS})
    payload = _read_json(f"{BINANCE_REST}/api/v3/klines?" + query)
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"币安没有 {symbol}USDT 的 {period} K线")
    bars = [bar for bar in (_parse_binance_candle(item) for item in payload) if bar]
    if not bars:
        raise ValueError("币安返回的 K线全都不合法")
    return sorted(bars, key=lambda bar: bar["ts"])


def fetch_gate_candles(symbol: str, period: str) -> list[dict]:
    interval = GATE_INTERVALS.get(period)
    if interval is None:
        raise ValueError(f"Gate.io 不支持 {period} K线")
    query = urlencode({"currency_pair": f"{symbol}_USDT", "interval": interval, "limit": MAX_CHART_BARS})
    payload = _read_json(f"{GATE_REST}/spot/candlesticks?" + query)
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Gate.io 没有 {symbol}_USDT 的 {period} K线")
    bars = [bar for bar in (_parse_gate_candle(item) for item in payload) if bar]
    if not bars:
        raise ValueError("Gate.io 返回的 K线全都不合法")
    return sorted(bars, key=lambda bar: bar["ts"])


def fetch_okx(symbols: list[str]) -> dict:
    # OKX 只能按单个 instId 查，所以这里并发请求
    from concurrent.futures import ThreadPoolExecutor

    def get_one(symbol: str) -> tuple[str, dict, int]:
        query = urlencode({"instId": f"{symbol}-USDT"})
        payload = _read_json(f"{OKX_REST}/api/v5/market/ticker?" + query)
        if payload.get("code") != "0" or not payload.get("data"):
            raise ValueError(f"OKX 没有 {symbol}-USDT 的行情")
        item = payload["data"][0]
        price = float(item["last"])
        opening = float(item["open24h"])
        if price <= 0 or opening <= 0:
            raise ValueError(f"OKX 返回了无效的 {symbol} 价格")
        return symbol, {"price": price, "change": (price / opening - 1) * 100}, int(item["ts"]) // 1000

    with ThreadPoolExecutor(max_workers=len(symbols)) as pool:
        results = list(pool.map(get_one, symbols))
    return {
        "quotes": {symbol: quote for symbol, quote, _ in results},
        "source": "OKX",
        "unit": "USDT",
        "timestamp": min(timestamp for _, _, timestamp in results),
    }


def fetch_okx_candles(symbol: str, period: str) -> list[dict]:
    query = urlencode({"instId": f"{symbol}-USDT", "bar": OKX_INTERVALS[period], "limit": MAX_CHART_BARS})
    payload = _read_json(f"{OKX_REST}/api/v5/market/candles?" + query)
    if payload.get("code") != "0" or not payload.get("data"):
        raise ValueError(f"OKX 没有 {symbol}-USDT 的 {period} K线")
    bars = [bar for bar in (_parse_okx_candle(item) for item in payload["data"]) if bar]
    if not bars:
        raise ValueError("OKX 返回的 K线全都不合法")
    return sorted(bars, key=lambda bar: bar["ts"])


PRICE_SOURCES = (("币安", fetch_binance), ("Gate.io", fetch_gate), ("OKX", fetch_okx))
CANDLE_SOURCES = (
    ("币安", fetch_binance_candles, BINANCE_INTERVALS),
    ("Gate.io", fetch_gate_candles, GATE_INTERVALS),
    ("OKX", fetch_okx_candles, OKX_INTERVALS),
)


def fetch_prices(symbols: list[str]) -> dict:
    """按 币安 -> Gate.io -> OKX 顺序取行情，前者失败自动换下一个。"""
    failures = []
    for name, fetcher in PRICE_SOURCES:
        try:
            result = fetcher(symbols)
        except Exception as error:
            failures.append(f"{name}: {error}")
            continue
        if failures:
            result["fallback_reason"] = "; ".join(failures)
        return result
    raise RuntimeError(" | ".join(failures))


def fetch_candles(symbol: str, period: str) -> dict:
    """返回 {"bars": [...], "source": 源名}。顺序同上，不支持该周期的源直接跳过。"""
    if symbol in PERPS:
        return fetch_perp_candles(symbol, period)
    if symbol in STOCKS:
        return fetch_stock_candles(symbol, period)
    if symbol not in COINS or period not in CHART_PERIOD_VALUES:
        raise ValueError("Unsupported chart symbol or period")
    failures = []
    for name, fetcher, intervals in CANDLE_SOURCES:
        if period not in intervals:
            failures.append(f"{name} 不支持 {period}")
            continue
        try:
            return {"bars": fetcher(symbol, period), "source": name}
        except Exception as error:
            failures.append(f"{name}: {error}")
    raise RuntimeError(" | ".join(failures))


def price_text(price: float, unit: str) -> str:
    if price >= 1000:
        number = f"{price:,.2f}"
    elif price >= 1:
        number = f"{price:,.3f}".rstrip("0").rstrip(".")
    elif price >= 0.01:
        number = f"{price:.4f}".rstrip("0").rstrip(".")
    else:
        number = f"{price:.8f}".rstrip("0").rstrip(".")
    return ("$" if unit == "USD" else "₮") + number


def load_config() -> dict:
    defaults = {"symbols": ["BTC", "ETH", "SOL", "MSTR", "STRC"], "opacity": 0.96, "position": None, "chart_symbol": "BTC", "chart_period": "1m", "chart_visible": True, "volume_visible": True, "chart_zoom": DEFAULT_VISIBLE_CANDLES, "stock_watchlist_added": True, "mstr_perp_added": True}
    try:
        saved = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        selected = saved.get("symbols", [])
        if isinstance(selected, list):
            selected = [symbol for symbol in selected if symbol in ASSETS]
            if 1 <= len(selected) <= MAX_SELECTED_ASSETS and len(set(selected)) == len(selected):
                defaults["symbols"] = selected
        if not saved.get("stock_watchlist_added", False):
            for symbol in ("MSTR", "STRC"):
                if symbol not in defaults["symbols"] and len(defaults["symbols"]) < MAX_SELECTED_ASSETS:
                    defaults["symbols"].append(symbol)
        if not saved.get("mstr_perp_added", False) and "MSTR" not in defaults["symbols"] and len(defaults["symbols"]) < MAX_SELECTED_ASSETS:
            defaults["symbols"].append("MSTR")
        opacity = float(saved.get("opacity", defaults["opacity"]))
        defaults["opacity"] = max(0.70, min(1.0, opacity))
        position = saved.get("position")
        if isinstance(position, list) and len(position) == 2 and all(isinstance(n, int) for n in position):
            defaults["position"] = position
        if saved.get("chart_symbol") in defaults["symbols"]:
            defaults["chart_symbol"] = saved["chart_symbol"]
        else:
            defaults["chart_symbol"] = defaults["symbols"][0]
        if saved.get("chart_period") in CHART_PERIOD_VALUES:
            defaults["chart_period"] = saved["chart_period"]
        if isinstance(saved.get("chart_visible"), bool):
            defaults["chart_visible"] = saved["chart_visible"]
        if isinstance(saved.get("volume_visible"), bool):
            defaults["volume_visible"] = saved["volume_visible"]
        zoom = saved.get("chart_zoom")
        if isinstance(zoom, int) and not isinstance(zoom, bool):
            defaults["chart_zoom"] = max(MIN_VISIBLE_CANDLES, min(MAX_VISIBLE_CANDLES, zoom))
    except (OSError, ValueError, TypeError):
        pass
    if defaults["chart_symbol"] not in COINS and defaults["chart_period"] == "1s":
        defaults["chart_period"] = "1m"
    return defaults


def make_tray_image() -> Image.Image:
    image = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((3, 3, 60, 60), radius=15, fill=HEADER, outline=BORDER, width=2)
    draw.line([(12, 45), (23, 37), (33, 41), (44, 22), (53, 27)], fill=GREEN, width=5, joint="curve")
    draw.ellipse((48, 22, 56, 30), fill=GREEN)
    return image


class MarketPeek:
    def __init__(self) -> None:
        self.config = load_config()
        self.events: queue.Queue[tuple[str, object]] = queue.Queue()
        self.latest: dict | None = None
        self.quote_cache: dict[str, dict] = {}
        self.quote_generation = 0
        self.stock_fetching = False
        self.stock_last_fetch_start = 0.0
        self.stock_errors: dict[str, str] = {}
        self.perp_fetching = False
        self.perp_last_fetch_start = 0.0
        self.last_perp_ws_update = 0.0
        self.perp_errors: dict[str, str] = {}
        self.perp_stream: PerpStream | None = None
        self.last_fetch_start = 0.0
        self.chart_source = initial_source(self.config["chart_symbol"])
        self.last_ws_update = 0.0
        self.fetching = False
        self.visible = True
        self.closing = False
        self.drag_offset: tuple[int, int] | None = None
        self.settings_window: tk.Toplevel | None = None
        self.hotkey_thread_id: int | None = None
        self.hotkey_enabled = False
        self.ws_stop = threading.Event()
        self.ws_app: websocket.WebSocketApp | None = None
        self.rows: dict[str, tuple[tk.Label, tk.Label]] = {}
        self.symbol_labels: dict[str, tk.Label] = {}
        self.chart_bars: list[dict] = []
        self.chart_preview: dict | None = None
        self.chart_display_ts: int | None = None
        self.chart_display_close: float | None = None
        self.chart_motion_job: str | None = None
        self.chart_fetching = False
        self.chart_request_id = 0
        self.chart_request_preview: dict | None = None
        self.chart_error = ""
        self.chart_last_ws_update = 0.0
        self.chart_last_rest = 0.0
        self.chart_last_request = 0.0
        self.chart_ws_stop = threading.Event()
        self.chart_ws_app: websocket.WebSocketApp | None = None

        self.root = tk.Tk()
        self.root.title(f"行情速览 V{APP_VERSION}")
        self.root.configure(bg=BORDER)
        self.root.overrideredirect("--verify-ui" not in sys.argv)
        self.root.attributes("-topmost", True)
        self.root.attributes("-alpha", self.config["opacity"])
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.bind("<Escape>", lambda _event: self.hide())
        self.root.report_callback_exception = self._log_ui_error

        self._build_window()
        self._set_geometry()
        self._start_tray()
        self._start_hotkey()
        self._start_websocket()
        self._start_chart_websocket()
        self.root.after(UI_QUEUE_MS, self._drain_events)
        self.root.after(250, self.refresh)
        self.root.after(300, self._stock_tick)
        self.root.after(300, self._perp_tick)
        self.root.after(400, self._chart_tick)

    def _window_height(self) -> int:
        return 45 + ROW_HEIGHT * self._body_rows() + 50 + 2

    def _body_rows(self) -> int:
        minimum = MIN_BODY_ROWS + 1 if self.config["chart_visible"] else MIN_BODY_ROWS
        return max(minimum, len(self.config["symbols"]))

    def _window_width(self) -> int:
        return WINDOW_WIDTH if self.config["chart_visible"] else COMPACT_WINDOW_WIDTH

    def _window_geometry(self) -> tuple[int, int, int, int]:
        height = self._window_height()
        width = self._window_width()
        position = self.config["position"]
        user32 = ctypes.windll.user32
        left = user32.GetSystemMetrics(76)
        top = user32.GetSystemMetrics(77)
        right = left + user32.GetSystemMetrics(78)
        bottom = top + user32.GetSystemMetrics(79)
        if position is None or not (
            left - width + 80 <= position[0] <= right - 80
            and top - height + 45 <= position[1] <= bottom - 45
        ):
            x = user32.GetSystemMetrics(0) - width - 26
            y = 88
        else:
            x, y = position
            x = min(max(x, left), max(left, right - width))
            y = min(max(y, top), max(top, bottom - height))
        return x, y, width, height

    def _set_geometry(self) -> None:
        self._apply_geometry(self._window_geometry())

    def _apply_geometry(self, rect: tuple[int, int, int, int]) -> None:
        x, y, width, height = rect
        self.root.geometry(f"{width}x{height}{x:+d}{y:+d}")

    def _build_window(self) -> None:
        self.shell = tk.Frame(self.root, bg=BG)
        self.shell.pack(fill="both", expand=True, padx=1, pady=1)

        header = tk.Frame(self.shell, bg=HEADER, height=45)
        header.pack(fill="x")
        header.pack_propagate(False)
        dot = tk.Canvas(header, bg=HEADER, highlightthickness=0, width=12, height=12)
        dot.create_oval(2, 2, 10, 10, fill=GREEN, outline="")
        dot.pack(side="left", padx=(15, 8))
        title = tk.Label(header, text="行情速览", bg=HEADER, fg=WHITE, font=("Microsoft YaHei UI", 10, "bold"))
        title.pack(side="left")
        self.unit_label = tk.Label(header, text="USD", bg=HEADER, fg=MUTED, font=("Segoe UI", 8))
        self.unit_label.pack(side="left", padx=(8, 0), pady=(2, 0))

        self._header_button(header, "×", self.close, "退出")
        self._header_button(header, "—", self.hide, "隐藏到托盘")
        self._header_button(header, "⚙", self.open_settings, "自选行情")
        self.chart_toggle_button = self._header_button(header, "K线", self.toggle_chart, "显示/隐藏K线")
        self.chart_toggle_button.configure(font=("Microsoft YaHei UI", 9, "bold"))
        self.chart_toggle_button.bind("<Leave>", lambda _event: self._update_chart_toggle_button())
        self._update_chart_toggle_button()
        for widget in (header, dot, title, self.unit_label):
            self._enable_drag(widget)

        self.row_container = tk.Frame(self.shell, bg=ROW_BG, height=ROW_HEIGHT * self._body_rows())
        self.row_container.pack(fill="both", expand=True)
        self.row_container.pack_propagate(False)
        self._build_rows()

        footer = tk.Frame(self.shell, bg=FOOTER, height=50)
        footer.pack(fill="x", side="bottom")
        footer.pack_propagate(False)
        status_row = tk.Frame(footer, bg=FOOTER, height=30)
        status_row.pack(fill="x")
        status_row.pack_propagate(False)
        self.status_dot = tk.Label(status_row, text="●", bg=FOOTER, fg=AMBER, font=("Segoe UI", 8))
        self.status_dot.pack(side="left", padx=(15, 6))
        self.status_label = tk.Label(status_row, text="连接行情中…", bg=FOOTER, fg=MUTED, anchor="w", font=("Microsoft YaHei UI", 8))
        refresh = tk.Label(status_row, text="↻", bg=FOOTER, fg=MUTED, cursor="hand2", font=("Segoe UI Symbol", 15))
        refresh.pack(side="right", padx=(4, 14))
        refresh.bind("<Button-1>", lambda _event: self.refresh(manual=True))
        self.status_label.pack(side="left", fill="x", expand=True)
        author_label = tk.Label(footer, text=AUTHOR_CREDIT, bg=FOOTER, fg=MUTED, font=("Microsoft YaHei UI", 8))
        author_label.pack(anchor="w", padx=15)
        for widget in (footer, status_row, self.status_dot, self.status_label, author_label):
            self._enable_drag(widget)

    def _header_button(self, parent: tk.Frame, label: str, command, hint: str) -> tk.Label:
        button = tk.Label(parent, text=label, bg=HEADER, fg=MUTED, width=3, cursor="hand2", font=("Segoe UI Symbol", 12))
        button.pack(side="right", fill="y")
        button.bind("<Button-1>", lambda _event: command())
        button.bind("<Enter>", lambda _event: button.configure(bg=BORDER, fg=WHITE))
        button.bind("<Leave>", lambda _event: button.configure(bg=HEADER, fg=MUTED))
        button.bind("<Button-3>", lambda _event: self._set_status(hint, MUTED))
        return button

    def _update_chart_toggle_button(self) -> None:
        self.chart_toggle_button.configure(bg=HEADER, fg=GREEN if self.config["chart_visible"] else MUTED)

    def toggle_chart(self) -> None:
        old_width = self._window_width()
        self.config["chart_visible"] = not self.config["chart_visible"]
        new_width = self._window_width()
        self.config["position"] = [self.root.winfo_x() + old_width - new_width, self.root.winfo_y()]
        if self.config["chart_visible"]:
            self.chart_panel.pack(side="left", fill="both", expand=True)
            for separator in self.compact_separators:
                separator.place_forget()
        else:
            self._cancel_chart_motion()
            self.chart_preview = None
            self.chart_panel.pack_forget()
            for index, separator in enumerate(self.compact_separators):
                separator.place(x=LEFT_COLUMN_WIDTH, y=(index + 1) * ROW_HEIGHT - 1, width=COMPACT_WINDOW_WIDTH - 2 - LEFT_COLUMN_WIDTH - RIGHT_COLUMN_WIDTH)
        self.row_container.configure(height=ROW_HEIGHT * self._body_rows())
        self._set_geometry()
        self._update_chart_toggle_button()
        self._save_config()
        if self.config["chart_visible"]:
            self.root.after_idle(self._draw_chart)

    def _build_rows(self) -> None:
        for widget in self.row_container.winfo_children():
            widget.destroy()
        self.rows.clear()
        self.symbol_labels.clear()
        self.compact_separators = []

        self.row_container.configure(height=ROW_HEIGHT * self._body_rows())
        left_column = tk.Frame(self.row_container, bg=ROW_BG, width=LEFT_COLUMN_WIDTH)
        left_column.pack(side="left", fill="y")
        left_column.pack_propagate(False)
        right_column = tk.Frame(self.row_container, bg=ROW_BG, width=RIGHT_COLUMN_WIDTH)
        right_column.pack(side="right", fill="y")
        right_column.pack_propagate(False)

        chart_panel = tk.Frame(self.row_container, bg=BG, highlightbackground=BORDER, highlightthickness=1, bd=0)
        self.chart_panel = chart_panel
        if self.config["chart_visible"]:
            chart_panel.pack(side="left", fill="both", expand=True)
        toolbar = tk.Frame(chart_panel, bg=HEADER, height=22)
        toolbar.pack(fill="x")
        toolbar.pack_propagate(False)
        self.chart_volume_button = tk.Label(toolbar, text="VOL", bg=HEADER, fg=MUTED, width=3, cursor="hand2", font=("Microsoft YaHei UI", 8, "bold"))
        self.chart_volume_button.pack(side="left", fill="y")
        self.chart_volume_button.bind("<Button-1>", lambda _event: self.toggle_volume())
        self.chart_volume_button.bind("<Enter>", lambda _event: self.chart_volume_button.configure(bg=BORDER, fg=WHITE))
        self.chart_volume_button.bind("<Leave>", lambda _event: self._update_volume_button())
        self.chart_volume_button.bind("<Button-3>", lambda _event: self._set_status("显示 / 隐藏 K 线量能柱", MUTED))
        for label, direction in (("+", 1), ("−", -1)):
            button = tk.Label(toolbar, text=label, bg=HEADER, fg=MUTED, width=2, cursor="hand2", font=("Segoe UI", 9, "bold"))
            button.pack(side="right", fill="y")
            button.bind("<Button-1>", lambda _event, value=direction: self.zoom_chart(value))
            button.bind("<Enter>", lambda _event, widget=button: widget.configure(bg=BORDER, fg=WHITE))
            button.bind("<Leave>", lambda _event, widget=button: widget.configure(bg=HEADER, fg=MUTED))
        self.chart_period_buttons = {}
        for label, period in CHART_PERIODS:
            button = tk.Label(toolbar, text=label, bg=HEADER, fg=MUTED, cursor="hand2", font=("Microsoft YaHei UI", 8))
            button.pack(side="left", fill="both", expand=True)
            button.bind("<Button-1>", lambda _event, value=period: self.set_chart_period(value))
            self.chart_period_buttons[period] = button
        self.chart_canvas = tk.Canvas(chart_panel, bg=BG, highlightthickness=0, bd=0)
        self.chart_canvas.pack(fill="both", expand=True)
        self.chart_canvas.bind("<Configure>", lambda _event: self._draw_chart())
        self.chart_canvas.bind("<MouseWheel>", self._on_chart_wheel)
        self._enable_drag(self.chart_canvas)

        for index, symbol in enumerate(self.config["symbols"]):
            left = tk.Frame(left_column, bg=ROW_BG, height=ROW_HEIGHT)
            left.pack(fill="x")
            left.pack_propagate(False)
            symbol_label = tk.Label(left, text=symbol, bg=ROW_BG, fg=WHITE, anchor="w", font=("Segoe UI", 10, "bold"))
            symbol_label.pack(anchor="w", padx=(15, 0), pady=(6, 0))
            name_label = tk.Label(left, text=ASSETS[symbol][1], bg=ROW_BG, fg=MUTED, anchor="w", font=("Microsoft YaHei UI", 8))
            name_label.pack(anchor="w", padx=(15, 0))
            self.symbol_labels[symbol] = symbol_label
            for widget in (left, symbol_label, name_label):
                widget.configure(cursor="hand2")
                widget.bind("<Button-1>", lambda _event, value=symbol: self.set_chart_symbol(value))

            right = tk.Frame(right_column, bg=ROW_BG, height=ROW_HEIGHT)
            right.pack(fill="x")
            right.pack_propagate(False)
            price_label = tk.Label(right, text="—", bg=ROW_BG, fg=WHITE, anchor="e", font=("Segoe UI", 10, "bold"))
            price_label.pack(anchor="e", padx=(0, 14), pady=(6, 0))
            change_label = tk.Label(right, text="日  —" if symbol in STOCKS else "24h  —", bg=ROW_BG, fg=MUTED, anchor="e", font=("Segoe UI", 8))
            change_label.pack(anchor="e", padx=(0, 14))
            self.rows[symbol] = (price_label, change_label)
            for widget in (right, price_label, change_label):
                self._enable_drag(widget)
                widget.bind("<Button-3>", lambda _event, value=symbol: self._show_quote_details(value))
            if index < len(self.config["symbols"]) - 1:
                tk.Frame(left_column, bg=BORDER, height=1).place(x=15, y=(index + 1) * ROW_HEIGHT - 1, width=LEFT_COLUMN_WIDTH - 15)
                tk.Frame(right_column, bg=BORDER, height=1).place(x=0, y=(index + 1) * ROW_HEIGHT - 1, width=RIGHT_COLUMN_WIDTH - 14)
                separator = tk.Frame(self.row_container, bg=BORDER, height=1)
                self.compact_separators.append(separator)
                if not self.config["chart_visible"]:
                    separator.place(x=LEFT_COLUMN_WIDTH, y=(index + 1) * ROW_HEIGHT - 1, width=COMPACT_WINDOW_WIDTH - 2 - LEFT_COLUMN_WIDTH - RIGHT_COLUMN_WIDTH)
        self._mark_selected_symbol()
        self._mark_selected_period()
        self._update_volume_button()
        self._draw_chart()
        self._show_quotes()

    def _mark_selected_symbol(self) -> None:
        for symbol, label in self.symbol_labels.items():
            label.configure(fg=GREEN if symbol == self.config["chart_symbol"] else WHITE)

    def _mark_selected_period(self) -> None:
        for period, button in self.chart_period_buttons.items():
            supported = self.config["chart_symbol"] in COINS or period != "1s"
            selected = period == self.config["chart_period"]
            button.configure(bg=BORDER if selected else HEADER, fg=(GREEN if selected else MUTED) if supported else BORDER, cursor="hand2" if supported else "arrow")

    def _update_volume_button(self) -> None:
        visible = self.config["volume_visible"]
        self.chart_volume_button.configure(bg=BORDER if visible else HEADER, fg=GREEN if visible else MUTED)

    def toggle_volume(self) -> None:
        self.config["volume_visible"] = not self.config["volume_visible"]
        self._save_config()
        self._update_volume_button()
        self._draw_chart()
        self._set_status("量能柱 已显示" if self.config["volume_visible"] else "量能柱 已隐藏", MUTED)

    def set_chart_symbol(self, symbol: str) -> None:
        if symbol == self.config["chart_symbol"] or symbol not in self.config["symbols"]:
            return
        self._cancel_chart_motion()
        self.chart_preview = None
        self.config["chart_symbol"] = symbol
        if symbol not in COINS and self.config["chart_period"] == "1s":
            self.config["chart_period"] = "1m"
        self.chart_source = initial_source(symbol)
        self._save_config()
        self._mark_selected_symbol()
        self._mark_selected_period()
        self._show_quotes()
        self.chart_bars = []
        self.chart_error = ""
        self.chart_last_ws_update = 0.0
        self.chart_last_rest = 0.0
        self._draw_chart()
        self._start_chart_websocket()
        self._request_chart(force=True)

    def set_chart_period(self, period: str) -> None:
        if self.config["chart_symbol"] not in COINS and period == "1s":
            self._set_status("当前行情源的 K 线最小周期为 1 分钟", MUTED)
            return
        if period == self.config["chart_period"] or period not in CHART_PERIOD_VALUES:
            return
        self._cancel_chart_motion()
        self.chart_preview = None
        self.config["chart_period"] = period
        self._save_config()
        self._mark_selected_period()
        self.chart_bars = []
        self.chart_error = ""
        self.chart_last_ws_update = 0.0
        self.chart_last_rest = 0.0
        self._draw_chart()
        self._start_chart_websocket()
        self._request_chart(force=True)

    def zoom_chart(self, direction: int) -> None:
        current = self.config["chart_zoom"]
        index = min(range(len(CHART_ZOOM_LEVELS)), key=lambda i: abs(CHART_ZOOM_LEVELS[i] - current))
        index = max(0, min(len(CHART_ZOOM_LEVELS) - 1, index - 1 if direction > 0 else index + 1))
        updated = CHART_ZOOM_LEVELS[index]
        if updated != current:
            self.config["chart_zoom"] = updated
            self._save_config()
            self._draw_chart()

    def _on_chart_wheel(self, event: tk.Event) -> str:
        self.zoom_chart(1 if event.delta > 0 else -1)
        return "break"

    def _chart_tick(self) -> None:
        if self.closing:
            return
        now = time.monotonic()
        if self.config["chart_symbol"] in STOCKS:
            if now - self.chart_last_request >= STOCK_CHART_REFRESH_SECONDS:
                self._request_chart()
            self.root.after(1000, self._chart_tick)
            return
        stream_is_current = now - self.chart_last_ws_update < 3 and len(self.chart_bars) >= 24
        if not stream_is_current or now - self.chart_last_rest >= 30:
            self._request_chart()
        # WS 通着时靠推送，这里只是兜底；WS 断了就按 1 秒拉一次
        self.root.after(1000, self._chart_tick)

    def _request_chart(self, force: bool = False) -> None:
        if self.closing or (self.chart_fetching and not force):
            return
        self.chart_fetching = True
        self.chart_last_request = time.monotonic()
        self.chart_request_id += 1
        request_id = self.chart_request_id
        self.chart_request_preview = self.chart_preview
        symbol = self.config["chart_symbol"]
        period = self.config["chart_period"]

        def worker() -> None:
            try:
                result = fetch_candles(symbol, period)
                self.events.put(("candles", (request_id, result["bars"], "", result["source"])))
            except Exception as error:
                self.events.put(("candles", (request_id, None, str(error), "")))

        threading.Thread(target=worker, daemon=True).start()

    def _cancel_chart_motion(self) -> None:
        if self.chart_motion_job is not None:
            self.root.after_cancel(self.chart_motion_job)
            self.chart_motion_job = None
        self.chart_display_ts = None
        self.chart_display_close = None

    def _start_chart_motion(self, stamp: int, start: float, target: float) -> None:
        self._cancel_chart_motion()
        if not self.visible or not self.config["chart_visible"] or start == target:
            self._draw_chart()
            return
        self.chart_display_ts = stamp
        self.chart_display_close = start
        started = time.monotonic()

        def frame() -> None:
            self.chart_motion_job = None
            if self.closing or not self.visible or not self.config["chart_visible"] or not self.chart_bars or self.chart_bars[-1]["ts"] != stamp:
                self._cancel_chart_motion()
                return
            progress = min(1.0, (time.monotonic() - started) * 1000 / CHART_SMOOTH_MS)
            eased = progress * progress * (3 - 2 * progress)
            self.chart_display_close = start + (target - start) * eased
            self._draw_chart()
            if progress < 1:
                self.chart_motion_job = self.root.after(CHART_FRAME_MS, frame)
            else:
                self._cancel_chart_motion()
                self._draw_chart()

        self._draw_chart()
        self.chart_motion_job = self.root.after(CHART_FRAME_MS, frame)

    def _preview_chart_from_ticker(self, symbol: str, price: float, timestamp_ms: int) -> None:
        if symbol != self.config["chart_symbol"] or not self.chart_bars:
            return
        quote = self.quote_cache.get(symbol)
        if quote and timestamp_ms < quote.get("timestamp_ms", quote["timestamp"] * 1000):
            return
        bar = self.chart_bars[-1]
        stamp = bar["ts"]
        if bar["closed"] or not stamp <= timestamp_ms < stamp + CHART_PERIOD_MS[self.config["chart_period"]]:
            return
        previous = self.chart_preview if self.chart_preview and self.chart_preview["bar_ts"] == stamp else None
        if previous and timestamp_ms <= previous["source_ts"]:
            return
        old_price = previous["close"] if previous else bar["close"]
        if price == old_price:
            return
        self.chart_preview = {
            "bar_ts": stamp, "source_ts": timestamp_ms, "close": price,
            "high": max(bar["high"], previous["high"] if previous else bar["high"], price),
            "low": min(bar["low"], previous["low"] if previous else bar["low"], price),
        }
        displayed = self.chart_display_close if self.chart_display_ts == stamp and self.chart_display_close is not None else old_price
        self._start_chart_motion(stamp, displayed, price)

    def _draw_chart(self) -> None:
        if not self.config["chart_visible"] or not hasattr(self, "chart_canvas") or not self.chart_canvas.winfo_exists():
            return
        canvas = self.chart_canvas
        canvas.delete("all")
        width = canvas.winfo_width()
        height = canvas.winfo_height()
        if width < 40 or height < 80:
            return
        symbol = self.config["chart_symbol"]
        unit = "USD" if symbol in STOCKS else "USDT"
        if not self.chart_bars:
            caption = "K线暂不可用" if self.chart_error else "加载 K 线…"
            canvas.create_text(width / 2, height / 2, text=caption, fill=MUTED, font=("Microsoft YaHei UI", 9))
            canvas.create_text(7, 8, text=f"{symbol}/{unit}", anchor="nw", fill=GREEN, font=("Segoe UI", 8, "bold"))
            return

        bars = self.chart_bars[-self.config["chart_zoom"]:].copy()
        if self.chart_preview and not bars[-1]["closed"] and self.chart_preview["bar_ts"] == bars[-1]["ts"]:
            preview = self.chart_preview
            bars[-1] = {**bars[-1], "close": preview["close"], "high": max(bars[-1]["high"], preview["high"]), "low": min(bars[-1]["low"], preview["low"])}
        display_close = self.chart_display_close if self.chart_display_ts == bars[-1]["ts"] else None
        if display_close is not None:
            bars[-1] = {**bars[-1], "high": max(bars[-1]["high"], display_close), "low": min(bars[-1]["low"], display_close)}
        low = min(bar["low"] for bar in bars)
        high = max(bar["high"] for bar in bars)
        span = max(high - low, high * 0.0001)
        low -= span * 0.08
        high += span * 0.08
        chart_top = 22
        volume_visible = self.config["volume_visible"]
        volume_bottom = height - 16
        volume_top = volume_bottom - 35
        chart_bottom = volume_top - 8 if volume_visible else height - 16
        chart_height = max(1, chart_bottom - chart_top)
        # 上下各留出一行标签的高度，"高/低"标签才能贴在 K 线外侧，既不压住 K 线也不越界
        label_pad = min(18, chart_height // 4)
        plot_top = chart_top + label_pad
        plot_height = max(1, chart_height - label_pad * 2)

        def y_for(price: float) -> float:
            return plot_top + (high - price) / (high - low) * plot_height

        for fraction in (0, 0.5, 1):
            y = plot_top + fraction * plot_height
            canvas.create_line(6, y, width - 6, y, fill="#20303A", dash=(2, 3))
        current = bars[-1]
        current_y = y_for(current["close"])
        plot_left = 6
        plot_right = width - 18
        step = (plot_right - plot_left) / len(bars)
        half_body = max(1, int(step * 0.28))
        latest_x = plot_left + (len(bars) - 0.5) * step
        line_gap = half_body + 5
        if latest_x - line_gap > plot_left:
            canvas.create_line(plot_left, current_y, latest_x - line_gap, current_y, fill=PRICE_LINE, dash=(4, 3), width=1, tags=("current_price_line",))
        if latest_x + line_gap < width - 9:
            canvas.create_line(latest_x + line_gap, current_y, width - 9, current_y, fill=PRICE_LINE, dash=(4, 3), width=1, tags=("current_price_line",))
        if volume_visible:
            canvas.create_line(6, volume_top - 4, width - 6, volume_top - 4, fill=BORDER)

        max_volume = max((bar.get("volume", 0.0) for bar in bars), default=0.0) if volume_visible else 0.0
        for index, bar in enumerate(bars):
            x = plot_left + (index + 0.5) * step
            is_current = index == len(bars) - 1
            candle_close = display_close if is_current and display_close is not None else bar["close"]
            color_close = bar["close"] if is_current and display_close is not None else candle_close
            color = GREEN if color_close >= bar["open"] else RED
            candle_tag = f"candle_{bar['ts']}"
            canvas.create_line(x, y_for(bar["high"]), x, y_for(bar["low"]), fill=color, width=1, tags=("candle_wick", candle_tag))
            body_top = y_for(max(bar["open"], candle_close))
            body_bottom = y_for(min(bar["open"], candle_close))
            if body_bottom - body_top < 2:
                center = (body_top + body_bottom) / 2
                body_top, body_bottom = center - 1, center + 1
            canvas.create_rectangle(x - half_body, body_top, x + half_body, body_bottom, fill=color, outline=color, tags=("candle_body", candle_tag))
            volume = bar.get("volume", 0.0)
            if volume_visible and max_volume > 0 and volume > 0:
                volume_top_y = max(volume_top, volume_bottom - volume / max_volume * (volume_bottom - volume_top))
                canvas.create_rectangle(x - half_body, volume_top_y, x + half_body, volume_bottom, fill=color, outline="", tags=("volume_bar", candle_tag))

        def mark_extreme(index: int, value: float, title: str, high_point: bool) -> None:
            x = plot_left + (index + 0.5) * step
            y = y_for(value)
            right_side = x < width / 2
            shift = 5 if right_side else -5
            # 高点锚在 K 线上方、低点锚在 K 线下方，锚点一律朝 K 线外侧，避免压住 K 线；
            # 标签不再铺底色方块，免得盖掉底下的 K 线。
            if high_point:
                anchor = "sw" if right_side else "se"
                label_y = y - 4
            else:
                anchor = "nw" if right_side else "ne"
                label_y = y + 4
            canvas.create_text(
                x + shift,
                label_y,
                text=f"{title} {price_text(value, unit).lstrip('$₮')}",
                anchor=anchor,
                fill=WHITE,
                font=("Segoe UI", 7, "bold"),
                tags=("high_price_label" if high_point else "low_price_label",),
            )

        highest_index = max(range(len(bars)), key=lambda index: bars[index]["high"])
        lowest_index = min(range(len(bars)), key=lambda index: bars[index]["low"])
        mark_extreme(highest_index, bars[highest_index]["high"], "高", True)
        mark_extreme(lowest_index, bars[lowest_index]["low"], "低", False)

        canvas.create_polygon(width - 2, current_y, width - 7, current_y - 3, width - 7, current_y + 3, fill=PRICE_LINE, outline="", tags=("current_price_marker",))

        stamp = datetime.fromtimestamp(bars[-1]["ts"] / 1000)
        time_label = stamp.strftime("%H:%M:%S" if self.config["chart_period"] == "1s" else "%m/%d %H:%M")
        canvas.create_text(7, 7, text=f"{symbol}/{unit} K线", anchor="nw", fill=GREEN, font=("Microsoft YaHei UI", 8, "bold"))
        price_label = price_text(current["close"], unit).lstrip("$₮")
        canvas.create_text(width - 6, 8, text=price_label, anchor="ne", fill=PRICE_LINE, font=("Segoe UI", 7, "bold"))
        if volume_visible:
            canvas.create_text(7, volume_top - 5, text="VOL", anchor="sw", fill=MUTED, font=("Segoe UI", 7))
        canvas.create_text(7, height - 2, text="网络中断" if self.chart_error else f"{self.chart_source} · {len(bars)}根", anchor="sw", fill=AMBER if self.chart_error else MUTED, font=("Microsoft YaHei UI", 7))
        canvas.create_text(width - 6, height - 2, text=time_label, anchor="se", fill=MUTED, font=("Segoe UI", 7))

    def _enable_drag(self, widget: tk.Widget) -> None:
        widget.bind("<ButtonPress-1>", self._drag_start)
        widget.bind("<B1-Motion>", self._drag_motion)
        widget.bind("<ButtonRelease-1>", self._drag_end)

    def _drag_start(self, event: tk.Event) -> None:
        self.drag_offset = (event.x_root - self.root.winfo_x(), event.y_root - self.root.winfo_y())

    def _drag_motion(self, event: tk.Event) -> None:
        if self.drag_offset is None:
            return
        x = event.x_root - self.drag_offset[0]
        y = event.y_root - self.drag_offset[1]
        self.root.geometry(f"+{x}+{y}")

    def _drag_end(self, _event: tk.Event) -> None:
        if self.drag_offset is None:
            return
        self.drag_offset = None
        self.config["position"] = [self.root.winfo_x(), self.root.winfo_y()]
        self._save_config()

    def _save_config(self) -> None:
        try:
            APP_DIR.mkdir(parents=True, exist_ok=True)
            CONFIG_PATH.write_text(json.dumps(self.config, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            self._set_status("设置保存失败", AMBER)

    def _set_status(self, text: str, color: str) -> None:
        self.status_label.configure(text=text)
        self.status_dot.configure(fg=color)

    def _show_quotes(self, result: dict | None = None) -> None:
        if result:
            for symbol, quote in result.get("quotes", {}).items():
                if symbol not in self.config["symbols"]:
                    continue
                item = {
                    "unit": result.get("unit", "USD" if symbol in STOCKS else "USDT"),
                    "source": result.get("source", initial_source(symbol)),
                    "timestamp": result.get("timestamp", 0),
                    "live": result.get("live", False),
                    "fallback_reason": result.get("fallback_reason", ""),
                    **quote,
                }
                previous = self.quote_cache.get(symbol)
                item_stamp = item.get("timestamp_ms", item["timestamp"] * 1000)
                previous_stamp = previous.get("timestamp_ms", previous["timestamp"] * 1000) if previous else 0
                if not previous or item_stamp >= previous_stamp:
                    self.quote_cache[symbol] = item
        units = {"USD" if symbol in STOCKS else "USDT" for symbol in self.config["symbols"]}
        self.unit_label.configure(text="混合" if len(units) > 1 else next(iter(units)))
        for symbol, (price_label, change_label) in self.rows.items():
            quote = self.quote_cache.get(symbol)
            change_title = "日" if symbol in STOCKS else "24h"
            error = symbol in self.stock_errors or symbol in self.perp_errors
            if quote is None:
                price_label.configure(text="—")
                change_label.configure(text="获取失败" if error else f"{change_title}  —", fg=AMBER if error else MUTED)
                continue
            change = quote["change"]
            price_label.configure(text=price_text(quote["price"], quote["unit"]))
            stale = error
            change_label.configure(text=f"{change_title} {change:+.2f}%" + (" · 旧" if stale else ""), fg=AMBER if stale else GREEN if change >= 0 else RED)
        if hasattr(self, "status_label"):
            self._show_quote_details(self.config["chart_symbol"])

    def _show_quote_details(self, symbol: str) -> None:
        quote = self.quote_cache.get(symbol)
        if not quote:
            if symbol in self.stock_errors:
                self._set_status("美股行情暂不可用 · 稍后重试", AMBER)
            elif symbol in self.perp_errors:
                self._set_status("MSTR 永续行情暂不可用 · 重试中", AMBER)
            else:
                self._set_status(f"{symbol} 连接行情中…", AMBER)
            return
        if symbol in STOCKS:
            updated = datetime.fromtimestamp(quote["timestamp"]).strftime("%m/%d %H:%M")
            session = quote.get("session", "参考")
            label = "更新失败·保留" if symbol in self.stock_errors else f"{quote['source']}参考·{session}"
            self._set_status(f"{label}·{updated}", AMBER)
            return
        if symbol in self.perp_errors:
            updated = datetime.fromtimestamp(quote["timestamp"]).strftime("%m/%d %H:%M:%S")
            self._set_status(f"Bitget永续·断线保留·{updated}", AMBER)
            return
        if symbol in PERPS:
            updated = datetime.fromtimestamp(quote["timestamp"]).strftime("%H:%M:%S")
            status = quote.get("market_status", "unknown")
            metadata_stale = quote.get("metadata_stale", True)
            if status != "normal":
                label = quote.get("market_status_label", "状态待确认")
            elif metadata_stale:
                label = "交易状态待确认"
            else:
                label = "实时" if quote.get("live") else "行情"
            if metadata_stale and status != "normal" and status != "unknown":
                label += "·状态待确认"
            self._set_status(f"Bitget永续·{label}·{updated}", GREEN if status == "normal" and not metadata_stale else AMBER)
            return
        updated = datetime.fromtimestamp(quote["timestamp"]).strftime("%H:%M:%S")
        source = quote.get("source", "")
        label = f"{source} 实时" if quote.get("live") else source
        if quote.get("fallback_reason"):
            label = f"{label}（主源失败）"
        self._set_status(f"{label} · 更新于 {updated}", AMBER if quote.get("fallback_reason") else GREEN)

    def _stock_tick(self) -> None:
        if self.closing:
            return
        self.refresh_stocks()
        self.root.after(1000, self._stock_tick)

    def refresh_stocks(self, manual: bool = False) -> None:
        symbols = [symbol for symbol in self.config["symbols"] if symbol in STOCKS]
        if self.closing or self.stock_fetching or not symbols:
            return
        interval = 30 if all(self.quote_cache.get(symbol, {}).get("session") == "休市" for symbol in symbols) else STOCK_REFRESH_SECONDS
        now = time.monotonic()
        if not manual and now - self.stock_last_fetch_start < interval:
            return
        self.stock_fetching = True
        self.stock_last_fetch_start = now
        generation = self.quote_generation

        def worker() -> None:
            try:
                result = fetch_stock_quotes(symbols)
            except Exception as error:
                result = {"quotes": {}, "errors": {symbol: str(error) for symbol in symbols}}
            self.events.put(("stock_prices", (generation, result)))

        threading.Thread(target=worker, daemon=True).start()

    def refresh(self, manual: bool = False) -> None:
        if manual:
            self.refresh_stocks(manual=True)
            self.refresh_perps(manual=True)
        if self.closing or self.fetching:
            return
        symbols = [symbol for symbol in self.config["symbols"] if symbol in COINS]
        if not symbols:
            return
        now = time.monotonic()
        if manual and now - self.last_fetch_start < MIN_MANUAL_REFRESH_SECONDS:
            return
        if not manual and now - self.last_ws_update < REST_REFRESH_SECONDS + 1:
            self.root.after(REST_REFRESH_SECONDS * 1000, self.refresh)
            return
        self.fetching = True
        self.last_fetch_start = now
        if self.latest is None:
            self._set_status("连接行情中…", AMBER)
        generation = self.quote_generation

        def worker() -> None:
            # fetch_prices 内部已经是「币安 -> Gate.io」级联，失败会自动降级
            try:
                self.events.put(("prices", (generation, fetch_prices(symbols))))
            except Exception as error:
                self.events.put(("error", (generation, str(error))))

        threading.Thread(target=worker, daemon=True).start()

    def _start_perp_stream(self) -> None:
        if self.perp_stream:
            self.perp_stream.stop()
            self.perp_stream = None
        self.last_perp_ws_update = 0.0
        if "MSTR" not in self.config["symbols"]:
            return
        generation = self.quote_generation
        period = self.config["chart_period"] if self.config["chart_symbol"] == "MSTR" else None

        def emit(kind, payload) -> None:
            if not self.closing and generation == self.quote_generation:
                self.events.put((kind, (generation, payload)))

        self.perp_stream = PerpStream("MSTR", period, emit, proxy_options=_websocket_proxy_options())
        self.perp_stream.start()

    def _perp_tick(self) -> None:
        if self.closing:
            return
        self.refresh_perps()
        self.root.after(1000, self._perp_tick)

    def refresh_perps(self, manual: bool = False) -> None:
        symbols = [symbol for symbol in self.config["symbols"] if symbol in PERPS]
        if self.closing or self.perp_fetching or not symbols:
            return
        now = time.monotonic()
        if not manual and (now - self.last_perp_ws_update < 3 or now - self.perp_last_fetch_start < PERP_REST_REFRESH_SECONDS):
            return
        self.perp_fetching = True
        self.perp_last_fetch_start = now
        generation = self.quote_generation

        def worker() -> None:
            try:
                result = fetch_perp_quotes(symbols)
            except Exception as error:
                result = {"quotes": {}, "errors": {symbol: str(error) for symbol in symbols}}
            self.events.put(("perp_prices", (generation, result)))

        threading.Thread(target=worker, daemon=True).start()

    def _drain_events(self) -> None:
        if self.closing:
            return
        try:
            while True:
                kind, value = self.events.get_nowait()
                if kind == "prices":
                    generation, result = value
                    if generation != self.quote_generation:
                        continue
                    self.latest = result
                    self._show_quotes(result)
                    self.fetching = False
                    self.root.after(REST_REFRESH_SECONDS * 1000, self.refresh)
                elif kind == "error":
                    generation, detail = value
                    if generation != self.quote_generation:
                        continue
                    self.fetching = False
                    if self.config["chart_symbol"] in COINS:
                        self._set_status("网络暂不可用 · 保留上次行情", AMBER)
                    self._write_error(str(detail))
                    self.root.after(REST_REFRESH_SECONDS * 1000, self.refresh)
                elif kind == "stock_prices":
                    generation, result = value
                    if generation != self.quote_generation:
                        continue
                    self.stock_fetching = False
                    self.stock_errors = {symbol: detail for symbol, detail in result.get("errors", {}).items() if symbol in self.config["symbols"]}
                    self._show_quotes(result)
                elif kind == "perp_prices":
                    generation, result = value
                    if generation != self.quote_generation:
                        continue
                    self.perp_fetching = False
                    self.perp_errors = {symbol: detail for symbol, detail in result.get("errors", {}).items() if symbol in self.config["symbols"]}
                    self._show_quotes(result)
                elif kind == "perp_quote":
                    generation, payload = value
                    if generation != self.quote_generation:
                        continue
                    symbol, quote, timestamp_ms = payload
                    if symbol not in self.config["symbols"] or symbol not in PERPS:
                        continue
                    self.last_perp_ws_update = time.monotonic()
                    self.perp_errors.pop(symbol, None)
                    self._show_quotes({"quotes": {symbol: {**quote, "live": True}}})
                    self._preview_chart_from_ticker(symbol, quote["price"], timestamp_ms)
                elif kind == "ws_quote":
                    symbol, quote, timestamp_ms = value
                    if symbol in self.config["symbols"] and symbol in COINS:
                        self.last_ws_update = time.monotonic()
                        source = quote.get("source", "币安")
                        if self.latest is None:
                            self.latest = {"quotes": {}}
                        self.latest.update({"source": source, "unit": "USDT", "live": True})
                        self.latest["quotes"][symbol] = {**quote, "source": source, "unit": "USDT", "timestamp": timestamp_ms // 1000, "timestamp_ms": timestamp_ms, "live": True, "fallback_reason": ""}
                        self.latest["timestamp"] = timestamp_ms // 1000
                        self.latest["live"] = True
                        self._show_quotes({"quotes": {symbol: self.latest["quotes"][symbol]}})
                        self._preview_chart_from_ticker(symbol, quote["price"], timestamp_ms)
                elif kind == "candles":
                    request_id, bars, error, source = value
                    if request_id == self.chart_request_id:
                        self.chart_fetching = False
                        self.chart_error = error
                        if source:
                            self.chart_source = source
                        if bars:
                            merged = {bar["ts"]: bar for bar in bars}
                            realtime_market = self.config["chart_symbol"] in COINS or self.config["chart_symbol"] in PERPS
                            if realtime_market:
                                for bar in self.chart_bars:
                                    if bar["ts"] >= bars[-1]["ts"]:
                                        merged[bar["ts"]] = _newer_candle(merged.get(bar["ts"]), bar)
                            self.chart_bars = [merged[stamp] for stamp in sorted(merged)][-MAX_CHART_BARS:]
                            self.chart_last_rest = time.monotonic()
                            preview = self.chart_preview
                            latest_bar = self.chart_bars[-1]
                            keep_preview = (realtime_market and preview is not None
                                            and preview is not self.chart_request_preview
                                            and preview["bar_ts"] == latest_bar["ts"]
                                            and not latest_bar["closed"])
                            if not keep_preview:
                                self.chart_preview = None
                                self._cancel_chart_motion()
                        self._draw_chart()
                elif kind in ("candle_ws", "perp_candle"):
                    if kind == "perp_candle":
                        generation, value = value
                        if generation != self.quote_generation:
                            continue
                    symbol, period, bar = value
                    if symbol == self.config["chart_symbol"] and period == self.config["chart_period"]:
                        if kind == "perp_candle":
                            self.chart_source = "Bitget永续"
                        merged = {previous["ts"]: previous for previous in self.chart_bars}
                        previous = merged.get(bar["ts"])
                        accepted = _newer_candle(previous, bar)
                        merged[bar["ts"]] = accepted
                        prior_display = self.chart_display_close if self.chart_display_ts == bar["ts"] else None
                        prior_preview = self.chart_preview if self.chart_preview and self.chart_preview["bar_ts"] == bar["ts"] else None
                        self.chart_bars = [merged[stamp] for stamp in sorted(merged)][-MAX_CHART_BARS:]
                        self.chart_last_ws_update = time.monotonic()
                        self.chart_error = ""
                        if self.chart_bars[-1]["ts"] == bar["ts"]:
                            self.chart_preview = None
                            if accepted is bar and not bar["closed"]:
                                start = prior_display if prior_display is not None else (prior_preview["close"] if prior_preview else (previous["close"] if previous else bar["open"]))
                                self._start_chart_motion(bar["ts"], start, bar["close"])
                            else:
                                self._cancel_chart_motion()
                                self._draw_chart()
                        else:
                            self._draw_chart()
                elif kind == "toggle":
                    self.toggle()
                elif kind == "show":
                    self.show()
                elif kind == "hide":
                    self.hide()
                elif kind == "toggle_volume":
                    self.toggle_volume()
                elif kind == "refresh":
                    self.refresh(manual=True)
                elif kind == "close":
                    self.close()
                elif kind == "hotkey_error":
                    self._set_status("快捷键被占用 · 托盘仍可恢复", AMBER)
        except queue.Empty:
            pass
        self.root.after(UI_QUEUE_MS, self._drain_events)

    def _start_tray(self) -> None:
        menu = pystray.Menu(
            pystray.MenuItem("显示 / 隐藏  Ctrl+Alt+M", lambda _icon, _item: self.events.put(("toggle", None)), default=True),
            pystray.MenuItem("量能柱  显示 / 隐藏", lambda _icon, _item: self.events.put(("toggle_volume", None))),
            pystray.MenuItem("刷新行情", lambda _icon, _item: self.events.put(("refresh", None))),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("退出", lambda _icon, _item: self.events.put(("close", None))),
        )
        self.tray = pystray.Icon(APP_NAME, make_tray_image(), f"行情速览 V{APP_VERSION} · {APP_AUTHOR}", menu)
        threading.Thread(target=self.tray.run, daemon=True).start()

    def _start_hotkey(self) -> None:
        def worker() -> None:
            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32
            self.hotkey_thread_id = kernel32.GetCurrentThreadId()
            # MOD_CONTROL | MOD_ALT | MOD_NOREPEAT, Ctrl+Alt+M.
            if not user32.RegisterHotKey(None, 1, 0x0002 | 0x0001 | 0x4000, ord("M")):
                self.events.put(("hotkey_error", None))
                return
            self.hotkey_enabled = True
            msg = wintypes.MSG()
            while user32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
                if msg.message == 0x0312 and msg.wParam == 1:
                    self.events.put(("toggle", None))
            user32.UnregisterHotKey(None, 1)

        threading.Thread(target=worker, daemon=True).start()

    def _start_websocket(self) -> None:
        self.ws_stop.set()
        if self.ws_app:
            self.ws_app.close()
        self.ws_stop = threading.Event()
        stop = self.ws_stop
        symbols = [symbol for symbol in self.config["symbols"] if symbol in COINS]
        if not symbols:
            return
        binance_streams = [f"{symbol.lower()}usdt@ticker" for symbol in symbols]
        gate_pairs = [f"{symbol}_USDT" for symbol in symbols]
        state = {"connected": False}

        def on_binance_open(ws: websocket.WebSocketApp) -> None:
            state["connected"] = True
            ws.send(json.dumps({"method": "SUBSCRIBE", "params": binance_streams, "id": 1}))

        def on_binance_message(_ws: websocket.WebSocketApp, message: str) -> None:
            try:
                parsed = _parse_binance_ticker(json.loads(message))
                if parsed and parsed[0] in symbols:
                    self.events.put(("ws_quote", parsed))
            except (KeyError, ValueError, TypeError):
                return

        def on_gate_open(ws: websocket.WebSocketApp) -> None:
            state["connected"] = True
            ws.send(json.dumps({"time": int(time.time()), "channel": "spot.tickers", "event": "subscribe", "payload": gate_pairs}))

        def on_gate_message(_ws: websocket.WebSocketApp, message: str) -> None:
            try:
                parsed = _parse_gate_ticker(json.loads(message))
                if parsed and parsed[0] in symbols:
                    symbol, quote, stamp = parsed
                    self.events.put(("ws_quote", (symbol, {**quote, "source": "Gate.io"}, stamp)))
            except (KeyError, ValueError, TypeError):
                return

        def worker() -> None:
            proxy_options = _websocket_proxy_options()
            use_binance = True
            failures = 0
            while not self.closing and not stop.is_set():
                state["connected"] = False
                if use_binance:
                    app = websocket.WebSocketApp(BINANCE_WS, on_open=on_binance_open, on_message=on_binance_message)
                else:
                    app = websocket.WebSocketApp(GATE_WS, on_open=on_gate_open, on_message=on_gate_message)
                self.ws_app = app
                app.run_forever(ping_interval=20, ping_timeout=8, **proxy_options)
                # 连上过的断线只需重连；连续两次连不上就换另一个源
                if state["connected"]:
                    failures = 0
                else:
                    failures += 1
                    if failures >= 2:
                        use_binance = not use_binance
                        failures = 0
                if stop.wait(2):
                    break

        threading.Thread(target=worker, daemon=True).start()

    def _start_chart_websocket(self) -> None:
        self.chart_ws_stop.set()
        if self.chart_ws_app:
            self.chart_ws_app.close()
        self.chart_ws_stop = threading.Event()
        self._start_perp_stream()
        stop = self.chart_ws_stop
        symbol = self.config["chart_symbol"]
        period = self.config["chart_period"]
        if symbol not in COINS:
            return
        binance_stream = f"{symbol.lower()}usdt@kline_{BINANCE_INTERVALS[period]}"
        gate_interval = GATE_INTERVALS.get(period)
        state = {"connected": False}

        def on_binance_open(ws: websocket.WebSocketApp) -> None:
            state["connected"] = True
            ws.send(json.dumps({"method": "SUBSCRIBE", "params": [binance_stream], "id": 2}))

        def on_binance_message(_ws: websocket.WebSocketApp, message: str) -> None:
            try:
                bar = _parse_binance_kline(json.loads(message))
                if bar:
                    self.events.put(("candle_ws", (symbol, period, bar)))
            except (KeyError, ValueError, TypeError):
                return

        def on_gate_open(ws: websocket.WebSocketApp) -> None:
            state["connected"] = True
            ws.send(json.dumps({"time": int(time.time()), "channel": "spot.candlesticks", "event": "subscribe",
                                "payload": [gate_interval, f"{symbol}_USDT"]}))

        def on_gate_message(_ws: websocket.WebSocketApp, message: str) -> None:
            try:
                bar = _parse_gate_kline(json.loads(message))
                if bar:
                    self.events.put(("candle_ws", (symbol, period, bar)))
            except (KeyError, ValueError, TypeError):
                return

        def worker() -> None:
            proxy_options = _websocket_proxy_options()
            use_binance = True
            failures = 0
            while not self.closing and not stop.is_set():
                state["connected"] = False
                if use_binance or not gate_interval:
                    # Gate.io 没有 1 秒 K 线，这种周期只能一直用币安
                    app = websocket.WebSocketApp(BINANCE_WS, on_open=on_binance_open, on_message=on_binance_message)
                    use_binance = True
                else:
                    app = websocket.WebSocketApp(GATE_WS, on_open=on_gate_open, on_message=on_gate_message)
                self.chart_ws_app = app
                app.run_forever(ping_interval=20, ping_timeout=8, **proxy_options)
                if state["connected"]:
                    failures = 0
                else:
                    failures += 1
                    if failures >= 2 and gate_interval:
                        use_binance = not use_binance
                        failures = 0
                if stop.wait(2):
                    break

        threading.Thread(target=worker, daemon=True).start()

    def hide(self) -> None:
        self._cancel_chart_motion()
        self.chart_preview = None
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.destroy()
            self.settings_window = None
            self.root.attributes("-alpha", self.config["opacity"])
        self.root.withdraw()
        self.visible = False

    def show(self) -> None:
        self.root.deiconify()
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.visible = True
        self._draw_chart()

    def toggle(self) -> None:
        self.hide() if self.visible else self.show()

    def open_settings(self) -> None:
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.lift()
            return
        dialog = tk.Toplevel(self.root)
        self.settings_window = dialog
        dialog.title("自选行情与窗口")
        dialog.configure(bg=HEADER)
        dialog.resizable(False, False)
        dialog.attributes("-topmost", True)
        dialog.transient(self.root)
        dialog_width, dialog_height = 320, 548
        user32 = ctypes.windll.user32
        screen_left = user32.GetSystemMetrics(76)
        screen_top = user32.GetSystemMetrics(77)
        screen_right = screen_left + user32.GetSystemMetrics(78)
        screen_bottom = screen_top + user32.GetSystemMetrics(79)
        dialog_x = min(max(self.root.winfo_x() + 8, screen_left), max(screen_left, screen_right - dialog_width))
        dialog_y = min(max(self.root.winfo_y() + 42, screen_top), max(screen_top, screen_bottom - dialog_height - 48))
        dialog.geometry(f"{dialog_width}x{dialog_height}{dialog_x:+d}{dialog_y:+d}")

        tk.Label(dialog, text="自选行情", bg=HEADER, fg=WHITE, font=("Microsoft YaHei UI", 12, "bold")).pack(anchor="w", padx=18, pady=(18, 2))
        tk.Label(dialog, text="选 1–8 项：数字币、永续合约、美股", bg=HEADER, fg=MUTED, font=("Microsoft YaHei UI", 9)).pack(anchor="w", padx=18)
        picker = tk.Frame(dialog, bg=HEADER)
        picker.pack(fill="x", padx=16, pady=(12, 12))
        variables = {}
        for index, (symbol, (_, name)) in enumerate(ASSETS.items()):
            var = tk.BooleanVar(value=symbol in self.config["symbols"])
            variables[symbol] = var
            checkbox = tk.Checkbutton(
                picker, text=f"{symbol}  {name}", variable=var, bg=HEADER, fg=WHITE,
                activebackground=HEADER, activeforeground=WHITE, selectcolor=ROW_BG,
                font=("Microsoft YaHei UI", 9), anchor="w", width=16,
            )
            checkbox.grid(row=index // 2, column=index % 2, sticky="w", pady=4)

        tk.Label(dialog, text="透明度", bg=HEADER, fg=WHITE, font=("Microsoft YaHei UI", 9)).pack(anchor="w", padx=18)
        alpha = tk.DoubleVar(value=self.config["opacity"] * 100)
        slider = tk.Scale(
            dialog, from_=70, to=100, resolution=1, orient="horizontal", variable=alpha,
            bg=HEADER, fg=MUTED, troughcolor=ROW_BG, highlightthickness=0, bd=0,
            activebackground=GREEN, showvalue=True, length=275,
            command=lambda value: self.root.attributes("-alpha", float(value) / 100),
        )
        slider.pack(padx=18)
        volume_enabled = tk.BooleanVar(value=self.config["volume_visible"])
        tk.Checkbutton(
            dialog, text="显示 K 线量能柱", variable=volume_enabled, bg=HEADER, fg=WHITE,
            activebackground=HEADER, activeforeground=WHITE, selectcolor=ROW_BG,
            font=("Microsoft YaHei UI", 9), anchor="w",
        ).pack(anchor="w", padx=18, pady=(2, 0))

        def cancel() -> None:
            self.root.attributes("-alpha", self.config["opacity"])
            dialog.destroy()
            self.settings_window = None

        def save() -> None:
            selected = [symbol for symbol, var in variables.items() if var.get()]
            if not 1 <= len(selected) <= MAX_SELECTED_ASSETS:
                messagebox.showwarning("自选行情", "请选择 1 到 8 项行情。", parent=dialog)
                return
            symbols_changed = selected != self.config["symbols"]
            self.config["symbols"] = selected
            if self.config["chart_symbol"] not in selected:
                self.config["chart_symbol"] = selected[0]
            if self.config["chart_symbol"] not in COINS and self.config["chart_period"] == "1s":
                self.config["chart_period"] = "1m"
            self.config["opacity"] = round(alpha.get() / 100, 2)
            self.config["volume_visible"] = bool(volume_enabled.get())
            self.config["position"] = [self.root.winfo_x(), self.root.winfo_y()]
            self._save_config()
            dialog.destroy()
            self.settings_window = None
            self.root.attributes("-alpha", self.config["opacity"])
            if not symbols_changed:
                self._update_volume_button()
                self._draw_chart()
                return
            self.quote_generation += 1
            self.fetching = False
            self.stock_fetching = False
            self.stock_last_fetch_start = 0.0
            self.stock_errors = {symbol: detail for symbol, detail in self.stock_errors.items() if symbol in selected}
            self.perp_fetching = False
            self.perp_last_fetch_start = 0.0
            self.perp_errors = {symbol: detail for symbol, detail in self.perp_errors.items() if symbol in selected}
            self._cancel_chart_motion()
            self.chart_preview = None
            self.chart_bars = []
            self.chart_source = initial_source(self.config["chart_symbol"])
            self._build_rows()
            self._set_geometry()
            self.latest = None
            self.chart_error = ""
            self.chart_last_ws_update = 0.0
            self.chart_last_rest = 0.0
            self._draw_chart()
            self._start_chart_websocket()
            self._request_chart(force=True)
            self.last_ws_update = 0.0
            self._start_websocket()
            self.refresh(manual=True)

        controls = tk.Frame(dialog, bg=HEADER)
        controls.pack(fill="x", padx=18, pady=(6, 0))
        tk.Button(controls, text="保存", command=save, bg=GREEN, fg=BG, relief="flat", width=10, font=("Microsoft YaHei UI", 9)).pack(side="right")
        tk.Button(controls, text="取消", command=cancel, bg=ROW_BG, fg=WHITE, relief="flat", width=10, font=("Microsoft YaHei UI", 9)).pack(side="right", padx=(0, 8))
        tk.Label(dialog, text="点击左侧代码切换 K 线；右键价格看来源", bg=HEADER, fg=MUTED, font=("Microsoft YaHei UI", 8)).pack(anchor="w", padx=18, pady=(10, 0))
        tk.Label(dialog, text="STRC 参考可能延时；MSTR 显示永续合约", bg=HEADER, fg=MUTED, font=("Microsoft YaHei UI", 8)).pack(anchor="w", padx=18, pady=(2, 0))
        tk.Label(dialog, text="Esc 隐藏  ·  Ctrl+Alt+M 显示/隐藏", bg=HEADER, fg=MUTED, font=("Microsoft YaHei UI", 8)).pack(anchor="w", padx=18, pady=(2, 0))
        dialog.protocol("WM_DELETE_WINDOW", cancel)

    def _write_error(self, detail: str) -> None:
        try:
            APP_DIR.mkdir(parents=True, exist_ok=True)
            with (APP_DIR / "error.log").open("a", encoding="utf-8") as log:
                log.write(f"{datetime.now().isoformat(timespec='seconds')} {detail}\n")
        except OSError:
            pass

    def _log_ui_error(self, error_type, error, traceback) -> None:
        import traceback as traceback_module

        self._write_error("".join(traceback_module.format_exception(error_type, error, traceback)))

    def close(self) -> None:
        if self.closing:
            return
        self.closing = True
        self._cancel_chart_motion()
        self.ws_stop.set()
        if self.ws_app:
            self.ws_app.close()
        self.chart_ws_stop.set()
        if self.chart_ws_app:
            self.chart_ws_app.close()
        if self.perp_stream:
            self.perp_stream.stop()
        if self.hotkey_thread_id and self.hotkey_enabled:
            ctypes.windll.user32.PostThreadMessageW(self.hotkey_thread_id, 0x0012, 0, 0)
        self.tray.stop()
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    MarketPeek().run()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        APP_DIR.mkdir(parents=True, exist_ok=True)
        (APP_DIR / "error.log").write_text(str(error), encoding="utf-8")
        ctypes.windll.user32.MessageBoxW(None, f"行情速览启动失败：\n{error}", "行情速览", 0x10)
        sys.exit(1)
