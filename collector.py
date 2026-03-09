"""
ZENDER COMMANDER TERMINAL — Data Collector
Этап 2+5: сбор данных + LLM-анализ (Claude API)
Запускается по расписанию, кладёт данные в Supabase.

ПЛАН: Hobbyist (80+ endpoints, 30 req/min).
Оптимизировано: 3 общих + 3 на монету, задержки между монетами.
"""

import asyncio
import logging
import httpx
import time
from datetime import datetime, timezone

from config import COINGLASS_API_KEY

# Anthropic API (для LLM-анализа)
try:
    from config import ANTHROPIC_KEY
    HAS_ANTHROPIC = bool(ANTHROPIC_KEY)
except (ImportError, AttributeError):
    ANTHROPIC_KEY = None
    HAS_ANTHROPIC = False

# Bitget API v2 (бесплатно, без ключа, работает из US серверов Railway)
# OKX, Binance, Bybit — НЕ работают из US, убраны
BITGET_BASE = "https://api.bitget.com"

# Kraken Futures (бесплатно, без ключа, работает в US — Kraken лицензирован в US)
KRAKEN_FUTURES_BASE = "https://futures.kraken.com/derivatives/api/v3"

# dYdX v4 Indexer (бесплатно, без ключа, DEX — нет гео-блокировки)
DYDX_BASE = "https://indexer.dydx.trade/v4"

# CoinCap (бесплатно, без ключа — fallback для цен если CoinGecko 429)
COINCAP_BASE = "https://api.coincap.io/v2"

# On-chain: бесплатные API (blockchain.info + BGeometrics + DeFiLlama), без ключа

from database import db

log = logging.getLogger(__name__)

# ── Coinglass API v4 ──────────────────────────────────────────────────────────
CG_BASE = "https://open-api-v4.coinglass.com"
CG_HEADERS = {
    "CG-API-KEY": COINGLASS_API_KEY,
    "Accept": "application/json",
}

# Монеты для сбора данных
COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX"]

# CoinGecko IDs
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "AVAX": "avalanche-2",
}


# ══════════════════════════════════════════════════════════════════════════════
# ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ — расчёт из исторических цен CoinGecko
# RSI(14), MACD(12,26,9), SMA50, SMA200 — стандартные формулы
# Для BTC данные приходят из BGeometrics, для остальных монет — считаем сами
# ══════════════════════════════════════════════════════════════════════════════

TECH_CACHE = {}  # {symbol: {"data": {...}, "ts": time.time()}}
TECH_CACHE_TTL = 30 * 60  # 30 минут — обновляем каждые 2 цикла бота (4 монеты × 1 req = мизер для CoinGecko)


def calc_rsi(prices: list[float], period: int = 14) -> float | None:
    """RSI(14) — Relative Strength Index. Стандартный Wilder's smoothing."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    # Первое среднее — простое
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Smoothed (Wilder's) — как на TradingView
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def calc_sma(prices: list[float], period: int) -> float | None:
    """Simple Moving Average."""
    if len(prices) < period:
        return None
    return round(sum(prices[-period:]) / period, 2)


def calc_ema(prices: list[float], period: int) -> list[float]:
    """Exponential Moving Average — возвращает весь массив EMA."""
    if len(prices) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]  # первое значение = SMA
    for i in range(period, len(prices)):
        ema.append((prices[i] - ema[-1]) * multiplier + ema[-1])
    return ema


def calc_macd(prices: list[float]) -> tuple[float | None, float | None]:
    """
    MACD(12, 26, 9) — стандартные параметры.
    Возвращает: (macd_line, signal_line)
    macd > 0 = бычий импульс, < 0 = медвежий
    """
    if len(prices) < 35:  # нужно 26 + 9 дней минимум
        return None, None
    ema12 = calc_ema(prices, 12)
    ema26 = calc_ema(prices, 26)

    # MACD line = EMA12 - EMA26 (выравниваем по длине)
    offset = len(ema12) - len(ema26)
    macd_line = []
    for i in range(len(ema26)):
        macd_line.append(ema12[i + offset] - ema26[i])

    if len(macd_line) < 9:
        return None, None

    # Signal line = EMA(9) от MACD line
    signal = calc_ema(macd_line, 9)

    if not signal:
        return round(macd_line[-1], 2), None

    return round(macd_line[-1], 2), round(signal[-1], 2)


async def fetch_tech_indicators(symbol: str) -> dict:
    """
    Загружает 200 дней цен из CoinGecko и считает RSI, MACD, SMA50, SMA200.
    Кэш 4 часа — дневные индикаторы не меняются часто.
    Для BTC пропускаем — данные приходят из BGeometrics.
    """
    if symbol == "BTC":
        return {}  # BTC получает из BGeometrics

    cache_key = f"tech_{symbol}"
    if cache_key in TECH_CACHE and (time.time() - TECH_CACHE[cache_key]["ts"]) < TECH_CACHE_TTL:
        return TECH_CACHE[cache_key]["data"]

    gecko_id = COINGECKO_IDS.get(symbol)
    if not gecko_id:
        return {}

    result = {}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Retry до 3 раз при 429 (CoinGecko rate limit)
            resp = None
            for attempt in range(3):
                resp = await client.get(
                    f"https://api.coingecko.com/api/v3/coins/{gecko_id}/market_chart",
                    params={"vs_currency": "usd", "days": "200", "interval": "daily"}
                )
                if resp.status_code == 429:
                    wait = 8 * (attempt + 1)  # 8, 16, 24 сек
                    log.warning(f"  ⚠️ CoinGecko 429 история {symbol} — ждём {wait}с (попытка {attempt+1}/3)")
                    await asyncio.sleep(wait)
                    continue
                break
            if resp is None or resp.status_code == 429:
                log.warning(f"  ❌ CoinGecko история {symbol}: 429 после 3 попыток")
                return {}
            if resp.status_code != 200:
                log.warning(f"  ⚠️ CoinGecko history {symbol}: HTTP {resp.status_code}")
                return {}

            data = resp.json()
            price_points = data.get("prices", [])
            if len(price_points) < 50:
                log.warning(f"  ⚠️ Tech {symbol}: мало данных ({len(price_points)} точек)")
                return {}

            # Извлекаем только цены (без timestamp)
            closes = [p[1] for p in price_points]

            # Считаем индикаторы
            rsi = calc_rsi(closes)
            if rsi is not None:
                result["rsi"] = rsi

            macd_val, macd_signal = calc_macd(closes)
            if macd_val is not None:
                result["macd"] = macd_val

            sma50 = calc_sma(closes, 50)
            if sma50 is not None:
                result["sma50"] = sma50

            sma200 = calc_sma(closes, 200)
            if sma200 is not None:
                result["sma200"] = sma200

            if result:
                TECH_CACHE[cache_key] = {"data": result, "ts": time.time()}
                log.info(f"  📊 Tech {symbol}: RSI={result.get('rsi','?')} | MACD={result.get('macd','?')} | SMA50={result.get('sma50','?')} | SMA200={result.get('sma200','?')}")
            else:
                log.warning(f"  ⚠️ Tech {symbol}: не удалось рассчитать индикаторы")

    except Exception as e:
        log.warning(f"Tech indicators {symbol} error: {e}")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def fmt_usd(value, compact=True):
    if value is None:
        return "—"
    try:
        v = float(value)
    except (ValueError, TypeError):
        return "—"
    if compact:
        if abs(v) >= 1_000_000_000:
            return f"${v / 1_000_000_000:.2f} млрд"
        elif abs(v) >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        elif abs(v) >= 1_000:
            return f"${v / 1_000:.1f}K"
    return f"${v:,.0f}"


def fmt_pct(value):
    if value is None:
        return "—"
    try:
        v = float(value)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}%"
    except (ValueError, TypeError):
        return "—"


def fmt_fr(value):
    if value is None:
        return "—"
    try:
        v = float(value)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.4f}%"
    except (ValueError, TypeError):
        return "—"


def fmt_price(value):
    if value is None:
        return "—"
    try:
        v = float(value)
        if v >= 1000:
            return f"${v:,.0f}"
        elif v >= 1:
            return f"${v:.2f}"
        else:
            return f"${v:.4f}"
    except (ValueError, TypeError):
        return "—"


# ══════════════════════════════════════════════════════════════════════════════
# API ЗАПРОСЫ
# ══════════════════════════════════════════════════════════════════════════════

async def cg_get(path: str, params: dict = None) -> dict | list | None:
    url = f"{CG_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=CG_HEADERS, params=params or {})
            resp.raise_for_status()
            body = resp.json()

            body_code = body.get("code")
            if body_code is not None and str(body_code) != "0":
                msg = body.get("msg", "unknown")
                if str(body_code) == "429":
                    log.warning(f"CG rate limit {path}")
                elif str(body_code) == "403":
                    log.warning(f"CG 403 (план не поддерживает) {path}")
                else:
                    log.warning(f"CG code={body_code} {path}: {msg}")
                return None

            if body.get("success") is False:
                log.warning(f"CG error {path}: {body.get('msg', 'unknown')}")
                return None

            return body.get("data")
    except httpx.HTTPStatusError as e:
        log.error(f"CG HTTP {path}: {e.response.status_code}")
        return None
    except Exception as e:
        log.error(f"CG request {path}: {e}")
        return None


async def fetch_prices() -> dict:
    ids = ",".join(COINGECKO_IDS.values())
    # Retry до 3 раз с задержкой (CoinGecko free tier: 429 при частых запросах)
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"}
                )
                if resp.status_code == 429:
                    wait = 5 * (attempt + 1)
                    log.warning(f"  ⚠️ CoinGecko 429 — ждём {wait}с (попытка {attempt+1}/3)")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                result = {}
                for symbol, gecko_id in COINGECKO_IDS.items():
                    if gecko_id in data:
                        result[symbol] = {
                            "price": data[gecko_id].get("usd"),
                            "change_24h": data[gecko_id].get("usd_24h_change"),
                        }
                return result
        except Exception as e:
            log.warning(f"CoinGecko price error (attempt {attempt+1}): {e}")
            if attempt < 2:
                await asyncio.sleep(5)
    log.warning("  ⚠️ CoinGecko: все 3 попытки провалились, пробуем CoinCap fallback...")
    return await fetch_prices_coincap()


# CoinCap IDs (маппинг наших символов)
COINCAP_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binance-coin",
    "AVAX": "avalanche",
}


async def fetch_prices_coincap() -> dict:
    """
    Fallback для цен: CoinCap API v2 (бесплатно, без ключа).
    Используется когда CoinGecko возвращает 429.
    """
    result = {}
    try:
        ids = ",".join(COINCAP_IDS.values())
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{COINCAP_BASE}/assets",
                params={"ids": ids}
            )
            if resp.status_code != 200:
                log.error(f"  ❌ CoinCap HTTP {resp.status_code}")
                return {}
            data = resp.json().get("data", [])
            # Обратный маппинг coincap_id → symbol
            reverse_map = {v: k for k, v in COINCAP_IDS.items()}
            for asset in data:
                asset_id = asset.get("id", "")
                symbol = reverse_map.get(asset_id)
                if symbol:
                    price = asset.get("priceUsd")
                    change = asset.get("changePercent24Hr")
                    result[symbol] = {
                        "price": float(price) if price else None,
                        "change_24h": float(change) if change else None,
                    }
            if result:
                log.info(f"  ✅ CoinCap fallback: получены цены для {list(result.keys())}")
            else:
                log.error("  ❌ CoinCap: пустой ответ")
    except Exception as e:
        log.error(f"  ❌ CoinCap error: {e}")
    return result


async def fetch_open_interest(symbol: str) -> dict:
    data = await cg_get("/api/futures/open-interest/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    all_item = None
    if isinstance(data, list):
        for item in data:
            if item.get("exchange") == "All":
                all_item = item
                break
        if not all_item and len(data) > 0:
            total_oi = sum(float(item.get("open_interest_usd", 0) or 0) for item in data)
            return {"oi": total_oi if total_oi > 0 else None, "oi_change_1h": None, "oi_change_24h": None}
    elif isinstance(data, dict):
        all_item = data

    if not all_item:
        return {}

    oi_usd = float(all_item.get("open_interest_usd", 0) or 0)
    oi_change_1h = all_item.get("open_interest_change_percent_1h")

    return {
        "oi": oi_usd if oi_usd > 0 else None,
        "oi_change_1h": float(oi_change_1h) if oi_change_1h is not None else None,
    }


async def fetch_funding_rate(symbol: str) -> dict:
    data = await cg_get("/api/futures/funding-rate/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    item = data[0] if isinstance(data, list) and len(data) > 0 else data
    if not isinstance(item, dict):
        return {}

    rates = []
    for entry in item.get("stablecoin_margin_list", []):
        fr = entry.get("funding_rate")
        if fr is not None:
            try:
                rates.append(float(fr))
            except (ValueError, TypeError):
                pass
    for entry in item.get("token_margin_list", []):
        fr = entry.get("funding_rate")
        if fr is not None:
            try:
                rates.append(float(fr))
            except (ValueError, TypeError):
                pass

    if not rates:
        return {}

    avg_rate = sum(rates) / len(rates)
    return {"funding_rate": avg_rate * 100}


async def fetch_long_short(symbol: str) -> dict:
    data = await cg_get("/api/futures/taker-buy-sell-volume/exchange-list", {
        "symbol": symbol,
        "range": "1h",
    })
    if data is None:
        log.warning(f"  L/S {symbol}: data is None")
        return {}

    target = None
    if isinstance(data, list):
        for item in data:
            if item.get("exchange") == "All":
                target = item
                break
        if not target and len(data) > 0:
            target = data[0]
    elif isinstance(data, dict):
        target = data

    if not target:
        return {}

    buy = target.get("buy_ratio")
    sell = target.get("sell_ratio")

    if buy is not None and sell is not None:
        try:
            buy_f = float(buy)
            sell_f = float(sell)
            if buy_f > 0 or sell_f > 0:
                return {"long_pct": round(buy_f, 1), "short_pct": round(sell_f, 1)}
        except (ValueError, TypeError):
            pass

    return {}


async def fetch_fear_greed() -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.alternative.me/fng/?limit=1")
            resp.raise_for_status()
            data = resp.json()
            item = data["data"][0]
            value = int(item["value"])
            label = item["value_classification"]

            label_ru = {
                "Extreme Fear": "сильный страх",
                "Fear": "страх",
                "Neutral": "нейтрально",
                "Greed": "жадность",
                "Extreme Greed": "сильная жадность",
            }.get(label, label)

            return {"fear_greed": value, "fear_greed_label": label_ru}
    except Exception as e:
        log.warning(f"Fear & Greed error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ON-CHAIN DATA (Blockchain.info + BGeometrics — бесплатно, без API ключа)
# ══════════════════════════════════════════════════════════════════════════════

# Blockchain.info Charts API — бесплатный, без ключа, только BTC
# Формат: https://api.blockchain.info/charts/{metric}?timespan=2days&format=json
# Возвращает: {"values": [{"x": timestamp, "y": value}, ...]}

# BGeometrics API — бесплатный, без ключа, BTC on-chain
# Формат: https://charts.bgeometrics.com/data/{metric}.json

BLOCKCHAIN_INFO_BASE = "https://api.blockchain.info/charts"
BGEOMETRICS_BASE = "https://bitcoin-data.com/v1"  # BGeometrics API v1 (бесплатно, 8 req/hour)

# ── BGeometrics кэш ──────────────────────────────────────────────────────────
# Лимит: 8 req/hour, 15 req/day!
# Стратегия: кэшируем на 6 часов, 3-4 метрики × 4 обновления/день = 12-16 req/day
# On-chain данные обновляются раз в день — кэш 6ч более чем достаточно
BGEOMETRICS_CACHE = {}  # {metric: {"data": ..., "ts": time.time()}}
BGEOMETRICS_CACHE_TTL = 6 * 3600  # 6 часов в секундах

# Метрики BGeometrics для сбора (правильные названия из документации!)
BGEOMETRICS_METRICS = [
    "sopr",                  # SOPR (>1 прибыль, <1 убыток)
    "exchange-reserve-btc",  # Общий баланс BTC на биржах
    "exchange-netflow-btc",  # Нетто поток: inflow - outflow
    "technical-indicators",  # RSI, MACD, SMA, EMA (всё в одном!)
]


def bgeometrics_cache_valid(metric: str) -> bool:
    """Проверяет, актуален ли кэш для метрики."""
    if metric not in BGEOMETRICS_CACHE:
        return False
    cached = BGEOMETRICS_CACHE[metric]
    return (time.time() - cached["ts"]) < BGEOMETRICS_CACHE_TTL


async def blockchain_chart_get(metric: str) -> list | None:
    """Запрос к Blockchain.info Charts API. Возвращает values или None."""
    url = f"{BLOCKCHAIN_INFO_BASE}/{metric}"
    params = {"timespan": "2days", "format": "json", "rollingAverage": "24hours"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                log.warning(f"Blockchain.info {resp.status_code} {metric}")
                return None
            data = resp.json()
            return data.get("values", [])
    except Exception as e:
        log.error(f"Blockchain.info {metric}: {e}")
        return None


async def bgeometrics_get(metric: str, last: bool = False) -> dict | list | None:
    """
    Запрос к BGeometrics API v1 (bitcoin-data.com).
    metric: 'sopr', 'exchange-reserves', etc.
    last=True → получить только последнее значение (/metric/{last})
    Формат ответа: {"d": "2026-03-08", "unixTs": 1741..., "sopr": 1.02, ...}
    """
    suffix = f"/{metric}/last" if last else f"/{metric}"
    url = f"{BGEOMETRICS_BASE}{suffix}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                log.warning(f"BGeometrics {resp.status_code} {metric}")
                return None
            data = resp.json()
            return data
    except Exception as e:
        log.error(f"BGeometrics {metric}: {e}")
        return None


def parse_chart_values(values: list | None) -> tuple:
    """Извлекает последнее значение и % изменения из массива [{x,y}]."""
    if not values or not isinstance(values, list) or len(values) < 1:
        return None, None
    try:
        latest = values[-1]
        if isinstance(latest, dict):
            val = latest.get("y") or latest.get("v") or latest.get("value")
        elif isinstance(latest, (list, tuple)) and len(latest) >= 2:
            val = latest[1]
        else:
            val = None

        if val is None:
            return None, None
        val = float(val)

        change = None
        if len(values) >= 2:
            prev_entry = values[-2]
            if isinstance(prev_entry, dict):
                prev = prev_entry.get("y") or prev_entry.get("v") or prev_entry.get("value")
            elif isinstance(prev_entry, (list, tuple)) and len(prev_entry) >= 2:
                prev = prev_entry[1]
            else:
                prev = None
            if prev and float(prev) > 0:
                change = round(((val - float(prev)) / float(prev)) * 100, 2)

        return val, change
    except (ValueError, TypeError, IndexError):
        return None, None


async def fetch_bgeometrics_batch():
    """
    Загружает все BGeometrics метрики с кэшированием.
    Вызывается раз за цикл collect_all(), но реально делает HTTP-запросы
    только если кэш устарел (каждые 6 часов).
    Лимит: 8 req/hour, 15 req/day — кэш экономит запросы.
    """
    metrics_to_fetch = [m for m in BGEOMETRICS_METRICS if not bgeometrics_cache_valid(m)]

    if not metrics_to_fetch:
        log.info("  🔗 BGeometrics: все метрики в кэше (ещё актуальны)")
        return

    log.info(f"  🔗 BGeometrics: обновляем {len(metrics_to_fetch)} метрик: {metrics_to_fetch}")

    for metric in metrics_to_fetch:
        try:
            data = await bgeometrics_get(metric, last=True)
            if data:
                BGEOMETRICS_CACHE[metric] = {"data": data, "ts": time.time()}
                log.info(f"  ✅ BGeometrics {metric}: OK")
            else:
                log.warning(f"  ⚠️ BGeometrics {metric}: пустой ответ")
            # Задержка между запросами (8 req/hour = 1 каждые 7.5 мин)
            await asyncio.sleep(2)
        except Exception as e:
            log.error(f"  ❌ BGeometrics {metric}: {e}")


def get_cached_bgeometrics(metric: str) -> dict | list | None:
    """Получает данные из кэша BGeometrics."""
    if metric in BGEOMETRICS_CACHE:
        return BGEOMETRICS_CACHE[metric]["data"]
    return None


# ══════════════════════════════════════════════════════════════════════════════
# COINGLASS INDICATORS (уже платим $95/мес STARTUP — используем больше endpoints!)
# ══════════════════════════════════════════════════════════════════════════════

# Кэш для Coinglass индикаторов (обновляются раз в день)
CG_INDICATOR_CACHE = {}  # {name: {"data": ..., "ts": time.time()}}
CG_INDICATOR_CACHE_TTL = 4 * 3600  # 4 часа — индикаторы медленно меняются


async def fetch_cg_indicators():
    """
    Загружает индикаторы из Coinglass API с кэшированием.
    Bull Market Peak, AHR999, Bitcoin Bubble Index, ETF flows.
    Эти эндпоинты могут быть доступны на STARTUP ($95/мес).
    """
    # ПРИМЕЧАНИЕ: все 4 эндпоинта возвращают 404 на STARTUP ($95/мес).
    # Доступны только на PROFESSIONAL ($195/мес+).
    # Оставляем код на будущее, но пока пропускаем чтобы не тратить запросы.
    endpoints = {
        # "bull_market_peak": "/api/indicator/bull-market-peak",  # 404 на STARTUP
        # "ahr999": "/api/indicator/ahr999",                      # 404 на STARTUP
        # "bitcoin_bubble": "/api/indicator/bitcoin-bubble-index", # 404 на STARTUP
        # "btc_etf": "/api/bitcoin-etf/list",                     # 404 на STARTUP
    }

    for name, path in endpoints.items():
        # Проверяем кэш
        if name in CG_INDICATOR_CACHE:
            cached = CG_INDICATOR_CACHE[name]
            if (time.time() - cached["ts"]) < CG_INDICATOR_CACHE_TTL:
                continue  # Кэш актуален

        try:
            data = await cg_get(path)
            if data is not None:
                CG_INDICATOR_CACHE[name] = {"data": data, "ts": time.time()}
                log.info(f"  📊 CG Indicator {name}: OK")
            else:
                log.warning(f"  ⚠️ CG Indicator {name}: нет данных (возможно не доступен на STARTUP)")
        except Exception as e:
            log.error(f"  ❌ CG Indicator {name}: {e}")
        await asyncio.sleep(1)  # Пауза между запросами


def parse_cg_indicators() -> dict:
    """Парсит кэшированные Coinglass индикаторы в удобный формат."""
    result = {}

    # Bull Market Peak — массив индикаторов
    bmp = CG_INDICATOR_CACHE.get("bull_market_peak", {}).get("data")
    if bmp and isinstance(bmp, list):
        hit_count = 0
        total = 0
        for ind in bmp:
            if isinstance(ind, dict):
                total += 1
                if ind.get("isHit"):
                    hit_count += 1
        if total > 0:
            result["bull_peak_ratio"] = f"{hit_count}/{total}"
            result["bull_peak_pct"] = round(hit_count / total * 100)
            log.info(f"  🔝 Bull Market Peak: {hit_count}/{total} индикаторов сработали")

    # AHR999 — индекс для определения зоны покупки BTC
    ahr = CG_INDICATOR_CACHE.get("ahr999", {}).get("data")
    if ahr:
        # Может быть список или dict
        if isinstance(ahr, list) and len(ahr) > 0:
            last = ahr[-1] if isinstance(ahr[-1], dict) else {}
            ahr999_val = last.get("ahr999") or last.get("value")
        elif isinstance(ahr, dict):
            ahr999_val = ahr.get("ahr999") or ahr.get("value")
        else:
            ahr999_val = None
        if ahr999_val is not None:
            try:
                result["ahr999"] = round(float(ahr999_val), 3)
                # AHR999 < 0.45 = зона покупки, > 1.2 = зона продажи
                log.info(f"  📊 AHR999: {result['ahr999']}")
            except (ValueError, TypeError):
                pass

    # Bitcoin Bubble Index
    bubble = CG_INDICATOR_CACHE.get("bitcoin_bubble", {}).get("data")
    if bubble:
        if isinstance(bubble, list) and len(bubble) > 0:
            last = bubble[-1] if isinstance(bubble[-1], dict) else {}
            bubble_val = last.get("index") or last.get("value") or last.get("bubbleIndex")
        elif isinstance(bubble, dict):
            bubble_val = bubble.get("index") or bubble.get("value") or bubble.get("bubbleIndex")
        else:
            bubble_val = None
        if bubble_val is not None:
            try:
                result["bitcoin_bubble"] = round(float(bubble_val), 1)
                log.info(f"  🫧 Bitcoin Bubble Index: {result['bitcoin_bubble']}")
            except (ValueError, TypeError):
                pass

    # BTC ETF flows
    etf = CG_INDICATOR_CACHE.get("btc_etf", {}).get("data")
    if etf and isinstance(etf, list):
        # Суммируем потоки всех ETF
        total_netflow = 0
        has_data = False
        for item in etf:
            if isinstance(item, dict):
                nf = item.get("netFlow") or item.get("net_flow") or item.get("netflow")
                if nf is not None:
                    try:
                        total_netflow += float(nf)
                        has_data = True
                    except (ValueError, TypeError):
                        pass
        if has_data:
            result["etf_netflow"] = round(total_netflow, 2)
            direction = "приток" if total_netflow > 0 else "отток"
            log.info(f"  💰 BTC ETF Netflow: ${total_netflow:,.0f} ({direction})")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# DEFI LLAMA (бесплатно, без API ключа!)
# ══════════════════════════════════════════════════════════════════════════════

DEFILLAMA_CACHE = {}
DEFILLAMA_CACHE_TTL = 2 * 3600  # 2 часа


async def fetch_defillama_data() -> dict:
    """
    Бесплатные данные из DeFiLlama:
    - Stablecoins market cap (USDT, USDC, etc.)
    - Общий TVL DeFi
    """
    result = {}

    # ── Stablecoins общая капитализация ──
    if "stablecoins" in DEFILLAMA_CACHE and (time.time() - DEFILLAMA_CACHE["stablecoins"]["ts"]) < DEFILLAMA_CACHE_TTL:
        return DEFILLAMA_CACHE["stablecoins"]["data"]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Stablecoins overview
            resp = await client.get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
            if resp.status_code == 200:
                data = resp.json()
                stables = data.get("peggedAssets", [])
                total_mcap = 0
                for s in stables:
                    # Суммируем market cap всех стейблкоинов
                    chains = s.get("chainCirculating", {})
                    for chain_data in chains.values():
                        current = chain_data.get("current", {})
                        peggedUSD = current.get("peggedUSD", 0)
                        if peggedUSD:
                            try:
                                total_mcap += float(peggedUSD)
                            except (ValueError, TypeError):
                                pass
                if total_mcap > 0:
                    result["stablecoin_mcap"] = total_mcap
                    log.info(f"  💵 Stablecoin Market Cap: ${total_mcap/1e9:.1f}B")

            await asyncio.sleep(1)

            # Общий TVL DeFi
            resp2 = await client.get("https://api.llama.fi/v2/historicalChainTvl")
            if resp2.status_code == 200:
                tvl_data = resp2.json()
                if isinstance(tvl_data, list) and len(tvl_data) > 0:
                    last_tvl = tvl_data[-1]
                    tvl_val = last_tvl.get("tvl")
                    if tvl_val:
                        result["defi_tvl"] = float(tvl_val)
                        log.info(f"  🏦 DeFi TVL: ${float(tvl_val)/1e9:.1f}B")

                    # Изменение TVL за день
                    if len(tvl_data) >= 2:
                        prev_tvl = tvl_data[-2].get("tvl")
                        if prev_tvl and float(prev_tvl) > 0:
                            tvl_change = ((float(tvl_val) - float(prev_tvl)) / float(prev_tvl)) * 100
                            result["defi_tvl_change"] = round(tvl_change, 2)

    except Exception as e:
        log.warning(f"DeFiLlama error: {e}")

    if result:
        DEFILLAMA_CACHE["stablecoins"] = {"data": result, "ts": time.time()}

    return result


# ══════════════════════════════════════════════════════════════════════════════
# BITGET (бесплатно, без ключа — работает из US серверов Railway)
# OKX, Binance, Bybit — ВСЕ блокируют US серверы (451/403/400)
# ══════════════════════════════════════════════════════════════════════════════

CROSS_EXCHANGE_CACHE = {}
CROSS_EXCHANGE_TTL = 15 * 60  # 15 минут


# Пары которые поддерживают L/S на Bitget (BNB, AVAX — не поддерживаются)
BITGET_LS_SUPPORTED = {"BTC", "ETH", "SOL"}


async def fetch_bitget_ls(symbol: str) -> dict:
    """
    Bitget: Long/Short Account Ratio + Position Ratio.
    Бесплатно, без ключа, public endpoint. 1 req/s rate limit.
    Поддерживает: BTC, ETH, SOL. BNB/AVAX — нет L/S данных.
    """
    if symbol not in BITGET_LS_SUPPORTED:
        return {}

    cache_key = f"bitget_ls_{symbol}"
    if cache_key in CROSS_EXCHANGE_CACHE and (time.time() - CROSS_EXCHANGE_CACHE[cache_key]["ts"]) < CROSS_EXCHANGE_TTL:
        return CROSS_EXCHANGE_CACHE[cache_key]["data"]

    result = {}
    pair = f"{symbol}USDT"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Account Long/Short Ratio
            resp_acc = await client.get(
                f"{BITGET_BASE}/api/v2/mix/market/account-long-short",
                params={"symbol": pair, "productType": "USDT-FUTURES", "period": "1h"}
            )
            # Задержка (rate limit 1/s)
            await asyncio.sleep(1.1)
            # Position Long/Short Ratio
            resp_pos = await client.get(
                f"{BITGET_BASE}/api/v2/mix/market/position-long-short",
                params={"symbol": pair, "productType": "USDT-FUTURES", "period": "1h"}
            )

            if resp_acc.status_code == 200:
                data = resp_acc.json()
                items = data.get("data", [])
                if items:
                    lr = float(items[0].get("longAccountRatio", 0))
                    sr = float(items[0].get("shortAccountRatio", 0))
                    result["bitget_long_acc"] = round(lr * 100, 1)
                    result["bitget_short_acc"] = round(sr * 100, 1)

            if resp_pos.status_code == 200:
                data = resp_pos.json()
                items = data.get("data", [])
                if items:
                    lr = float(items[0].get("longPositionRatio", 0))
                    sr = float(items[0].get("shortPositionRatio", 0))
                    result["bitget_long_pos"] = round(lr * 100, 1)
                    result["bitget_short_pos"] = round(sr * 100, 1)

            if result:
                log.info(f"  📊 Bitget {symbol}: AccL/S={result.get('bitget_long_acc','?')}%/{result.get('bitget_short_acc','?')}% | PosL/S={result.get('bitget_long_pos','?')}%/{result.get('bitget_short_pos','?')}%")

    except Exception as e:
        log.warning(f"Bitget LS {symbol} error: {e}")

    if result:
        CROSS_EXCHANGE_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


async def fetch_bitget_oi(symbol: str) -> dict:
    """
    Bitget: Open Interest.
    Бесплатно, без ключа. Для сравнения с Coinglass.
    """
    cache_key = f"bitget_oi_{symbol}"
    if cache_key in CROSS_EXCHANGE_CACHE and (time.time() - CROSS_EXCHANGE_CACHE[cache_key]["ts"]) < CROSS_EXCHANGE_TTL:
        return CROSS_EXCHANGE_CACHE[cache_key]["data"]

    result = {}
    pair = f"{symbol}USDT"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{BITGET_BASE}/api/v2/mix/market/open-interest",
                params={"symbol": pair, "productType": "USDT-FUTURES"}
            )
            if resp.status_code == 200:
                data = resp.json()
                oi_data = data.get("data", {})
                if oi_data:
                    oi_val = float(oi_data.get("openInterestUsd", 0))
                    if oi_val > 0:
                        result["bitget_oi_usd"] = oi_val
                        log.info(f"  📊 Bitget OI {symbol}: ${oi_val/1e6:.1f}M")

    except Exception as e:
        log.warning(f"Bitget OI {symbol} error: {e}")

    if result:
        CROSS_EXCHANGE_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


# ══════════════════════════════════════════════════════════════════════════════
# KRAKEN FUTURES (бесплатно, без ключа, US-лицензирован — работает из Railway)
# ══════════════════════════════════════════════════════════════════════════════

# Маппинг наших символов на Kraken Futures тикеры
KRAKEN_FUTURES_SYMBOLS = {
    "BTC": "PF_XBTUSD",
    "ETH": "PF_ETHUSD",
    "SOL": "PF_SOLUSD",
    # BNB и AVAX не торгуются на Kraken Futures
}

KRAKEN_CACHE = {}
KRAKEN_CACHE_TTL = 15 * 60  # 15 минут


async def fetch_kraken_futures(symbol: str) -> dict:
    """
    Kraken Futures: Funding Rate + Open Interest.
    Бесплатно, без ключа. Kraken лицензирован в US — нет гео-блокировки.
    Поддерживает: BTC, ETH, SOL.
    """
    if symbol not in KRAKEN_FUTURES_SYMBOLS:
        return {}

    cache_key = f"kraken_{symbol}"
    if cache_key in KRAKEN_CACHE and (time.time() - KRAKEN_CACHE[cache_key]["ts"]) < KRAKEN_CACHE_TTL:
        return KRAKEN_CACHE[cache_key]["data"]

    result = {}
    ticker = KRAKEN_FUTURES_SYMBOLS[symbol]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{KRAKEN_FUTURES_BASE}/tickers")
            if resp.status_code != 200:
                log.warning(f"Kraken Futures HTTP {resp.status_code}")
                return {}
            data = resp.json()
            tickers = data.get("tickers", [])
            for t in tickers:
                if t.get("symbol") == ticker:
                    # Funding Rate — ПРОПУСКАЕМ
                    # Kraken возвращает ВСЕ funding rates как annualized (годовые):
                    # fundingRate и fundingRatePrediction — оба годовые
                    # BTC: -0.59 (= -59% годовых), ETH: -0.006 (= -0.6% годовых)
                    # Не конвертируем — слишком ненадёжно. FR берём из Coinglass + dYdX.

                    # Open Interest — БЕРЁМ (данные корректные)
                    oi = t.get("openInterest")
                    if oi is not None:
                        result["kraken_oi"] = float(oi)

                    # Mark Price (для сравнения)
                    mark = t.get("markPrice")
                    if mark is not None:
                        result["kraken_mark_price"] = float(mark)

                    # Volume 24h
                    vol = t.get("vol24h")
                    if vol is not None:
                        result["kraken_volume_24h"] = float(vol)

                    break

            if result:
                log.info(f"  📊 Kraken {symbol}: FR={result.get('kraken_funding','?')}% | OI={result.get('kraken_oi','?')}")

    except Exception as e:
        log.warning(f"Kraken Futures {symbol} error: {e}")

    if result:
        KRAKEN_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


# ══════════════════════════════════════════════════════════════════════════════
# dYdX v4 INDEXER (бесплатно, без ключа, DEX — нет гео-блокировки)
# ══════════════════════════════════════════════════════════════════════════════

# Маппинг наших символов на dYdX perpetual markets
DYDX_SYMBOLS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "AVAX": "AVAX-USD",
    # BNB не торгуется на dYdX
}

DYDX_CACHE = {}
DYDX_CACHE_TTL = 15 * 60  # 15 минут


async def fetch_dydx_data(symbol: str) -> dict:
    """
    dYdX v4 Indexer: Open Interest + Funding Rate.
    Бесплатно, без ключа. DEX — нет гео-блокировки, работает отовсюду.
    Поддерживает: BTC, ETH, SOL, AVAX.
    """
    if symbol not in DYDX_SYMBOLS:
        return {}

    cache_key = f"dydx_{symbol}"
    if cache_key in DYDX_CACHE and (time.time() - DYDX_CACHE[cache_key]["ts"]) < DYDX_CACHE_TTL:
        return DYDX_CACHE[cache_key]["data"]

    result = {}
    market = DYDX_SYMBOLS[symbol]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{DYDX_BASE}/perpetualMarkets")
            if resp.status_code != 200:
                log.warning(f"dYdX HTTP {resp.status_code}")
                return {}
            data = resp.json()
            markets = data.get("markets", {})
            m = markets.get(market)
            if m:
                # Open Interest
                oi = m.get("openInterest")
                if oi is not None:
                    result["dydx_oi"] = float(oi)

                # Next Funding Rate (для сравнения с CEX)
                fr = m.get("nextFundingRate")
                if fr is not None:
                    result["dydx_funding"] = float(fr) * 100  # в проценты

                # Volume 24h
                vol = m.get("volume24H")
                if vol is not None:
                    result["dydx_volume_24h"] = float(vol)

                # Oracle Price
                oracle = m.get("oraclePrice")
                if oracle is not None:
                    result["dydx_oracle_price"] = float(oracle)

            if result:
                log.info(f"  📊 dYdX {symbol}: FR={result.get('dydx_funding','?')}% | OI={result.get('dydx_oi','?')}")

    except Exception as e:
        log.warning(f"dYdX {symbol} error: {e}")

    if result:
        DYDX_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


async def fetch_onchain_data(symbol: str) -> dict:
    """
    On-chain метрики для BTC (бесплатные API, без ключа).
    Blockchain.info: активные адреса, транзакции.
    BGeometrics (кэш): SOPR, Exchange Reserve, Exchange Netflow, RSI/MACD.
    Для не-BTC монет — пустой dict.
    """
    if symbol != "BTC":
        return {}

    result = {}

    # ── Blockchain.info: адреса + транзакции ──
    try:
        active_data, tx_data = await asyncio.gather(
            blockchain_chart_get("n-unique-addresses"),
            blockchain_chart_get("n-transactions"),
            return_exceptions=True,
        )
    except Exception as e:
        log.warning(f"On-chain gather error: {e}")
        return {}

    if isinstance(active_data, Exception):
        active_data = None
    if isinstance(tx_data, Exception):
        tx_data = None

    val, change = parse_chart_values(active_data)
    if val:
        result["active_addresses"] = int(val)
        if change is not None:
            result["active_addresses_change"] = change

    val, change = parse_chart_values(tx_data)
    if val:
        result["tx_count"] = int(val)

    # ── BGeometrics: SOPR (из кэша) ──
    try:
        sopr_data = get_cached_bgeometrics("sopr")
        if sopr_data and isinstance(sopr_data, dict):
            sopr_val = sopr_data.get("sopr")
            if sopr_val is not None:
                result["sopr"] = float(sopr_val)
                log.info(f"  📊 SOPR BTC: {sopr_val}")
        elif sopr_data and isinstance(sopr_data, list) and len(sopr_data) > 0:
            last_item = sopr_data[-1]
            sopr_val = last_item.get("sopr") if isinstance(last_item, dict) else None
            if sopr_val is not None:
                result["sopr"] = float(sopr_val)
    except Exception as e:
        log.warning(f"SOPR parse error: {e}")

    # ── BGeometrics: Exchange Reserve BTC (из кэша) ──
    try:
        reserve_data = get_cached_bgeometrics("exchange-reserve-btc")
        if reserve_data and isinstance(reserve_data, dict):
            # Ожидаемый формат: {"d": "2026-03-08", "exchangeReserveBtc": 2345678.12}
            reserve_val = reserve_data.get("exchangeReserveBtc") or reserve_data.get("exchange_reserve_btc") or reserve_data.get("value")
            if reserve_val is not None:
                result["exchange_reserve_btc"] = float(reserve_val)
                log.info(f"  🏦 Exchange Reserve BTC: {float(reserve_val):,.0f}")
    except Exception as e:
        log.warning(f"Exchange Reserve parse error: {e}")

    # ── BGeometrics: Exchange Netflow BTC (из кэша) ──
    try:
        netflow_data = get_cached_bgeometrics("exchange-netflow-btc")
        if netflow_data and isinstance(netflow_data, dict):
            # Ожидаемый формат: {"d": "...", "exchangeNetflowBtc": -1234.56}
            netflow_val = netflow_data.get("exchangeNetflowBtc") or netflow_data.get("exchange_netflow_btc") or netflow_data.get("value")
            if netflow_val is not None:
                result["exchange_netflow_btc"] = float(netflow_val)
                direction = "📤 выводят (бычий)" if float(netflow_val) < 0 else "📥 заводят (медвежий)"
                log.info(f"  🔄 Exchange Netflow BTC: {float(netflow_val):,.2f} — {direction}")
    except Exception as e:
        log.warning(f"Exchange Netflow parse error: {e}")

    # ── BGeometrics: Technical Indicators — RSI, MACD, SMA, EMA (из кэша) ──
    try:
        tech_data = get_cached_bgeometrics("technical-indicators")
        if tech_data and isinstance(tech_data, dict):
            # Ожидаемый формат: {"d": "...", "rsi": 45.2, "macd": ..., "sma7": ..., ...}
            rsi_val = tech_data.get("rsi") or tech_data.get("RSI")
            if rsi_val is not None:
                result["rsi"] = float(rsi_val)
                log.info(f"  📈 RSI BTC: {float(rsi_val):.1f}")

            macd_val = tech_data.get("macd") or tech_data.get("MACD")
            if macd_val is not None:
                result["macd"] = float(macd_val)

            sma50_val = tech_data.get("sma50") or tech_data.get("SMA50")
            if sma50_val is not None:
                result["sma50"] = float(sma50_val)

            sma200_val = tech_data.get("sma200") or tech_data.get("SMA200")
            if sma200_val is not None:
                result["sma200"] = float(sma200_val)
    except Exception as e:
        log.warning(f"Technical Indicators parse error: {e}")

    if result:
        log.info(f"  🔗 On-chain BTC: {list(result.keys())}")
    else:
        log.info(f"  🔗 On-chain BTC: no data (APIs may be unavailable)")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# LLM-АНАЛИЗ (Claude API)
# ══════════════════════════════════════════════════════════════════════════════

async def generate_llm_analysis(symbol: str, coin_data: dict) -> dict:
    """
    Отправляет метрики монеты в Claude API.
    Возвращает: {llm_text, recommendation, buy_zone, sell_zone}
    """
    if not HAS_ANTHROPIC:
        return {}

    price = coin_data.get("price")
    change = coin_data.get("change_24h")
    oi = coin_data.get("oi")
    oi_change = coin_data.get("oi_change_1h")
    fr = coin_data.get("funding_rate")
    long_pct = coin_data.get("long_pct")
    short_pct = coin_data.get("short_pct")
    liq_long = coin_data.get("liq_long")
    liq_short = coin_data.get("liq_short")
    fg = coin_data.get("fear_greed")
    fg_label = coin_data.get("fear_greed_label")
    mkt_liq_long = coin_data.get("mkt_liq_long")
    mkt_liq_short = coin_data.get("mkt_liq_short")

    # Безопасное форматирование (None → "нет данных")
    def safe_usd(v):
        try:
            return f"${float(v):,.0f}"
        except (TypeError, ValueError):
            return "нет данных"

    def safe_pct(v):
        try:
            val = float(v)
            return f"{'+' if val > 0 else ''}{val:.2f}%"
        except (TypeError, ValueError):
            return "нет данных"

    # On-chain данные
    active_addr = coin_data.get("active_addresses")
    active_addr_change = coin_data.get("active_addresses_change")
    exchange_reserve = coin_data.get("exchange_reserve_btc")
    exchange_netflow = coin_data.get("exchange_netflow_btc")
    sopr = coin_data.get("sopr")
    rsi = coin_data.get("rsi")
    macd = coin_data.get("macd")
    sma50 = coin_data.get("sma50")
    sma200 = coin_data.get("sma200")

    # Новые индикаторы
    ahr999 = coin_data.get("ahr999")
    bull_peak_ratio = coin_data.get("bull_peak_ratio")
    bull_peak_pct = coin_data.get("bull_peak_pct")
    bitcoin_bubble = coin_data.get("bitcoin_bubble")
    etf_netflow = coin_data.get("etf_netflow")
    stablecoin_mcap = coin_data.get("stablecoin_mcap")
    defi_tvl = coin_data.get("defi_tvl")
    defi_tvl_change = coin_data.get("defi_tvl_change")
    # Cross-exchange данные (Bitget + Kraken + dYdX)
    bitget_long_acc = coin_data.get("bitget_long_acc")
    bitget_short_acc = coin_data.get("bitget_short_acc")
    bitget_long_pos = coin_data.get("bitget_long_pos")
    bitget_short_pos = coin_data.get("bitget_short_pos")
    bitget_oi_usd = coin_data.get("bitget_oi_usd")
    kraken_funding = coin_data.get("kraken_funding")
    kraken_oi = coin_data.get("kraken_oi")
    dydx_funding = coin_data.get("dydx_funding")
    dydx_oi = coin_data.get("dydx_oi")

    # Блок on-chain + технических данных для промпта
    onchain_block = ""
    onchain_lines = []
    if any([active_addr, exchange_reserve, exchange_netflow, sopr]):
        onchain_lines.append(f"\nON-CHAIN ДАННЫЕ (BTC блокчейн):")
        if active_addr:
            onchain_lines.append(f"- Активные адреса: {active_addr:,} ({safe_pct(active_addr_change)} за день)")
        if exchange_reserve:
            onchain_lines.append(f"- Резерв BTC на биржах: {exchange_reserve:,.0f} BTC")
        if exchange_netflow is not None:
            direction = "ОТТОК с бирж (бычий — холдят)" if exchange_netflow < 0 else "ПРИТОК на биржи (медвежий — готовятся продавать)" if exchange_netflow > 0 else "баланс"
            onchain_lines.append(f"- Нетто поток бирж: {exchange_netflow:,.2f} BTC — {direction}")
        if sopr:
            sopr_hint = "продают в прибыль" if sopr > 1 else "продают в убыток (капитуляция)" if sopr < 1 else "безубыток"
            onchain_lines.append(f"- SOPR: {sopr} — {sopr_hint}")

    # Технические индикаторы
    if any([rsi, macd, sma50, sma200]):
        onchain_lines.append(f"\nТЕХНИЧЕСКИЕ ИНДИКАТОРЫ:")
        if rsi is not None:
            rsi_hint = "перекуплен (>70)" if rsi > 70 else "перепродан (<30)" if rsi < 30 else "нейтральная зона"
            onchain_lines.append(f"- RSI: {rsi:.1f} — {rsi_hint}")
        if macd is not None:
            macd_hint = "бычий импульс" if macd > 0 else "медвежий импульс"
            onchain_lines.append(f"- MACD: {macd:.2f} — {macd_hint}")
        if sma50 and sma200:
            if sma50 > sma200:
                onchain_lines.append(f"- SMA50/200: golden cross (SMA50 ${sma50:,.0f} > SMA200 ${sma200:,.0f}) — бычий")
            else:
                onchain_lines.append(f"- SMA50/200: death cross (SMA50 ${sma50:,.0f} < SMA200 ${sma200:,.0f}) — медвежий")

    # Макро/индикаторы блок
    if any([ahr999, bull_peak_ratio, bitcoin_bubble, etf_netflow, stablecoin_mcap, defi_tvl]):
        onchain_lines.append(f"\nМАКРО ИНДИКАТОРЫ:")
        if ahr999 is not None:
            ahr_hint = "зона накопления (покупка)" if ahr999 < 0.45 else "переоценён (продажа)" if ahr999 > 1.2 else "нормальная зона"
            onchain_lines.append(f"- AHR999: {ahr999} — {ahr_hint}")
        if bull_peak_ratio:
            onchain_lines.append(f"- Bull Market Peak: {bull_peak_ratio} индикаторов сработали ({bull_peak_pct}%)")
        if bitcoin_bubble is not None:
            onchain_lines.append(f"- Bitcoin Bubble Index: {bitcoin_bubble}")
        if etf_netflow is not None:
            etf_dir = "приток (институционалы покупают)" if etf_netflow > 0 else "отток (институционалы продают)"
            onchain_lines.append(f"- BTC ETF Netflow: ${etf_netflow:,.0f} — {etf_dir}")
        if stablecoin_mcap is not None:
            onchain_lines.append(f"- Стейблкоины (общ. капитализация): ${stablecoin_mcap/1e9:.1f}B")
        if defi_tvl is not None:
            tvl_hint = f" ({'+' if defi_tvl_change > 0 else ''}{defi_tvl_change:.1f}% за день)" if defi_tvl_change else ""
            onchain_lines.append(f"- DeFi TVL: ${defi_tvl/1e9:.1f}B{tvl_hint}")

    # Cross-exchange блок (Bitget + Kraken + dYdX vs Coinglass)
    if any([bitget_long_acc, bitget_long_pos, kraken_funding, dydx_funding]):
        onchain_lines.append(f"\nCROSS-EXCHANGE ДАННЫЕ (мульти-биржевое сравнение):")
        if bitget_long_acc is not None:
            hint = "ритейл в лонгах" if bitget_long_acc > 60 else "ритейл в шортах" if bitget_long_acc < 40 else "баланс"
            onchain_lines.append(f"- Bitget аккаунты: L {bitget_long_acc}% / S {bitget_short_acc}% — {hint}")
        if bitget_long_pos is not None:
            hint_pos = "позиции в лонгах" if bitget_long_pos > 55 else "позиции в шортах" if bitget_long_pos < 45 else "баланс"
            onchain_lines.append(f"- Bitget позиции: L {bitget_long_pos}% / S {bitget_short_pos}% — {hint_pos}")
        if bitget_oi_usd:
            onchain_lines.append(f"- Bitget OI: ${bitget_oi_usd/1e6:.1f}M")
        if kraken_funding is not None:
            kr_hint = "лонги платят (перегрев)" if kraken_funding > 0.01 else "шорты платят (разворот вверх)" if kraken_funding < -0.005 else "нейтральный"
            onchain_lines.append(f"- Kraken Futures FR: {kraken_funding:.4f}% — {kr_hint}")
        if kraken_oi is not None:
            onchain_lines.append(f"- Kraken Futures OI: {kraken_oi:,.0f} контрактов")
        if dydx_funding is not None:
            dx_hint = "лонги платят" if dydx_funding > 0.01 else "шорты платят" if dydx_funding < -0.005 else "нейтральный"
            onchain_lines.append(f"- dYdX (DEX) FR: {dydx_funding:.4f}% — {dx_hint}")
        if dydx_oi is not None:
            onchain_lines.append(f"- dYdX OI: {dydx_oi:,.0f}")

    if onchain_lines:
        onchain_block = "\n".join(onchain_lines)

    prompt = f"""Ты — опытный крипто-аналитик. Проанализируй данные {symbol} и дай РЕШИТЕЛЬНЫЙ анализ.

ДАННЫЕ {symbol} ПРЯМО СЕЙЧАС:
- Цена: ${price}, изменение 24ч: {safe_pct(change)}
- Открытый интерес (OI): {safe_usd(oi)} ({safe_pct(oi_change)} за 1ч)
- Funding Rate: {safe_pct(fr)}
- Покупатели/Продавцы (taker): {long_pct or '?'}% / {short_pct or '?'}%
- Ликвидации {symbol} (1ч): лонги {safe_usd(liq_long)}, шорты {safe_usd(liq_short)}
- Ликвидации РЫНОК (1ч): лонги {safe_usd(mkt_liq_long)}, шорты {safe_usd(mkt_liq_short)}
- Fear & Greed: {fg or '?'} ({fg_label or '?'}){onchain_block}

ПРАВИЛА ОЦЕНКИ (следуй им строго):

ПОКУПАТЬ если 2+ условий совпадают:
- Покупатели > 52% (быки доминируют)
- OI растёт > +0.5% за 1ч (новые деньги заходят)
- Funding Rate отрицательный (шорты переплачивают — разворот вверх вероятен)
- Fear & Greed < 30 (сильный страх — возможность покупки на панике)
- Ликвидации шортов > ликвидаций лонгов в 2+ раза (шортов выдавливают)
- Нетто поток бирж отрицательный (BTC выводят — холдят — бычий)
- SOPR < 1 (капитуляция — дно может быть близко)
- RSI < 30 (перепродан — разворот вверх вероятен)
- MACD > 0 и растёт (бычий импульс)
- AHR999 < 0.45 (зона накопления — покупай)
- BTC ETF netflow > 0 (институционалы покупают)
- Киты выводят с бирж > заводят на биржи (холдят — бычий)

ПРОДАВАТЬ если 2+ условий совпадают:
- Продавцы > 52% (медведи доминируют)
- OI падает < -0.5% за 1ч (деньги уходят)
- Funding Rate > +0.05% (лонги переплачивают — рынок перегрет)
- Fear & Greed > 75 (сильная жадность — пора фиксировать прибыль)
- Ликвидации лонгов > ликвидаций шортов в 2+ раза (лонги ликвидируют)
- Нетто поток бирж положительный (BTC заводят на биржи — медвежий)
- SOPR > 1.05 (массово фиксируют прибыль — возможна коррекция)
- RSI > 70 (перекуплен — коррекция вероятна)
- MACD < 0 и падает (медвежий импульс)
- AHR999 > 1.2 (переоценён — продавай)
- BTC ETF netflow < 0 (институционалы продают)
- Bull Market Peak > 60% индикаторов сработали (пик рынка вероятен)
- Киты заводят на биржи > выводят (готовятся продавать — медвежий)

ПРИОРИТЕТ ТРЕНДА (САМОЕ ВАЖНОЕ ПРАВИЛО!):
Технический тренд ВАЖНЕЕ сентимента. Страх и SOPR < 1 — это НЕ автоматический сигнал покупки!
- Если MACD < 0 И death cross (SMA50 < SMA200) → тренд МЕДВЕЖИЙ. В медвежьем тренде:
  → Страх (Fear & Greed < 30) = оправдан, это НЕ сигнал покупки
  → SOPR < 1 = может падать ДАЛЬШЕ, капитуляция не значит дно
  → Рекомендуй ПРОДАВАТЬ или ВЫЖИДАТЬ, НЕ покупать
- Если MACD > 0 И golden cross (SMA50 > SMA200) → тренд БЫЧИЙ. В бычьем тренде:
  → Страх = возможность покупки на откате
  → SOPR < 1 = локальная коррекция, можно покупать

ВЫЖИДАТЬ если сигналы явно противоречат друг другу и нет перевеса ни в одну сторону.

ВАЖНО: НЕ выбирай "выжидать" по умолчанию! Если есть 2+ совпадающих сигнала в одну сторону — давай направление.
Но НИКОГДА не рекомендуй "покупать" если MACD отрицательный И death cross — тренд важнее!

ПРАВИЛА ДЛЯ ЗОН (СТРОГО!):
- Зона покупки ДОЛЖНА ВКЛЮЧАТЬ текущую цену! Формат: от (цена - 1-2%) до (цена + 0.5%)
- Зона продажи = от (цена + 2-3%) до (цена + 4-5%)
- Пример: цена $67,500 → покупка $66,200–$67,800 | продажа $69,500–$71,000
- НИКОГДА не давай зону покупки которая ПОЛНОСТЬЮ ниже текущей цены — человек должен иметь возможность купить СЕЙЧАС

ОТВЕТЬ СТРОГО В ФОРМАТЕ (3 строки, без лишнего):
АНАЛИЗ: [2-3 предложения простым языком — что происходит и почему ты выбрал это направление]
РЕКОМЕНДАЦИЯ: [одно слово: покупать / продавать / выжидать]
ЗОНЫ: покупка $XXX,XXX–$XXX,XXX | продажа $XXX,XXX–$XXX,XXX"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 300,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            if resp.status_code != 200:
                body = resp.text
                log.warning(f"LLM API {symbol} status={resp.status_code}: {body[:500]}")
                return {}
            data = resp.json()
            text = data["content"][0]["text"].strip()

            # Парсим ответ (гибкий парсинг — ловим разные форматы)
            result = {}
            for line in text.split("\n"):
                clean = line.strip().lstrip("*").lstrip("#").strip()
                upper = clean.upper()

                if upper.startswith("АНАЛИЗ:") or upper.startswith("АНАЛИЗ :"):
                    val = clean.split(":", 1)[1].strip() if ":" in clean else ""
                    val = val.replace("**", "").replace("__", "")  # убираем markdown bold
                    if val:
                        result["llm_text"] = val
                elif upper.startswith("РЕКОМЕНДАЦИЯ:") or upper.startswith("РЕКОМЕНДАЦИЯ :"):
                    val = clean.split(":", 1)[1].strip().lower() if ":" in clean else ""
                    # Убираем markdown звёздочки (**bold**) и подчёркивания
                    val = val.replace("*", "").replace("_", "").strip()
                    if val:
                        result["recommendation"] = val
                elif upper.startswith("ЗОНЫ:") or upper.startswith("ЗОНЫ :"):
                    val = clean.split(":", 1)[1].strip() if ":" in clean else ""
                    parts = val.split("|")
                    if len(parts) >= 2:
                        result["buy_zone"] = parts[0].replace("покупка", "").replace("Покупка", "").strip()
                        result["sell_zone"] = parts[1].replace("продажа", "").replace("Продажа", "").strip()

            # Fallback: ищем ключевые слова в тексте если парсер не нашёл
            if not result.get("recommendation"):
                text_lower = text.lower()
                if "покупать" in text_lower:
                    result["recommendation"] = "покупать"
                elif "продавать" in text_lower:
                    result["recommendation"] = "продавать"
                elif "выжидать" in text_lower:
                    result["recommendation"] = "выжидать"

            if not result.get("llm_text") and text:
                # Берём первое осмысленное предложение как анализ
                for line in text.split("\n"):
                    clean = line.strip().lstrip("*").strip()
                    if len(clean) > 20 and not clean.upper().startswith(("РЕКОМЕНДАЦИЯ", "ЗОНЫ")):
                        result["llm_text"] = clean.split(":", 1)[1].strip() if ":" in clean and clean.upper().startswith("АНАЛИЗ") else clean
                        break

            if result.get("llm_text"):
                log.info(f"  🤖 LLM {symbol}: {result.get('recommendation', '?')}")
            return result

    except Exception as e:
        log.warning(f"LLM error {symbol}: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# СИГНАЛ (расчёт силы сигнала)
# ══════════════════════════════════════════════════════════════════════════════

def calculate_signal(coin_data: dict) -> tuple[str, str]:
    """
    Расчёт силы сигнала на основе всех метрик.
    Считаем бычьи и медвежьи сигналы отдельно.
    Сила = количество совпадающих сигналов.
    """
    bull = 0  # бычьи сигналы
    bear = 0  # медвежьи сигналы

    # 1. Taker Buy/Sell
    long_pct = coin_data.get("long_pct")
    if long_pct is not None:
        if long_pct > 52:
            bull += 1
        elif long_pct < 48:
            bear += 1

    # 2. OI change
    oi_change = coin_data.get("oi_change_1h")
    if oi_change is not None:
        if oi_change > 0.5:
            bull += 1  # новые деньги заходят
        elif oi_change < -0.5:
            bear += 1  # деньги уходят

    # 3. Funding Rate
    fr = coin_data.get("funding_rate")
    if fr is not None:
        if fr < -0.005:
            bull += 1  # шорты переплачивают → разворот вверх
        elif fr > 0.05:
            bear += 1  # лонги переплачивают → перегрев

    # 4. Fear & Greed
    fg = coin_data.get("fear_greed")
    if fg is not None:
        if fg < 30:
            bull += 1  # сильный страх = возможность покупки
        elif fg > 75:
            bear += 1  # жадность = пора фиксировать

    # 5. Цена 24ч
    change = coin_data.get("change_24h")
    if change is not None:
        if change > 3:
            bull += 1
        elif change < -3:
            bear += 1

    # 6. Ликвидации
    liq_long = coin_data.get("liq_long")
    liq_short = coin_data.get("liq_short")
    if liq_long and liq_short:
        try:
            ll = float(liq_long)
            ls = float(liq_short)
            if ls > ll * 2 and ls > 0:
                bull += 1  # шортов ликвидируют → рост
            elif ll > ls * 2 and ll > 0:
                bear += 1  # лонгов ликвидируют → падение
        except (ValueError, TypeError):
            pass

    # 7. On-chain: Exchange Netflow (BGeometrics)
    exchange_netflow = coin_data.get("exchange_netflow_btc")
    if exchange_netflow is not None:
        if exchange_netflow < -500:
            bull += 1  # отток с бирж → холдят → бычий (>500 BTC)
        elif exchange_netflow > 500:
            bear += 1  # приток на биржи → готовятся продавать (>500 BTC)

    # 8. On-chain: SOPR
    sopr = coin_data.get("sopr")
    if sopr is not None:
        if sopr < 0.98:
            bull += 1  # капитуляция → дно близко
        elif sopr > 1.05:
            bear += 1  # массово фиксируют прибыль

    # 9. RSI (технический индикатор)
    rsi = coin_data.get("rsi")
    if rsi is not None:
        if rsi < 30:
            bull += 1  # перепродан → разворот вверх
        elif rsi > 70:
            bear += 1  # перекуплен → коррекция

    # 10. MACD
    macd = coin_data.get("macd")
    if macd is not None:
        if macd > 0:
            bull += 1  # бычий импульс
        elif macd < 0:
            bear += 1  # медвежий импульс

    # 11. AHR999 (Coinglass)
    ahr999 = coin_data.get("ahr999")
    if ahr999 is not None:
        if ahr999 < 0.45:
            bull += 1  # зона накопления
        elif ahr999 > 1.2:
            bear += 1  # переоценён

    # 12. BTC ETF Netflow
    etf_netflow = coin_data.get("etf_netflow")
    if etf_netflow is not None:
        if etf_netflow > 50_000_000:  # > $50M приток
            bull += 1  # институционалы покупают
        elif etf_netflow < -50_000_000:  # > $50M отток
            bear += 1  # институционалы продают

    # 13. Bull Market Peak
    bull_peak_pct = coin_data.get("bull_peak_pct")
    if bull_peak_pct is not None:
        if bull_peak_pct > 60:
            bear += 1  # более 60% индикаторов пика сработали

    # 14. Cross-Exchange: Bitget Account L/S (ритейл настроение)
    bitget_acc_l = coin_data.get("bitget_long_acc")
    if bitget_acc_l is not None:
        # Контр-индикатор: если ритейл массово в лонгах — медвежий сигнал
        if bitget_acc_l > 70:
            bear += 1  # ритейл перегрет в лонгах (контр-индикатор)
        elif bitget_acc_l < 35:
            bull += 1  # ритейл массово в шортах (контр-индикатор — покупай)

    # 15. Cross-Exchange: Bitget Position L/S vs Coinglass L/S divergence
    bitget_pos_l = coin_data.get("bitget_long_pos")
    cg_long_pct = coin_data.get("long_pct")
    if bitget_pos_l is not None and cg_long_pct is not None:
        # Если Bitget и Coinglass согласны — сильный сигнал
        if bitget_pos_l > 55 and cg_long_pct > 55:
            bull += 1  # обе платформы: позиции в лонгах
        elif bitget_pos_l < 45 and cg_long_pct < 45:
            bear += 1  # обе платформы: позиции в шортах

    # 16. Cross-Exchange Funding Rate consensus (Kraken + dYdX vs Coinglass)
    # Если funding на нескольких биржах совпадает — сильный сигнал
    cg_fr = coin_data.get("funding_rate")
    kraken_fr = coin_data.get("kraken_funding")
    dydx_fr = coin_data.get("dydx_funding")
    fr_negative_count = 0
    fr_positive_count = 0
    for fr_val in [cg_fr, kraken_fr, dydx_fr]:
        if fr_val is not None:
            if fr_val < -0.005:
                fr_negative_count += 1
            elif fr_val > 0.03:
                fr_positive_count += 1
    if fr_negative_count >= 2:
        bull += 1  # 2+ бирж: шорты платят → разворот вверх вероятен
    if fr_positive_count >= 2:
        bear += 1  # 2+ бирж: лонги переплачивают → перегрев

    # 17. dYdX OI divergence (DEX vs CEX)
    # Если OI на dYdX растёт при падении на CEX — smart money позиционируется
    dydx_oi_val = coin_data.get("dydx_oi")
    cg_oi_change = coin_data.get("oi_change_1h")
    # Просто учитываем наличие dYdX OI данных как дополнительный фактор уверенности
    # (без исторических данных пока не можем сравнивать изменения)

    # Сила = максимум из бычьих/медвежьих
    strength = max(bull, bear)

    if strength >= 4:
        normalized = 5
        label = "СИЛЬНЫЙ"
    elif strength >= 3:
        normalized = 4
        label = "СИЛЬНЫЙ"
    elif strength >= 2:
        normalized = 3
        label = "СРЕДНИЙ"
    elif strength >= 1:
        normalized = 2
        label = "СРЕДНИЙ"
    else:
        normalized = 1
        label = "СЛАБЫЙ"

    bars = "▓" * normalized + "░" * (5 - normalized)
    return bars, label


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ СБОРА
# ══════════════════════════════════════════════════════════════════════════════

async def collect_all():
    """
    Собирает данные по всем монетам + LLM-анализ, сохраняет в Supabase.
    """
    log.info("📡 Начинаем сбор данных...")

    # Обновляем кэш BGeometrics (реальные запросы только если кэш устарел)
    await fetch_bgeometrics_batch()

    # Обновляем Coinglass индикаторы (кэш 4ч)
    await fetch_cg_indicators()
    cg_indicators = parse_cg_indicators()

    # DeFiLlama: стейблкоины + TVL (бесплатно)
    defillama_data = await fetch_defillama_data()

    # Технические индикаторы для не-BTC монет (CoinGecko история, кэш 4ч)
    # Загружаем заранее с паузами, чтобы не попасть в rate limit CoinGecko
    for coin in COINS:
        if coin == "BTC":
            continue  # BTC получает из BGeometrics
        cache_key = f"tech_{coin}"
        if cache_key in TECH_CACHE and (time.time() - TECH_CACHE[cache_key]["ts"]) < TECH_CACHE_TTL:
            continue  # Кэш актуален
        await fetch_tech_indicators(coin)
        await asyncio.sleep(6)  # Пауза 6с между запросами CoinGecko (free tier: ~10 req/min)

    # Общие данные
    fg_data = await fetch_fear_greed()
    prices = await fetch_prices()

    # Ликвидации — 1 запрос на все монеты
    liq_all = await cg_get("/api/futures/liquidation/coin-list")
    liq_by_coin = {}
    total_liq_long_1h = 0.0
    total_liq_short_1h = 0.0

    if liq_all and isinstance(liq_all, list):
        for item in liq_all:
            sym = (item.get("symbol") or "").upper()

            # Суммируем общие ликвидации рынка (ВСЕ монеты) — 1ч
            try:
                ll = float(item.get("long_liquidation_usd_1h") or 0)
                ls = float(item.get("short_liquidation_usd_1h") or 0)
                total_liq_long_1h += ll
                total_liq_short_1h += ls
            except (ValueError, TypeError):
                pass

            # Сохраняем по нашим монетам — 1ч (фоллбэк на 4ч)
            if sym in COINS:
                liq_long = item.get("long_liquidation_usd_1h")
                liq_short = item.get("short_liquidation_usd_1h")
                if liq_long is None:
                    liq_long = item.get("long_liquidation_usd_4h")
                if liq_short is None:
                    liq_short = item.get("short_liquidation_usd_4h")
                try:
                    ll_coin = float(liq_long) if liq_long is not None else None
                    ls_coin = float(liq_short) if liq_short is not None else None
                    liq_by_coin[sym] = {
                        "liq_long": ll_coin if ll_coin and ll_coin > 0 else None,
                        "liq_short": ls_coin if ls_coin and ls_coin > 0 else None,
                    }
                except (ValueError, TypeError):
                    pass

    if liq_by_coin:
        log.info(f"  📊 Ликвидации: {len(liq_by_coin)} монет | Рынок 1ч: лонги {fmt_usd(total_liq_long_1h)}, шорты {fmt_usd(total_liq_short_1h)}")

    for i, symbol in enumerate(COINS):
        try:
            log.info(f"  ⏳ {symbol}...")

            # Задержка между монетами (кроме первой)
            if i > 0:
                await asyncio.sleep(3)

            price_data = prices.get(symbol, {})
            liq_data = liq_by_coin.get(symbol, {})

            # Coinglass данные — 3 запроса параллельно
            oi_data, fr_data, ls_data = await asyncio.gather(
                fetch_open_interest(symbol),
                fetch_funding_rate(symbol),
                fetch_long_short(symbol),
            )

            # On-chain данные (бесплатно, без ключа)
            gn_data = await fetch_onchain_data(symbol)

            # Технические индикаторы (RSI, MACD, SMA — для не-BTC монет, из CoinGecko истории)
            tech_data = await fetch_tech_indicators(symbol)

            # Cross-exchange данные: Bitget + Kraken + dYdX (бесплатно, без ключа)
            bitget_ls_data, bitget_oi_data, kraken_data, dydx_data = await asyncio.gather(
                fetch_bitget_ls(symbol),
                fetch_bitget_oi(symbol),
                fetch_kraken_futures(symbol),
                fetch_dydx_data(symbol),
            )

            # Объединяем все данные
            coin_data = {
                **price_data,
                **oi_data,
                **fr_data,
                **ls_data,
                **liq_data,
                **fg_data,
                **gn_data,
                **cg_indicators,      # Bull Market Peak, AHR999, Bubble, ETF
                **defillama_data,     # Stablecoin mcap, DeFi TVL
                **bitget_ls_data,     # Bitget Account + Position L/S
                **bitget_oi_data,     # Bitget OI
                **kraken_data,        # Kraken Futures: funding + OI
                **dydx_data,          # dYdX: funding + OI
                **tech_data,          # RSI, MACD, SMA50, SMA200 (для не-BTC монет)
                "mkt_liq_long": total_liq_long_1h,
                "mkt_liq_short": total_liq_short_1h,
            }

            # Рассчитываем сигнал
            signal_bar, signal_label = calculate_signal(coin_data)

            # LLM-анализ (если есть ключ и достаточно данных)
            llm_data = {}
            if HAS_ANTHROPIC and coin_data.get("price") and coin_data.get("oi"):
                llm_data = await generate_llm_analysis(symbol, coin_data)

            # Формируем запись для Supabase
            oi_val = coin_data.get("oi")
            long_pct_val = coin_data.get("long_pct")
            short_pct_val = coin_data.get("short_pct")

            record = {
                "coin": symbol,
                "price": fmt_price(coin_data.get("price")),
                "change": fmt_pct(coin_data.get("change_24h")),
                "oi": fmt_usd(oi_val),
                "oi_raw": oi_val,
                "oi_change": fmt_pct(coin_data.get("oi_change_1h")),
                "funding_rate": fmt_fr(coin_data.get("funding_rate")),
                "long_pct": f"{long_pct_val}%" if long_pct_val is not None else "—",
                "short_pct": f"{short_pct_val}%" if short_pct_val is not None else "—",
                "long_vol": "—",
                "short_vol": "—",
                "liq_up": fmt_usd(coin_data.get("liq_short")),
                "liq_dn": fmt_usd(coin_data.get("liq_long")),
                "mkt_liq_long": fmt_usd(total_liq_long_1h),
                "mkt_liq_short": fmt_usd(total_liq_short_1h),
                "active_addresses": str(coin_data.get("active_addresses", "—")),
                "active_addresses_change": fmt_pct(coin_data.get("active_addresses_change")),
                "exchange_reserve_btc": f"{coin_data['exchange_reserve_btc']:,.0f}" if coin_data.get("exchange_reserve_btc") else "—",
                "exchange_netflow_btc": f"{coin_data['exchange_netflow_btc']:+,.2f}" if coin_data.get("exchange_netflow_btc") is not None else "—",
                "sopr": str(round(coin_data["sopr"], 4)) if coin_data.get("sopr") else "—",
                "rsi": f"{coin_data['rsi']:.1f}" if coin_data.get("rsi") is not None else "—",
                "macd": f"{coin_data['macd']:.2f}" if coin_data.get("macd") is not None else "—",
                "sma50": fmt_price(coin_data.get("sma50")),
                "sma200": fmt_price(coin_data.get("sma200")),
                "exchange_flow": "—",
                "bitget_long_acc": f"{coin_data.get('bitget_long_acc', '—')}%" if coin_data.get("bitget_long_acc") is not None else "—",
                "bitget_short_acc": f"{coin_data.get('bitget_short_acc', '—')}%" if coin_data.get("bitget_short_acc") is not None else "—",
                "bitget_long_pos": f"{coin_data.get('bitget_long_pos', '—')}%" if coin_data.get("bitget_long_pos") is not None else "—",
                "bitget_short_pos": f"{coin_data.get('bitget_short_pos', '—')}%" if coin_data.get("bitget_short_pos") is not None else "—",
                "bitget_oi_usd": fmt_usd(coin_data.get("bitget_oi_usd")) if coin_data.get("bitget_oi_usd") else "—",
                "kraken_funding": fmt_fr(coin_data.get("kraken_funding")),
                "kraken_oi": str(round(coin_data["kraken_oi"])) if coin_data.get("kraken_oi") else "—",
                "dydx_funding": fmt_fr(coin_data.get("dydx_funding")),
                "dydx_oi": str(round(coin_data["dydx_oi"])) if coin_data.get("dydx_oi") else "—",
                "stablecoin_mcap": fmt_usd(coin_data.get("stablecoin_mcap")) if coin_data.get("stablecoin_mcap") else "—",
                "defi_tvl": fmt_usd(coin_data.get("defi_tvl")) if coin_data.get("defi_tvl") else "—",
                "defi_tvl_change": fmt_pct(coin_data.get("defi_tvl_change")),
                "etf_netflow": fmt_usd(coin_data.get("etf_netflow")) if coin_data.get("etf_netflow") else "—",
                "ahr999": str(round(coin_data["ahr999"], 3)) if coin_data.get("ahr999") else "—",
                "bull_peak_ratio": coin_data.get("bull_peak_ratio", "—"),
                "bitcoin_bubble": str(coin_data.get("bitcoin_bubble", "—")),
                "fear_greed": str(coin_data.get("fear_greed", "—")),
                "fear_greed_label": coin_data.get("fear_greed_label", "—"),
                "signal": signal_bar,
                "label": signal_label,
                "signal_label": signal_label,
                "llm_text": llm_data.get("llm_text", ""),
                "recommendation": llm_data.get("recommendation", ""),
                "buy_zone": llm_data.get("buy_zone", ""),
                "sell_zone": llm_data.get("sell_zone", ""),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            await db.upsert_market_data(record)
            rec = llm_data.get("recommendation", "—")
            log.info(f"  ✅ {symbol}: {record['price']} {record['change']} | OI:{record['oi']} | FR:{record['funding_rate']} | L/S:{record['long_pct']}/{record['short_pct']} | LIQ:{record['liq_up']}/{record['liq_dn']} | {signal_bar} {signal_label} | REC:{rec}")

        except Exception as e:
            log.error(f"  ❌ {symbol} error: {e}")
            continue

    log.info("📡 Сбор данных завершён.")


async def collector_loop(interval_minutes: int = 15):
    log.info(f"🔄 Коллектор запущен. Интервал: {interval_minutes} мин")

    await collect_all()

    while True:
        await asyncio.sleep(interval_minutes * 60)
        try:
            await collect_all()
        except Exception as e:
            log.error(f"Collector loop error: {e}")
