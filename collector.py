"""
ZENDER TERMINAL — Data Collector
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

# LLM вызывается раз в 15 мин (экономия ~$120/мес). Данные обновляются каждые 5 мин.
# TODO: переключить на 5 мин когда пойдут платные подписки
LLM_INTERVAL_SEC = 15 * 60  # 15 минут
_last_llm_run = 0.0  # timestamp последнего LLM прогона

# Bitget API v2 (бесплатно, без ключа, работает из US серверов Railway)
# OKX, Binance, Bybit — НЕ работают из US, убраны
BITGET_BASE = "https://api.bitget.com"

# Kraken Futures (бесплатно, без ключа, работает в US — Kraken лицензирован в US)
KRAKEN_FUTURES_BASE = "https://futures.kraken.com/derivatives/api/v3"

# dYdX v4 Indexer (бесплатно, без ключа, DEX — нет гео-блокировки)
DYDX_BASE = "https://indexer.dydx.trade/v4"

# CryptoCompare (бесплатно, без ключа — fallback для цен если CoinGecko 429)
# CoinCap DNS не резолвится на Railway, заменён на CryptoCompare
CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com"

# On-chain: бесплатные API (blockchain.info + BGeometrics + DeFiLlama), без ключа

# Etherscan (бесплатно, 5 calls/sec, 100k/day) — ETH on-chain
try:
    from config import ETHERSCAN_KEY
    HAS_ETHERSCAN = bool(ETHERSCAN_KEY)
except (ImportError, AttributeError):
    ETHERSCAN_KEY = None
    HAS_ETHERSCAN = False

ETHERSCAN_BASE = "https://api.etherscan.io/api"

# Whale Alert API v1 (Custom Alerts $29.95/мес — 100 alerts/hour, 10 req/min)
try:
    from config import WHALE_ALERT_KEY
    HAS_WHALE_ALERT = bool(WHALE_ALERT_KEY)
except (ImportError, AttributeError):
    WHALE_ALERT_KEY = None
    HAS_WHALE_ALERT = False

WHALE_ALERT_BASE = "https://api.whale-alert.io/v1"

# Маппинг наших символов → Whale Alert currency (lowercase)
WHALE_ALERT_CURRENCIES = {
    "BTC": "btc", "ETH": "eth", "BNB": "bnb", "SOL": "sol", "XRP": "xrp",
    "ADA": "ada", "DOGE": "doge", "AVAX": "avax", "DOT": "dot", "LINK": "link",
    "POL": "matic", "TRX": "trx", "SHIB": "shib", "UNI": "uni", "LTC": "ltc",
    "ATOM": "atom", "NEAR": "near", "APT": "apt", "ARB": "arb", "OP": "op",
}

# Кэш whale данных — обновляем раз в 5 мин (коллектор цикл)
WHALE_CACHE = {}  # {symbol: {"data": {...}, "ts": time.time()}}
WHALE_CACHE_TTL = 5 * 60  # 5 минут

# Blockchair — УБРАН (данные без контекста: кто, куда, зачем — бесполезно)
# Вместо этого используем BGeometrics Exchange Netflow (уже подключён)

from database import db

log = logging.getLogger(__name__)

# ── Coinglass API v4 ──────────────────────────────────────────────────────────
CG_BASE = "https://open-api-v4.coinglass.com"
CG_HEADERS = {
    "CG-API-KEY": COINGLASS_API_KEY,
    "Accept": "application/json",
}

# Монеты для сбора данных — ТОП-20 по капитализации
COINS = [
    "BTC", "ETH", "BNB", "SOL", "XRP",
    "ADA", "DOGE", "AVAX", "DOT", "LINK",
    "POL", "TRX", "SHIB", "UNI", "LTC",
    "ATOM", "NEAR", "APT", "ARB", "OP",
]

# CoinGecko IDs
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "AVAX": "avalanche-2",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "POL": "polygon-ecosystem-token",
    "TRX": "tron",
    "SHIB": "shiba-inu",
    "UNI": "uniswap",
    "LTC": "litecoin",
    "ATOM": "cosmos",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
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


async def _fetch_history_coingecko(symbol: str, client: httpx.AsyncClient) -> list[float] | None:
    """Попытка получить 200 дней цен из CoinGecko. Одна попытка, без ретраев."""
    gecko_id = COINGECKO_IDS.get(symbol)
    if not gecko_id:
        return None
    try:
        resp = await client.get(
            f"https://api.coingecko.com/api/v3/coins/{gecko_id}/market_chart",
            params={"vs_currency": "usd", "days": "200", "interval": "daily"}
        )
        if resp.status_code == 429:
            log.warning(f"  ⚠️ CoinGecko 429 история {symbol} — переключаемся на CryptoCompare")
            return None
        if resp.status_code != 200:
            log.warning(f"  ⚠️ CoinGecko history {symbol}: HTTP {resp.status_code}")
            return None
        data = resp.json()
        price_points = data.get("prices", [])
        if len(price_points) < 50:
            log.warning(f"  ⚠️ CoinGecko history {symbol}: мало данных ({len(price_points)} точек)")
            return None
        return [p[1] for p in price_points]
    except Exception as e:
        log.warning(f"  ⚠️ CoinGecko history {symbol} error: {e}")
        return None


async def _fetch_history_cryptocompare(symbol: str, client: httpx.AsyncClient) -> list[float] | None:
    """Fallback 2: 200 дней цен из CryptoCompare."""
    try:
        resp = await client.get(
            f"{CRYPTOCOMPARE_BASE}/data/v2/histoday",
            params={"fsym": symbol, "tsym": "USD", "limit": 200}
        )
        if resp.status_code != 200:
            log.warning(f"  ⚠️ CryptoCompare history {symbol}: HTTP {resp.status_code}")
            return None
        data = resp.json().get("Data", {}).get("Data", [])
        if len(data) < 50:
            log.warning(f"  ⚠️ CryptoCompare history {symbol}: мало данных ({len(data)} точек)")
            return None
        closes = [d["close"] for d in data if d.get("close") and d["close"] > 0]
        if len(closes) < 50:
            return None
        log.info(f"  📊 CryptoCompare history {symbol}: {len(closes)} дней загружено")
        return closes
    except Exception as e:
        log.warning(f"  ⚠️ CryptoCompare history {symbol} error: {e}")
        return None


# Binance символы — маппинг наших тикеров на Binance торговые пары
BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "ADA": "ADAUSDT", "DOGE": "DOGEUSDT", "AVAX": "AVAXUSDT",
    "DOT": "DOTUSDT", "LINK": "LINKUSDT", "POL": "POLUSDT", "TRX": "TRXUSDT",
    "SHIB": "SHIBUSDT", "UNI": "UNIUSDT", "LTC": "LTCUSDT", "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT", "APT": "APTUSDT", "ARB": "ARBUSDT", "OP": "OPUSDT",
}


async def _fetch_history_binance(symbol: str, client: httpx.AsyncClient) -> list[float] | None:
    """Fallback 3: 200 дней цен из Binance klines API (бесплатно, надёжно, без ключа)."""
    binance_sym = BINANCE_SYMBOLS.get(symbol)
    if not binance_sym:
        return None
    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": binance_sym, "interval": "1d", "limit": 201}
        )
        if resp.status_code != 200:
            log.warning(f"  ⚠️ Binance klines {symbol}: HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not isinstance(data, list) or len(data) < 50:
            log.warning(f"  ⚠️ Binance klines {symbol}: мало данных ({len(data) if isinstance(data, list) else 0} точек)")
            return None
        # Binance kline: [open_time, open, high, low, close, volume, ...]
        closes = [float(candle[4]) for candle in data if float(candle[4]) > 0]
        if len(closes) < 50:
            return None
        log.info(f"  📊 Binance klines {symbol}: {len(closes)} дней загружено")
        return closes
    except Exception as e:
        log.warning(f"  ⚠️ Binance klines {symbol} error: {e}")
        return None


def _calc_indicators_from_closes(closes: list[float]) -> dict:
    """Считает RSI, MACD, SMA50, SMA200 из списка цен закрытия."""
    result = {}
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
    return result


async def fetch_tech_indicators(symbol: str) -> dict:
    """
    Загружает 200 дней цен и считает RSI, MACD, SMA50, SMA200.
    Источники: CoinGecko (1 попытка) → CryptoCompare fallback.
    Кэш 30 минут. Для BTC — данные из BGeometrics.
    """
    if symbol == "BTC":
        return {}  # BTC получает из BGeometrics

    cache_key = f"tech_{symbol}"
    if cache_key in TECH_CACHE and (time.time() - TECH_CACHE[cache_key]["ts"]) < TECH_CACHE_TTL:
        return TECH_CACHE[cache_key]["data"]

    closes = None
    source = ""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Попытка 1: CoinGecko (одна попытка, без ретраев)
            closes = await _fetch_history_coingecko(symbol, client)
            if closes:
                source = "CoinGecko"
            else:
                # Попытка 2: Binance klines (надёжный, без rate limit)
                closes = await _fetch_history_binance(symbol, client)
                if closes:
                    source = "Binance"
                else:
                    # Попытка 3: CryptoCompare fallback
                    closes = await _fetch_history_cryptocompare(symbol, client)
                    if closes:
                        source = "CryptoCompare"
    except Exception as e:
        log.warning(f"Tech indicators {symbol} error: {e}")

    if not closes:
        log.warning(f"  ❌ Tech {symbol}: не удалось загрузить историю цен")
        return {}

    result = _calc_indicators_from_closes(closes)

    if result:
        TECH_CACHE[cache_key] = {"data": result, "ts": time.time()}
        log.info(f"  📊 Tech {symbol} ({source}): RSI={result.get('rsi','?')} | MACD={result.get('macd','?')} | SMA50={result.get('sma50','?')} | SMA200={result.get('sma200','?')}")
    else:
        log.warning(f"  ⚠️ Tech {symbol}: не удалось рассчитать индикаторы")

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
    log.warning("  ⚠️ CoinGecko: все 3 попытки провалились, пробуем CryptoCompare fallback...")
    return await fetch_prices_cryptocompare()


async def fetch_prices_cryptocompare() -> dict:
    """
    Fallback для цен: CryptoCompare API (бесплатно, без ключа).
    Используется когда CoinGecko возвращает 429.
    min-api.cryptocompare.com — стабильный DNS, работает на Railway.
    """
    result = {}
    try:
        fsyms = ",".join(COINS)  # BTC,ETH,SOL,BNB,AVAX
        async with httpx.AsyncClient(timeout=15) as client:
            # Текущие цены
            resp = await client.get(
                f"{CRYPTOCOMPARE_BASE}/data/pricemultifull",
                params={"fsyms": fsyms, "tsyms": "USD"}
            )
            if resp.status_code != 200:
                log.error(f"  ❌ CryptoCompare HTTP {resp.status_code}")
                return {}
            raw = resp.json().get("RAW", {})
            for symbol in COINS:
                coin_raw = raw.get(symbol, {}).get("USD", {})
                if coin_raw:
                    price = coin_raw.get("PRICE")
                    change_pct = coin_raw.get("CHANGEPCT24HOUR")
                    result[symbol] = {
                        "price": float(price) if price is not None else None,
                        "change_24h": float(change_pct) if change_pct is not None else None,
                    }
            if result:
                log.info(f"  ✅ CryptoCompare fallback: получены цены для {list(result.keys())}")
            else:
                log.error("  ❌ CryptoCompare: пустой ответ")
    except Exception as e:
        log.error(f"  ❌ CryptoCompare error: {e}")
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
# OPTIONS DATA — Deribit (primary) + Binance + OKX (бесплатно, публичные API)
# Put/Call Ratio, IV (Implied Volatility), Max Pain, Open Interest по опционам
# Работает только из EU сервера (Deribit заблокирован в US)
# Кэш 30 мин — опционы меняются быстро, но не нужно дёргать каждый цикл
# ══════════════════════════════════════════════════════════════════════════════

OPTIONS_CACHE = {}  # {key: {"data": ..., "ts": time.time()}}
OPTIONS_CACHE_TTL = 30 * 60  # 30 минут


async def fetch_deribit_options(symbol: str) -> dict:
    """
    Получает опционные данные с Deribit (JSON-RPC 2.0).
    BTC и ETH — основные опционные рынки.
    Возвращает: put_call_ratio, total_oi_calls, total_oi_puts, avg_iv, max_pain
    """
    if symbol not in ("BTC", "ETH"):
        return {}

    cache_key = f"deribit_{symbol}"
    if cache_key in OPTIONS_CACHE and (time.time() - OPTIONS_CACHE[cache_key]["ts"]) < OPTIONS_CACHE_TTL:
        return OPTIONS_CACHE[cache_key]["data"]

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Получаем book summary для всех опционов по валюте
            resp = await client.get(
                "https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
                params={"currency": symbol, "kind": "option"}
            )
            if resp.status_code != 200:
                log.warning(f"  ⚠️ Deribit {symbol}: HTTP {resp.status_code}")
                return {}

            data = resp.json()
            results = data.get("result", [])
            if not results:
                log.warning(f"  ⚠️ Deribit {symbol}: пустой ответ")
                return {}

            # Считаем Put/Call Ratio и средний IV
            total_oi_calls = 0.0
            total_oi_puts = 0.0
            iv_values = []
            strikes_data = {}  # {strike: {"call_oi": X, "put_oi": Y}} для Max Pain

            expiry_oi = {}  # {expiry_str: total_oi} для экспираций

            for item in results:
                name = item.get("instrument_name", "")
                oi = float(item.get("open_interest", 0) or 0)
                mark_iv = item.get("mark_iv")

                # Парсим тип опциона и страйк из имени: BTC-28MAR25-90000-C
                parts = name.split("-")
                if len(parts) < 4:
                    continue
                opt_type = parts[-1]  # C или P
                expiry_str = parts[1]  # 28MAR25
                try:
                    strike = float(parts[-2])
                except (ValueError, IndexError):
                    continue

                # Суммируем OI по экспирациям
                if oi > 0:
                    expiry_oi[expiry_str] = expiry_oi.get(expiry_str, 0) + oi

                if opt_type == "C":
                    total_oi_calls += oi
                    if strike not in strikes_data:
                        strikes_data[strike] = {"call_oi": 0, "put_oi": 0}
                    strikes_data[strike]["call_oi"] += oi
                elif opt_type == "P":
                    total_oi_puts += oi
                    if strike not in strikes_data:
                        strikes_data[strike] = {"call_oi": 0, "put_oi": 0}
                    strikes_data[strike]["put_oi"] += oi

                if mark_iv and mark_iv > 0 and oi > 0:
                    iv_values.append(mark_iv)

            # Put/Call Ratio
            pcr = round(total_oi_puts / total_oi_calls, 3) if total_oi_calls > 0 else None

            # Средний IV (взвешенный по OI было бы лучше, но для простоты — медиана)
            avg_iv = round(sorted(iv_values)[len(iv_values) // 2], 1) if iv_values else None

            # Max Pain — страйк где сумма убытков опционных холдеров максимальна
            max_pain = None
            if strikes_data:
                # Получаем текущую цену индекса
                idx_resp = await client.get(
                    "https://www.deribit.com/api/v2/public/get_index_price",
                    params={"index_name": f"{symbol.lower()}_usd"}
                )
                index_price = None
                if idx_resp.status_code == 200:
                    idx_data = idx_resp.json()
                    index_price = idx_data.get("result", {}).get("index_price")

                # Считаем Max Pain: для каждого страйка суммируем ITM стоимость
                max_pain_loss = float("inf")
                for test_strike in sorted(strikes_data.keys()):
                    total_loss = 0.0
                    for s, oi_data in strikes_data.items():
                        # Для колл: если test_strike > strike, колл ITM
                        if test_strike > s:
                            total_loss += oi_data["call_oi"] * (test_strike - s)
                        # Для пут: если test_strike < strike, пут ITM
                        if test_strike < s:
                            total_loss += oi_data["put_oi"] * (s - test_strike)

                    if total_loss < max_pain_loss:
                        max_pain_loss = total_loss
                        max_pain = test_strike

            # Топ-3 экспирации по OI + парсинг дат
            import calendar
            MONTHS = {v: k for k, v in enumerate(calendar.month_abbr) if v}
            top_expiries = []
            now_ts = datetime.now(timezone.utc)
            for exp_str, exp_oi in sorted(expiry_oi.items(), key=lambda x: -x[1]):
                # Парсим "28MAR25" → datetime
                try:
                    day = int(exp_str[:len(exp_str)-5])
                    mon_str = exp_str[len(exp_str)-5:len(exp_str)-2].upper()
                    yr = int("20" + exp_str[-2:])
                    mon = MONTHS.get(mon_str.capitalize(), 0)
                    if mon == 0:
                        continue
                    exp_date = datetime(yr, mon, day, 8, 0, tzinfo=timezone.utc)
                    days_left = (exp_date - now_ts).days
                    if days_left < 0:
                        continue
                    top_expiries.append({
                        "date": exp_str,
                        "oi": round(exp_oi),
                        "days": days_left,
                    })
                except (ValueError, IndexError):
                    continue
            # Сортируем по дате (ближайшие первые), берём топ-3
            top_expiries.sort(key=lambda x: x["days"])
            top_expiries = top_expiries[:3]
            # Помечаем макс OI среди топ-3
            if top_expiries:
                max_oi_val = max(e["oi"] for e in top_expiries)
                for e in top_expiries:
                    e["is_max"] = (e["oi"] == max_oi_val)

            result = {}
            if pcr is not None:
                result["options_pcr"] = pcr
            if avg_iv is not None:
                result["options_iv"] = avg_iv
            if max_pain is not None:
                result["options_max_pain"] = max_pain
            if total_oi_calls > 0:
                result["options_oi_calls"] = round(total_oi_calls, 2)
            if total_oi_puts > 0:
                result["options_oi_puts"] = round(total_oi_puts, 2)
            if top_expiries:
                result["options_expiries"] = top_expiries

            if result:
                OPTIONS_CACHE[cache_key] = {"data": result, "ts": time.time()}
                pcr_str = f"PCR={pcr}" if pcr else ""
                iv_str = f"IV={avg_iv}%" if avg_iv else ""
                mp_str = f"MaxPain=${max_pain:,.0f}" if max_pain else ""
                exp_str = f"Expiries={len(top_expiries)}" if top_expiries else ""
                log.info(f"  📊 Deribit {symbol} Options: {pcr_str} | {iv_str} | {mp_str} | {exp_str}")

            return result

    except Exception as e:
        log.warning(f"  ⚠️ Deribit {symbol} Options: {e}")
        return {}


async def fetch_binance_options(symbol: str) -> dict:
    """
    Binance European Options — дополнительный источник IV и Greeks.
    Берём Mark IV агрегированно.
    """
    if symbol not in ("BTC", "ETH"):
        return {}

    cache_key = f"binance_opt_{symbol}"
    if cache_key in OPTIONS_CACHE and (time.time() - OPTIONS_CACHE[cache_key]["ts"]) < OPTIONS_CACHE_TTL:
        return OPTIONS_CACHE[cache_key]["data"]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://eapi.binance.com/eapi/v1/mark",
            )
            if resp.status_code != 200:
                log.warning(f"  ⚠️ Binance Options {symbol}: HTTP {resp.status_code}")
                return {}

            data = resp.json()
            if not data:
                return {}

            # Фильтруем по символу (BTC-YYMMDD-STRIKE-C/P)
            iv_list = []
            for item in data:
                inst = item.get("symbol", "")
                if not inst.startswith(symbol):
                    continue
                mark_iv = item.get("markIV")
                if mark_iv:
                    try:
                        iv_val = float(mark_iv)
                        if iv_val > 0:
                            iv_list.append(iv_val)
                    except (ValueError, TypeError):
                        pass

            result = {}
            if iv_list:
                # Медианный IV
                sorted_iv = sorted(iv_list)
                median_iv = sorted_iv[len(sorted_iv) // 2]
                result["binance_options_iv"] = round(median_iv * 100, 1)  # конвертируем в %
                log.info(f"  📊 Binance Options {symbol}: IV={result['binance_options_iv']}% ({len(iv_list)} инструментов)")

            if result:
                OPTIONS_CACHE[cache_key] = {"data": result, "ts": time.time()}

            return result

    except Exception as e:
        log.warning(f"  ⚠️ Binance Options {symbol}: {e}")
        return {}


async def fetch_okx_options(symbol: str) -> dict:
    """
    OKX Options — дополнительный источник IV.
    """
    if symbol not in ("BTC", "ETH"):
        return {}

    cache_key = f"okx_opt_{symbol}"
    if cache_key in OPTIONS_CACHE and (time.time() - OPTIONS_CACHE[cache_key]["ts"]) < OPTIONS_CACHE_TTL:
        return OPTIONS_CACHE[cache_key]["data"]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.okx.com/api/v5/public/opt-summary",
                params={"instFamily": f"{symbol}-USD"}
            )
            if resp.status_code != 200:
                log.warning(f"  ⚠️ OKX Options {symbol}: HTTP {resp.status_code}")
                return {}

            data = resp.json()
            items = data.get("data", [])
            if not items:
                return {}

            iv_list = []
            for item in items:
                mark_vol = item.get("markVol")
                if mark_vol:
                    try:
                        iv_val = float(mark_vol)
                        if iv_val > 0:
                            iv_list.append(iv_val)
                    except (ValueError, TypeError):
                        pass

            result = {}
            if iv_list:
                sorted_iv = sorted(iv_list)
                median_iv = sorted_iv[len(sorted_iv) // 2]
                result["okx_options_iv"] = round(median_iv * 100, 1)
                log.info(f"  📊 OKX Options {symbol}: IV={result['okx_options_iv']}% ({len(iv_list)} инструментов)")

            if result:
                OPTIONS_CACHE[cache_key] = {"data": result, "ts": time.time()}

            return result

    except Exception as e:
        log.warning(f"  ⚠️ OKX Options {symbol}: {e}")
        return {}


async def fetch_options_data(symbol: str) -> dict:
    """
    Агрегирует опционные данные из Deribit (primary) + Binance + OKX.
    Deribit — основной (PCR, MaxPain, IV, OI).
    Binance/OKX — дополнительные IV для кросс-проверки.
    """
    if symbol not in ("BTC", "ETH"):
        return {}

    # Deribit — главный, Binance и OKX — параллельно
    deribit_data, binance_data, okx_data = await asyncio.gather(
        fetch_deribit_options(symbol),
        fetch_binance_options(symbol),
        fetch_okx_options(symbol),
    )

    combined = {**deribit_data, **binance_data, **okx_data}
    return combined


# ══════════════════════════════════════════════════════════════════════════════
# CVD (Cumulative Volume Delta) — Binance Futures API (бесплатно, без ключа)
# Показывает кто реально давит рынок: агрессивные покупатели или продавцы.
# ══════════════════════════════════════════════════════════════════════════════

CVD_CACHE = {}
CVD_CACHE_TTL = 5 * 60  # 5 минут — CVD меняется быстро

BINANCE_FUTURES_BASE = "https://fapi.binance.com"

# Маппинг символов на Binance futures тикеры
BINANCE_FUTURES_MAP = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "BNB": "BNBUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
    "ADA": "ADAUSDT",
    "DOGE": "DOGEUSDT",
    "AVAX": "AVAXUSDT",
    "DOT": "DOTUSDT",
    "LINK": "LINKUSDT",
    "POL": "POLUSDT",
    "TRX": "TRXUSDT",
    "SHIB": "SHIBUSDT",
    "UNI": "UNIUSDT",
    "LTC": "LTCUSDT",
    "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT",
    "APT": "APTUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
}


async def fetch_cvd(symbol: str) -> dict:
    """
    CVD из Binance Futures Aggr Trades.
    Берём последние 1000 сделок (aggrTrades) и считаем:
    CVD = sum(qty где maker=True) - sum(qty где maker=False)
    maker=True значит покупатель был maker → это market SELL
    maker=False значит покупатель был taker → это market BUY
    Также берём takerlongshortRatio как дополнительный сигнал.
    """
    ticker = BINANCE_FUTURES_MAP.get(symbol)
    if not ticker:
        return {}

    cache_key = f"cvd_{symbol}"
    if cache_key in CVD_CACHE and (time.time() - CVD_CACHE[cache_key]["ts"]) < CVD_CACHE_TTL:
        return CVD_CACHE[cache_key]["data"]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # 1. Aggr Trades — реальные сделки для CVD
            trades_resp = await client.get(
                f"{BINANCE_FUTURES_BASE}/fapi/v1/aggTrades",
                params={"symbol": ticker, "limit": 1000}
            )

            # 2. Taker L/S Ratio — дополнительный сигнал
            ratio_resp = await client.get(
                f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
                params={"symbol": ticker, "period": "5m", "limit": 6}
            )

        cvd_value = 0.0
        if trades_resp.status_code == 200:
            trades = trades_resp.json()
            if trades:
                # Считаем CVD: market buy (+) vs market sell (-)
                # m=True → buyer is maker → taker SOLD → это sell
                # m=False → buyer is taker → taker BOUGHT → это buy
                mid_point = len(trades) // 2
                price = float(trades[-1]["p"]) if trades else 1

                def calc_cvd_window(window):
                    delta = 0.0
                    for t in window:
                        qty_usd = float(t["q"]) * float(t["p"])
                        if t["m"]:  # buyer is maker = market sell
                            delta -= qty_usd
                        else:       # buyer is taker = market buy
                            delta += qty_usd
                    return delta

                cvd_current = calc_cvd_window(trades[mid_point:])
                cvd_previous = calc_cvd_window(trades[:mid_point])
                cvd_value = cvd_current
        else:
            log.warning(f"  ⚠️ CVD {symbol}: aggTrades status={trades_resp.status_code}")
            cvd_current = 0
            cvd_previous = 0

        # Taker L/S ratio для дополнительного контекста
        taker_buy_ratio = None
        if ratio_resp.status_code == 200:
            ratio_data = ratio_resp.json()
            if ratio_data:
                # buySellRatio > 1 = больше покупателей, < 1 = больше продавцов
                latest = ratio_data[-1]
                taker_buy_ratio = float(latest.get("buySellRatio", 1))

        # Определяем тренд CVD
        if cvd_previous != 0:
            cvd_trend = "rising" if cvd_current > cvd_previous else "falling"
        else:
            cvd_trend = "rising" if cvd_current > 0 else "falling"

        # Определяем доминирующую сторону
        cvd_side = "buyers" if cvd_current > 0 else "sellers"

        result = {
            "cvd_value": round(cvd_value / 1e6, 2),  # в миллионах USD
            "cvd_trend": cvd_trend,    # rising / falling
            "cvd_side": cvd_side,      # buyers / sellers
        }
        if taker_buy_ratio is not None:
            result["taker_buy_ratio"] = round(taker_buy_ratio, 3)
        CVD_CACHE[cache_key] = {"data": result, "ts": time.time()}
        log.info(f"  📊 CVD {symbol}: {result['cvd_value']:+.2f}M ({cvd_trend}, {cvd_side})")
        return result

    except Exception as e:
        log.warning(f"  ⚠️ CVD {symbol} error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ORDER BOOK IMBALANCE (OBI) — Binance Futures API (бесплатно, без ключа)
# Показывает дисбаланс лимитных ордеров: где больше стен — покупка или продажа.
# ══════════════════════════════════════════════════════════════════════════════

OBI_CACHE = {}
OBI_CACHE_TTL = 2 * 60  # 2 минуты — стакан меняется быстро


async def fetch_order_book_imbalance(symbol: str) -> dict:
    """
    Order Book Imbalance из Binance Futures depth.
    Берём 500 уровней bids и asks для надёжного поиска стен.
    OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)
    Также находим крупнейшие стены поддержки и сопротивления.
    """
    ticker = BINANCE_FUTURES_MAP.get(symbol)
    if not ticker:
        return {}

    cache_key = f"obi_{symbol}"
    if cache_key in OBI_CACHE and (time.time() - OBI_CACHE[cache_key]["ts"]) < OBI_CACHE_TTL:
        return OBI_CACHE[cache_key]["data"]

    try:
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/depth"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params={
                "symbol": ticker,
                "limit": 500,
            })
            if resp.status_code != 200:
                log.warning(f"  ⚠️ OBI {symbol}: Binance status={resp.status_code}")
                return {}
            data = resp.json()

        bids = data.get("bids", [])  # [[price, qty], ...]
        asks = data.get("asks", [])

        if not bids or not asks:
            return {}

        # Считаем суммарный объём (price * qty для USD value)
        bid_volume = sum(float(b[0]) * float(b[1]) for b in bids)
        ask_volume = sum(float(a[0]) * float(a[1]) for a in asks)
        total = bid_volume + ask_volume

        if total == 0:
            return {}

        # OBI: от -1 (все продают) до +1 (все покупают)
        obi = (bid_volume - ask_volume) / total

        # Определяем сторону
        if obi > 0.1:
            obi_side = "BUY"
        elif obi < -0.1:
            obi_side = "SELL"
        else:
            obi_side = "NEUTRAL"

        # Находим крупнейшую стену поддержки (bid) и сопротивления (ask)
        # Группируем по ценовым уровням (округляя до целых для BTC, до 1 для остальных)
        # чтобы найти реальные стены, а не единичные ордера
        biggest_bid = max(bids, key=lambda b: float(b[0]) * float(b[1]))
        biggest_ask = max(asks, key=lambda a: float(a[0]) * float(a[1]))

        support_price = float(biggest_bid[0])
        support_vol = float(biggest_bid[0]) * float(biggest_bid[1])
        resistance_price = float(biggest_ask[0])
        resistance_vol = float(biggest_ask[0]) * float(biggest_ask[1])

        result = {
            "obi_value": round(obi, 4),
            "obi_side": obi_side,                          # BUY / SELL / NEUTRAL
            "obi_bid_vol": round(bid_volume),               # USD
            "obi_ask_vol": round(ask_volume),               # USD
            "obi_support_price": round(support_price, 2),   # крупнейшая стена покупки
            "obi_support_vol": round(support_vol),           # объём стены
            "obi_resistance_price": round(resistance_price, 2),
            "obi_resistance_vol": round(resistance_vol),
        }
        OBI_CACHE[cache_key] = {"data": result, "ts": time.time()}
        log.info(f"  📊 OBI {symbol}: {obi:+.3f} ({obi_side}) | support ${support_price:,.0f} (${support_vol:,.0f}) | resist ${resistance_price:,.0f} (${resistance_vol:,.0f})")
        return result

    except Exception as e:
        log.warning(f"  ⚠️ OBI {symbol} error: {e}")
        return {}


async def fetch_flow_data(symbol: str) -> dict:
    """Агрегатор: CVD + Order Book Imbalance параллельно."""
    cvd, obi = await asyncio.gather(
        fetch_cvd(symbol),
        fetch_order_book_imbalance(symbol),
    )
    return {**cvd, **obi}


# ══════════════════════════════════════════════════════════════════════════════
# SPOT vs PERP VOLUME — Binance API (бесплатно)
# Показывает кто двигает рынок: реальные покупки (spot) или деривативы (perp)
# ══════════════════════════════════════════════════════════════════════════════

SPOT_PERP_CACHE = {}
SPOT_PERP_CACHE_TTL = 10 * 60  # 10 мин — объёмы меняются медленно

# Маппинг символов на Binance Spot тикеры
BINANCE_SPOT_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "ADA": "ADAUSDT", "DOGE": "DOGEUSDT", "AVAX": "AVAXUSDT",
    "DOT": "DOTUSDT", "LINK": "LINKUSDT", "POL": "POLUSDT", "TRX": "TRXUSDT",
    "SHIB": "SHIBUSDT", "UNI": "UNIUSDT", "LTC": "LTCUSDT", "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT", "APT": "APTUSDT", "ARB": "ARBUSDT", "OP": "OPUSDT",
}


async def fetch_spot_perp_volume(symbol: str) -> dict:
    """
    Спот vs Перп объёмы за 24ч из Binance.
    Spot: GET /api/v3/ticker/24hr
    Futures: GET /fapi/v1/ticker/24hr
    """
    spot_ticker = BINANCE_SPOT_MAP.get(symbol)
    futures_ticker = BINANCE_FUTURES_MAP.get(symbol)
    if not spot_ticker or not futures_ticker:
        return {}

    cache_key = f"spv_{symbol}"
    if cache_key in SPOT_PERP_CACHE and (time.time() - SPOT_PERP_CACHE[cache_key]["ts"]) < SPOT_PERP_CACHE_TTL:
        return SPOT_PERP_CACHE[cache_key]["data"]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            spot_resp, perp_resp = await asyncio.gather(
                client.get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={spot_ticker}"),
                client.get(f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={futures_ticker}"),
                return_exceptions=True,
            )

        spot_vol = 0.0
        perp_vol = 0.0

        if not isinstance(spot_resp, Exception) and spot_resp.status_code == 200:
            sd = spot_resp.json()
            spot_vol = float(sd.get("quoteVolume", 0))

        if not isinstance(perp_resp, Exception) and perp_resp.status_code == 200:
            pd_data = perp_resp.json()
            perp_vol = float(pd_data.get("quoteVolume", 0))

        if spot_vol == 0 and perp_vol == 0:
            return {}

        total = spot_vol + perp_vol
        spot_pct = round(spot_vol / total * 100) if total > 0 else 50

        result = {
            "spot_volume": round(spot_vol),
            "perp_volume": round(perp_vol),
            "spot_dominance": spot_pct,
        }

        SPOT_PERP_CACHE[cache_key] = {"data": result, "ts": time.time()}
        log.info(f"  📊 Spot/Perp {symbol}: spot ${spot_vol/1e9:.2f}B / perp ${perp_vol/1e9:.2f}B ({spot_pct}% spot)")
        return result

    except Exception as e:
        log.warning(f"  ⚠️ Spot/Perp {symbol} error: {e}")
        return {}


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


# ── Solana DeFi данные (DEX volume + TVL Solana) ──────────────────────────────

SOLANA_DEFI_CACHE = {}
SOLANA_DEFI_CACHE_TTL = 10 * 60  # 10 минут


async def fetch_solana_defi() -> dict:
    """
    DeFiLlama: DEX volume Solana (24h) + TVL Solana.
    Бесплатно, без ключа. Кэш 10 мин.
    """
    if "sol_defi" in SOLANA_DEFI_CACHE and (time.time() - SOLANA_DEFI_CACHE["sol_defi"]["ts"]) < SOLANA_DEFI_CACHE_TTL:
        return SOLANA_DEFI_CACHE["sol_defi"]["data"]

    result = {}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # 1. DEX Volume Solana (24h)
            resp = await client.get("https://api.llama.fi/overview/dexs/solana?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume")
            if resp.status_code == 200:
                data = resp.json()
                total_24h = data.get("total24h")
                total_48to24h = data.get("total48hto24h")
                if total_24h:
                    result["sol_dex_volume"] = float(total_24h)
                    if total_48to24h and float(total_48to24h) > 0:
                        chg = ((float(total_24h) - float(total_48to24h)) / float(total_48to24h)) * 100
                        result["sol_dex_volume_change"] = round(chg, 1)
                    log.info(f"  🔷 Solana DEX Vol: ${float(total_24h)/1e9:.2f}B")

            await asyncio.sleep(0.5)

            # 2. TVL Solana
            resp2 = await client.get("https://api.llama.fi/v2/historicalChainTvl/Solana")
            if resp2.status_code == 200:
                tvl_data = resp2.json()
                if isinstance(tvl_data, list) and len(tvl_data) > 0:
                    last = tvl_data[-1].get("tvl")
                    if last:
                        result["sol_tvl"] = float(last)
                    if len(tvl_data) >= 2:
                        prev = tvl_data[-2].get("tvl")
                        if prev and float(prev) > 0:
                            chg = ((float(last) - float(prev)) / float(prev)) * 100
                            result["sol_tvl_change"] = round(chg, 2)
                    log.info(f"  🔷 Solana TVL: ${float(last)/1e9:.2f}B")

    except Exception as e:
        log.warning(f"Solana DeFi error: {e}")

    if result:
        SOLANA_DEFI_CACHE["sol_defi"] = {"data": result, "ts": time.time()}

    return result


# ══════════════════════════════════════════════════════════════════════════════
# OKX Top Traders L/S (бесплатно, без ключа — работает из EU сервера Railway)
# ══════════════════════════════════════════════════════════════════════════════

OKX_CACHE = {}
OKX_CACHE_TTL = 15 * 60  # 15 минут

# Маппинг наших символов на OKX instId (USDT-margined perp)
OKX_INSTRUMENTS = {
    "BTC": "BTC-USDT-SWAP",
    "ETH": "ETH-USDT-SWAP",
    "SOL": "SOL-USDT-SWAP",
    "XRP": "XRP-USDT-SWAP",
    "ADA": "ADA-USDT-SWAP",
    "DOGE": "DOGE-USDT-SWAP",
    "AVAX": "AVAX-USDT-SWAP",
    "DOT": "DOT-USDT-SWAP",
    "LINK": "LINK-USDT-SWAP",
    "BNB": "BNB-USDT-SWAP",
    "POL": "POL-USDT-SWAP",
    "TRX": "TRX-USDT-SWAP",
    "SHIB": "SHIB-USDT-SWAP",
    "UNI": "UNI-USDT-SWAP",
    "LTC": "LTC-USDT-SWAP",
    "ATOM": "ATOM-USDT-SWAP",
    "NEAR": "NEAR-USDT-SWAP",
    "APT": "APT-USDT-SWAP",
    "ARB": "ARB-USDT-SWAP",
    "OP": "OP-USDT-SWAP",
}


async def fetch_okx_top_traders(symbol: str) -> dict:
    """
    OKX: Long/Short ratio of top traders (contract).
    Бесплатно, без ключа. Работает из EU сервера Railway (Amsterdam).
    Возвращает okx_top_long и okx_top_short в процентах.
    """
    if symbol not in OKX_INSTRUMENTS:
        return {}

    cache_key = f"okx_top_{symbol}"
    if cache_key in OKX_CACHE and (time.time() - OKX_CACHE[cache_key]["ts"]) < OKX_CACHE_TTL:
        return OKX_CACHE[cache_key]["data"]

    result = {}
    inst_id = OKX_INSTRUMENTS[symbol]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://www.okx.com/api/v5/rubik/stat/contracts-long-short-account-ratio-contract-top-trader",
                params={"instId": inst_id, "period": "1H"},
            )
            if resp.status_code != 200:
                log.warning(f"OKX Top Traders {symbol}: HTTP {resp.status_code}")
                return {}
            data = resp.json()
            items = data.get("data", [])
            if items:
                # Берём самое свежее значение (первый элемент)
                latest = items[0]
                ratio_str = latest.get("ratio")
                if ratio_str:
                    ratio = float(ratio_str)
                    # ratio = long accounts / short accounts
                    # long% = ratio / (1 + ratio) * 100
                    long_pct = ratio / (1 + ratio) * 100
                    short_pct = 100 - long_pct
                    result["okx_top_long"] = round(long_pct, 1)
                    result["okx_top_short"] = round(short_pct, 1)
                    log.info(f"  📊 OKX Top {symbol}: L={long_pct:.1f}% / S={short_pct:.1f}%")
    except Exception as e:
        log.warning(f"OKX Top Traders {symbol} error: {e}")

    if result:
        OKX_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


# ══════════════════════════════════════════════════════════════════════════════
# BITGET (бесплатно, без ключа — работает из US и EU серверов Railway)
# ══════════════════════════════════════════════════════════════════════════════

CROSS_EXCHANGE_CACHE = {}
CROSS_EXCHANGE_TTL = 15 * 60  # 15 минут


# Пары которые поддерживают L/S на Bitget (BNB, AVAX — не поддерживаются)
BITGET_LS_SUPPORTED = {"BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "DOT", "LINK", "AVAX", "POL", "UNI", "LTC", "ATOM", "NEAR", "APT", "ARB", "OP"}


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
    "XRP": "PF_XRPUSD",
    "ADA": "PF_ADAUSD",
    "DOGE": "PF_DOGEUSD",
    "DOT": "PF_DOTUSD",
    "LINK": "PF_LINKUSD",
    "LTC": "PF_LTCUSD",
    "ATOM": "PF_ATOMUSD",
    # BNB, AVAX, POL, TRX, SHIB, UNI, NEAR, APT, ARB, OP — нет на Kraken Futures
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


# ── Kraken Order Book (Bid/Ask Imbalance) ─────────────────────────────────

KRAKEN_OB_CACHE = {}
KRAKEN_OB_CACHE_TTL = 5 * 60  # 5 минут — order book меняется быстро


async def fetch_kraken_orderbook(symbol: str) -> dict:
    """
    Kraken Futures Order Book — Bid/Ask Imbalance.
    Показывает перекос стакана: кто давит — покупатели или продавцы.
    Бесплатно, без ключа. Поддерживает: BTC, ETH, SOL.
    """
    if symbol not in KRAKEN_FUTURES_SYMBOLS:
        return {}

    cache_key = f"kraken_ob_{symbol}"
    if cache_key in KRAKEN_OB_CACHE and (time.time() - KRAKEN_OB_CACHE[cache_key]["ts"]) < KRAKEN_OB_CACHE_TTL:
        return KRAKEN_OB_CACHE[cache_key]["data"]

    result = {}
    ticker = KRAKEN_FUTURES_SYMBOLS[symbol]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{KRAKEN_FUTURES_BASE}/orderbook",
                params={"symbol": ticker}
            )
            if resp.status_code != 200:
                log.warning(f"Kraken OB {symbol} HTTP {resp.status_code}")
                return {}
            data = resp.json()
            ob = data.get("orderBook", {})
            bids = ob.get("bids", [])  # [[price, qty], ...]
            asks = ob.get("asks", [])

            if bids and asks:
                # Суммируем объём в USD: price × qty для топ-20 уровней
                bid_depth = sum(b[0] * b[1] for b in bids[:20]) if len(bids) >= 20 else sum(b[0] * b[1] for b in bids)
                ask_depth = sum(a[0] * a[1] for a in asks[:20]) if len(asks) >= 20 else sum(a[0] * a[1] for a in asks)

                result["bid_depth_usd"] = bid_depth
                result["ask_depth_usd"] = ask_depth

                # Imbalance: >0.5 = покупатели давят, <0.5 = продавцы давят
                total = bid_depth + ask_depth
                if total > 0:
                    result["bid_ask_ratio"] = round(bid_depth / total, 3)

                log.info(f"  📕 Kraken OB {symbol}: Bids ${bid_depth/1e6:.1f}M | Asks ${ask_depth/1e6:.1f}M | Ratio={result.get('bid_ask_ratio', '?')}")

    except Exception as e:
        log.warning(f"Kraken OB {symbol} error: {e}")

    if result:
        KRAKEN_OB_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


# ══════════════════════════════════════════════════════════════════════════════
# dYdX v4 INDEXER (бесплатно, без ключа, DEX — нет гео-блокировки)
# ══════════════════════════════════════════════════════════════════════════════

# Маппинг наших символов на dYdX perpetual markets
DYDX_SYMBOLS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
    "ADA": "ADA-USD",
    "DOGE": "DOGE-USD",
    "AVAX": "AVAX-USD",
    "DOT": "DOT-USD",
    "LINK": "LINK-USD",
    "POL": "POL-USD",
    "TRX": "TRX-USD",
    "SHIB": "SHIB-USD",
    "UNI": "UNI-USD",
    "LTC": "LTC-USD",
    "ATOM": "ATOM-USD",
    "NEAR": "NEAR-USD",
    "APT": "APT-USD",
    "ARB": "ARB-USD",
    "OP": "OP-USD",
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
# ETHERSCAN — ETH Gas + Whale Wallets (бесплатно, 5 calls/sec)
# ══════════════════════════════════════════════════════════════════════════════

ETHERSCAN_CACHE = {}  # {key: {"data": ..., "ts": time.time()}}
ETHERSCAN_CACHE_TTL = 15 * 60  # 15 минут

async def fetch_etherscan_data() -> dict:
    """
    ETH on-chain данные из Etherscan:
    - Gas Price (Gwei) — показывает загрузку сети
    - ETH Supply — общее предложение
    """
    if not HAS_ETHERSCAN:
        return {}

    cache_key = "etherscan_main"
    if cache_key in ETHERSCAN_CACHE and (time.time() - ETHERSCAN_CACHE[cache_key]["ts"]) < ETHERSCAN_CACHE_TTL:
        return ETHERSCAN_CACHE[cache_key]["data"]

    result = {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Gas Price
            resp = await client.get(ETHERSCAN_BASE, params={
                "module": "gastracker",
                "action": "gasoracle",
                "apikey": ETHERSCAN_KEY,
            })
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "1" and data.get("result"):
                    r = data["result"]
                    result["eth_gas_low"] = int(r.get("SafeGasPrice", 0))
                    result["eth_gas_avg"] = int(r.get("ProposeGasPrice", 0))
                    result["eth_gas_high"] = int(r.get("FastGasPrice", 0))
                    log.info(f"  ⛽ ETH Gas: {result['eth_gas_low']}/{result['eth_gas_avg']}/{result['eth_gas_high']} Gwei")

            await asyncio.sleep(0.3)  # Rate limit respect

            # ETH Supply (total)
            resp2 = await client.get(ETHERSCAN_BASE, params={
                "module": "stats",
                "action": "ethsupply",
                "apikey": ETHERSCAN_KEY,
            })
            if resp2.status_code == 200:
                data2 = resp2.json()
                if data2.get("status") == "1" and data2.get("result"):
                    # Result in Wei, convert to ETH
                    supply_wei = int(data2["result"])
                    supply_eth = supply_wei / 1e18
                    result["eth_supply"] = supply_eth
                    log.info(f"  📊 ETH Supply: {supply_eth:,.0f} ETH")

    except Exception as e:
        log.warning(f"Etherscan error: {e}")

    if result:
        ETHERSCAN_CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result



# ══════════════════════════════════════════════════════════════════════════════
# LLM-АНАЛИЗ (Claude API)
# ══════════════════════════════════════════════════════════════════════════════

async def generate_llm_analysis(symbol: str, coin_data: dict, pipeline: dict = None) -> dict:
    """
    Отправляет метрики монеты в Claude API.
    pipeline — результат run_signal_pipeline() (3-слойный анализ).
    LLM НЕ решает что покупать/продавать — он только формулирует текст
    на основе уже посчитанного scoring.
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
    # Cross-exchange данные (OKX + Bitget + Kraken + dYdX)
    okx_top_long = coin_data.get("okx_top_long")
    okx_top_short = coin_data.get("okx_top_short")
    bitget_long_acc = coin_data.get("bitget_long_acc")
    bitget_short_acc = coin_data.get("bitget_short_acc")
    bitget_long_pos = coin_data.get("bitget_long_pos")
    bitget_short_pos = coin_data.get("bitget_short_pos")
    bitget_oi_usd = coin_data.get("bitget_oi_usd")
    kraken_funding = coin_data.get("kraken_funding")
    kraken_oi = coin_data.get("kraken_oi")
    dydx_funding = coin_data.get("dydx_funding")
    dydx_oi = coin_data.get("dydx_oi")
    # Order Book (Bid/Ask Imbalance)
    bid_depth = coin_data.get("bid_depth_usd")
    ask_depth = coin_data.get("ask_depth_usd")
    bid_ask_ratio = coin_data.get("bid_ask_ratio")
    # ETH Gas (Etherscan)
    eth_gas = coin_data.get("eth_gas_avg")
    # CVD + Order Book Imbalance (Binance Futures)
    cvd_value = coin_data.get("cvd_value")
    cvd_trend = coin_data.get("cvd_trend")
    cvd_side = coin_data.get("cvd_side")
    obi_value = coin_data.get("obi_value")
    obi_side = coin_data.get("obi_side")
    obi_bid_vol = coin_data.get("obi_bid_vol")
    obi_ask_vol = coin_data.get("obi_ask_vol")
    obi_support_price = coin_data.get("obi_support_price")
    obi_support_vol = coin_data.get("obi_support_vol")
    obi_resistance_price = coin_data.get("obi_resistance_price")
    obi_resistance_vol = coin_data.get("obi_resistance_vol")
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
                onchain_lines.append(f"- SMA50/200: тренд вверх (SMA50 ${sma50:,.0f} > SMA200 ${sma200:,.0f}) — бычий")
            else:
                onchain_lines.append(f"- SMA50/200: тренд вниз (SMA50 ${sma50:,.0f} < SMA200 ${sma200:,.0f}) — медвежий")

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

    # Cross-exchange блок (OKX + Bitget + Kraken + dYdX vs Coinglass)
    if any([okx_top_long, bitget_long_acc, bitget_long_pos, kraken_funding, dydx_funding, bid_ask_ratio]):
        onchain_lines.append(f"\nCROSS-EXCHANGE ДАННЫЕ (мульти-биржевое сравнение):")
        if okx_top_long is not None:
            okx_hint = "топ-трейдеры в лонгах" if okx_top_long > 55 else "топ-трейдеры в шортах" if okx_top_long < 45 else "баланс"
            onchain_lines.append(f"- OKX Top Traders: L {okx_top_long:.1f}% / S {okx_top_short:.1f}% — {okx_hint}")
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

    # Order Book Imbalance (Kraken Futures)
    if bid_ask_ratio is not None and bid_depth is not None:
        onchain_lines.append(f"\nORDER BOOK (Kraken Futures — глубина стакана):")
        onchain_lines.append(f"- Bids (покупки): ${bid_depth/1e6:.1f}M")
        onchain_lines.append(f"- Asks (продажи): ${ask_depth/1e6:.1f}M")
        if bid_ask_ratio > 0.55:
            ob_hint = "ПОКУПАТЕЛИ давят — сильная поддержка снизу"
        elif bid_ask_ratio < 0.45:
            ob_hint = "ПРОДАВЦЫ давят — давление сверху"
        else:
            ob_hint = "баланс покупателей/продавцов"
        onchain_lines.append(f"- Bid/Ask Ratio: {bid_ask_ratio:.1%} — {ob_hint}")

    # ETH Gas (Etherscan)
    if eth_gas and symbol == "ETH":
        onchain_lines.append(f"\nETH СЕТЬ:")
        onchain_lines.append(f"- Gas Price: {eth_gas} Gwei")
        gas_hint = "высокая нагрузка" if eth_gas > 50 else "умеренная нагрузка" if eth_gas > 20 else "низкая нагрузка"
        onchain_lines.append(f"- Активность сети: {gas_hint}")

    # OPTIONS DATA (Deribit + Binance + OKX)
    opt_pcr = coin_data.get("options_pcr")
    opt_iv = coin_data.get("options_iv")
    opt_max_pain = coin_data.get("options_max_pain")
    opt_oi_calls = coin_data.get("options_oi_calls")
    opt_oi_puts = coin_data.get("options_oi_puts")
    if any([opt_pcr, opt_iv, opt_max_pain]):
        onchain_lines.append(f"\nОПЦИОНЫ (Deribit — крупнейший рынок криптоопционов):")
        if opt_pcr is not None:
            pcr_hint = "медвежий настрой (хеджируют падение)" if opt_pcr > 1.0 else "бычий настрой (ставят на рост)" if opt_pcr < 0.7 else "нейтральный"
            onchain_lines.append(f"- Put/Call Ratio: {opt_pcr} — {pcr_hint}")
        if opt_iv is not None:
            iv_hint = "ВЫСОКАЯ волатильность — ожидают сильное движение" if opt_iv > 80 else "повышенная волатильность" if opt_iv > 60 else "умеренная волатильность" if opt_iv > 40 else "низкая волатильность — затишье"
            onchain_lines.append(f"- Implied Volatility: {opt_iv}% — {iv_hint}")
        if opt_max_pain is not None:
            price = coin_data.get("price")
            if price:
                mp_dir = "цена ВЫШЕ Max Pain (давление вниз к экспирации)" if price > opt_max_pain else "цена НИЖЕ Max Pain (давление вверх к экспирации)"
                onchain_lines.append(f"- Max Pain: ${opt_max_pain:,.0f} — {mp_dir}")
            else:
                onchain_lines.append(f"- Max Pain: ${opt_max_pain:,.0f}")
        if opt_oi_calls and opt_oi_puts:
            onchain_lines.append(f"- OI: {opt_oi_calls:,.0f} calls / {opt_oi_puts:,.0f} puts")

    # ПОТОК ДЕНЕГ: CVD + Order Book Imbalance (Binance Futures)
    if any([cvd_value is not None, obi_value is not None]):
        onchain_lines.append(f"\nПОТОК ДЕНЕГ (кто реально двигает рынок):")
        if cvd_value is not None:
            cvd_arrow = "↑" if cvd_trend == "rising" else "↓"
            cvd_who = "покупатели давят" if cvd_side == "buyers" else "продавцы давят"
            onchain_lines.append(f"- CVD: {cvd_arrow} ${cvd_value:+.1f}M — {cvd_who}")
            # Дивергенция CVD vs цена — самый сильный сигнал!
            if change is not None:
                try:
                    change_val = float(change)
                    if change_val > 0 and cvd_value < 0:
                        onchain_lines.append(f"  ⚠️ ДИВЕРГЕНЦИЯ: цена растёт, но CVD падает — рост может быть искусственным!")
                    elif change_val < 0 and cvd_value > 0:
                        onchain_lines.append(f"  ⚠️ ДИВЕРГЕНЦИЯ: цена падает, но CVD растёт — возможен разворот вверх (киты аккумулируют)")
                except (ValueError, TypeError):
                    pass
        if obi_value is not None:
            obi_hint = "покупатели (сильная поддержка снизу)" if obi_side == "BUY" else "продавцы (стена продаж сверху)" if obi_side == "SELL" else "баланс"
            onchain_lines.append(f"- Order Book Imbalance: {obi_value:+.3f} — {obi_hint}")
            if obi_bid_vol and obi_ask_vol:
                onchain_lines.append(f"  Bids: ${obi_bid_vol/1e6:.1f}M / Asks: ${obi_ask_vol/1e6:.1f}M")
            if obi_support_price and obi_support_vol:
                onchain_lines.append(f"  Поддержка: ${obi_support_price:,.0f} (${obi_support_vol/1e6:.1f}M)")
            if obi_resistance_price and obi_resistance_vol:
                onchain_lines.append(f"  Сопротивление: ${obi_resistance_price:,.0f} (${obi_resistance_vol/1e6:.1f}M)")

        # WHALE ALERT: крупные транзакции китов (если есть данные)
        whale_txs = coin_data.get("whale_txs", 0)
        whale_to_ex = coin_data.get("whale_to_exchange", 0)
        whale_from_ex = coin_data.get("whale_from_exchange", 0)
        whale_total = coin_data.get("whale_total_usd", 0)
        whale_dir = coin_data.get("whale_direction", "")

        if whale_txs and whale_total and whale_total > 0:
            onchain_lines.append(f"\nКИТЫ (Whale Alert, транзакции > $500K за 1ч):")
            onchain_lines.append(f"- {whale_txs} крупных транзакций, объём ${whale_total/1e6:.1f}M")
            if whale_to_ex > 0:
                onchain_lines.append(f"- На биржи: ${whale_to_ex/1e6:.1f}M (готовятся продавать — медвежий)")
            if whale_from_ex > 0:
                onchain_lines.append(f"- С бирж: ${whale_from_ex/1e6:.1f}M (накопление — бычий)")
            if whale_dir == "bullish":
                onchain_lines.append(f"  → Киты ВЫВОДЯТ с бирж — накопление, бычий сигнал")
            elif whale_dir == "bearish":
                onchain_lines.append(f"  → Киты ЗАВОДЯТ на биржи — готовятся продавать, медвежий сигнал")

        # КОМБО-СИГНАЛ: FR отрицательный + CVD растёт + OBI BUY = short squeeze
        if fr is not None and cvd_value is not None and obi_side is not None:
            try:
                fr_val = float(fr)
                if fr_val < 0 and cvd_value > 0 and obi_side == "BUY":
                    onchain_lines.append(f"  🔥 КОМБО: FR отрицательный + CVD растёт + стакан BUY = вероятен short squeeze!")
                elif fr_val > 0.05 and cvd_value < 0 and obi_side == "SELL":
                    onchain_lines.append(f"  🔥 КОМБО: FR высокий + CVD падает + стакан SELL = вероятен long squeeze!")
            except (ValueError, TypeError):
                pass

    if onchain_lines:
        onchain_block = "\n".join(onchain_lines)

    # 3-слойный scoring (уже посчитан rule-based)
    p = pipeline or {}
    direction_label = p.get("direction_label", "?")
    state_label = p.get("state_label", "?")
    quality_label = p.get("quality_label", "?")
    pre_recommendation = p.get("recommendation", "выжидать")
    pre_strength = p.get("strength", "слабо")
    pre_trap = p.get("trap", "нет")
    pre_trap_display = p.get("trap_display", pre_trap)
    pre_horizon = p.get("horizon", "1-2 дня")
    pre_prob_bull = p.get("prob_bull", 50)
    pre_prob_bear = p.get("prob_bear", 50)
    pre_funding_conflict = p.get("funding_conflict", "")
    scoring_factors = p.get("factors", [])
    scoring_conflicts = p.get("conflicts", [])
    dir_factors_bull = p.get("dir_factors_bull", [])
    dir_factors_bear = p.get("dir_factors_bear", [])

    factors_text = ""
    if dir_factors_bull:
        factors_text += f"\nБычьи факторы: {', '.join(dir_factors_bull)}"
    if dir_factors_bear:
        factors_text += f"\nМедвежьи факторы: {', '.join(dir_factors_bear)}"
    if scoring_factors:
        factors_text += f"\nПодтверждающие: {', '.join(scoring_factors)}"
    if scoring_conflicts:
        factors_text += f"\nКонфликты: {', '.join(scoring_conflicts)}"

    prompt = f"""Ты — копирайтер крипто-терминала. Тебе УЖЕ ПОСЧИТАН анализ алгоритмом.
Твоя задача — ТОЛЬКО сформулировать текст для пользователя. НЕ ПЕРЕСЧИТЫВАЙ анализ.

{symbol} СЕЙЧАС:
- Цена: ${price}, изменение 24ч: {safe_pct(change)}
- Funding Rate: {safe_pct(fr)}
- Покупатели/Продавцы: {long_pct or '?'}% / {short_pct or '?'}%
- Fear & Greed: {fg or '?'} ({fg_label or '?'}){onchain_block}

АЛГОРИТМ УЖЕ РЕШИЛ (НЕ МЕНЯЙ ЭТО):
- НАПРАВЛЕНИЕ: {direction_label}
- СОСТОЯНИЕ РЫНКА: {state_label}
- КАЧЕСТВО СЕТАПА: {quality_label}
- РЕКОМЕНДАЦИЯ: {pre_recommendation}
- СИЛА: {pre_strength}
- ЛОВУШКА: {pre_trap}
- ГОРИЗОНТ: {pre_horizon}{factors_text}

ТВОЯ ЗАДАЧА — написать ТОЛЬКО:
1. ЧТО_ПРОИСХОДИТ — короткое объяснение простым языком (до 120 символов)
2. ВХОД / СТОП / ЦЕЛЬ — конкретные цены

ПРАВИЛА:
- ЧТО_ПРОИСХОДИТ: МАКСИМУМ 120 СИМВОЛОВ. Одно предложение! Объясни простым языком.
- ОБЯЗАТЕЛЬНО упомяни {symbol} и хотя бы одну конкретную цифру (цену, %, объём). Текст должен быть УНИКАЛЬНЫМ для каждой монеты!
- ЗАПРЕЩЁННЫЕ СЛОВА: RSI, MACD, SOPR, Bid/Ask, death cross, golden cross, short squeeze, funding rate, Fear & Greed, netflow, leverage. Используй: "тренд вниз", "страх на рынке", "покупатели давят", "шорты в ловушке" и т.д.
- ВХОД: текущая цена или чуть ниже для покупки / чуть выше для продажи
- СТОП: 1-2% от входа
- ЦЕЛЬ: 2-4% от входа
- Без ** звёздочек и markdown
- НЕ ПИШИ БОЛЬШЕ 4 СТРОК

ОТВЕТЬ СТРОГО В ФОРМАТЕ (4 строки, не больше):
ЧТО_ПРОИСХОДИТ: одно предложение, до 120 символов
ВХОД: $XXX,XXX
СТОП: $XXX,XXX
ЦЕЛЬ: $XXX,XXX"""

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
                    "max_tokens": 200,
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

            # Парсим ответ LLM (только what_happening + entry/stop/target)
            # Recommendation, strength, trap, horizon — из pipeline (rule-based)
            result = {}
            for line in text.split("\n"):
                clean = line.strip().lstrip("*").lstrip("#").strip()
                upper = clean.upper()
                val = clean.split(":", 1)[1].strip().replace("**", "").replace("__", "").replace("*", "").replace("_", "") if ":" in clean else ""

                if upper.startswith("ЧТО_ПРОИСХОДИТ:") or upper.startswith("ЧТО ПРОИСХОДИТ:"):
                    if val:
                        clean_val = val.strip()
                        if len(clean_val) > 120:
                            clean_val = clean_val[:117] + "..."
                        result["what_happening"] = clean_val
                elif upper.startswith("ВХОД:"):
                    if val:
                        result["entry"] = val.strip().replace("[", "").replace("]", "")
                elif upper.startswith("СТОП:"):
                    if val:
                        result["stop"] = val.strip().replace("[", "").replace("]", "")
                elif upper.startswith("ЦЕЛЬ:"):
                    if val:
                        result["target"] = val.strip().replace("[", "").replace("]", "")

            # Fallback: если what_happening не нашли — берём первую длинную строку
            if not result.get("what_happening") and text:
                for line in text.split("\n"):
                    clean = line.strip().lstrip("*").strip()
                    if len(clean) > 20 and not clean.upper().startswith(("ВХОД", "СТОП", "ЦЕЛЬ")):
                        result["what_happening"] = clean.split(":", 1)[1].strip() if ":" in clean else clean
                        if len(result["what_happening"]) > 120:
                            result["what_happening"] = result["what_happening"][:117] + "..."
                        break

            # Замена запрещённых англицизмов (LLM может игнорировать бан)
            if result.get("what_happening"):
                _banned = {
                    "death cross": "тренд вниз",
                    "golden cross": "тренд вверх",
                    "short squeeze": "шорты в ловушке",
                    "long squeeze": "лонги в ловушке",
                }
                wh = result["what_happening"]
                for eng, rus in _banned.items():
                    wh = wh.replace(eng, rus).replace(eng.title(), rus).replace(eng.upper(), rus)
                result["what_happening"] = wh

            # Pipeline данные — rule-based (НЕ из LLM!)
            result["recommendation"] = pre_recommendation
            result["strength"] = pre_strength
            result["trap"] = pre_trap if pre_trap != "нет" else ""
            result["trap_display"] = pre_trap_display if pre_trap_display != "нет" else ""
            result["horizon"] = pre_horizon
            result["prob_bull"] = pre_prob_bull
            result["prob_bear"] = pre_prob_bear
            result["funding_conflict"] = pre_funding_conflict

            # Backward compat
            if result.get("what_happening"):
                result["llm_text"] = result["what_happening"]

            log.info(f"  🤖 LLM {symbol}: {pre_recommendation} ({pre_strength}) | {state_label}")
            return result

    except Exception as e:
        log.warning(f"LLM error {symbol}: {e}")
        # Даже без LLM — возвращаем pipeline данные
        return {
            "recommendation": pre_recommendation,
            "strength": pre_strength,
            "trap": pre_trap if pre_trap != "нет" else "",
            "trap_display": pre_trap_display if pre_trap_display != "нет" else "",
            "horizon": pre_horizon,
            "prob_bull": pre_prob_bull,
            "prob_bear": pre_prob_bear,
            "funding_conflict": pre_funding_conflict,
            "what_happening": "",
            "llm_text": "",
        }


# ══════════════════════════════════════════════════════════════════════════════
# 3-СЛОЙНАЯ АРХИТЕКТУРА СИГНАЛА
# ══════════════════════════════════════════════════════════════════════════════
#
#   RAW DATA → СЛОЙ 1 (направление) → СЛОЙ 2 (состояние) → СЛОЙ 3 (качество)
#                                                               ↓
async def generate_options_ai(symbol: str, opts: dict, lang: str = "ru") -> str:
    """
    AI-анализ опционных данных для BTC/ETH.
    Короткий текст 3-4 предложения, объясняющий что значат опционные метрики.
    """
    if not HAS_ANTHROPIC:
        return ""

    pcr = opts.get("pcr", "—")
    max_pain = opts.get("max_pain", "—")
    iv = opts.get("iv", "—")
    oi_calls = opts.get("oi_calls", "—")
    oi_puts = opts.get("oi_puts", "—")
    price = opts.get("price", "—")
    expiries = opts.get("expiries", "—")

    lang_instruction = "Отвечай на русском." if lang == "ru" else "Answer in English."

    prompt = f"""Ты крипто-аналитик. Объясни опционные данные {symbol} простым языком.
{lang_instruction}

Данные:
- Цена: {price}
- Put/Call Ratio: {pcr}
- Max Pain: {max_pain}
- Implied Volatility: {iv}%
- OI Calls: {oi_calls} | OI Puts: {oi_puts}
- Ближайшие экспирации: {expiries}

Напиши 3-4 коротких предложения:
1. Что говорит PCR — бычий или медвежий рынок опционов
2. Куда тянет Max Pain цену и что это значит
3. Что значит текущая IV — ждать ли резких движений
4. Если скоро экспирация — предупреди о волатильности

Максимум 200 символов. Без звёздочек. Конкретно и полезно для трейдера."""

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
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                text = data.get("content", [{}])[0].get("text", "")
                text = text.replace("**", "").replace("*", "").strip()
                if len(text) > 300:
                    text = text[:297] + "..."
                log.info(f"  🤖 Options AI {symbol}: {len(text)} chars")
                return text
            else:
                log.warning(f"  ⚠️ Options AI {symbol}: HTTP {resp.status_code}")
                return ""
    except Exception as e:
        log.warning(f"  ⚠️ Options AI {symbol}: {e}")
        return ""


#                                                        LLM FORMATTER
#                                                               ↓
#                                                       TELEGRAM OUTPUT
#
# Математика считает — LLM только объясняет.
# ══════════════════════════════════════════════════════════════════════════════


def calculate_direction(coin_data: dict) -> dict:
    """
    СЛОЙ 1: НАПРАВЛЕНИЕ РЫНКА
    Отвечает на вопрос: куда сейчас давление — вверх, вниз или боковик?

    Смотрит на: цена, funding, long/short, OI, buy/sell pressure, ликвидации
    Это индикаторы ПОТОКА ДЕНЕГ — куда двигаются деньги прямо сейчас.
    """
    bull = 0
    bear = 0
    factors_bull = []
    factors_bear = []

    # 1. Taker Buy/Sell (давление маркет-ордеров)
    long_pct = coin_data.get("long_pct")
    if long_pct is not None:
        if long_pct > 55:
            bull += 1
            factors_bull.append("BUY давление сильное")
        elif long_pct > 52:
            bull += 1
            factors_bull.append("BUY давление выше")
        elif long_pct < 45:
            bear += 1
            factors_bear.append("SELL давление сильное")
        elif long_pct < 48:
            bear += 1
            factors_bear.append("SELL давление выше")

    # 2. OI change (поток новых денег)
    oi_change = coin_data.get("oi_change_1h")
    if oi_change is not None:
        if oi_change > 0.5:
            bull += 1
            factors_bull.append("новые деньги заходят (OI растёт)")
        elif oi_change < -0.5:
            bear += 1
            factors_bear.append("деньги уходят (OI падает)")

    # 3. Funding Rate (кто переплачивает)
    fr = coin_data.get("funding_rate")
    if fr is not None:
        if fr < -0.005:
            bull += 1
            factors_bull.append("шорты переплачивают (short squeeze)")
        elif fr > 0.05:
            bear += 1
            factors_bear.append("лонги переплачивают (перегрев)")

    # 4. Ликвидации (кого выносит)
    liq_long = coin_data.get("liq_long")
    liq_short = coin_data.get("liq_short")
    if liq_long and liq_short:
        try:
            ll = float(liq_long)
            ls = float(liq_short)
            if ls > ll * 2 and ls > 0:
                bull += 1
                factors_bull.append("шортов ликвидируют")
            elif ll > ls * 2 and ll > 0:
                bear += 1
                factors_bear.append("лонгов ликвидируют")
        except (ValueError, TypeError):
            pass

    # 5. Цена 24ч (краткосрочный импульс)
    change = coin_data.get("change_24h")
    if change is not None:
        if change > 3:
            bull += 1
            factors_bull.append("цена растёт")
        elif change < -3:
            bear += 1
            factors_bear.append("цена падает")

    # 6. MACD (технический импульс)
    macd = coin_data.get("macd")
    if macd is not None:
        if macd > 0:
            bull += 1
            factors_bull.append("бычий импульс (MACD)")
        elif macd < 0:
            bear += 1
            factors_bear.append("медвежий импульс (MACD)")

    # 7. Cross-Exchange Funding consensus
    cg_fr = coin_data.get("funding_rate")
    kraken_fr = coin_data.get("kraken_funding")
    dydx_fr = coin_data.get("dydx_funding")
    fr_neg = sum(1 for f in [cg_fr, kraken_fr, dydx_fr] if f is not None and f < -0.005)
    fr_pos = sum(1 for f in [cg_fr, kraken_fr, dydx_fr] if f is not None and f > 0.03)
    if fr_neg >= 2:
        bull += 1
        factors_bull.append("мульти-биржевой short squeeze")
    if fr_pos >= 2:
        bear += 1
        factors_bear.append("мульти-биржевой перегрев")

    # 8. Bitget + Coinglass позиции согласны
    bitget_pos_l = coin_data.get("bitget_long_pos")
    cg_long_pct = coin_data.get("long_pct")
    if bitget_pos_l is not None and cg_long_pct is not None:
        if bitget_pos_l > 55 and cg_long_pct > 55:
            bull += 1
            factors_bull.append("позиции на двух биржах в лонгах")
        elif bitget_pos_l < 45 and cg_long_pct < 45:
            bear += 1
            factors_bear.append("позиции на двух биржах в шортах")

    # 9. Whale Alert (крупные транзакции китов)
    whale_dir = coin_data.get("whale_direction")
    whale_total = coin_data.get("whale_total_usd", 0)
    if whale_dir and whale_total and whale_total > 1_000_000:  # > $1M суммарно
        if whale_dir == "bullish":
            bull += 1
            factors_bull.append("киты выводят с бирж (Whale Alert)")
        elif whale_dir == "bearish":
            bear += 1
            factors_bear.append("киты заводят на биржи (Whale Alert)")

    # Определяем направление
    if bull > bear and bull >= 2:
        direction = "UP"
        direction_label = "вверх"
    elif bear > bull and bear >= 2:
        direction = "DOWN"
        direction_label = "вниз"
    else:
        direction = "SIDEWAYS"
        direction_label = "боковик"

    return {
        "direction": direction,
        "direction_label": direction_label,
        "dir_bull": bull,
        "dir_bear": bear,
        "dir_factors_bull": factors_bull,
        "dir_factors_bear": factors_bear,
    }


def calculate_market_state(coin_data: dict, direction: dict) -> dict:
    """
    СЛОЙ 2: СОСТОЯНИЕ РЫНКА
    Отвечает на вопрос: что это за фаза рынка?

    Смотрит на: RSI, fear/greed, crowd bias, киты, netflow, ликвидационный дисбаланс
    Это индикаторы НАСТРОЕНИЯ и ПОЗИЦИОНИРОВАНИЯ.
    """
    rsi = coin_data.get("rsi")
    fg = coin_data.get("fear_greed")
    fr = coin_data.get("funding_rate")
    long_pct = coin_data.get("long_pct")
    exchange_netflow = coin_data.get("exchange_netflow_btc")
    bitget_acc_l = coin_data.get("bitget_long_acc")
    sopr = coin_data.get("sopr")
    liq_long = coin_data.get("liq_long")
    liq_short = coin_data.get("liq_short")
    change = coin_data.get("change_24h")

    # Short squeeze: нужна КОМБИНАЦИЯ факторов (ужесточено — раньше срабатывало слишком часто)
    is_short_squeeze = False
    _ss_signals = 0
    if fr is not None and fr < -0.01:
        _ss_signals += 1
    if fg is not None and fg < 25:
        _ss_signals += 1
    if liq_short and liq_long:
        try:
            if float(liq_short) > float(liq_long) * 2.0:
                _ss_signals += 1
        except (ValueError, TypeError):
            pass
    if bitget_acc_l is not None and bitget_acc_l < 30:
        _ss_signals += 1
    # Нужно минимум 3 из 4 факторов
    if _ss_signals >= 3:
        is_short_squeeze = True

    # Long squeeze: нужна КОМБИНАЦИЯ факторов (ужесточено)
    is_long_squeeze = False
    _ls_signals = 0
    if fr is not None and fr > 0.05:
        _ls_signals += 1
    if fg is not None and fg > 75:
        _ls_signals += 1
    if liq_long and liq_short:
        try:
            if float(liq_long) > float(liq_short) * 2.0:
                _ls_signals += 1
        except (ValueError, TypeError):
            pass
    if bitget_acc_l is not None and bitget_acc_l > 70:
        _ls_signals += 1
    # Нужно минимум 3 из 4 факторов
    if _ls_signals >= 3:
        is_long_squeeze = True

    # Перегруз лонгов (толпа в лонгах + опасные признаки)
    crowd_long_overload = (bitget_acc_l is not None and bitget_acc_l > 70)
    crowd_short_overload = (bitget_acc_l is not None and bitget_acc_l < 30)

    # Паника
    is_panic = (fg is not None and fg < 20 and rsi is not None and rsi < 35)

    # Накопление (киты выводят + рынок спокойный)
    is_accumulation = (exchange_netflow is not None and exchange_netflow < -500
                       and sopr is not None and sopr < 1.0)

    # Распределение (киты заводят на биржи + рынок на пике)
    is_distribution = (exchange_netflow is not None and exchange_netflow > 500
                       and fg is not None and fg > 65)

    # Определяем состояние (приоритет)
    dir_code = direction.get("direction", "SIDEWAYS")
    if is_short_squeeze and dir_code == "UP":
        state = "SHORT_SQUEEZE"
        state_label = "short squeeze"
    elif is_long_squeeze and dir_code == "DOWN":
        state = "LONG_SQUEEZE"
        state_label = "long squeeze"
    elif crowd_long_overload and dir_code != "UP":
        state = "LONG_OVERLOAD"
        state_label = "перегруз лонгов"
    elif crowd_short_overload and dir_code != "DOWN":
        state = "SHORT_OVERLOAD"
        state_label = "перегруз шортов"
    elif is_panic:
        state = "PANIC"
        state_label = "паника"
    elif is_accumulation:
        state = "ACCUMULATION"
        state_label = "накопление"
    elif is_distribution:
        state = "DISTRIBUTION"
        state_label = "распределение"
    elif rsi is not None and rsi < 30 and change is not None and change < -3:
        state = "BOUNCE"
        state_label = "отскок"
    else:
        state = "NEUTRAL"
        state_label = "боковик"

    # Определяем ловушку (только при реальном squeeze, crowd_overload сам по себе не триггерит)
    trap = "нет"
    if is_short_squeeze:
        trap = "шорты в ловушке"
    elif is_long_squeeze:
        trap = "лонги в ловушке"

    return {
        "state": state,
        "state_label": state_label,
        "trap": trap,
        "is_short_squeeze": is_short_squeeze,
        "is_long_squeeze": is_long_squeeze,
    }


def calculate_setup_quality(coin_data: dict, direction: dict, market_state: dict) -> dict:
    """
    СЛОЙ 3: КАЧЕСТВО СЕТАПА
    Отвечает на вопрос: насколько хороший момент для входа?

    Смотрит на: конфликты между индикаторами, совпадение китов/funding/pressure,
    близость стопов/ликвидаций, не поздний ли вход.
    """
    score = 0
    factors = []
    conflicts = []

    dir_code = direction.get("direction", "SIDEWAYS")
    dir_bull = direction.get("dir_bull", 0)
    dir_bear = direction.get("dir_bear", 0)
    state_code = market_state.get("state", "NEUTRAL")
    trap = market_state.get("trap", "нет")

    # 1. Совпадение направления и состояния
    if dir_code == "UP" and state_code in ("SHORT_SQUEEZE", "ACCUMULATION", "PANIC", "BOUNCE", "SHORT_OVERLOAD"):
        score += 2
        factors.append("направление и состояние совпадают")
    elif dir_code == "DOWN" and state_code in ("LONG_SQUEEZE", "DISTRIBUTION", "LONG_OVERLOAD"):
        score += 2
        factors.append("направление и состояние совпадают")
    elif dir_code == "SIDEWAYS":
        score -= 1
        conflicts.append("нет выраженного направления")

    # 2. Сила направления (много факторов согласны)
    agreement = max(dir_bull, dir_bear)
    total = dir_bull + dir_bear
    if agreement >= 5:
        score += 2
        factors.append(f"сильное согласие ({agreement} факторов)")
    elif agreement >= 3:
        score += 1
        factors.append(f"умеренное согласие ({agreement} факторов)")

    # 3. Есть ловушка? (это усиливает сигнал)
    if trap != "нет":
        score += 1
        factors.append(f"есть ловушка ({trap})")

    # 4. Киты + давление совпадают
    exchange_netflow = coin_data.get("exchange_netflow_btc")
    long_pct = coin_data.get("long_pct")
    if exchange_netflow is not None and long_pct is not None:
        if exchange_netflow < -500 and long_pct > 52:
            score += 1
            factors.append("киты и давление совпадают (бычий)")
        elif exchange_netflow > 500 and long_pct < 48:
            score += 1
            factors.append("киты и давление совпадают (медвежий)")
        elif (exchange_netflow < -500 and long_pct < 48) or (exchange_netflow > 500 and long_pct > 52):
            score -= 1
            conflicts.append("киты и давление расходятся")

    # 5. Конфликт: направление вверх но толпа уже перегружена лонгами
    bitget_acc_l = coin_data.get("bitget_long_acc")
    if dir_code == "UP" and bitget_acc_l is not None and bitget_acc_l > 70:
        score -= 1
        conflicts.append("толпа уже перегружена лонгами")

    # 6. Конфликт: направление вниз но толпа уже перегружена шортами
    if dir_code == "DOWN" and bitget_acc_l is not None and bitget_acc_l < 30:
        score -= 1
        conflicts.append("толпа уже перегружена шортами")

    # 7. RSI + Fear/Greed согласны с направлением
    rsi = coin_data.get("rsi")
    fg = coin_data.get("fear_greed")
    if dir_code == "UP":
        if rsi is not None and rsi < 35 and fg is not None and fg < 35:
            score += 1
            factors.append("перепродан + страх = хороший вход для покупки")
    elif dir_code == "DOWN":
        if rsi is not None and rsi > 65 and fg is not None and fg > 65:
            score += 1
            factors.append("перекуплен + жадность = хороший вход для продажи")

    # Определяем качество
    if score >= 4:
        quality = "STRONG"
        quality_label = "сильный"
    elif score >= 2:
        quality = "MEDIUM"
        quality_label = "средний"
    elif score >= 0:
        quality = "WEAK"
        quality_label = "слабый"
    else:
        quality = "POOR"
        quality_label = "плохой"

    # ── УВЕРЕННОСТЬ (на основе качества + согласия) ──
    if total == 0:
        conf_level = 1
        conf_label = "нет данных"
    else:
        ratio = agreement / total if total > 0 else 0
        if ratio >= 0.85 and agreement >= 5 and score >= 3:
            conf_level = 5
            conf_label = "очень высокая"
        elif ratio >= 0.75 and agreement >= 4 and score >= 2:
            conf_level = 4
            conf_label = "высокая"
        elif ratio >= 0.65 and agreement >= 3 and score >= 1:
            conf_level = 3
            conf_label = "средняя"
        elif ratio >= 0.55 and score >= 0:
            conf_level = 2
            conf_label = "ниже средней"
        else:
            conf_level = 1
            conf_label = "слабая"

    conf_bars = "🟩" * conf_level + "⬜" * (5 - conf_level)

    # ── СИГНАЛ (направление + сила) ──
    if dir_code == "UP" and quality in ("STRONG", "MEDIUM"):
        recommendation = "покупать"
    elif dir_code == "DOWN" and quality in ("STRONG", "MEDIUM"):
        recommendation = "продавать"
    else:
        recommendation = "выжидать"

    # Сила сигнала
    if quality == "STRONG":
        strength = "сильно"
        sig_normalized = 5
    elif quality == "MEDIUM":
        strength = "умеренно"
        sig_normalized = 3
    elif quality == "WEAK":
        strength = "слабо"
        sig_normalized = 2
    else:
        strength = "слабо"
        sig_normalized = 1

    signal_bar = "🟩" * sig_normalized + "⬜" * (5 - sig_normalized)
    signal_label = strength.upper()

    # Горизонт
    if state_code in ("BOUNCE", "SHORT_SQUEEZE", "LONG_SQUEEZE"):
        horizon = "4-12 часов"
    elif state_code in ("PANIC", "SHORT_OVERLOAD", "LONG_OVERLOAD"):
        horizon = "краткосрочный отскок"
    elif state_code in ("ACCUMULATION", "DISTRIBUTION"):
        horizon = "среднесрочно"
    else:
        horizon = "1-2 дня"

    # ── FIX: Согласование ловушки с рекомендацией ──
    # Если шорты в ловушке но сигнал "продавать" — объясняем конфликт
    # Если лонги в ловушке но сигнал "покупать" — объясняем конфликт
    trap_display = trap
    if trap == "шорты в ловушке" and recommendation == "продавать":
        trap_display = "шорты в ловушке — возможен short squeeze, но тренд вниз"
    elif trap == "лонги в ловушке" and recommendation == "покупать":
        trap_display = "лонги в ловушке — возможен long squeeze, но тренд вверх"
    elif trap == "шорты в ловушке":
        trap_display = "шорты в ловушке — возможен рост (short squeeze)"
    elif trap == "лонги в ловушке":
        trap_display = "лонги в ловушке — возможно падение (long squeeze)"

    # ── ВЕРОЯТНОСТЬ ДВИЖЕНИЯ (на основе bull/bear факторов) ──
    if total > 0:
        prob_bull = round((dir_bull / total) * 100)
        prob_bear = 100 - prob_bull
    else:
        prob_bull = 50
        prob_bear = 50

    # Корректируем по качеству сетапа
    if quality == "STRONG":
        boost = 10
    elif quality == "MEDIUM":
        boost = 5
    else:
        boost = 0

    if dir_code == "UP":
        prob_bull = min(95, prob_bull + boost)
        prob_bear = 100 - prob_bull
    elif dir_code == "DOWN":
        prob_bear = min(95, prob_bear + boost)
        prob_bull = 100 - prob_bear

    # ── КОНФЛИКТ FUNDING vs СИГНАЛ ──
    fr = coin_data.get("funding_rate")
    funding_conflict = ""
    if fr is not None:
        if fr < -0.005 and recommendation == "продавать":
            funding_conflict = "FR отрицательный (бычий), но тренд и объёмы давят вниз"
        elif fr > 0.03 and recommendation == "покупать":
            funding_conflict = "FR высокий (медвежий), но покупатели сильнее"

    return {
        "quality": quality,
        "quality_label": quality_label,
        "score": score,
        "factors": factors,
        "conflicts": conflicts,
        "confidence_bar": conf_bars,
        "confidence_label": conf_label,
        "recommendation": recommendation,
        "strength": strength,
        "signal_bar": signal_bar,
        "signal_label": signal_label,
        "horizon": horizon,
        "trap_display": trap_display,
        "prob_bull": prob_bull,
        "prob_bear": prob_bear,
        "funding_conflict": funding_conflict,
    }


def run_signal_pipeline(coin_data: dict) -> dict:
    """
    Главная функция: запускает 3-слойный pipeline.
    Возвращает полный scoring для LLM и для отображения.
    """
    # Слой 1: Направление
    direction = calculate_direction(coin_data)

    # Слой 2: Состояние рынка
    market_state = calculate_market_state(coin_data, direction)

    # Слой 3: Качество сетапа
    setup = calculate_setup_quality(coin_data, direction, market_state)

    return {
        # Слой 1
        "direction": direction["direction"],
        "direction_label": direction["direction_label"],
        "dir_bull": direction["dir_bull"],
        "dir_bear": direction["dir_bear"],
        "dir_factors_bull": direction["dir_factors_bull"],
        "dir_factors_bear": direction["dir_factors_bear"],
        # Слой 2
        "state": market_state["state"],
        "state_label": market_state["state_label"],
        "trap": market_state["trap"],
        "trap_display": setup.get("trap_display", market_state["trap"]),
        # Слой 3
        "quality": setup["quality"],
        "quality_label": setup["quality_label"],
        "score": setup["score"],
        "factors": setup["factors"],
        "conflicts": setup["conflicts"],
        # Финальные выходы
        "signal_bar": setup["signal_bar"],
        "signal_label": setup["signal_label"],
        "confidence_bar": setup["confidence_bar"],
        "confidence_label": setup["confidence_label"],
        "recommendation": setup["recommendation"],
        "strength": setup["strength"],
        "horizon": setup["horizon"],
        "prob_bull": setup.get("prob_bull", 50),
        "prob_bear": setup.get("prob_bear", 50),
        "funding_conflict": setup.get("funding_conflict", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# КАРТА ЛИКВИДНОСТИ (расчёт уровней ликвидаций)
# ══════════════════════════════════════════════════════════════════════════════

def calculate_liquidation_levels(coin_data: dict) -> dict:
    """
    Расчёт ключевых уровней ликвидаций на основе текущей цены и плечей.

    Логика: большинство трейдеров используют плечи 10x-25x.
    При 10x лонги ликвидируются при падении ~10%, шорты при росте ~10%.
    При 25x — при движении ~4%.

    Кластеры ликвидаций (магниты цены):
    - Ближний: 3-5% от цены (20-25x плечи) — самые уязвимые позиции
    - Дальний: 8-10% от цены (10x плечи) — основная масса

    Учитываем перекос лонг/шорт: если больше лонгов — ликвидации лонгов плотнее.
    """
    price = coin_data.get("price")
    if not price or price <= 0:
        return {}

    # Соотношение лонг/шорт — определяет плотность ликвидаций
    long_pct = coin_data.get("long_pct")
    short_pct = 100 - long_pct if long_pct is not None else None

    # Средневзвешенное расстояние ликвидаций по плечам
    # Самые популярные плечи: 10x (10%), 20x (5%), 25x (4%)
    # Средний кластер: ~5-7% от цены (смесь плечей)
    # Ближний кластер: ~3-4% (высокие плечи 20-25x)

    # Основной уровень — плотный кластер ликвидаций (~5% от цены)
    base_pct = 0.05  # 5% — среднее по популярным плечам

    # Если есть сильный перекос — ближе к текущей цене
    # (больше позиций = больше ликвидаций = сильнее магнит)
    if long_pct is not None:
        if long_pct > 60:
            # Много лонгов — ликвидации лонгов (ниже) ближе и плотнее
            long_dist = base_pct * 0.85  # 4.25%
            short_dist = base_pct * 1.15  # 5.75%
        elif long_pct < 40:
            # Много шортов — ликвидации шортов (выше) ближе
            long_dist = base_pct * 1.15
            short_dist = base_pct * 0.85
        else:
            long_dist = base_pct
            short_dist = base_pct
    else:
        long_dist = base_pct
        short_dist = base_pct

    # Уровни ликвидаций
    liq_shorts_price = price * (1 + short_dist)   # выше цены — ликвидации шортов
    liq_longs_price = price * (1 - long_dist)      # ниже цены — ликвидации лонгов

    # Округляем красиво
    if price > 10000:
        # BTC — до сотен
        liq_shorts_price = round(liq_shorts_price / 100) * 100
        liq_longs_price = round(liq_longs_price / 100) * 100
    elif price > 100:
        # ETH, BNB, SOL — до единиц
        liq_shorts_price = round(liq_shorts_price)
        liq_longs_price = round(liq_longs_price)
    else:
        # Мелкие — до десятых
        liq_shorts_price = round(liq_shorts_price, 1)
        liq_longs_price = round(liq_longs_price, 1)

    return {
        "liq_level_shorts": liq_shorts_price,   # Уровень где ликвидируют шортов (выше)
        "liq_level_longs": liq_longs_price,      # Уровень где ликвидируют лонгов (ниже)
    }


# ══════════════════════════════════════════════════════════════════════════════
# WHALE ALERT — крупные транзакции китов
# API v1: https://api.whale-alert.io/v1/transactions
# Custom Alerts план: 100 alerts/hour, 10 req/min, min_value $500K+
# Один запрос возвращает ВСЕ монеты — парсим и раскидываем по символам
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_whale_transactions() -> dict:
    """
    Получает крупные транзакции за последний час со ВСЕХ блокчейнов.
    Один запрос → парсим по монетам.
    Возвращает: {symbol: {"whale_txs": N, "whale_to_exchange": $, "whale_from_exchange": $, "whale_total_usd": $, "whale_summary": "..."}}
    """
    if not HAS_WHALE_ALERT:
        return {}

    # Проверяем глобальный кэш (один запрос на все монеты)
    cache_key = "_whale_all"
    if cache_key in WHALE_CACHE and (time.time() - WHALE_CACHE[cache_key]["ts"]) < WHALE_CACHE_TTL:
        return WHALE_CACHE[cache_key]["data"]

    try:
        # Запрашиваем транзакции за последний час, min $500K
        start_ts = int(time.time()) - 3600  # 1 час назад
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{WHALE_ALERT_BASE}/transactions",
                params={
                    "api_key": WHALE_ALERT_KEY,
                    "min_value": 500000,  # $500K минимум
                    "start": start_ts,
                }
            )
            if resp.status_code != 200:
                log.warning(f"  ⚠️ Whale Alert: HTTP {resp.status_code}")
                return {}

            data = resp.json()
            if data.get("result") != "success":
                log.warning(f"  ⚠️ Whale Alert: {data.get('message', 'unknown error')}")
                return {}

            transactions = data.get("transactions", [])
            if not transactions:
                log.info("  🐋 Whale Alert: нет крупных транзакций за час")
                WHALE_CACHE[cache_key] = {"data": {}, "ts": time.time()}
                return {}

            # Группируем по нашим монетам
            # Структура: {symbol: {to_exchange_usd, from_exchange_usd, tx_count, big_txs[]}}
            whale_by_coin = {}

            for tx in transactions:
                symbol_lower = (tx.get("symbol") or "").lower()
                # Найти наш символ
                our_symbol = None
                for coin, wa_cur in WHALE_ALERT_CURRENCIES.items():
                    if wa_cur == symbol_lower:
                        our_symbol = coin
                        break

                if not our_symbol:
                    continue  # Не наша монета

                if our_symbol not in whale_by_coin:
                    whale_by_coin[our_symbol] = {
                        "to_exchange_usd": 0.0,
                        "from_exchange_usd": 0.0,
                        "tx_count": 0,
                        "total_usd": 0.0,
                    }

                amount_usd = float(tx.get("amount_usd", 0) or 0)
                from_type = (tx.get("from", {}).get("owner_type") or "").lower()
                to_type = (tx.get("to", {}).get("owner_type") or "").lower()

                whale_by_coin[our_symbol]["tx_count"] += 1
                whale_by_coin[our_symbol]["total_usd"] += amount_usd

                # Классифицируем направление
                if from_type != "exchange" and to_type == "exchange":
                    # Кто-то заводит на биржу → готовится продавать (медвежий)
                    whale_by_coin[our_symbol]["to_exchange_usd"] += amount_usd
                elif from_type == "exchange" and to_type != "exchange":
                    # Кто-то выводит с биржи → накопление (бычий)
                    whale_by_coin[our_symbol]["from_exchange_usd"] += amount_usd

            # Формируем результат
            result = {}
            for symbol, wd in whale_by_coin.items():
                to_ex = wd["to_exchange_usd"]
                from_ex = wd["from_exchange_usd"]
                total = wd["total_usd"]
                txs = wd["tx_count"]

                # Определяем направление
                if from_ex > to_ex * 1.5:
                    direction = "bullish"  # Выводят с бирж — накопление
                elif to_ex > from_ex * 1.5:
                    direction = "bearish"  # Заводят на биржи — продажа
                else:
                    direction = "neutral"

                result[symbol] = {
                    "whale_txs": txs,
                    "whale_to_exchange": to_ex,
                    "whale_from_exchange": from_ex,
                    "whale_total_usd": total,
                    "whale_direction": direction,
                }

            log.info(f"  🐋 Whale Alert: {len(transactions)} транзакций, {len(result)} наших монет")
            for sym, wd in result.items():
                log.info(f"    {sym}: {wd['whale_txs']} txs, total ${wd['whale_total_usd']/1e6:.1f}M, to_ex ${wd['whale_to_exchange']/1e6:.1f}M, from_ex ${wd['whale_from_exchange']/1e6:.1f}M → {wd['whale_direction']}")

            WHALE_CACHE[cache_key] = {"data": result, "ts": time.time()}
            return result

    except Exception as e:
        log.error(f"  ❌ Whale Alert error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ СБОРА
# ══════════════════════════════════════════════════════════════════════════════

async def collect_all():
    """
    Собирает данные по всем монетам + LLM-анализ, сохраняет в Supabase.
    """
    global _last_llm_run
    _now = time.time()
    _run_llm_this_cycle = (_now - _last_llm_run) >= LLM_INTERVAL_SEC
    if _run_llm_this_cycle:
        _last_llm_run = _now
        log.info("📡 Начинаем сбор данных... (+ LLM анализ)")
    else:
        _mins_left = int((LLM_INTERVAL_SEC - (_now - _last_llm_run)) / 60)
        log.info(f"📡 Начинаем сбор данных... (LLM через ~{_mins_left} мин)")

    # Обновляем кэш BGeometrics (реальные запросы только если кэш устарел)
    await fetch_bgeometrics_batch()

    # Обновляем Coinglass индикаторы (кэш 4ч)
    await fetch_cg_indicators()
    cg_indicators = parse_cg_indicators()

    # DeFiLlama: стейблкоины + TVL (бесплатно)
    defillama_data = await fetch_defillama_data()

    # Solana DeFi: DEX volume + TVL Solana (бесплатно)
    solana_defi_data = await fetch_solana_defi()

    # Etherscan: ETH gas + supply (бесплатно, ключ нужен)
    etherscan_data = await fetch_etherscan_data()

    # Whale Alert: крупные транзакции китов за последний час (1 запрос на все монеты)
    whale_data_all = await fetch_whale_transactions()

    # Общие данные (СНАЧАЛА цены — приоритет!)
    fg_data = await fetch_fear_greed()
    prices = await fetch_prices()

    # Технические индикаторы для не-BTC монет (кэш 30мин)
    # CoinGecko (1 попытка) → CryptoCompare fallback (мгновенно)
    # Загружаем ПОСЛЕ цен с паузами между CoinGecko запросами
    await asyncio.sleep(8)  # Пауза после запроса цен
    for coin in COINS:
        if coin == "BTC":
            continue  # BTC получает из BGeometrics
        cache_key = f"tech_{coin}"
        if cache_key in TECH_CACHE and (time.time() - TECH_CACHE[cache_key]["ts"]) < TECH_CACHE_TTL:
            continue  # Кэш актуален
        await fetch_tech_indicators(coin)
        await asyncio.sleep(6)  # Пауза между запросами

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

            # Options данные: Deribit + Binance + OKX (бесплатно, EU сервер)
            options_data = await fetch_options_data(symbol)

            # CVD + Order Book Imbalance (Binance Futures, бесплатно)
            flow_data = await fetch_flow_data(symbol)

            # Spot vs Perp Volume (Binance, бесплатно)
            spot_perp_data = await fetch_spot_perp_volume(symbol)

            # Технические индикаторы (RSI, MACD, SMA — для не-BTC монет, из CoinGecko истории)
            tech_data = await fetch_tech_indicators(symbol)

            # Cross-exchange данные: OKX + Bitget + Kraken + dYdX + Order Book (бесплатно, без ключа)
            okx_data, bitget_ls_data, bitget_oi_data, kraken_data, dydx_data, ob_data = await asyncio.gather(
                fetch_okx_top_traders(symbol),
                fetch_bitget_ls(symbol),
                fetch_bitget_oi(symbol),
                fetch_kraken_futures(symbol),
                fetch_dydx_data(symbol),
                fetch_kraken_orderbook(symbol),
            )

            # Whale Alert данные для этой монеты
            whale_data = whale_data_all.get(symbol, {})

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
                **okx_data,           # OKX Top Traders: L/S ratio
                **bitget_ls_data,     # Bitget Account + Position L/S
                **bitget_oi_data,     # Bitget OI
                **kraken_data,        # Kraken Futures: OI
                **dydx_data,          # dYdX: funding + OI
                **ob_data,            # Kraken Order Book: bid/ask imbalance
                **tech_data,          # RSI, MACD, SMA50, SMA200 (для не-BTC монет)
                **etherscan_data,     # Etherscan: ETH gas
                **options_data,       # Deribit + Binance + OKX: IV, PCR, MaxPain
                **flow_data,         # CVD + Order Book Imbalance (Binance Futures)
                **spot_perp_data,    # Spot vs Perp Volume (Binance)
                **whale_data,        # Whale Alert: крупные транзакции китов
                **(solana_defi_data if symbol == "SOL" else {}),  # Solana DeFi (только SOL)
                "mkt_liq_long": total_liq_long_1h,
                "mkt_liq_short": total_liq_short_1h,
            }

            # ══ 3-СЛОЙНЫЙ PIPELINE ══
            pipeline = run_signal_pipeline(coin_data)
            signal_bar = pipeline["signal_bar"]
            signal_label = pipeline["signal_label"]
            conf_bar = pipeline["confidence_bar"]
            conf_label = pipeline["confidence_label"]

            # ══ AI SCORE (нормализация pipeline score в 0-100) ══
            raw_score = pipeline.get("score", 0)  # обычно от -3 до +7
            dir_code = pipeline.get("direction", "SIDEWAYS")
            dir_bull = pipeline.get("dir_bull", 0)
            dir_bear = pipeline.get("dir_bear", 0)

            # Нормализуем: raw_score от -3..+7 → 0..100
            # Центр (score=0) = 50, каждый +1 = +7 баллов
            ai_score_raw = 50 + (raw_score * 7)
            # Добавляем перекос от направления (dir_bull vs dir_bear)
            total_dir = dir_bull + dir_bear
            if total_dir > 0:
                dir_bias = ((dir_bull - dir_bear) / total_dir) * 15  # до ±15
                ai_score_raw += dir_bias
            ai_score = max(0, min(100, round(ai_score_raw)))

            # Лейбл AI Score
            if ai_score >= 70:
                ai_score_label = "STRONG BUY"
            elif ai_score >= 60:
                ai_score_label = "BUY"
            elif ai_score >= 45:
                ai_score_label = "NEUTRAL"
            elif ai_score >= 30:
                ai_score_label = "SELL"
            else:
                ai_score_label = "STRONG SELL"

            # Топ факторы для отображения (берём из pipeline)
            top_factors_bull = ", ".join(pipeline.get("dir_factors_bull", [])[:3])
            top_factors_bear = ", ".join(pipeline.get("dir_factors_bear", [])[:3])

            # Рассчитываем карту ликвидности
            liq_levels = calculate_liquidation_levels(coin_data)

            # LLM-анализ (получает pre-scored pipeline, только формулирует текст)
            # LLM вызывается раз в LLM_INTERVAL_SEC (15 мин), данные — каждые 5 мин
            llm_data = {}
            _do_llm = HAS_ANTHROPIC and coin_data.get("price") and coin_data.get("oi") and _run_llm_this_cycle
            if _do_llm:
                llm_data = await generate_llm_analysis(symbol, coin_data, pipeline)
            else:
                # Без LLM — используем pipeline данные напрямую
                llm_data = {
                    "recommendation": pipeline["recommendation"],
                    "strength": pipeline["strength"],
                    "trap": pipeline["trap"] if pipeline["trap"] != "нет" else "",
                    "trap_display": pipeline.get("trap_display", "") if pipeline.get("trap_display", "нет") != "нет" else "",
                    "horizon": pipeline["horizon"],
                    "prob_bull": pipeline.get("prob_bull", 50),
                    "prob_bear": pipeline.get("prob_bear", 50),
                    "funding_conflict": pipeline.get("funding_conflict", ""),
                    "what_happening": "",
                }

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
                "okx_top_long": f"{coin_data['okx_top_long']}%" if coin_data.get("okx_top_long") is not None else "—",
                "okx_top_short": f"{coin_data['okx_top_short']}%" if coin_data.get("okx_top_short") is not None else "—",
                "bitget_long_acc": f"{coin_data.get('bitget_long_acc', '—')}%" if coin_data.get("bitget_long_acc") is not None else "—",
                "bitget_short_acc": f"{coin_data.get('bitget_short_acc', '—')}%" if coin_data.get("bitget_short_acc") is not None else "—",
                "bitget_long_pos": f"{coin_data.get('bitget_long_pos', '—')}%" if coin_data.get("bitget_long_pos") is not None else "—",
                "bitget_short_pos": f"{coin_data.get('bitget_short_pos', '—')}%" if coin_data.get("bitget_short_pos") is not None else "—",
                "bitget_oi_usd": fmt_usd(coin_data.get("bitget_oi_usd")) if coin_data.get("bitget_oi_usd") else "—",
                "kraken_funding": fmt_fr(coin_data.get("kraken_funding")),
                "kraken_oi": str(round(coin_data["kraken_oi"])) if coin_data.get("kraken_oi") else "—",
                "dydx_funding": fmt_fr(coin_data.get("dydx_funding")),
                "dydx_oi": str(round(coin_data["dydx_oi"])) if coin_data.get("dydx_oi") else "—",
                "bid_depth_usd": str(round(coin_data["bid_depth_usd"])) if coin_data.get("bid_depth_usd") else "—",
                "ask_depth_usd": str(round(coin_data["ask_depth_usd"])) if coin_data.get("ask_depth_usd") else "—",
                "bid_ask_ratio": str(round(coin_data["bid_ask_ratio"], 3)) if coin_data.get("bid_ask_ratio") else "—",
                "stablecoin_mcap": fmt_usd(coin_data.get("stablecoin_mcap")) if coin_data.get("stablecoin_mcap") else "—",
                "defi_tvl": fmt_usd(coin_data.get("defi_tvl")) if coin_data.get("defi_tvl") else "—",
                "defi_tvl_change": fmt_pct(coin_data.get("defi_tvl_change")),
                "etf_netflow": fmt_usd(coin_data.get("etf_netflow")) if coin_data.get("etf_netflow") else "—",
                "ahr999": str(round(coin_data["ahr999"], 3)) if coin_data.get("ahr999") else "—",
                "bull_peak_ratio": coin_data.get("bull_peak_ratio", "—"),
                "bitcoin_bubble": str(coin_data.get("bitcoin_bubble", "—")),
                "fear_greed": str(coin_data.get("fear_greed", "—")),
                "fear_greed_label": coin_data.get("fear_greed_label", "—"),
                "eth_gas_avg": str(coin_data.get("eth_gas_avg", "—")),
                "options_pcr": str(coin_data.get("options_pcr", "—")),
                "options_iv": str(coin_data.get("options_iv", "—")),
                "options_max_pain": str(coin_data.get("options_max_pain", "—")),
                "options_oi_calls": str(coin_data.get("options_oi_calls", "—")),
                "options_oi_puts": str(coin_data.get("options_oi_puts", "—")),
                "options_expiries": str(coin_data.get("options_expiries", "—")),
                "cvd_value": str(coin_data.get("cvd_value", "—")),
                "cvd_trend": str(coin_data.get("cvd_trend", "—")),
                "cvd_side": str(coin_data.get("cvd_side", "—")),
                "obi_value": str(coin_data.get("obi_value", "—")),
                "obi_side": str(coin_data.get("obi_side", "—")),
                "obi_bid_vol": str(coin_data.get("obi_bid_vol", "—")),
                "obi_ask_vol": str(coin_data.get("obi_ask_vol", "—")),
                "obi_support_price": str(coin_data.get("obi_support_price", "—")),
                "obi_support_vol": str(coin_data.get("obi_support_vol", "—")),
                "obi_resistance_price": str(coin_data.get("obi_resistance_price", "—")),
                "obi_resistance_vol": str(coin_data.get("obi_resistance_vol", "—")),
                "spot_volume": str(coin_data.get("spot_volume", "")),
                "perp_volume": str(coin_data.get("perp_volume", "")),
                "spot_dominance": str(coin_data.get("spot_dominance", "")),
                "whale_txs": str(coin_data.get("whale_txs", "0")),
                "whale_to_exchange": fmt_usd(coin_data.get("whale_to_exchange")) if coin_data.get("whale_to_exchange") else "—",
                "whale_from_exchange": fmt_usd(coin_data.get("whale_from_exchange")) if coin_data.get("whale_from_exchange") else "—",
                "whale_total_usd": fmt_usd(coin_data.get("whale_total_usd")) if coin_data.get("whale_total_usd") else "—",
                "whale_direction": str(coin_data.get("whale_direction", "—")),
                "sol_dex_volume": fmt_usd(coin_data.get("sol_dex_volume")) if coin_data.get("sol_dex_volume") else "—",
                "sol_dex_volume_change": fmt_pct(coin_data.get("sol_dex_volume_change")) if coin_data.get("sol_dex_volume_change") is not None else "—",
                "sol_tvl": fmt_usd(coin_data.get("sol_tvl")) if coin_data.get("sol_tvl") else "—",
                "sol_tvl_change": fmt_pct(coin_data.get("sol_tvl_change")) if coin_data.get("sol_tvl_change") is not None else "—",
                "signal": signal_bar,
                "label": signal_label,
                "signal_label": signal_label,
                "confidence_bar": conf_bar,
                "confidence_label": conf_label,
                "liq_level_shorts": fmt_price(liq_levels.get("liq_level_shorts")) if liq_levels.get("liq_level_shorts") else "",
                "liq_level_longs": fmt_price(liq_levels.get("liq_level_longs")) if liq_levels.get("liq_level_longs") else "",
                "llm_text": llm_data.get("llm_text", ""),
                "what_happening": llm_data.get("what_happening", ""),
                "horizon": llm_data.get("horizon", ""),
                "trap": llm_data.get("trap", ""),
                "trap_display": llm_data.get("trap_display", ""),
                "prob_bull": str(llm_data.get("prob_bull", 50)),
                "prob_bear": str(llm_data.get("prob_bear", 50)),
                "funding_conflict": llm_data.get("funding_conflict", ""),
                "ai_score": str(ai_score),
                "ai_score_label": ai_score_label,
                "top_factors_bull": top_factors_bull,
                "top_factors_bear": top_factors_bear,
                "recommendation": llm_data.get("recommendation", ""),
                "strength": llm_data.get("strength", ""),
                "entry": llm_data.get("entry", ""),
                "stop": llm_data.get("stop", ""),
                "target": llm_data.get("target", ""),
                "buy_zone": llm_data.get("buy_zone", ""),
                "sell_zone": llm_data.get("sell_zone", ""),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            # Если LLM не запускался в этом цикле — не перезаписываем LLM-поля в БД
            if not _run_llm_this_cycle:
                for _llm_key in ("llm_text", "what_happening", "recommendation", "strength",
                                 "entry", "stop", "target", "buy_zone", "sell_zone",
                                 "horizon", "trap", "trap_display", "funding_conflict"):
                    record.pop(_llm_key, None)

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
