"""
ZENDER COMMANDER TERMINAL — Data Collector
Этап 2: сбор данных из Coinglass API v4 + Fear & Greed Index
Запускается по расписанию, кладёт данные в Supabase.

ПЛАН: Hobbyist (80+ endpoints, 30 req/min).
Оптимизировано: 4 запроса на монету, задержки между монетами.
"""

import asyncio
import logging
import httpx
from datetime import datetime, timezone

from config import COINGLASS_API_KEY
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

# CoinGecko IDs (для цен, т.к. Binance блокирует US серверы Railway)
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "AVAX": "avalanche-2",
}


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def fmt_usd(value, compact=True):
    """Форматирование суммы в доллары: $1.23M, $4.56B"""
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
    """Форматирование процентов: +4.1%"""
    if value is None:
        return "—"
    try:
        v = float(value)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}%"
    except (ValueError, TypeError):
        return "—"


def fmt_fr(value):
    """Форматирование funding rate с 4 знаками: +0.0045%"""
    if value is None:
        return "—"
    try:
        v = float(value)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.4f}%"
    except (ValueError, TypeError):
        return "—"


def fmt_price(value):
    """Форматирование цены: $83,420"""
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
    """GET запрос к Coinglass API v4. Возвращает data из ответа."""
    url = f"{CG_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=CG_HEADERS, params=params or {})
            resp.raise_for_status()
            body = resp.json()

            # Проверяем код ответа в теле JSON (API возвращает HTTP 200 даже при ошибках)
            body_code = body.get("code")
            if body_code is not None and str(body_code) != "0":
                msg = body.get("msg", "unknown")
                if str(body_code) == "429":
                    log.warning(f"CG rate limit {path}")
                elif str(body_code) == "403":
                    log.debug(f"CG 403 (план) {path}")
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
    """
    Цены всех монет через CoinGecko (бесплатно, без ключа).
    Возвращает: {BTC: {price, change_24h}, ETH: {...}, ...}
    """
    ids = ",".join(COINGECKO_IDS.values())
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={
                    "ids": ids,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true",
                }
            )
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
        log.warning(f"CoinGecko price error: {e}")
        return {}


async def fetch_open_interest(symbol: str) -> dict:
    """
    Открытый интерес по всем биржам.
    Поля: open_interest_usd, open_interest_change_percent_4h/24h
    Ищем exchange='All' для агрегата.
    """
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
            return {"oi": total_oi if total_oi > 0 else None, "oi_change_4h": None, "oi_change_24h": None}
    elif isinstance(data, dict):
        all_item = data

    if not all_item:
        return {}

    oi_usd = float(all_item.get("open_interest_usd", 0) or 0)
    oi_change_4h = all_item.get("open_interest_change_percent_4h")
    oi_change_24h = all_item.get("open_interest_change_percent_24h")

    return {
        "oi": oi_usd if oi_usd > 0 else None,
        "oi_change_4h": float(oi_change_4h) if oi_change_4h is not None else None,
        "oi_change_24h": float(oi_change_24h) if oi_change_24h is not None else None,
    }


async def fetch_funding_rate(symbol: str) -> dict:
    """
    Funding Rate по биржам.
    Данные вложены: stablecoin_margin_list[].funding_rate + token_margin_list[].funding_rate
    """
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
    # API отдаёт в десятичной форме (-0.002744 = -0.27%), умножаем на 100
    return {"funding_rate": avg_rate * 100}


async def fetch_long_short(symbol: str) -> dict:
    """
    Taker Buy/Sell Volume — соотношение покупок/продаж.
    Используем как замену L/S ratio (который требует Startup план).
    Требует параметр range: 1h, 4h, 12h, 24h.
    """
    # Пробуем разные значения range
    for range_val in ["4h", "24h", "1h", "12h"]:
        data = await cg_get("/api/futures/taker-buy-sell-volume/exchange-list", {
            "symbol": symbol,
            "range": range_val,
        })
        if data is not None:
            break
    else:
        return {}

    # Ищем агрегат по всем биржам
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

    # DEBUG для BTC: логируем поля один раз
    if symbol == "BTC":
        log.info(f"  TAKER keys: {list(target.keys())}")
        sample = {k: target[k] for k in list(target.keys())[:12]}
        log.info(f"  TAKER sample: {sample}")

    # Пробуем разные имена полей для buy/sell ratio
    buy = None
    sell = None

    # Вариант 1: прямые ratio
    for key in ["buy_ratio", "buyRatio", "buy_vol_rate", "buyVolRate", "taker_buy_ratio"]:
        val = target.get(key)
        if val is not None:
            try:
                buy = float(val)
                break
            except (ValueError, TypeError):
                pass

    for key in ["sell_ratio", "sellRatio", "sell_vol_rate", "sellVolRate", "taker_sell_ratio"]:
        val = target.get(key)
        if val is not None:
            try:
                sell = float(val)
                break
            except (ValueError, TypeError):
                pass

    if buy is not None and sell is not None and (buy > 0 or sell > 0):
        if buy <= 1 and sell <= 1:
            return {"long_pct": round(buy * 100, 1), "short_pct": round(sell * 100, 1)}
        else:
            return {"long_pct": round(buy, 1), "short_pct": round(sell, 1)}

    # Вариант 2: объёмы → вычисляем ratio
    buy_vol = None
    sell_vol = None

    for key in ["buy_vol_usd", "buyVolUsd", "taker_buy_vol_usd", "takerBuyVolUsd", "buy"]:
        val = target.get(key)
        if val is not None:
            try:
                buy_vol = float(val)
                break
            except (ValueError, TypeError):
                pass

    for key in ["sell_vol_usd", "sellVolUsd", "taker_sell_vol_usd", "takerSellVolUsd", "sell"]:
        val = target.get(key)
        if val is not None:
            try:
                sell_vol = float(val)
                break
            except (ValueError, TypeError):
                pass

    if buy_vol is not None and sell_vol is not None:
        total = buy_vol + sell_vol
        if total > 0:
            return {
                "long_pct": round(buy_vol / total * 100, 1),
                "short_pct": round(sell_vol / total * 100, 1),
            }

    return {}


async def fetch_liquidations(symbol: str) -> dict:
    """
    Ликвидации через coin-list (работает на Hobbyist).
    Реальные поля из API:
      long_liquidation_usd_4h, short_liquidation_usd_4h
      long_liquidation_usd_24h, short_liquidation_usd_24h
      long_liquidation_usd_12h, short_liquidation_usd_12h
      long_liquidation_usd_1h, short_liquidation_usd_1h
    """
    data = await cg_get("/api/futures/liquidation/coin-list")
    if not data:
        return {}

    # Ищем нашу монету в списке
    target = None
    if isinstance(data, list):
        for item in data:
            sym = (item.get("symbol") or item.get("coin") or "").upper()
            if sym == symbol.upper():
                target = item
                break
    elif isinstance(data, dict):
        target = data

    if not target:
        return {}

    # Берём 4h данные (или 24h как фоллбэк)
    liq_long = target.get("long_liquidation_usd_4h")
    liq_short = target.get("short_liquidation_usd_4h")

    # Фоллбэк на 24h если 4h нет
    if liq_long is None:
        liq_long = target.get("long_liquidation_usd_24h")
    if liq_short is None:
        liq_short = target.get("short_liquidation_usd_24h")

    try:
        liq_long_f = float(liq_long) if liq_long is not None else None
        liq_short_f = float(liq_short) if liq_short is not None else None
    except (ValueError, TypeError):
        return {}

    return {
        "liq_long": liq_long_f if liq_long_f and liq_long_f > 0 else None,
        "liq_short": liq_short_f if liq_short_f and liq_short_f > 0 else None,
    }


async def fetch_fear_greed() -> dict:
    """Fear & Greed Index (бесплатный, Alternative.me)."""
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
# СИГНАЛ (расчёт силы сигнала)
# ══════════════════════════════════════════════════════════════════════════════

def calculate_signal(coin_data: dict) -> tuple[str, str]:
    """
    Рассчитывает силу сигнала на основе собранных данных.
    Возвращает: (signal_bar, signal_label)
    """
    score = 0
    max_score = 0

    long_pct = coin_data.get("long_pct")
    if long_pct is not None:
        max_score += 1
        if long_pct > 60:
            score += 1
        elif long_pct < 40:
            score -= 0.5

    oi_change = coin_data.get("oi_change_4h")
    if oi_change is not None:
        max_score += 1
        if oi_change > 2:
            score += 1
        elif oi_change < -2:
            score -= 0.5

    fr = coin_data.get("funding_rate")
    if fr is not None:
        max_score += 1
        if 0 < fr < 0.05:
            score += 1
        elif fr > 0.1:
            score -= 0.5
        elif fr < -0.01:
            score -= 0.5

    fg = coin_data.get("fear_greed")
    if fg is not None:
        max_score += 1
        if fg > 60:
            score += 1
        elif fg < 30:
            score -= 0.5

    change = coin_data.get("change_24h")
    if change is not None:
        max_score += 1
        if change > 2:
            score += 1
        elif change < -2:
            score -= 0.5

    if max_score > 0:
        normalized = max(0, min(5, int((score / max_score) * 5 + 2.5)))
    else:
        normalized = 0

    bars = "▓" * normalized + "░" * (5 - normalized)

    if normalized >= 4:
        label = "СИЛЬНЫЙ"
    elif normalized >= 2:
        label = "СРЕДНИЙ"
    else:
        label = "СЛАБЫЙ"

    return bars, label


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ СБОРА
# ══════════════════════════════════════════════════════════════════════════════

async def collect_all():
    """
    Собирает данные по всем монетам и сохраняет в Supabase.
    Вызывается по расписанию каждые 15 минут.

    Оптимизация для Hobbyist (30 req/min):
    - 2 общих запроса (Fear&Greed + CoinGecko)
    - 4 запроса на монету (OI, FR, Taker B/S, LIQ coin-list)
    - = 2 + 4*5 = 22 запроса на цикл
    - Задержка 3 сек между монетами чтобы не превысить лимит
    """
    log.info("📡 Начинаем сбор данных...")

    # Общие данные (1 запрос для всех монет)
    fg_data = await fetch_fear_greed()
    prices = await fetch_prices()

    # Ликвидации — 1 запрос на все монеты (coin-list возвращает весь список)
    liq_all = await cg_get("/api/futures/liquidation/coin-list")
    liq_by_coin = {}
    if liq_all and isinstance(liq_all, list):
        for item in liq_all:
            sym = (item.get("symbol") or "").upper()
            if sym in COINS:
                liq_long = item.get("long_liquidation_usd_4h")
                liq_short = item.get("short_liquidation_usd_4h")
                if liq_long is None:
                    liq_long = item.get("long_liquidation_usd_24h")
                if liq_short is None:
                    liq_short = item.get("short_liquidation_usd_24h")
                try:
                    ll = float(liq_long) if liq_long is not None else None
                    ls = float(liq_short) if liq_short is not None else None
                    liq_by_coin[sym] = {
                        "liq_long": ll if ll and ll > 0 else None,
                        "liq_short": ls if ls and ls > 0 else None,
                    }
                except (ValueError, TypeError):
                    pass

    if liq_by_coin:
        log.info(f"  📊 Ликвидации загружены для {len(liq_by_coin)} монет")

    for i, symbol in enumerate(COINS):
        try:
            log.info(f"  ⏳ {symbol}...")

            # Задержка между монетами (кроме первой) — 3 сек
            if i > 0:
                await asyncio.sleep(3)

            # Цена из общего запроса CoinGecko
            price_data = prices.get(symbol, {})

            # Ликвидации из общего запроса
            liq_data = liq_by_coin.get(symbol, {})

            # Coinglass данные — 3 запроса параллельно
            oi_data, fr_data, ls_data = await asyncio.gather(
                fetch_open_interest(symbol),
                fetch_funding_rate(symbol),
                fetch_long_short(symbol),
            )

            # Объединяем все данные
            coin_data = {
                **price_data,
                **oi_data,
                **fr_data,
                **ls_data,
                **liq_data,
                **fg_data,
            }

            # Рассчитываем сигнал
            signal_bar, signal_label = calculate_signal(coin_data)

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
                "oi_change": fmt_pct(coin_data.get("oi_change_4h")),
                "funding_rate": fmt_fr(coin_data.get("funding_rate")),
                "long_pct": f"{long_pct_val}%" if long_pct_val is not None else "—",
                "short_pct": f"{short_pct_val}%" if short_pct_val is not None else "—",
                "long_vol": fmt_usd(oi_val * (long_pct_val / 100)) if oi_val and long_pct_val else "—",
                "short_vol": fmt_usd(oi_val * (short_pct_val / 100)) if oi_val and short_pct_val else "—",
                "liq_up": fmt_usd(coin_data.get("liq_short")),
                "liq_dn": fmt_usd(coin_data.get("liq_long")),
                "exchange_flow": "—",
                "whale_buy1h": "—",
                "whale_buy24h": "—",
                "whale_sell24h": "—",
                "fear_greed": str(coin_data.get("fear_greed", "—")),
                "fear_greed_label": coin_data.get("fear_greed_label", "—"),
                "signal": signal_bar,
                "label": signal_label,
                "signal_label": signal_label,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            # Сохраняем в Supabase
            await db.upsert_market_data(record)
            log.info(f"  ✅ {symbol}: {record['price']} {record['change']} | OI:{record['oi']} | FR:{record['funding_rate']} | L/S:{record['long_pct']}/{record['short_pct']} | LIQ:{record['liq_up']}/{record['liq_dn']} | {signal_bar} {signal_label}")

        except Exception as e:
            log.error(f"  ❌ {symbol} error: {e}")
            continue

    log.info("📡 Сбор данных завершён.")


async def collector_loop(interval_minutes: int = 15):
    """
    Бесконечный цикл сбора данных.
    Запускается как фоновая задача при старте бота.
    """
    log.info(f"🔄 Коллектор запущен. Интервал: {interval_minutes} мин")

    # Первый сбор сразу при запуске
    await collect_all()

    while True:
        await asyncio.sleep(interval_minutes * 60)
        try:
            await collect_all()
        except Exception as e:
            log.error(f"Collector loop error: {e}")
