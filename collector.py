"""
ZENDER COMMANDER TERMINAL — Data Collector
Этап 2: сбор данных из Coinglass API v4 + Fear & Greed Index
Запускается по расписанию, кладёт данные в Supabase.
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

async def cg_get(path: str, params: dict = None) -> dict | None:
    """GET запрос к Coinglass API v4"""
    url = f"{CG_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=CG_HEADERS, params=params or {})
            resp.raise_for_status()
            data = resp.json()
            if data.get("success") is False:
                log.warning(f"CG API error {path}: {data.get('msg', 'unknown')}")
                return None
            return data.get("data")
    except httpx.HTTPStatusError as e:
        log.error(f"CG HTTP error {path}: {e.response.status_code}")
        return None
    except Exception as e:
        log.error(f"CG request error {path}: {e}")
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
    Реальные поля API v4:
      open_interest_usd, open_interest_change_percent_4h, open_interest_change_percent_24h
    Есть запись с exchange='All' — это агрегат по всем биржам.
    """
    data = await cg_get("/api/futures/open-interest/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    # Ищем агрегированную запись (exchange = "All")
    all_item = None
    if isinstance(data, list):
        for item in data:
            if item.get("exchange") == "All":
                all_item = item
                break
        # Если нет "All" — суммируем вручную
        if not all_item and len(data) > 0:
            total_oi = sum(float(item.get("open_interest_usd", 0) or 0) for item in data)
            return {
                "oi": total_oi if total_oi > 0 else None,
                "oi_change_4h": None,
                "oi_change_24h": None,
            }
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
    Реальная структура API v4:
      {symbol, stablecoin_margin_list: [{exchange, funding_rate, ...}], token_margin_list: [...]}
    Данные вложены в списки по типу маржи.
    """
    data = await cg_get("/api/futures/funding-rate/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    # data может быть списком (1 элемент) или словарём
    item = data[0] if isinstance(data, list) and len(data) > 0 else data
    if not isinstance(item, dict):
        return {}

    rates = []

    # Собираем funding_rate из stablecoin_margin_list
    for entry in item.get("stablecoin_margin_list", []):
        fr = entry.get("funding_rate")
        if fr is not None:
            try:
                rates.append(float(fr))
            except (ValueError, TypeError):
                pass

    # Также из token_margin_list
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
    return {"funding_rate": avg_rate}


async def fetch_long_short(symbol: str) -> dict:
    """
    Long/Short Account Ratio.
    Coinglass v4: пути требуют /history суффикс.
    """
    paths = [
        "/api/futures/global-long-short-account-ratio/history",
        "/api/futures/top-long-short-account-ratio/history",
        "/api/futures/top-long-short-position-ratio/history",
    ]

    for path in paths:
        data = await cg_get(path, {"symbol": symbol, "interval": "1h", "limit": 1})
        if data:
            break
    else:
        return {}

    try:
        if isinstance(data, list) and len(data) > 0:
            item = data[-1]
        elif isinstance(data, dict):
            item = data
        else:
            return {}

        # DEBUG: логируем поля для отладки
        if symbol == "BTC":
            log.info(f"  🔍 DEBUG L/S keys: {list(item.keys())}")
            log.info(f"  🔍 DEBUG L/S item: {item}")

        # Пробуем разные имена полей (snake_case и camelCase)
        long_ratio = float(
            item.get("long_rate", 0) or item.get("longRate", 0) or
            item.get("long_account", 0) or item.get("longAccount", 0) or
            item.get("long_ratio", 0) or item.get("longRatio", 0) or
            item.get("buy_ratio", 0) or item.get("buyRatio", 0) or 0
        )
        short_ratio = float(
            item.get("short_rate", 0) or item.get("shortRate", 0) or
            item.get("short_account", 0) or item.get("shortAccount", 0) or
            item.get("short_ratio", 0) or item.get("shortRatio", 0) or
            item.get("sell_ratio", 0) or item.get("sellRatio", 0) or 0
        )

        if long_ratio == 0 and short_ratio == 0:
            return {}

        # Нормализуем в проценты
        if long_ratio > 0 and long_ratio <= 1 and short_ratio > 0 and short_ratio <= 1:
            # Значения 0-1 → конвертируем в %
            long_pct = long_ratio * 100
            short_pct = short_ratio * 100
        elif long_ratio > 0 and long_ratio < 10 and short_ratio > 0 and short_ratio < 10:
            total = long_ratio + short_ratio
            long_pct = (long_ratio / total) * 100
            short_pct = (short_ratio / total) * 100
        else:
            long_pct = long_ratio
            short_pct = short_ratio

        return {
            "long_pct": round(long_pct, 1),
            "short_pct": round(short_pct, 1),
        }
    except (ValueError, TypeError):
        return {}


async def fetch_liquidations(symbol: str) -> dict:
    """
    Ликвидации за последние 4 часа.
    """
    data = await cg_get("/api/futures/liquidation/history", {
        "symbol": symbol,
        "interval": "4h",
        "limit": 1,
    })
    if not data:
        return {}

    try:
        if isinstance(data, list) and len(data) > 0:
            item = data[-1]
        elif isinstance(data, dict):
            item = data
        else:
            return {}

        # DEBUG: логируем поля для отладки
        if symbol == "BTC":
            log.info(f"  🔍 DEBUG LIQ keys: {list(item.keys())}")
            log.info(f"  🔍 DEBUG LIQ item: {item}")

        # Пробуем snake_case и camelCase
        liq_long = float(
            item.get("long_liquidation_usd", 0) or item.get("longLiquidationUsd", 0) or
            item.get("buy_vol_usd", 0) or item.get("buyVolUsd", 0) or
            item.get("long_vol_usd", 0) or item.get("longVolUsd", 0) or 0
        )
        liq_short = float(
            item.get("short_liquidation_usd", 0) or item.get("shortLiquidationUsd", 0) or
            item.get("sell_vol_usd", 0) or item.get("sellVolUsd", 0) or
            item.get("short_vol_usd", 0) or item.get("shortVolUsd", 0) or 0
        )

        return {
            "liq_long": liq_long if liq_long > 0 else None,
            "liq_short": liq_short if liq_short > 0 else None,
        }
    except (ValueError, TypeError):
        return {}


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
    """
    log.info("📡 Начинаем сбор данных...")

    # Общие данные (1 запрос для всех монет)
    fg_data = await fetch_fear_greed()
    prices = await fetch_prices()

    for symbol in COINS:
        try:
            log.info(f"  ⏳ {symbol}...")

            # Цена из общего запроса CoinGecko
            price_data = prices.get(symbol, {})

            # Coinglass данные параллельно
            oi_data, fr_data, ls_data, liq_data = await asyncio.gather(
                fetch_open_interest(symbol),
                fetch_funding_rate(symbol),
                fetch_long_short(symbol),
                fetch_liquidations(symbol),
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
                "funding_rate": fmt_pct(coin_data.get("funding_rate")),
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
            log.info(f"  ✅ {symbol}: {record['price']} {record['change']} | OI:{record['oi']} | FR:{record['funding_rate']} | L/S:{record['long_pct']}/{record['short_pct']} | {signal_bar} {signal_label}")

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
