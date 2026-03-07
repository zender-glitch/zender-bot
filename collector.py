"""
ZENDER COMMANDER TERMINAL — Data Collector
Этап 2: сбор данных из Coinglass API + Fear & Greed Index
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


async def fetch_open_interest(symbol: str) -> dict:
    """
    Открытый интерес по всем биржам (агрегированный).
    Возвращает: {oi: float, oi_change_4h: float}
    """
    data = await cg_get("/api/futures/openInterest/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    total_oi = 0
    for item in data:
        total_oi += float(item.get("openInterest", 0))

    # Также получаем историю OI для расчёта изменения за 4ч
    history = await cg_get("/api/futures/openInterest/ohlc-history", {
        "symbol": symbol,
        "interval": "4h",
        "limit": 2,
    })
    oi_change_4h = None
    if history and len(history) >= 2:
        try:
            prev = float(history[-2].get("c", 0) or history[-2].get("close", 0))
            curr = float(history[-1].get("c", 0) or history[-1].get("close", 0))
            if prev > 0:
                oi_change_4h = ((curr - prev) / prev) * 100
        except (ValueError, TypeError, IndexError):
            pass

    return {
        "oi": total_oi,
        "oi_change_4h": oi_change_4h,
    }


async def fetch_funding_rate(symbol: str) -> dict:
    """
    Текущий Funding Rate по биржам.
    Возвращает: {funding_rate: float (средний по биржам)}
    """
    data = await cg_get("/api/futures/fundingRate/exchange-list", {"symbol": symbol})
    if not data:
        return {}

    rates = []
    for item in data:
        rate = item.get("rate") or item.get("fundingRate")
        if rate is not None:
            try:
                rates.append(float(rate))
            except (ValueError, TypeError):
                pass

    if not rates:
        return {}

    avg_rate = sum(rates) / len(rates)
    return {"funding_rate": avg_rate}


async def fetch_long_short(symbol: str) -> dict:
    """
    Long/Short Account Ratio (глобальный).
    Возвращает: {long_pct, short_pct, long_vol, short_vol}
    """
    data = await cg_get("/api/futures/globalLongShortAccountRatio", {
        "symbol": symbol,
        "interval": "1h",
        "limit": 1,
    })
    if not data:
        # Пробуем альтернативный эндпоинт
        data = await cg_get("/api/futures/longShort/exchange-list", {"symbol": symbol})

    if not data:
        return {}

    try:
        if isinstance(data, list) and len(data) > 0:
            item = data[-1] if isinstance(data, list) else data
            long_ratio = float(item.get("longRate", 0) or item.get("longAccount", 0))
            short_ratio = float(item.get("shortRate", 0) or item.get("shortAccount", 0))
        elif isinstance(data, dict):
            long_ratio = float(data.get("longRate", 0) or data.get("longAccount", 0))
            short_ratio = float(data.get("shortRate", 0) or data.get("shortAccount", 0))
        else:
            return {}

        # Нормализуем если пришло как ratio (1.5) а не как проценты (60%)
        if long_ratio > 0 and long_ratio < 10 and short_ratio > 0 and short_ratio < 10:
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
    Возвращает: {liq_long: float, liq_short: float}
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

        liq_long = float(item.get("longLiquidationUsd", 0) or item.get("buyVolUsd", 0) or 0)
        liq_short = float(item.get("shortLiquidationUsd", 0) or item.get("sellVolUsd", 0) or 0)

        return {
            "liq_long": liq_long,
            "liq_short": liq_short,
        }
    except (ValueError, TypeError):
        return {}


async def fetch_fear_greed() -> dict:
    """
    Fear & Greed Index (бесплатный, Alternative.me).
    Возвращает: {fear_greed: int, fear_greed_label: str}
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.alternative.me/fng/?limit=1")
            resp.raise_for_status()
            data = resp.json()
            item = data["data"][0]
            value = int(item["value"])
            label = item["value_classification"]

            # Переводим на русский
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


async def fetch_price(symbol: str) -> dict:
    """
    Текущая цена с Binance (бесплатно, без ключа).
    Возвращает: {price: float, change_24h: float}
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.binance.com/api/v3/ticker/24hr",
                params={"symbol": f"{symbol}USDT"}
            )
            resp.raise_for_status()
            data = resp.json()
            price = float(data["lastPrice"])
            change = float(data["priceChangePercent"])
            return {"price": price, "change_24h": change}
    except Exception as e:
        log.warning(f"Binance price error {symbol}: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# СИГНАЛ (расчёт силы сигнала)
# ══════════════════════════════════════════════════════════════════════════════

def calculate_signal(coin_data: dict) -> tuple[str, str]:
    """
    Рассчитывает силу сигнала на основе собранных данных.
    Возвращает: (signal_bar, signal_label)
    Логика: считаем "бычьи" факторы.
    """
    score = 0
    max_score = 0

    # 1. Long/Short > 60% = бычий сигнал
    long_pct = coin_data.get("long_pct")
    if long_pct is not None:
        max_score += 1
        if long_pct > 60:
            score += 1
        elif long_pct < 40:
            score -= 0.5

    # 2. OI растёт = бычий сигнал
    oi_change = coin_data.get("oi_change_4h")
    if oi_change is not None:
        max_score += 1
        if oi_change > 2:
            score += 1
        elif oi_change < -2:
            score -= 0.5

    # 3. Funding Rate умеренно положительный = бычий
    fr = coin_data.get("funding_rate")
    if fr is not None:
        max_score += 1
        if 0 < fr < 0.05:
            score += 1
        elif fr > 0.1:
            score -= 0.5  # перегрет
        elif fr < -0.01:
            score -= 0.5

    # 4. Fear & Greed > 50 = бычий настрой
    fg = coin_data.get("fear_greed")
    if fg is not None:
        max_score += 1
        if fg > 60:
            score += 1
        elif fg < 30:
            score -= 0.5

    # 5. Цена растёт за 24ч
    change = coin_data.get("change_24h")
    if change is not None:
        max_score += 1
        if change > 2:
            score += 1
        elif change < -2:
            score -= 0.5

    # Нормализуем в 0-5 шкалу
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

    # Fear & Greed общий для всех монет
    fg_data = await fetch_fear_greed()

    for symbol in COINS:
        try:
            log.info(f"  ⏳ {symbol}...")

            # Собираем данные параллельно
            price_data, oi_data, fr_data, ls_data, liq_data = await asyncio.gather(
                fetch_price(symbol),
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
            record = {
                "coin": symbol,
                "price": fmt_price(coin_data.get("price")),
                "change": fmt_pct(coin_data.get("change_24h")),
                "oi": fmt_usd(coin_data.get("oi")),
                "oi_change": fmt_pct(coin_data.get("oi_change_4h")),
                "funding_rate": fmt_pct(coin_data.get("funding_rate")),
                "long_pct": f"{coin_data.get('long_pct', 0)}%",
                "short_pct": f"{coin_data.get('short_pct', 0)}%",
                "long_vol": fmt_usd(coin_data.get("oi", 0) * (coin_data.get("long_pct", 50) / 100)) if coin_data.get("oi") else "—",
                "short_vol": fmt_usd(coin_data.get("oi", 0) * (coin_data.get("short_pct", 50) / 100)) if coin_data.get("oi") else "—",
                "liq_up": fmt_usd(coin_data.get("liq_short")),
                "liq_dn": fmt_usd(coin_data.get("liq_long")),
                "exchange_flow": "—",  # TODO: Glassnode (Этап 7)
                "whale_buy1h": "—",    # TODO: Glassnode/Nansen (Этап 7)
                "whale_buy24h": "—",   # TODO: Glassnode/Nansen (Этап 7)
                "whale_sell24h": "—",  # TODO: Glassnode/Nansen (Этап 7)
                "fear_greed": str(coin_data.get("fear_greed", "—")),
                "fear_greed_label": coin_data.get("fear_greed_label", "—"),
                "signal": signal_bar,
                "label": signal_label,
                "signal_label": signal_label,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            # Сохраняем в Supabase
            await db.upsert_market_data(record)
            log.info(f"  ✅ {symbol}: {record['price']} {record['change']} | {signal_bar} {signal_label}")

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
