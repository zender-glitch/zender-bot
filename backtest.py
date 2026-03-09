"""
ZENDER COMMANDER TERMINAL — Backtest v7 — Bybit OI/FR + Bounce Detection
Прогоняет LLM-анализ по историческим данным за 30 дней.
Все 5 монет: BTC, ETH, SOL, BNB, AVAX.
v7 улучшения:
  - Bybit API для OI и FR истории (бесплатный, без ключа, US-friendly)
  - Улучшенный промпт: bounce detection, price momentum, менее жёсткий sell bias
  - 3d/7d momentum для детекции перепроданности и отскоков
"""

import asyncio
import httpx
import os
import sys
from datetime import datetime

# ── Ключи ──
COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

CG_BASE = "https://open-api-v4.coinglass.com"
CG_HEADERS = {"CG-API-KEY": COINGLASS_API_KEY}
BGEOMETRICS_BASE = "https://bitcoin-data.com/v1"
CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com"
BYBIT_BASE = "https://api.bybit.com"

# Bybit символы для фьючерсов
BYBIT_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
    "BNB": "BNBUSDT", "AVAX": "AVAXUSDT",
}

# Монеты для бэктеста
ALL_COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX"]
COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "BNB": "binancecoin", "AVAX": "avalanche-2",
}

DAYS = 30  # 30 дней для лучшей статистики


# ══════════════════════════════════════════════════════════════════════════════
# ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ — те же формулы что в collector.py
# ══════════════════════════════════════════════════════════════════════════════

def calc_rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def calc_sma(prices: list[float], period: int) -> float | None:
    if len(prices) < period:
        return None
    return round(sum(prices[-period:]) / period, 2)


def calc_ema(prices: list[float], period: int) -> list[float]:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema


def calc_macd(prices: list[float]) -> tuple[float | None, float | None]:
    ema12 = calc_ema(prices, 12)
    ema26 = calc_ema(prices, 26)
    if not ema12 or not ema26:
        return None, None
    min_len = min(len(ema12), len(ema26))
    offset = len(ema12) - min_len
    macd_line = [ema12[offset + i] - ema26[i] for i in range(min_len)]
    if len(macd_line) < 9:
        return round(macd_line[-1], 2) if macd_line else None, None
    signal = calc_ema(macd_line, 9)
    return round(macd_line[-1], 2), round(signal[-1], 2) if signal else None


def calc_indicators_from_closes(closes: list[float], day_idx: int) -> dict:
    """Считает индикаторы для среза цен до day_idx включительно."""
    if day_idx < 50:
        return {}
    subset = closes[:day_idx + 1]
    result = {}
    rsi = calc_rsi(subset)
    if rsi is not None:
        result["rsi"] = rsi
    macd_val, _ = calc_macd(subset)
    if macd_val is not None:
        result["macd"] = macd_val
    sma50 = calc_sma(subset, 50)
    if sma50 is not None:
        result["sma50"] = sma50
    sma200 = calc_sma(subset, 200)
    if sma200 is not None:
        result["sma200"] = sma200
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════

def fmt_usd(val):
    if val is None:
        return "нет данных"
    try:
        v = float(val)
    except (ValueError, TypeError):
        return "нет данных"
    if abs(v) >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f} млрд"
    elif abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.2f}"


def fmt_pct(val):
    if val is None:
        return "нет данных"
    try:
        v = float(val)
        return f"{'+' if v > 0 else ''}{v:.2f}%"
    except (ValueError, TypeError):
        return "нет данных"


# ══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

async def cg_get(path, params=None):
    url = f"{CG_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=CG_HEADERS, params=params or {})
            resp.raise_for_status()
            body = resp.json()
            code = body.get("code")
            if code is not None and str(code) != "0":
                return None
            return body.get("data")
    except Exception as e:
        print(f"  ❌ CG {path}: {e}")
        return None


async def bgeometrics_get(metric):
    url = f"{BGEOMETRICS_BASE}/{metric}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    except Exception as e:
        print(f"  ❌ BGeometrics {metric}: {e}")
        return []


async def fetch_price_history_cryptocompare(symbol: str, days: int) -> list[dict]:
    """Цены через CryptoCompare (стабильный, без rate limit проблем)."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{CRYPTOCOMPARE_BASE}/data/v2/histoday",
                params={"fsym": symbol, "tsym": "USD", "limit": days + 200}  # +200 для SMA200
            )
            if resp.status_code != 200:
                print(f"  ❌ CryptoCompare history {symbol}: HTTP {resp.status_code}")
                return []
            data = resp.json().get("Data", {}).get("Data", [])
            prices = []
            for item in data:
                if item.get("close") and item["close"] > 0:
                    dt = datetime.fromtimestamp(item["time"])
                    prices.append({
                        "date": dt.strftime("%Y-%m-%d"),
                        "price": item["close"],
                        "ts": item["time"] * 1000,
                    })
            return prices
    except Exception as e:
        print(f"  ❌ CryptoCompare history {symbol}: {e}")
        return []


async def fetch_bybit_oi_history(symbol: str, days: int) -> list:
    """Bybit Open Interest history — бесплатный, без ключа, US-friendly."""
    bybit_sym = BYBIT_SYMBOLS.get(symbol, f"{symbol}USDT")
    result = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Bybit даёт OI за intervalTime, максимум 200 записей
            resp = await client.get(
                f"{BYBIT_BASE}/v5/market/open-interest",
                params={
                    "category": "linear",
                    "symbol": bybit_sym,
                    "intervalTime": "1D",
                    "limit": min(days + 5, 200),
                }
            )
            if resp.status_code != 200:
                print(f"  ⚠️ Bybit OI {symbol}: HTTP {resp.status_code}")
                return []
            body = resp.json()
            if body.get("retCode") != 0:
                print(f"  ⚠️ Bybit OI {symbol}: {body.get('retMsg')}")
                return []
            data = body.get("result", {}).get("list", [])
            for item in data:
                ts = int(item.get("timestamp", 0))
                oi = item.get("openInterest")
                if ts and oi:
                    result.append({
                        "t": ts,
                        "openInterest": float(oi),
                    })
            result.sort(key=lambda x: x["t"])
            print(f"  ✅ Bybit OI {symbol}: {len(result)} дней")
    except Exception as e:
        print(f"  ❌ Bybit OI {symbol}: {e}")
    return result


async def fetch_bybit_fr_history(symbol: str, days: int) -> list:
    """Bybit Funding Rate history — бесплатный, без ключа, US-friendly."""
    bybit_sym = BYBIT_SYMBOLS.get(symbol, f"{symbol}USDT")
    result = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Bybit даёт FR за каждые 8 часов, берём побольше
            resp = await client.get(
                f"{BYBIT_BASE}/v5/market/funding/history",
                params={
                    "category": "linear",
                    "symbol": bybit_sym,
                    "limit": min(days * 3 + 10, 200),  # 3 раза в день × дни
                }
            )
            if resp.status_code != 200:
                print(f"  ⚠️ Bybit FR {symbol}: HTTP {resp.status_code}")
                return []
            body = resp.json()
            if body.get("retCode") != 0:
                print(f"  ⚠️ Bybit FR {symbol}: {body.get('retMsg')}")
                return []
            data = body.get("result", {}).get("list", [])
            # Группируем по дате — средний FR за день
            daily_fr = {}
            for item in data:
                ts = int(item.get("fundingRateTimestamp", 0))
                fr = item.get("fundingRate")
                if ts and fr:
                    date = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                    daily_fr.setdefault(date, []).append(float(fr))

            for date, rates in sorted(daily_fr.items()):
                avg_rate = sum(rates) / len(rates)
                # Конвертируем дату в timestamp
                dt = datetime.strptime(date, "%Y-%m-%d")
                result.append({
                    "t": int(dt.timestamp()) * 1000,
                    "date": date,
                    "fundingRate": avg_rate,
                })
            print(f"  ✅ Bybit FR {symbol}: {len(result)} дней")
    except Exception as e:
        print(f"  ❌ Bybit FR {symbol}: {e}")
    return result


async def fetch_coinglass_history(symbol: str, days: int) -> dict:
    """Загружает OI, FR, L/S, ликвидации. Coinglass + Bybit fallback для OI/FR."""
    result = {"oi": [], "fr": [], "ls": [], "liq": [], "bybit_oi": [], "bybit_fr": []}

    # OI history — сначала Coinglass, потом Bybit fallback
    data = await cg_get("/api/futures/open-interest/ohlc-history", {
        "symbol": symbol, "interval": "1d", "limit": days + 1,
    })
    if data and isinstance(data, list):
        result["oi"] = data
    await asyncio.sleep(0.5)

    # Если CG не дал OI → Bybit fallback
    if not result["oi"]:
        bybit_oi = await fetch_bybit_oi_history(symbol, days)
        result["bybit_oi"] = bybit_oi
        await asyncio.sleep(0.5)

    # FR history — сначала Coinglass, потом Bybit fallback
    for params in [
        {"symbol": symbol, "interval": "1d", "limit": days + 1},
        {"exchange": "Binance", "symbol": f"{symbol}USDT", "interval": "1d", "limit": days + 1},
    ]:
        data = await cg_get("/api/futures/funding-rate/ohlc-history", params)
        if data and isinstance(data, list):
            result["fr"] = data
            break
    await asyncio.sleep(0.5)

    # Если CG не дал FR → Bybit fallback
    if not result["fr"]:
        bybit_fr = await fetch_bybit_fr_history(symbol, days)
        result["bybit_fr"] = bybit_fr
        await asyncio.sleep(0.5)

    # L/S ratio history (Coinglass — работает на Hobbyist)
    for params in [
        {"exchange": "Binance", "symbol": f"{symbol}USDT", "interval": "1d", "limit": days + 1},
        {"symbol": symbol, "interval": "1d", "limit": days + 1},
    ]:
        data = await cg_get("/api/futures/global-long-short-account-ratio/history", params)
        if data and isinstance(data, list):
            result["ls"] = data
            break
    await asyncio.sleep(0.5)

    # Liquidation history (Coinglass — работает на Hobbyist)
    for exchange_list in ["Binance", "Binance,OKX,Bybit"]:
        data = await cg_get("/api/futures/liquidation/aggregated-history", {
            "symbol": symbol, "interval": "1d", "limit": days + 1,
            "exchange_list": exchange_list,
        })
        if data and isinstance(data, list):
            result["liq"] = data
            break
    await asyncio.sleep(0.5)

    return result


async def fetch_fear_greed_history(days: int) -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"https://api.alternative.me/fng/?limit={days + 1}")
            resp.raise_for_status()
            return resp.json().get("data", [])
    except Exception as e:
        print(f"  ❌ F&G: {e}")
        return []


async def fetch_bgeometrics_history() -> dict:
    """BGeometrics данные для BTC: SOPR, Netflow, Reserve, Tech indicators."""
    bg_data = {}

    for metric, fields in [
        ("sopr", [("sopr", "sopr")]),
        ("exchange-netflow-btc", [("exchangeNetflowBtc", "netflow"), ("value", "netflow")]),
        ("exchange-reserve-btc", [("exchangeReserveBtc", "reserve"), ("value", "reserve")]),
        ("technical-indicators", [("rsi", "rsi"), ("RSI", "rsi"), ("macd", "macd"), ("MACD", "macd"),
                                   ("sma50", "sma50"), ("SMA50", "sma50"), ("sma200", "sma200"), ("SMA200", "sma200")]),
    ]:
        hist = await bgeometrics_get(metric)
        await asyncio.sleep(2)
        for item in hist:
            d = item.get("d")
            if not d:
                continue
            bg_data.setdefault(d, {})
            for src_key, dst_key in fields:
                val = item.get(src_key)
                if val is not None and dst_key not in bg_data[d]:
                    try:
                        bg_data[d][dst_key] = float(val)
                    except (ValueError, TypeError):
                        pass

    print(f"  ✅ BGeometrics: данные за {len(bg_data)} дней")
    return bg_data


def find_by_date(history, target_date):
    if not history:
        return None
    for item in history:
        ts = item.get("t") or item.get("time") or item.get("timestamp") or item.get("createTime")
        if ts:
            try:
                ts_val = int(ts)
                item_date = datetime.fromtimestamp(ts_val / 1000 if ts_val > 1e10 else ts_val).strftime("%Y-%m-%d")
                if item_date == target_date:
                    return item
            except (ValueError, TypeError):
                pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# LLM ВЫЗОВ
# ══════════════════════════════════════════════════════════════════════════════

async def call_llm(prompt: str) -> dict:
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
                print(f"  ⚠️ LLM {resp.status_code}: {resp.text[:200]}")
                return {}

            body = resp.json()
            text = body["content"][0]["text"].strip()

            result = {}
            for line in text.split("\n"):
                clean = line.strip().lstrip("*").lstrip("#").strip()
                upper = clean.upper()
                if upper.startswith("АНАЛИЗ:") or upper.startswith("АНАЛИЗ :"):
                    val = clean.split(":", 1)[1].strip() if ":" in clean else ""
                    if val:
                        result["analysis"] = val
                elif upper.startswith("РЕКОМЕНДАЦИЯ:") or upper.startswith("РЕКОМЕНДАЦИЯ :"):
                    val = clean.split(":", 1)[1].strip().lower() if ":" in clean else ""
                    if val:
                        result["recommendation"] = val
                elif upper.startswith("ЗОНЫ:") or upper.startswith("ЗОНЫ :"):
                    val = clean.split(":", 1)[1].strip() if ":" in clean else ""
                    if val:
                        result["zones"] = val

            if not result.get("recommendation"):
                text_lower = text.lower()
                if "покупать" in text_lower:
                    result["recommendation"] = "покупать"
                elif "продавать" in text_lower:
                    result["recommendation"] = "продавать"
                elif "выжидать" in text_lower:
                    result["recommendation"] = "выжидать"

            return result
    except Exception as e:
        print(f"  ❌ LLM: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ПОСТРОЕНИЕ ПРОМПТА (идентичен collector.py)
# ══════════════════════════════════════════════════════════════════════════════

def build_prompt(symbol: str, date: str, coin_data: dict) -> str:
    """Строит промпт v7 — с momentum, bounce detection, без жёсткого sell bias."""
    price = coin_data.get("price")
    day_change = coin_data.get("day_change")
    oi_val = coin_data.get("oi")
    oi_change = coin_data.get("oi_change")
    fr_val = coin_data.get("fr")
    long_pct = coin_data.get("long_pct")
    short_pct = coin_data.get("short_pct")
    liq_long = coin_data.get("liq_long")
    liq_short = coin_data.get("liq_short")
    fg_val = coin_data.get("fg_val")
    fg_label = coin_data.get("fg_label")

    # On-chain (BTC only)
    sopr = coin_data.get("sopr")
    netflow = coin_data.get("netflow")
    reserve = coin_data.get("reserve")

    # Tech indicators (all coins)
    rsi = coin_data.get("rsi")
    macd = coin_data.get("macd")
    sma50 = coin_data.get("sma50")
    sma200 = coin_data.get("sma200")

    # Momentum
    momentum_3d = coin_data.get("momentum_3d")
    momentum_7d = coin_data.get("momentum_7d")

    onchain_lines = []

    # On-chain блок (BTC only)
    if any([sopr, netflow, reserve]):
        onchain_lines.append(f"\nON-CHAIN ДАННЫЕ (BTC блокчейн):")
        if reserve:
            onchain_lines.append(f"- Резерв BTC на биржах: {reserve:,.0f} BTC")
        if netflow is not None:
            direction = "ОТТОК с бирж (бычий — холдят)" if netflow < 0 else "ПРИТОК на биржи (медвежий — готовятся продавать)" if netflow > 0 else "баланс"
            onchain_lines.append(f"- Нетто поток бирж: {netflow:,.2f} BTC — {direction}")
        if sopr:
            sopr_hint = "продают в прибыль" if sopr > 1 else "продают в убыток (капитуляция)" if sopr < 1 else "безубыток"
            onchain_lines.append(f"- SOPR: {sopr} — {sopr_hint}")

    # Tech indicators блок (all coins)
    if any([rsi, macd, sma50, sma200]):
        onchain_lines.append(f"\nТЕХНИЧЕСКИЕ ИНДИКАТОРЫ:")
        if rsi is not None:
            rsi_hint = "СИЛЬНО перепродан!" if rsi < 25 else "перепродан (<30)" if rsi < 30 else "перекуплен (>70)" if rsi > 70 else "нейтральная зона"
            onchain_lines.append(f"- RSI: {rsi:.1f} — {rsi_hint}")
        if macd is not None:
            macd_hint = "бычий импульс" if macd > 0 else "медвежий импульс"
            onchain_lines.append(f"- MACD: {macd:.2f} — {macd_hint}")
        if sma50 and sma200:
            if sma50 > sma200:
                onchain_lines.append(f"- SMA50/200: golden cross (SMA50 ${sma50:,.0f} > SMA200 ${sma200:,.0f}) — бычий")
            else:
                onchain_lines.append(f"- SMA50/200: death cross (SMA50 ${sma50:,.0f} < SMA200 ${sma200:,.0f}) — медвежий")

    # Momentum блок
    if momentum_3d is not None or momentum_7d is not None:
        onchain_lines.append(f"\nЦЕНОВОЙ МОМЕНТУМ:")
        if momentum_3d is not None:
            onchain_lines.append(f"- Изменение за 3 дня: {momentum_3d:+.2f}%")
        if momentum_7d is not None:
            onchain_lines.append(f"- Изменение за 7 дней: {momentum_7d:+.2f}%")
        # Подсказки
        if momentum_7d is not None and momentum_7d < -8:
            onchain_lines.append(f"  ⚠️ СИЛЬНОЕ ПАДЕНИЕ за неделю — вероятность отскока высокая!")
        elif momentum_3d is not None and momentum_3d < -5:
            onchain_lines.append(f"  ⚠️ Резкое падение за 3 дня — возможен технический отскок")
        elif momentum_7d is not None and momentum_7d > 8:
            onchain_lines.append(f"  ⚠️ Сильный рост за неделю — возможна коррекция")

    onchain_block = "\n".join(onchain_lines) if onchain_lines else ""

    return f"""Ты — опытный крипто-аналитик. Проанализируй данные {symbol} и дай РЕШИТЕЛЬНЫЙ анализ на СЛЕДУЮЩИЕ 24 ЧАСА.

ДАННЫЕ {symbol} на {date}:
- Цена: ${price:,.0f}, изменение 24ч: {fmt_pct(day_change)}
- Открытый интерес (OI): {fmt_usd(oi_val)} ({fmt_pct(oi_change)} за день)
- Funding Rate: {fmt_pct(fr_val)}
- Покупатели/Продавцы (taker): {long_pct or '?'}% / {short_pct or '?'}%
- Ликвидации {symbol}: лонги {fmt_usd(liq_long)}, шорты {fmt_usd(liq_short)}
- Fear & Greed: {fg_val or '?'} ({fg_label or '?'}){onchain_block}

ПРАВИЛА ОЦЕНКИ (следуй им строго):

ПОКУПАТЬ если 2+ условий совпадают:
- Покупатели > 52% (быки доминируют)
- OI растёт > +0.5% за день (новые деньги заходят)
- Funding Rate отрицательный (шорты переплачивают — short squeeze вероятен)
- Fear & Greed < 25 (экстремальный страх — контр-сигнал, рынок перепродан)
- Ликвидации шортов > ликвидаций лонгов в 2+ раза
- RSI < 30 (перепродан — разворот вверх вероятен)
- Цена упала > 5% за 3 дня ИЛИ > 8% за 7 дней (перепроданность, отскок вероятен)

ПРОДАВАТЬ если 2+ условий совпадают:
- Продавцы > 52% (медведи доминируют)
- OI падает < -0.5% за день (деньги уходят)
- Funding Rate > +0.05% (лонги переплачивают — рынок перегрет)
- Fear & Greed > 75 (сильная жадность)
- Ликвидации лонгов > ликвидаций шортов в 2+ раза
- RSI > 70 (перекуплен)
- Цена выросла > 5% за 3 дня ИЛИ > 10% за 7 дней (перегрев, коррекция вероятна)

ДЕТЕКЦИЯ ОТСКОКОВ (КРИТИЧЕСКИ ВАЖНО!):
Даже в медвежьем тренде (death cross, MACD < 0) бывают ОТСКОКИ на 3-6%.
Рекомендуй ПОКУПАТЬ на отскок если ВСЕ условия совпадают:
- RSI < 30 (перепродан)
- Цена упала > 5% за последние 3-7 дней
- Fear & Greed < 30 (экстремальный страх)
- Funding Rate отрицательный или нейтральный (шорты не перегреты)
Это краткосрочная покупка на отскок, а не разворот тренда!

ТРЕНД КАК КОНТЕКСТ (не абсолютное правило):
- Death cross (SMA50 < SMA200) + MACD < 0 = медвежий тренд — общий BIAS на продажу
- Но в медвежьем тренде случаются отскоки на 3-8% после сильных падений
- Golden cross + MACD > 0 = бычий тренд — общий BIAS на покупку
- Но в бычьем тренде случаются коррекции на 3-8% после сильных ростов

ВЫЖИДАТЬ если:
- Сигналы 50/50, нет перевеса
- RSI в зоне 40-60, MACD около 0, нет экстремумов

ВАЖНО: НЕ давай "продавать" 10 дней подряд! Если уже 3+ дня подряд падение — ищи отскок!

ПРАВИЛА ДЛЯ ЗОН (СТРОГО!):
- Зона покупки ДОЛЖНА ВКЛЮЧАТЬ текущую цену
- Зона продажи = от (цена + 2-3%) до (цена + 4-5%)

ОТВЕТЬ СТРОГО В ФОРМАТЕ (3 строки, без лишнего):
АНАЛИЗ: [2-3 предложения простым языком]
РЕКОМЕНДАЦИЯ: [одно слово: покупать / продавать / выжидать]
ЗОНЫ: покупка $XXX,XXX–$XXX,XXX | продажа $XXX,XXX–$XXX,XXX"""


# ══════════════════════════════════════════════════════════════════════════════
# ОСНОВНОЙ БЭКТЕСТ
# ══════════════════════════════════════════════════════════════════════════════

async def run_backtest_coin(symbol: str, fg_hist: list, bg_hist: dict) -> list:
    """Бэктест одной монеты. Возвращает список результатов."""
    print(f"\n{'━' * 60}")
    print(f"📊 {symbol} — загрузка данных...")
    print(f"{'━' * 60}")

    # Цены из CryptoCompare (стабильный, 200+ дней для SMA200)
    all_prices = await fetch_price_history_cryptocompare(symbol, DAYS)
    if len(all_prices) < DAYS + 50:
        print(f"  ⚠️ {symbol}: мало ценовых данных ({len(all_prices)})")
        return []
    print(f"  ✅ Цены: {len(all_prices)} дней")

    # Все closes для расчёта индикаторов на каждый день
    all_closes = [p["price"] for p in all_prices]

    # Coinglass исторические данные
    print(f"  📡 Coinglass: загрузка OI/FR/LS/LIQ...")
    cg_data = await fetch_coinglass_history(symbol, DAYS)
    oi_src = f"CG:{len(cg_data['oi'])}" if cg_data['oi'] else f"Bybit:{len(cg_data.get('bybit_oi', []))}"
    fr_src = f"CG:{len(cg_data['fr'])}" if cg_data['fr'] else f"Bybit:{len(cg_data.get('bybit_fr', []))}"
    print(f"  ✅ OI({oi_src}) FR({fr_src}) LS:{len(cg_data['ls'])} LIQ:{len(cg_data['liq'])}")

    await asyncio.sleep(2)

    # Определяем окно для бэктеста (последние DAYS дней)
    test_start = len(all_prices) - DAYS - 1  # -1 потому что нужен next_day
    if test_start < 50:
        test_start = 50  # Минимум 50 дней для RSI

    results = []
    for i in range(test_start, len(all_prices) - 1):
        day = all_prices[i]
        next_day = all_prices[i + 1]
        date = day["date"]
        price = day["price"]
        next_price = next_day["price"]
        change_pct = ((next_price - price) / price) * 100

        # Изменение за 24ч
        day_change = 0
        if i > 0:
            prev_price = all_prices[i - 1]["price"]
            if prev_price > 0:
                day_change = ((price - prev_price) / prev_price) * 100

        # Coinglass данные по дате (с Bybit fallback для OI/FR)
        oi_val = None
        oi_change = None
        fr_val = None
        long_pct = None
        short_pct = None
        liq_long = None
        liq_short = None

        # OI — сначала Coinglass, потом Bybit
        oi_item = find_by_date(cg_data["oi"], date)
        if oi_item:
            oi_val = oi_item.get("c") or oi_item.get("close") or oi_item.get("openInterest")
            if i > 0:
                prev_date = all_prices[i - 1]["date"]
                prev_oi_item = find_by_date(cg_data["oi"], prev_date)
                if prev_oi_item:
                    prev_oi = prev_oi_item.get("c") or prev_oi_item.get("close")
                    if prev_oi and oi_val:
                        try:
                            oi_change = ((float(oi_val) - float(prev_oi)) / float(prev_oi)) * 100
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass

        # Bybit OI fallback
        if oi_val is None and cg_data.get("bybit_oi"):
            bybit_oi_item = find_by_date(cg_data["bybit_oi"], date)
            if bybit_oi_item:
                oi_val = bybit_oi_item.get("openInterest")
                if i > 0:
                    prev_date = all_prices[i - 1]["date"]
                    prev_bybit = find_by_date(cg_data["bybit_oi"], prev_date)
                    if prev_bybit and prev_bybit.get("openInterest"):
                        try:
                            oi_change = ((float(oi_val) - float(prev_bybit["openInterest"])) / float(prev_bybit["openInterest"])) * 100
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass

        # FR — сначала Coinglass, потом Bybit
        fr_item = find_by_date(cg_data["fr"], date)
        if fr_item:
            fr_raw = fr_item.get("c") or fr_item.get("close") or fr_item.get("fundingRate")
            if fr_raw is not None:
                try:
                    fr_val = float(fr_raw) * 100
                except (ValueError, TypeError):
                    pass

        # Bybit FR fallback
        if fr_val is None and cg_data.get("bybit_fr"):
            for bfr in cg_data["bybit_fr"]:
                if bfr.get("date") == date:
                    try:
                        fr_val = float(bfr["fundingRate"]) * 100
                    except (ValueError, TypeError):
                        pass
                    break

        ls_item = find_by_date(cg_data["ls"], date)
        if ls_item:
            long_pct = ls_item.get("longRatio") or ls_item.get("longAccount") or ls_item.get("buyRatio")
            short_pct = ls_item.get("shortRatio") or ls_item.get("shortAccount") or ls_item.get("sellRatio")

        liq_item = find_by_date(cg_data["liq"], date)
        if liq_item:
            liq_long = liq_item.get("longVolUsd") or liq_item.get("longLiquidationUsd")
            liq_short = liq_item.get("shortVolUsd") or liq_item.get("shortLiquidationUsd")

        # Fear & Greed
        fg_val = None
        fg_label = None
        for fg_item in fg_hist:
            try:
                fg_date = datetime.fromtimestamp(int(fg_item.get("timestamp", 0))).strftime("%Y-%m-%d")
                if fg_date == date:
                    fg_val = fg_item.get("value")
                    fg_label = fg_item.get("value_classification")
                    break
            except (ValueError, TypeError):
                pass

        # BGeometrics (BTC only)
        sopr = None
        netflow = None
        reserve = None
        if symbol == "BTC":
            bg = bg_hist.get(date, {})
            sopr = bg.get("sopr")
            netflow = bg.get("netflow")
            reserve = bg.get("reserve")

        # Технические индикаторы — считаем из цен до этого дня
        tech = calc_indicators_from_closes(all_closes, i)
        rsi = tech.get("rsi")
        macd = tech.get("macd")
        sma50 = tech.get("sma50")
        sma200 = tech.get("sma200")

        # Для BTC берём из BGeometrics если есть (более точные)
        if symbol == "BTC":
            bg_day = bg_hist.get(date, {})
            if bg_day.get("rsi") is not None:
                rsi = bg_day["rsi"]
            if bg_day.get("macd") is not None:
                macd = bg_day["macd"]
            if bg_day.get("sma50") is not None:
                sma50 = bg_day["sma50"]
            if bg_day.get("sma200") is not None:
                sma200 = bg_day["sma200"]

        # Price momentum — 3d и 7d изменение цены
        momentum_3d = None
        momentum_7d = None
        if i >= 3:
            momentum_3d = ((price - all_prices[i - 3]["price"]) / all_prices[i - 3]["price"]) * 100
        if i >= 7:
            momentum_7d = ((price - all_prices[i - 7]["price"]) / all_prices[i - 7]["price"]) * 100

        # Собираем данные для промпта
        coin_data = {
            "price": price, "day_change": day_change,
            "oi": oi_val, "oi_change": oi_change,
            "fr": fr_val, "long_pct": long_pct, "short_pct": short_pct,
            "liq_long": liq_long, "liq_short": liq_short,
            "fg_val": fg_val, "fg_label": fg_label,
            "sopr": sopr, "netflow": netflow, "reserve": reserve,
            "rsi": rsi, "macd": macd, "sma50": sma50, "sma200": sma200,
            "momentum_3d": momentum_3d, "momentum_7d": momentum_7d,
        }

        prompt = build_prompt(symbol, date, coin_data)

        # Лог
        extras = []
        if oi_change is not None:
            extras.append(f"OI:{oi_change:+.1f}%")
        if fr_val is not None:
            extras.append(f"FR:{fr_val:+.3f}%")
        if rsi is not None:
            extras.append(f"RSI:{rsi:.0f}")
        if macd is not None:
            extras.append(f"MACD:{macd:.0f}")
        if sopr:
            extras.append(f"SOPR:{sopr:.3f}")
        if momentum_3d is not None:
            extras.append(f"3d:{momentum_3d:+.1f}%")
        if momentum_7d is not None:
            extras.append(f"7d:{momentum_7d:+.1f}%")
        extra_str = f" | {' '.join(extras)}" if extras else ""

        print(f"  📅 {date} | ${price:,.0f}{extra_str}")
        llm_result = await call_llm(prompt)
        rec = llm_result.get("recommendation", "—")

        correct = None
        if "покупать" in rec:
            correct = change_pct > 0
        elif "продавать" in rec:
            correct = change_pct < 0
        elif "выжидать" in rec:
            correct = abs(change_pct) < 2

        icon = "✅" if correct else ("❌" if correct is False else "⏸️")
        direction = f"{'🔺' if change_pct > 0 else '🔻'} {change_pct:+.2f}%"

        results.append({
            "coin": symbol, "date": date, "price": price,
            "next_price": next_price, "change": change_pct,
            "recommendation": rec, "correct": correct,
            "rsi": rsi, "macd": macd,
        })

        print(f"     🤖 {rec} | Через 24ч: ${next_price:,.0f} ({direction}) {icon}")
        await asyncio.sleep(2)  # Rate limit LLM

    return results


def print_coin_stats(symbol: str, results: list):
    """Статистика по одной монете."""
    if not results:
        print(f"\n{symbol}: нет данных")
        return

    total = len(results)
    correct = sum(1 for r in results if r["correct"] is True)
    wrong = sum(1 for r in results if r["correct"] is False)
    neutral = sum(1 for r in results if r["correct"] is None)
    accuracy = correct / (correct + wrong) * 100 if (correct + wrong) > 0 else 0

    buy_recs = [r for r in results if "покупать" in r.get("recommendation", "")]
    sell_recs = [r for r in results if "продавать" in r.get("recommendation", "")]
    wait_recs = [r for r in results if "выжидать" in r.get("recommendation", "")]

    pnl = 0.0
    for r in results:
        rec = r.get("recommendation", "")
        if "покупать" in rec:
            pnl += r["change"]
        elif "продавать" in rec:
            pnl -= r["change"]

    print(f"\n  {symbol}: {total} дней | ✅ {correct} ({accuracy:.0f}%) | ❌ {wrong} | ⏸️ {neutral}")
    print(f"    📈 Покупать: {len(buy_recs)} | 📉 Продавать: {len(sell_recs)} | ⏸️ Выжидать: {len(wait_recs)}")
    print(f"    💰 PnL: {pnl:+.2f}%")

    if buy_recs:
        avg = sum(r["change"] for r in buy_recs) / len(buy_recs)
        ok = sum(1 for r in buy_recs if r["correct"])
        print(f"    'Покупать': avg {avg:+.2f}%, верных {ok}/{len(buy_recs)}")
    if sell_recs:
        avg = sum(r["change"] for r in sell_recs) / len(sell_recs)
        ok = sum(1 for r in sell_recs if r["correct"])
        print(f"    'Продавать': avg {avg:+.2f}%, верных {ok}/{len(sell_recs)}")


async def run_backtest():
    # Парсим аргументы: можно передать монеты и кол-во дней
    coins = ALL_COINS
    global DAYS

    args = sys.argv[1:]
    for arg in args:
        if arg.upper() in ALL_COINS:
            coins = [arg.upper()]
        elif arg.isdigit():
            DAYS = int(arg)

    print("=" * 60)
    print(f"🔬 ZENDER BACKTEST v7 — Bybit OI/FR + Bounce Detection")
    print(f"   Монеты: {', '.join(coins)} | Дней: {DAYS}")
    print("=" * 60)

    # Общие данные (загружаем один раз)
    print("\n📡 Загрузка общих данных...")
    fg_hist = await fetch_fear_greed_history(DAYS)
    print(f"  ✅ Fear & Greed: {len(fg_hist)} дней")

    bg_hist = {}
    if "BTC" in coins:
        print("  🔗 BGeometrics on-chain...")
        bg_hist = await fetch_bgeometrics_history()
        await asyncio.sleep(2)

    # Бэктест по каждой монете
    all_results = {}
    for symbol in coins:
        results = await run_backtest_coin(symbol, fg_hist, bg_hist)
        all_results[symbol] = results
        await asyncio.sleep(3)  # Пауза между монетами

    # ── ИТОГИ ──
    print("\n" + "=" * 60)
    print("📊 ИТОГИ БЭКТЕСТА v7")
    print("=" * 60)

    total_all = 0
    correct_all = 0
    wrong_all = 0
    pnl_all = 0.0

    for symbol in coins:
        results = all_results.get(symbol, [])
        print_coin_stats(symbol, results)

        total_all += len(results)
        correct_all += sum(1 for r in results if r["correct"] is True)
        wrong_all += sum(1 for r in results if r["correct"] is False)
        for r in results:
            rec = r.get("recommendation", "")
            if "покупать" in rec:
                pnl_all += r["change"]
            elif "продавать" in rec:
                pnl_all -= r["change"]

    # Общая статистика
    if total_all > 0:
        overall_accuracy = correct_all / (correct_all + wrong_all) * 100 if (correct_all + wrong_all) > 0 else 0
        print(f"\n{'─' * 60}")
        print(f"📈 ОБЩАЯ ТОЧНОСТЬ: {correct_all}/{correct_all + wrong_all} = {overall_accuracy:.1f}%")
        print(f"💰 ОБЩИЙ PnL: {pnl_all:+.2f}%")
        print(f"📊 Всего дней проанализировано: {total_all}")
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    if not COINGLASS_API_KEY:
        print("❌ Нет COINGLASS_API_KEY!")
    elif not ANTHROPIC_KEY:
        print("❌ Нет ANTHROPIC_KEY!")
    else:
        asyncio.run(run_backtest())
