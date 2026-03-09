"""
ZENDER COMMANDER TERMINAL — Backtest LLM Recommendations v4
Прогоняет LLM-анализ по историческим данным за 14 дней.
Промпт v4: RSI, MACD, SMA, Exchange Netflow, SOPR + temperature=0.
BGeometrics исторические данные (SOPR, Exchange Netflow, RSI, MACD, SMA).
"""

import asyncio
import httpx
import os
import json
from datetime import datetime

# ── Ключи ──
COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

CG_BASE = "https://open-api-v4.coinglass.com"
CG_HEADERS = {"CG-API-KEY": COINGLASS_API_KEY}
BGEOMETRICS_BASE = "https://bitcoin-data.com/v1"

SYMBOL = "BTC"
DAYS = 14


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


async def cg_get(path, params=None):
    url = f"{CG_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=CG_HEADERS, params=params or {})
            resp.raise_for_status()
            body = resp.json()
            code = body.get("code")
            if code is not None and str(code) != "0":
                print(f"  ⚠️ CG {path}: code={code} msg={body.get('msg')}")
                return None
            return body.get("data")
    except Exception as e:
        print(f"  ❌ CG {path}: {e}")
        return None


async def bgeometrics_get(metric):
    """Загружает полную историю метрики из BGeometrics."""
    url = f"{BGEOMETRICS_BASE}/{metric}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                print(f"  ⚠️ BGeometrics {metric}: HTTP {resp.status_code}")
                return []
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
    except Exception as e:
        print(f"  ❌ BGeometrics {metric}: {e}")
        return []


async def fetch_price_history():
    """Цены BTC за 14 дней с CoinGecko"""
    print("📈 Загружаем цены BTC за 14 дней...")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
                params={"vs_currency": "usd", "days": str(DAYS + 1), "interval": "daily"}
            )
            resp.raise_for_status()
            data = resp.json()
            prices = []
            for ts, price in data["prices"]:
                dt = datetime.fromtimestamp(ts / 1000)
                prices.append({"date": dt.strftime("%Y-%m-%d"), "price": price, "ts": ts})
            print(f"  ✅ Получено {len(prices)} дней цен")
            return prices
    except Exception as e:
        print(f"  ❌ CoinGecko: {e}")
        return []


async def fetch_liquidation_history():
    """Исторические ликвидации"""
    print("💥 Загружаем ликвидации history...")
    for exchange_list in ["Binance", "Binance,OKX,Bybit"]:
        data = await cg_get("/api/futures/liquidation/aggregated-history", {
            "symbol": SYMBOL,
            "interval": "1d",
            "limit": DAYS + 1,
            "exchange_list": exchange_list,
        })
        if data and isinstance(data, list):
            print(f"  ✅ Получено {len(data)} записей ликвидаций")
            return data
    data = await cg_get("/api/futures/liquidation/history", {
        "exchange": "Binance", "symbol": f"{SYMBOL}USDT",
        "interval": "1d", "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей ликвидаций (pair)")
        return data
    print("  ⚠️ Liquidation history недоступен")
    return []


async def fetch_ls_history():
    """Исторический Long/Short ratio"""
    print("📐 Загружаем L/S history...")
    PAIR = f"{SYMBOL}USDT"
    for params in [
        {"exchange": "Binance", "symbol": PAIR, "interval": "1d", "limit": DAYS + 1},
        {"symbol": SYMBOL, "interval": "1d", "limit": DAYS + 1},
    ]:
        data = await cg_get("/api/futures/global-long-short-account-ratio/history", params)
        if data and isinstance(data, list):
            print(f"  ✅ Получено {len(data)} записей L/S")
            return data
    for params in [
        {"symbol": SYMBOL, "interval": "1d", "limit": DAYS + 1},
        {"exchange": "Binance", "symbol": PAIR, "interval": "1d", "limit": DAYS + 1},
    ]:
        data = await cg_get("/api/futures/aggregated-taker-buy-sell-volume/history", params)
        if data and isinstance(data, list):
            print(f"  ✅ Получено {len(data)} записей taker B/S")
            return data
    print("  ⚠️ L/S history недоступен")
    return []


async def fetch_oi_history():
    """Исторический Open Interest из Coinglass"""
    print("📊 Загружаем OI history...")
    data = await cg_get("/api/futures/open-interest/ohlc-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей OI")
        return data
    # Fallback
    data = await cg_get("/api/futures/open-interest/ohlc-aggregated-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей OI (aggregated)")
        return data
    print("  ⚠️ OI history недоступен")
    return []


async def fetch_funding_rate_history():
    """Исторический Funding Rate из Coinglass"""
    print("💰 Загружаем Funding Rate history...")
    PAIR = f"{SYMBOL}USDT"
    for params in [
        {"symbol": SYMBOL, "interval": "1d", "limit": DAYS + 1},
        {"exchange": "Binance", "symbol": PAIR, "interval": "1d", "limit": DAYS + 1},
    ]:
        data = await cg_get("/api/futures/funding-rate/ohlc-history", params)
        if data and isinstance(data, list):
            print(f"  ✅ Получено {len(data)} записей FR")
            return data
    print("  ⚠️ Funding Rate history недоступен")
    return []


async def fetch_fear_greed_history():
    """Fear & Greed за 14 дней"""
    print("😱 Загружаем Fear & Greed history...")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"https://api.alternative.me/fng/?limit={DAYS + 1}")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            print(f"  ✅ Получено {len(data)} дней F&G")
            return data
    except Exception as e:
        print(f"  ❌ F&G: {e}")
        return []


async def fetch_bgeometrics_history():
    """
    Загружаем BGeometrics исторические данные: SOPR, Exchange Netflow, Technical Indicators.
    4 запроса — укладываемся в лимит (8 req/hour, 15 req/day).
    Возвращаем dict индексированный по дате.
    """
    print("🔗 Загружаем BGeometrics on-chain history...")
    bg_data = {}  # {date: {sopr, netflow, rsi, macd, sma50, sma200}}

    # SOPR history
    sopr_hist = await bgeometrics_get("sopr")
    await asyncio.sleep(2)
    for item in sopr_hist:
        d = item.get("d")
        sopr_val = item.get("sopr")
        if d and sopr_val is not None:
            bg_data.setdefault(d, {})["sopr"] = float(sopr_val)
    print(f"  📊 SOPR: {len([i for i in sopr_hist if i.get('sopr')])} записей")

    # Exchange Netflow
    netflow_hist = await bgeometrics_get("exchange-netflow-btc")
    await asyncio.sleep(2)
    for item in netflow_hist:
        d = item.get("d")
        nf_val = item.get("exchangeNetflowBtc") or item.get("exchange_netflow_btc") or item.get("value")
        if d and nf_val is not None:
            bg_data.setdefault(d, {})["netflow"] = float(nf_val)
    print(f"  🔄 Netflow: {len([i for i in netflow_hist if i.get('exchangeNetflowBtc') or i.get('value')])} записей")

    # Exchange Reserve
    reserve_hist = await bgeometrics_get("exchange-reserve-btc")
    await asyncio.sleep(2)
    for item in reserve_hist:
        d = item.get("d")
        r_val = item.get("exchangeReserveBtc") or item.get("exchange_reserve_btc") or item.get("value")
        if d and r_val is not None:
            bg_data.setdefault(d, {})["reserve"] = float(r_val)
    print(f"  🏦 Reserve: {len([i for i in reserve_hist if i.get('exchangeReserveBtc') or i.get('value')])} записей")

    # Technical Indicators (RSI, MACD, SMA50, SMA200)
    tech_hist = await bgeometrics_get("technical-indicators")
    await asyncio.sleep(2)
    for item in tech_hist:
        d = item.get("d")
        if not d:
            continue
        bg_data.setdefault(d, {})
        rsi = item.get("rsi") or item.get("RSI")
        macd = item.get("macd") or item.get("MACD")
        sma50 = item.get("sma50") or item.get("SMA50")
        sma200 = item.get("sma200") or item.get("SMA200")
        if rsi is not None:
            bg_data[d]["rsi"] = float(rsi)
        if macd is not None:
            bg_data[d]["macd"] = float(macd)
        if sma50 is not None:
            bg_data[d]["sma50"] = float(sma50)
        if sma200 is not None:
            bg_data[d]["sma200"] = float(sma200)
    print(f"  📈 Tech Indicators: {len([i for i in tech_hist if i.get('rsi') or i.get('RSI')])} записей")

    print(f"  ✅ BGeometrics: данные за {len(bg_data)} дней")
    return bg_data


def find_by_date(history, target_date):
    """Ищет запись в исторических данных по дате"""
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


async def call_llm(prompt: str) -> dict:
    """Вызов Claude API с temperature=0"""
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


async def run_backtest():
    print("=" * 60)
    print("🔬 ZENDER BACKTEST v5 — Trend Priority + OI/FR + On-chain")
    print("=" * 60)
    print()

    prices = await fetch_price_history()
    if len(prices) < 3:
        print("❌ Недостаточно ценовых данных!")
        return

    await asyncio.sleep(1)
    liq_hist = await fetch_liquidation_history()
    await asyncio.sleep(1)
    ls_hist = await fetch_ls_history()
    await asyncio.sleep(1)
    oi_hist = await fetch_oi_history()
    await asyncio.sleep(1)
    fr_hist = await fetch_funding_rate_history()
    await asyncio.sleep(1)
    fg_hist = await fetch_fear_greed_history()
    await asyncio.sleep(1)

    # BGeometrics история (SOPR, Netflow, RSI, MACD, SMA)
    bg_hist = await fetch_bgeometrics_history()

    print()
    print("=" * 60)
    print("🤖 Прогоняем LLM по каждому дню...")
    print("=" * 60)
    print()

    results = []

    for i in range(len(prices) - 1):
        day = prices[i]
        next_day = prices[i + 1]
        date = day["date"]
        price = day["price"]
        next_price = next_day["price"]
        change_pct = ((next_price - price) / price) * 100

        if i > 0:
            prev_price = prices[i - 1]["price"]
            day_change = ((price - prev_price) / prev_price) * 100
        else:
            day_change = 0

        # Coinglass данные
        liq_long = None
        liq_short = None
        long_pct = None
        short_pct = None
        fg_val = None
        fg_label = None
        oi_val = None
        oi_change_pct = None
        fr_val = None

        liq_item = find_by_date(liq_hist, date)
        if liq_item:
            liq_long = liq_item.get("longVolUsd") or liq_item.get("longLiquidationUsd") or liq_item.get("long_liquidation_usd")
            liq_short = liq_item.get("shortVolUsd") or liq_item.get("shortLiquidationUsd") or liq_item.get("short_liquidation_usd")

        ls_item = find_by_date(ls_hist, date)
        if ls_item:
            long_pct = ls_item.get("longRatio") or ls_item.get("longAccount") or ls_item.get("buyRatio") or ls_item.get("buy_ratio")
            short_pct = ls_item.get("shortRatio") or ls_item.get("shortAccount") or ls_item.get("sellRatio") or ls_item.get("sell_ratio")

        # OI history
        oi_item = find_by_date(oi_hist, date)
        if oi_item:
            oi_val = oi_item.get("c") or oi_item.get("close") or oi_item.get("openInterest")
            # OI change: сравниваем с предыдущим днём
            if i > 0:
                prev_date = prices[i-1]["date"]
                prev_oi_item = find_by_date(oi_hist, prev_date)
                if prev_oi_item:
                    prev_oi = prev_oi_item.get("c") or prev_oi_item.get("close") or prev_oi_item.get("openInterest")
                    if prev_oi and oi_val:
                        try:
                            oi_change_pct = ((float(oi_val) - float(prev_oi)) / float(prev_oi)) * 100
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass

        # Funding Rate history
        fr_item = find_by_date(fr_hist, date)
        if fr_item:
            fr_val = fr_item.get("c") or fr_item.get("close") or fr_item.get("fundingRate")
            if fr_val is not None:
                try:
                    fr_val = float(fr_val) * 100  # в проценты
                except (ValueError, TypeError):
                    fr_val = None

        if fg_hist:
            for fg_item in fg_hist:
                try:
                    fg_date = datetime.fromtimestamp(int(fg_item.get("timestamp", 0))).strftime("%Y-%m-%d")
                    if fg_date == date:
                        fg_val = fg_item.get("value")
                        fg_label = fg_item.get("value_classification")
                        break
                except (ValueError, TypeError):
                    pass

        # BGeometrics данные по дате
        bg = bg_hist.get(date, {})
        sopr = bg.get("sopr")
        netflow = bg.get("netflow")
        reserve = bg.get("reserve")
        rsi = bg.get("rsi")
        macd = bg.get("macd")
        sma50 = bg.get("sma50")
        sma200 = bg.get("sma200")

        # On-chain блок для промпта
        onchain_block = ""
        onchain_lines = []
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

        if onchain_lines:
            onchain_block = "\n".join(onchain_lines)

        prompt = f"""Ты — опытный крипто-аналитик. Проанализируй данные {SYMBOL} и дай РЕШИТЕЛЬНЫЙ анализ.

ДАННЫЕ {SYMBOL} на {date}:
- Цена: ${price:,.0f}, изменение 24ч: {fmt_pct(day_change)}
- Открытый интерес (OI): {fmt_usd(oi_val)} ({fmt_pct(oi_change_pct)} за день)
- Funding Rate: {fmt_pct(fr_val)}
- Покупатели/Продавцы (taker): {long_pct or '?'}% / {short_pct or '?'}%
- Ликвидации {SYMBOL}: лонги {fmt_usd(liq_long)}, шорты {fmt_usd(liq_short)}
- Fear & Greed: {fg_val or '?'} ({fg_label or '?'}){onchain_block}

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

ВАЖНО: НЕ выбирай "выжидать" по умолчанию! Если есть 2+ совпадающих сигнала — давай направление.
Но НИКОГДА не рекомендуй "покупать" если MACD отрицательный И death cross — тренд важнее!

ПРАВИЛА ДЛЯ ЗОН (СТРОГО!):
- Зона покупки ДОЛЖНА ВКЛЮЧАТЬ текущую цену! Формат: от (цена - 1-2%) до (цена + 0.5%)
- Зона продажи = от (цена + 2-3%) до (цена + 4-5%)
- НИКОГДА не давай зону покупки которая ПОЛНОСТЬЮ ниже текущей цены

ОТВЕТЬ СТРОГО В ФОРМАТЕ (3 строки, без лишнего):
АНАЛИЗ: [2-3 предложения простым языком]
РЕКОМЕНДАЦИЯ: [одно слово: покупать / продавать / выжидать]
ЗОНЫ: покупка $XXX,XXX–$XXX,XXX | продажа $XXX,XXX–$XXX,XXX"""

        # Лог дня
        extras = []
        if oi_change_pct is not None:
            extras.append(f"OI:{oi_change_pct:+.1f}%")
        if fr_val is not None:
            extras.append(f"FR:{fr_val:+.3f}%")
        if sopr:
            extras.append(f"SOPR:{sopr:.3f}")
        if rsi is not None:
            extras.append(f"RSI:{rsi:.0f}")
        if macd is not None:
            extras.append(f"MACD:{macd:.0f}")
        if netflow is not None:
            extras.append(f"NF:{netflow:+,.0f}")
        extra_str = f" | {' '.join(extras)}" if extras else ""

        print(f"📅 {date} | Цена: ${price:,.0f}{extra_str}")
        llm_result = await call_llm(prompt)
        rec = llm_result.get("recommendation", "—")
        analysis = llm_result.get("analysis", "—")

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
            "date": date,
            "price": price,
            "next_price": next_price,
            "change": change_pct,
            "recommendation": rec,
            "correct": correct,
            "analysis": analysis,
            "sopr": sopr,
            "rsi": rsi,
            "macd": macd,
            "netflow": netflow,
        })

        print(f"  🤖 Рекомендация: {rec}")
        print(f"  📊 Через 24ч: ${next_price:,.0f} ({direction})")
        print(f"  {icon} {'Верно' if correct else ('Неверно' if correct is False else 'Нейтрально')}")
        print()

        await asyncio.sleep(3)

    # ── Итоги ──
    print("=" * 60)
    print("📊 ИТОГИ БЭКТЕСТА v5 (Trend Priority + OI/FR + On-chain)")
    print("=" * 60)

    total = len(results)
    if total == 0:
        print("Нет результатов!")
        return

    correct_count = sum(1 for r in results if r["correct"] is True)
    wrong_count = sum(1 for r in results if r["correct"] is False)
    neutral_count = sum(1 for r in results if r["correct"] is None)

    buy_recs = [r for r in results if "покупать" in r["recommendation"]]
    sell_recs = [r for r in results if "продавать" in r["recommendation"]]
    wait_recs = [r for r in results if "выжидать" in r["recommendation"]]

    print(f"\nВсего дней: {total}")
    print(f"✅ Верных: {correct_count} ({correct_count/total*100:.0f}%)")
    print(f"❌ Неверных: {wrong_count} ({wrong_count/total*100:.0f}%)")
    print(f"⏸️  Нейтральных: {neutral_count}")
    print(f"\n📈 Покупать: {len(buy_recs)} раз")
    print(f"📉 Продавать: {len(sell_recs)} раз")
    print(f"⏸️  Выжидать: {len(wait_recs)} раз")

    if buy_recs:
        avg_buy = sum(r["change"] for r in buy_recs) / len(buy_recs)
        correct_buy = sum(1 for r in buy_recs if r["correct"])
        print(f"\n'Покупать' — среднее изменение: {avg_buy:+.2f}%, верных: {correct_buy}/{len(buy_recs)}")
    if sell_recs:
        avg_sell = sum(r["change"] for r in sell_recs) / len(sell_recs)
        correct_sell = sum(1 for r in sell_recs if r["correct"])
        print(f"'Продавать' — среднее изменение: {avg_sell:+.2f}%, верных: {correct_sell}/{len(sell_recs)}")
    if wait_recs:
        avg_wait = sum(r["change"] for r in wait_recs) / len(wait_recs)
        correct_wait = sum(1 for r in wait_recs if r["correct"])
        print(f"'Выжидать' — среднее изменение: {avg_wait:+.2f}%, верных: {correct_wait}/{len(wait_recs)}")

    # PnL симуляция
    pnl = 0.0
    for r in results:
        if "покупать" in r["recommendation"]:
            pnl += r["change"]
        elif "продавать" in r["recommendation"]:
            pnl -= r["change"]
    print(f"\n💰 Суммарный PnL (если следовать рекомендациям): {pnl:+.2f}%")

    # Доп. статистика по on-chain
    days_with_bg = sum(1 for r in results if r.get("sopr") or r.get("rsi"))
    print(f"\n📊 Дней с BGeometrics данными: {days_with_bg}/{total}")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    if not COINGLASS_API_KEY:
        print("❌ Нет COINGLASS_API_KEY!")
    elif not ANTHROPIC_KEY:
        print("❌ Нет ANTHROPIC_KEY!")
    else:
        asyncio.run(run_backtest())
