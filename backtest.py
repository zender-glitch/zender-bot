"""
ZENDER COMMANDER TERMINAL — Backtest LLM Recommendations
Прогоняет LLM-анализ по историческим данным за 14 дней и сравнивает с реальностью.
"""

import asyncio
import httpx
import os
import json
import time
from datetime import datetime, timedelta

# ── Ключи ──
COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

CG_BASE = "https://open-api-v4.coinglass.com"
CG_HEADERS = {"CG-API-KEY": COINGLASS_API_KEY}

SYMBOL = "BTC"  # Тестируем на BTC
DAYS = 14


def fmt_usd(val):
    if val is None:
        return "нет данных"
    v = float(val)
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
    except:
        return "нет данных"


async def cg_get(path, params=None):
    """Запрос к Coinglass API с логированием ответа при ошибке"""
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


async def fetch_oi_history():
    """Исторический OI — camelCase эндпоинты Coinglass v4"""
    print("📊 Загружаем OI history...")
    # Основной эндпоинт (camelCase!)
    data = await cg_get("/api/futures/openInterest/ohlc-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей OI")
        return data
    # Альтернативный — aggregated
    data = await cg_get("/api/futures/openInterest/ohlc-aggregated-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей OI (aggregated)")
        return data
    print("  ⚠️ OI history недоступен")
    return []


async def fetch_fr_history():
    """Исторический Funding Rate — camelCase"""
    print("💰 Загружаем FR history...")
    data = await cg_get("/api/futures/fundingRate/ohlc-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей FR")
        return data
    # Альтернативный — OI-weighted
    data = await cg_get("/api/futures/fundingRate/oi-weight-ohlc-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей FR (oi-weight)")
        return data
    print("  ⚠️ FR history недоступен")
    return []


async def fetch_liquidation_history():
    """Исторические ликвидации — camelCase"""
    print("💥 Загружаем ликвидации history...")
    # aggregated-history (не ohlc!)
    data = await cg_get("/api/futures/liquidation/aggregated-history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей ликвидаций")
        return data
    # Пробуем с exchange
    data = await cg_get("/api/futures/liquidation/history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей ликвидаций (history)")
        return data
    print("  ⚠️ Liquidation history недоступен")
    return []


async def fetch_ls_history():
    """Исторический Long/Short ratio — camelCase"""
    print("📐 Загружаем L/S history...")
    # globalLongShortAccountRatio (camelCase!)
    data = await cg_get("/api/futures/globalLongShortAccountRatio/history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей L/S")
        return data
    # Пробуем top account ratio
    data = await cg_get("/api/futures/topLongShortAccountRatio/history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей L/S (top account)")
        return data
    # Пробуем taker buy/sell history
    data = await cg_get("/api/futures/aggregatedTakerBuySellVolume/history", {
        "symbol": SYMBOL,
        "interval": "1d",
        "limit": DAYS + 1,
    })
    if data and isinstance(data, list):
        print(f"  ✅ Получено {len(data)} записей taker B/S")
        return data
    print("  ⚠️ L/S history недоступен")
    return []


async def fetch_fear_greed_history():
    """Fear & Greed за 14 дней"""
    print("😱 Загружаем Fear & Greed history...")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"https://api.alternative.me/fng/?limit={DAYS + 1}"
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            print(f"  ✅ Получено {len(data)} дней F&G")
            return data
    except Exception as e:
        print(f"  ❌ F&G: {e}")
        return []


async def call_llm(prompt: str) -> dict:
    """Вызов Claude API"""
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
                }
            )
            if resp.status_code != 200:
                print(f"  ⚠️ LLM {resp.status_code}: {resp.text[:200]}")
                return {}

            body = resp.json()
            text = body["content"][0]["text"].strip()

            result = {}
            for line in text.split("\n"):
                line = line.strip()
                if line.startswith("АНАЛИЗ:"):
                    result["analysis"] = line.replace("АНАЛИЗ:", "").strip()
                elif line.startswith("РЕКОМЕНДАЦИЯ:"):
                    result["recommendation"] = line.replace("РЕКОМЕНДАЦИЯ:", "").strip().lower()
                elif line.startswith("ЗОНЫ:"):
                    result["zones"] = line.replace("ЗОНЫ:", "").strip()
            return result
    except Exception as e:
        print(f"  ❌ LLM: {e}")
        return {}


async def run_backtest():
    print("=" * 60)
    print("🔬 ZENDER BACKTEST — LLM рекомендации за 14 дней")
    print("=" * 60)
    print()

    # 1. Собираем все исторические данные
    prices = await fetch_price_history()
    if len(prices) < 3:
        print("❌ Недостаточно ценовых данных!")
        return

    oi_hist = await fetch_oi_history()
    await asyncio.sleep(1)
    fr_hist = await fetch_fr_history()
    await asyncio.sleep(1)
    liq_hist = await fetch_liquidation_history()
    await asyncio.sleep(1)
    ls_hist = await fetch_ls_history()
    await asyncio.sleep(1)
    fg_hist = await fetch_fear_greed_history()

    print()
    print("=" * 60)
    print("🤖 Прогоняем LLM по каждому дню...")
    print("=" * 60)
    print()

    results = []

    # Для каждого дня (кроме последнего — нужен для проверки)
    for i in range(len(prices) - 1):
        day = prices[i]
        next_day = prices[i + 1]
        date = day["date"]
        price = day["price"]
        next_price = next_day["price"]
        change_24h = ((next_price - price) / price) * 100

        # Подбираем данные за этот день
        oi_val = None
        oi_change = None
        fr_val = None
        liq_long = None
        liq_short = None
        long_pct = None
        short_pct = None
        fg_val = None
        fg_label = None

        # OI
        if oi_hist:
            for item in oi_hist:
                ts = item.get("t") or item.get("time") or item.get("timestamp") or item.get("createTime")
                if ts:
                    item_date = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts).strftime("%Y-%m-%d")
                    if item_date == date:
                        oi_val = item.get("c") or item.get("close") or item.get("openInterest") or item.get("open_interest")
                        break

        # FR
        if fr_hist:
            for item in fr_hist:
                ts = item.get("t") or item.get("time") or item.get("timestamp") or item.get("createTime")
                if ts:
                    item_date = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts).strftime("%Y-%m-%d")
                    if item_date == date:
                        fr_val = item.get("c") or item.get("close") or item.get("fundingRate") or item.get("funding_rate")
                        if fr_val:
                            fr_val = float(fr_val) * 100
                        break

        # Liquidations
        if liq_hist:
            for item in liq_hist:
                ts = item.get("t") or item.get("time") or item.get("timestamp") or item.get("createTime")
                if ts:
                    item_date = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts).strftime("%Y-%m-%d")
                    if item_date == date:
                        liq_long = item.get("longVolUsd") or item.get("long_volUsd") or item.get("longLiquidationUsd") or item.get("long_liquidation_usd")
                        liq_short = item.get("shortVolUsd") or item.get("short_volUsd") or item.get("shortLiquidationUsd") or item.get("short_liquidation_usd")
                        break

        # L/S
        if ls_hist:
            for item in ls_hist:
                ts = item.get("t") or item.get("time") or item.get("timestamp") or item.get("createTime")
                if ts:
                    item_date = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts).strftime("%Y-%m-%d")
                    if item_date == date:
                        long_pct = item.get("longRatio") or item.get("long_ratio") or item.get("buyRatio") or item.get("buy_ratio") or item.get("longAccount")
                        short_pct = item.get("shortRatio") or item.get("short_ratio") or item.get("sellRatio") or item.get("sell_ratio") or item.get("shortAccount")
                        break

        # Fear & Greed
        if fg_hist:
            for item in fg_hist:
                item_date = datetime.fromtimestamp(int(item.get("timestamp", 0))).strftime("%Y-%m-%d")
                if item_date == date:
                    fg_val = item.get("value")
                    fg_label = item.get("value_classification")
                    break

        # Формируем промпт
        prompt = f"""Ты — крипто-аналитик. Проанализируй данные {SYMBOL} и дай краткий анализ на русском языке.

ДАННЫЕ {SYMBOL} на {date}:
- Цена: ${price:,.0f}
- Открытый интерес: {fmt_usd(oi_val) if oi_val else 'нет данных'}
- Funding Rate: {fmt_pct(fr_val) if fr_val else 'нет данных'}
- Лонг/Шорт: {long_pct or '?'}% / {short_pct or '?'}%
- Ликвидации: лонги {fmt_usd(liq_long)}, шорты {fmt_usd(liq_short)}
- Fear & Greed: {fg_val or '?'} ({fg_label or '?'})

ОТВЕТЬ СТРОГО В ФОРМАТЕ (3 строки, без лишнего):
АНАЛИЗ: [2-3 предложения простым языком]
РЕКОМЕНДАЦИЯ: [одно слово: покупать / продавать / выжидать]
ЗОНЫ: покупка $XXX,XXX–$XXX,XXX | продажа $XXX,XXX–$XXX,XXX"""

        print(f"📅 {date} | Цена: ${price:,.0f}")
        llm_result = await call_llm(prompt)
        rec = llm_result.get("recommendation", "—")
        analysis = llm_result.get("analysis", "—")

        # Оценка: правильно ли
        correct = None
        if "покупать" in rec:
            correct = change_24h > 0  # Цена должна вырасти
        elif "продавать" in rec:
            correct = change_24h < 0  # Цена должна упасть
        elif "выжидать" in rec:
            correct = abs(change_24h) < 2  # Цена не сильно изменилась (±2%)

        icon = "✅" if correct else ("❌" if correct is False else "⏸️")
        direction = f"{'🔺' if change_24h > 0 else '🔻'} {change_24h:+.2f}%"

        results.append({
            "date": date,
            "price": price,
            "next_price": next_price,
            "change": change_24h,
            "recommendation": rec,
            "correct": correct,
            "analysis": analysis,
        })

        print(f"  🤖 Рекомендация: {rec}")
        print(f"  📊 Через 24ч: ${next_price:,.0f} ({direction})")
        print(f"  {icon} {'Верно' if correct else ('Неверно' if correct is False else 'Нейтрально')}")
        print()

        await asyncio.sleep(1)  # Не спамить API

    # Итоги
    print("=" * 60)
    print("📊 ИТОГИ БЭКТЕСТА")
    print("=" * 60)

    total = len(results)
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
    print(f"\nПокупать: {len(buy_recs)} раз")
    print(f"Продавать: {len(sell_recs)} раз")
    print(f"Выжидать: {len(wait_recs)} раз")

    if buy_recs:
        avg_buy = sum(r["change"] for r in buy_recs) / len(buy_recs)
        print(f"\nСреднее изменение после 'покупать': {avg_buy:+.2f}%")
    if sell_recs:
        avg_sell = sum(r["change"] for r in sell_recs) / len(sell_recs)
        print(f"Среднее изменение после 'продавать': {avg_sell:+.2f}%")
    if wait_recs:
        avg_wait = sum(r["change"] for r in wait_recs) / len(wait_recs)
        print(f"Среднее изменение после 'выжидать': {avg_wait:+.2f}%")

    # Сохраняем результаты
    print(f"\n{'='*60}")
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    if not COINGLASS_API_KEY:
        print("❌ Нет COINGLASS_API_KEY!")
    elif not ANTHROPIC_KEY:
        print("❌ Нет ANTHROPIC_KEY!")
    else:
        asyncio.run(run_backtest())
