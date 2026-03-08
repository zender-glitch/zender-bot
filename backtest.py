"""
ZENDER COMMANDER TERMINAL — Backtest LLM Recommendations v2
Прогоняет LLM-анализ по историческим данным за 14 дней.
Обновлённый промпт (решительный, не "выжидать" по умолчанию).
Работает даже без OI/FR history (использует доступные данные).
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

            # Fallback: ищем ключевые слова если парсер не нашёл
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
    print("🔬 ZENDER BACKTEST v2 — улучшенный промпт")
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
    fg_hist = await fetch_fear_greed_history()

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

        # Изменение 24ч для текущего дня
        if i > 0:
            prev_price = prices[i - 1]["price"]
            day_change = ((price - prev_price) / prev_price) * 100
        else:
            day_change = 0

        # Подбираем данные
        liq_long = None
        liq_short = None
        long_pct = None
        short_pct = None
        fg_val = None
        fg_label = None

        liq_item = find_by_date(liq_hist, date)
        if liq_item:
            liq_long = liq_item.get("longVolUsd") or liq_item.get("longLiquidationUsd") or liq_item.get("long_liquidation_usd")
            liq_short = liq_item.get("shortVolUsd") or liq_item.get("shortLiquidationUsd") or liq_item.get("short_liquidation_usd")

        ls_item = find_by_date(ls_hist, date)
        if ls_item:
            long_pct = ls_item.get("longRatio") or ls_item.get("longAccount") or ls_item.get("buyRatio") or ls_item.get("buy_ratio")
            short_pct = ls_item.get("shortRatio") or ls_item.get("shortAccount") or ls_item.get("sellRatio") or ls_item.get("sell_ratio")

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

        prompt = f"""Ты — опытный крипто-аналитик. Проанализируй данные {SYMBOL} и дай РЕШИТЕЛЬНЫЙ анализ.

ДАННЫЕ {SYMBOL} на {date}:
- Цена: ${price:,.0f}, изменение 24ч: {fmt_pct(day_change)}
- Открытый интерес (OI): нет данных
- Funding Rate: нет данных
- Покупатели/Продавцы (taker): {long_pct or '?'}% / {short_pct or '?'}%
- Ликвидации {SYMBOL}: лонги {fmt_usd(liq_long)}, шорты {fmt_usd(liq_short)}
- Fear & Greed: {fg_val or '?'} ({fg_label or '?'})

ПРАВИЛА ОЦЕНКИ (следуй им строго):

ПОКУПАТЬ если 2+ условий совпадают:
- Покупатели > 52% (быки доминируют)
- OI растёт > +0.5% за 1ч (новые деньги заходят)
- Funding Rate отрицательный (шорты переплачивают — разворот вверх вероятен)
- Fear & Greed < 30 (сильный страх — возможность покупки на панике)
- Ликвидации шортов > ликвидаций лонгов в 2+ раза (шортов выдавливают)

ПРОДАВАТЬ если 2+ условий совпадают:
- Продавцы > 52% (медведи доминируют)
- OI падает < -0.5% за 1ч (деньги уходят)
- Funding Rate > +0.05% (лонги переплачивают — рынок перегрет)
- Fear & Greed > 75 (сильная жадность — пора фиксировать прибыль)
- Ликвидации лонгов > ликвидаций шортов в 2+ раза (лонги ликвидируют)

ВЫЖИДАТЬ только если сигналы явно противоречат друг другу и нет перевеса ни в одну сторону.

ВАЖНО: НЕ выбирай "выжидать" по умолчанию! Если есть 2+ совпадающих сигнала — давай направление.

ОТВЕТЬ СТРОГО В ФОРМАТЕ (3 строки, без лишнего):
АНАЛИЗ: [2-3 предложения простым языком]
РЕКОМЕНДАЦИЯ: [одно слово: покупать / продавать / выжидать]
ЗОНЫ: покупка $XXX,XXX–$XXX,XXX | продажа $XXX,XXX–$XXX,XXX"""

        print(f"📅 {date} | Цена: ${price:,.0f}")
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
        })

        print(f"  🤖 Рекомендация: {rec}")
        print(f"  📊 Через 24ч: ${next_price:,.0f} ({direction})")
        print(f"  {icon} {'Верно' if correct else ('Неверно' if correct is False else 'Нейтрально')}")
        print()

        await asyncio.sleep(3)  # 3 сек между запросами — не упираться в rate limit

    # ── Итоги ──
    print("=" * 60)
    print("📊 ИТОГИ БЭКТЕСТА v2")
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

    print(f"\n{'='*60}")


if __name__ == "__main__":
    if not COINGLASS_API_KEY:
        print("❌ Нет COINGLASS_API_KEY!")
    elif not ANTHROPIC_KEY:
        print("❌ Нет ANTHROPIC_KEY!")
    else:
        asyncio.run(run_backtest())
