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
from datetime import datetime, timezone

from config import COINGLASS_API_KEY

# Anthropic API (для LLM-анализа)
try:
    from config import ANTHROPIC_KEY
    HAS_ANTHROPIC = bool(ANTHROPIC_KEY)
except (ImportError, AttributeError):
    ANTHROPIC_KEY = None
    HAS_ANTHROPIC = False

# On-chain: бесплатные API (blockchain.info + BGeometrics), без ключа

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
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"}
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


async def fetch_onchain_data(symbol: str) -> dict:
    """
    On-chain метрики для BTC (бесплатные API, без ключа).
    Blockchain.info: активные адреса, транзакции.
    BGeometrics: SOPR, баланс на биржах.
    Для не-BTC монет — пустой dict.
    """
    if symbol != "BTC":
        return {}

    result = {}

    try:
        active_data, tx_data = await asyncio.gather(
            blockchain_chart_get("n-unique-addresses"),
            blockchain_chart_get("n-transactions"),
            return_exceptions=True,
        )
    except Exception as e:
        log.warning(f"On-chain gather error: {e}")
        return {}

    # Обработка исключений из gather
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

    # Exchange Balance — берём из Coinglass (уже платим STARTUP $95/мес)
    try:
        exch_data = await cg_get("/api/exchange/balance/list", {"symbol": "BTC"})
        if exch_data and isinstance(exch_data, list):
            # Ищем агрегат "All" или суммируем по биржам
            total_balance = 0.0
            for item in exch_data:
                try:
                    bal = float(item.get("balance") or item.get("total") or 0)
                    total_balance += bal
                except (ValueError, TypeError):
                    pass
            if total_balance > 0:
                result["exchange_balance"] = total_balance
                # Считаем изменение если есть поле change
                for item in exch_data:
                    change_pct = item.get("change_percent_1d") or item.get("balance_change_percent_1d")
                    if change_pct is not None:
                        try:
                            result["exchange_balance_change"] = float(change_pct)
                        except (ValueError, TypeError):
                            pass
                        break
                log.info(f"  🏦 Exchange Balance BTC: {total_balance:,.0f} BTC")
        elif exch_data and isinstance(exch_data, dict):
            # Может вернуть dict с общими данными
            bal = exch_data.get("balance") or exch_data.get("total")
            if bal:
                result["exchange_balance"] = float(bal)
                change_pct = exch_data.get("change_percent_1d") or exch_data.get("balance_change_percent_1d")
                if change_pct is not None:
                    result["exchange_balance_change"] = float(change_pct)
                log.info(f"  🏦 Exchange Balance BTC: {float(bal):,.0f} BTC")
    except Exception as e:
        log.warning(f"Exchange Balance error: {e}")

    # SOPR — BGeometrics API v1 (bitcoin-data.com/v1/sopr)
    # Бесплатно, без ключа, лимит 8 req/hour
    try:
        sopr_data = await bgeometrics_get("sopr", last=True)
        if sopr_data and isinstance(sopr_data, dict):
            # Ответ: {"d": "2026-03-08", "unixTs": ..., "sopr": 1.02}
            sopr_val = sopr_data.get("sopr")
            if sopr_val is not None:
                result["sopr"] = float(sopr_val)
                log.info(f"  📊 SOPR BTC: {sopr_val}")
        elif sopr_data and isinstance(sopr_data, list) and len(sopr_data) > 0:
            # Если вернул массив — берём последний элемент
            last_item = sopr_data[-1]
            sopr_val = last_item.get("sopr") if isinstance(last_item, dict) else None
            if sopr_val is not None:
                result["sopr"] = float(sopr_val)
                log.info(f"  📊 SOPR BTC: {sopr_val}")
    except Exception as e:
        log.warning(f"SOPR error: {e}")

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
    exchange_bal = coin_data.get("exchange_balance")
    exchange_bal_change = coin_data.get("exchange_balance_change")
    sopr = coin_data.get("sopr")
    new_addr = coin_data.get("new_addresses")

    # Блок on-chain данных для промпта
    onchain_block = ""
    if any([active_addr, exchange_bal, sopr, new_addr]):
        onchain_lines = [f"\nON-CHAIN ДАННЫЕ (BTC блокчейн):"]
        if active_addr:
            onchain_lines.append(f"- Активные адреса: {active_addr:,} ({safe_pct(active_addr_change)} за день)")
        if new_addr:
            onchain_lines.append(f"- Новые адреса: {new_addr:,}")
        if exchange_bal:
            direction = "выводят с бирж (бычий)" if exchange_bal_change and exchange_bal_change < 0 else "заводят на биржи (медвежий)" if exchange_bal_change and exchange_bal_change > 0 else "стабильно"
            onchain_lines.append(f"- Баланс на биржах: {exchange_bal:,.0f} ({safe_pct(exchange_bal_change)} за день) — {direction}")
        if sopr:
            sopr_hint = "продают в прибыль" if sopr > 1 else "продают в убыток (капитуляция)" if sopr < 1 else "безубыток"
            onchain_lines.append(f"- SOPR: {sopr} — {sopr_hint}")
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
- Баланс на биржах падает (выводят на холодные кошельки — бычий сигнал)
- SOPR < 1 (капитуляция — дно может быть близко)

ПРОДАВАТЬ если 2+ условий совпадают:
- Продавцы > 52% (медведи доминируют)
- OI падает < -0.5% за 1ч (деньги уходят)
- Funding Rate > +0.05% (лонги переплачивают — рынок перегрет)
- Fear & Greed > 75 (сильная жадность — пора фиксировать прибыль)
- Ликвидации лонгов > ликвидаций шортов в 2+ раза (лонги ликвидируют)
- Баланс на биржах растёт (заводят для продажи — медвежий сигнал)
- SOPR > 1.05 (массово фиксируют прибыль — возможна коррекция)

ВЫЖИДАТЬ только если сигналы явно противоречат друг другу и нет перевеса ни в одну сторону.

ВАЖНО: НЕ выбирай "выжидать" по умолчанию! Если есть 2+ совпадающих сигнала в одну сторону — давай направление.

ПРАВИЛА ДЛЯ ЗОН:
- Зона покупки = текущая цена ± 2-3% ВНИЗ (куда поставить лимитку на покупку)
- Зона продажи = текущая цена ± 3-5% ВВЕРХ (куда поставить тейк-профит)
- Зоны ВСЕГДА привязаны к ТЕКУЩЕЙ цене! Не давай зоны далеко от цены.
- Если рекомендация "покупать" — зона покупки должна быть РЯДОМ с текущей ценой (не ниже 5%)
- Если рекомендация "продавать" — зона продажи должна быть РЯДОМ с текущей ценой (не выше 5%)

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
                    if val:
                        result["llm_text"] = val
                elif upper.startswith("РЕКОМЕНДАЦИЯ:") or upper.startswith("РЕКОМЕНДАЦИЯ :"):
                    val = clean.split(":", 1)[1].strip().lower() if ":" in clean else ""
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

    # 7. On-chain: баланс на биржах (Glassnode)
    exchange_bal_change = coin_data.get("exchange_balance_change")
    if exchange_bal_change is not None:
        if exchange_bal_change < -0.5:
            bull += 1  # выводят с бирж → холдят → бычий
        elif exchange_bal_change > 0.5:
            bear += 1  # заводят на биржи → готовятся продавать

    # 8. On-chain: SOPR
    sopr = coin_data.get("sopr")
    if sopr is not None:
        if sopr < 0.98:
            bull += 1  # капитуляция → дно близко
        elif sopr > 1.05:
            bear += 1  # массово фиксируют прибыль

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

            # Объединяем все данные
            coin_data = {
                **price_data,
                **oi_data,
                **fr_data,
                **ls_data,
                **liq_data,
                **fg_data,
                **gn_data,
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
                "exchange_balance": fmt_usd(coin_data.get("exchange_balance")) if coin_data.get("exchange_balance") else "—",
                "exchange_balance_change": fmt_pct(coin_data.get("exchange_balance_change")),
                "sopr": str(round(coin_data["sopr"], 4)) if coin_data.get("sopr") else "—",
                "new_addresses": str(coin_data.get("new_addresses", "—")),
                "exchange_flow": "—",
                "whale_buy1h": "—",
                "whale_buy24h": "—",
                "whale_sell24h": "—",
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
