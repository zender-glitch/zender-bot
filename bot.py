"""
ZENDER TERMINAL — Telegram Bot
Этап 1-2-5-6: бот + коллектор + LLM + i18n + top-20 + навигация.
"""

import asyncio
import logging
import html as html_lib
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    LinkPreviewOptions
)
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode

from config import BOT_TOKEN
from database import db

NO_PREVIEW = LinkPreviewOptions(is_disabled=True)
from collector import collector_loop

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Bot и Dispatcher ──────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# ── Монеты: ТОП-20 по капитализации ──────────────────────────────────────────
COINS = [
    "BTC", "ETH", "BNB", "SOL", "XRP",
    "ADA", "DOGE", "AVAX", "DOT", "LINK",
    "POL", "TRX", "SHIB", "UNI", "LTC",
    "ATOM", "NEAR", "APT", "ARB", "OP",
    "RUNE",
]

# Монеты с опционными данными (Deribit)
COINS_WITH_OPTIONS = {"BTC", "ETH"}

# ══════════════════════════════════════════════════════════════════════════════
# ЛОКАЛИЗАЦИЯ (i18n) — RU / EN
# ══════════════════════════════════════════════════════════════════════════════

TEXTS = {
    "ru": {
        "welcome": """<b>⚡ ZENDER TERMINAL</b>

Агрегатор крипто-данных с 30+ сервисов + LLM-анализ.
Трейдер платит $14/мес вместо $200–800+ по отдельности.

<b>Тарифы:</b>
🆓 <b>Free</b> — 1 монета · LLM-анализ · обновление 15 мин
🟢 <b>Basic $14</b> — топ-20 монет · LLM-анализ · 5/15/60 мин
🟡 <b>Pro $29</b> — все метрики · дашборд · 3 темы · без LLM
🔴 <b>Pro+ $49</b> — алерты 1-2 мин · сканер 200 монет

Используй кнопки ниже 👇""",

        "help": """<b>⚡ ZENDER TERMINAL — Помощь</b>

<b>Команды:</b>
/start — главное меню
/summary — сводка по монетам
/settings — настройки
/status — статус подписки

<b>Как работает:</b>
• Сводка приходит автоматически по расписанию
• Нажми на монету — получи полный анализ
• Кнопка ◀ Назад — вернуться к сводке

<b>Данные обновляются из:</b>
Coinglass · Glassnode · Hyblock · CryptoQuant
Santiment · Deribit · Nansen · и ещё 20+ сервисов

⚡ t.me/ZenderTerminal_bot""",

        # Кнопки
        "btn_radar": "📡 Радар",
        "btn_scanner": "⚡ Сканер",
        "btn_danger": "🚨 Danger",
        "btn_settings": "⚙️ Настройки",
        "btn_subscription": "💳 Подписка",
        "btn_help": "❓ Помощь",
        "btn_refresh": "🔄 Обновить",
        "btn_back": "◀ Назад",
        "btn_back_radar": "◀ Назад к радару",
        "btn_language": "🌐 Язык: Русский",
        "btn_view_basic": "📋 Basic",
        "btn_view_basic_on": "📋 Basic ✓",
        "btn_view_pro": "📊 Pro",
        "btn_view_pro_on": "📊 Pro ✓",
        "pro_promo": """📊 <b>PRO-режим</b> — развёрнутый анализ

Что даёт Pro-вид:
• Полный AI-разбор (не 2-3 предложения, а детальный анализ)
• Опционы прямо в карточке
• Расширенный order flow
• On-chain детали
• Исторические сравнения

🔓 Подпишись на <b>Pro $29/мес</b> чтобы разблокировать""",

        # Настройки
        "settings_title": "⚙️ Настройки",
        "settings_plan": "Тариф",
        "settings_interval": "Обновление",
        "settings_every": "каждые {interval} мин",
        "settings_alerts": "Алерты",
        "settings_choose_interval": "Выбери интервал обновления:",
        "alerts_on": "🔔 Алерты: ВКЛ",
        "alerts_off": "🔕 Алерты: ВЫКЛ",
        "alerts_enabled": "🔔 Включены",
        "alerts_disabled": "🔕 Выключены",
        "alerts_on_short": "🔔 Алерты включены",
        "alerts_off_short": "🔕 Алерты выключены",
        "interval_set": "✅ Интервал: {interval} мин",
        "refreshed": "🔄 Обновлено!",

        # Подписка
        "sub_title": "💳 Выбери тариф",
        "sub_free": '🆓 <b>Free</b> — 1 монета, LLM-анализ, 15 мин',
        "sub_basic": '🟢 <b>Basic $14/мес</b> — топ-20, LLM-анализ, 5/15/60 мин',
        "sub_pro": '🟡 <b>Pro $29/мес</b> — все метрики, дашборд, 3 темы',
        "sub_pro_plus": '🔴 <b>Pro+ $49/мес</b> — алерты 1-2 мин, сканер 200 монет',
        "payment_soon": "💳 Оплата {name} {price} — скоро будет доступно!",

        # Статус
        "status_title": "📋 Твой статус",
        "status_plan": "Тариф",
        "status_coins": "Монет отслеживается",
        "status_interval": "Интервал обновления",
        "not_registered": "Ты ещё не зарегистрирован. Напиши /start",

        # Радар
        "radar_title": "📡 РАДАР РЫНКА",
        "market_mood": "Настроение рынка",
        "press_coin": "Нажми монету для анализа ⬇",

        # Анализ монеты
        "what_happening": "ЧТО ПРОИСХОДИТ",
        "trap": "ЛОВУШКА",
        "signal": "СИГНАЛ",
        "trend_up": "📈 Тренд: вверх",
        "trend_down": "📉 Тренд: вниз",
        "ls_bulls": "быки давят",
        "ls_bears": "медведи давят",
        "ls_balance": "баланс",
        "ls_label": "Лонг/Шорт",
        "funding_longs_pay": "лонги платят шортам",
        "funding_shorts_pay": "шорты платят лонгам",
        "funding_balance": "баланс",
        "funding_label": "Фандинг",
        "oi_rising": "растёт",
        "oi_falling": "падает",
        "oi_stable": "стабильно",
        "oi_label": "Открытый интерес",
        "rsi_overbought": "перекуплен",
        "rsi_heated": "разогрет",
        "rsi_oversold": "перепродан",
        "rsi_cooling": "охлаждается",
        "rsi_normal": "норма",
        "state_label": "Состояние",
        "mood_panic": "паника",
        "mood_fear": "страх",
        "mood_calm": "спокойствие",
        "mood_greed": "жадность",
        "mood_euphoria": "эйфория",
        "mood_label": "Настроение рынка",
        "whales_vs_crowd": "🐋 КИТЫ vs ТОЛПА",
        "whales_buying": "киты покупают (выводят с бирж)",
        "whales_selling": "киты продают (заводят на биржи)",
        "whales_waiting": "киты выжидают",
        "whale_alert_title": "🐋 Whale Alert (1ч)",
        "whale_txs": "{n} крупных переводов",
        "whale_to_exchange": "⬆️ На биржи: {usd}",
        "whale_from_exchange": "⬇️ С бирж: {usd}",
        "whale_bullish": "накопление — бычий",
        "whale_bearish": "готовятся продавать — медвежий",
        "whale_neutral": "нейтрально",
        "probability_title": "ВЕРОЯТНОСТЬ ДВИЖЕНИЯ",
        "prob_up": "Рост",
        "prob_down": "Падение",
        "crowd_overlong": "толпа перегружена лонгами ({pct}%)",
        "crowd_long": "толпа в лонгах ({pct}%)",
        "crowd_overshort": "толпа перегружена шортами ({pct}%)",
        "crowd_short": "толпа в шортах ({pct}%)",
        "crowd_balance": "толпа в балансе",
        "gas_high": "высокая нагрузка",
        "gas_medium": "умеренная",
        "gas_low": "низкая",
        "section_market": "── РЫНОК ──",
        "section_liquidity": "── ЛИКВИДНОСТЬ ──",
        "section_levels": "── УРОВНИ ──",
        "liq_1h": "Ликвидации (1ч)",
        "liq_shorts": "шорты",
        "liq_longs": "лонги",
        "shorts_stops": "стопы шортов",
        "longs_stops": "стопы лонгов",
        "entry_label": "🎯 Вход",
        "stop_label": "🛑 Стоп",
        "target_label": "✅ Цель",
        "buy_label": "🎯 Покупка",
        "sell_label": "✅ Продажа",

        # Навигация
        "page_label": "Стр. {page}/{total}",
        "next_page": "▶",
        "prev_page": "◀",

        # Опционы
        "btn_options": "📊 Опционы {coin}",
        "btn_back_coin": "◀ Назад к {coin}",
        "options_title": "⚡ ZENDER TERMINAL · {coin} OPTIONS",
        "options_pcr_label": "📊 Put/Call Ratio",
        "options_pcr_bullish": "Больше колов — рынок ждёт рост",
        "options_pcr_bearish": "Больше путов — рынок ждёт падение",
        "options_pcr_neutral": "Баланс колов и путов — неопределённость",
        "options_maxpain_label": "🎯 Max Pain",
        "options_maxpain_above": "Тянет вверх к Max Pain ({pct})",
        "options_maxpain_below": "Тянет вниз к Max Pain ({pct})",
        "options_maxpain_at": "Цена у Max Pain — магнит работает",
        "options_iv_label": "📈 IV (волатильность)",
        "options_iv_low": "тихий рынок",
        "options_iv_normal": "нормальная",
        "options_iv_high": "повышенная — жди движение",
        "options_iv_extreme": "шторм — резкое движение близко",
        "options_oi_title": "── OPEN INTEREST ──",
        "options_oi_bulls": "Быки доминируют в опционах",
        "options_oi_bears": "Медведи доминируют в опционах",
        "options_oi_balanced": "Баланс быков и медведей",
        "options_exp_title": "── ЭКСПИРАЦИИ ──",
        "options_exp_days": "через {days} д.",
        "options_exp_warning": "Возможна волатильность",
        "options_exp_max": "Крупная экспирация = магнит цены",
        "options_ai_title": "── 🤖 AI-АНАЛИЗ ──",
        "options_teaser": "── ОПЦИОНЫ {coin} ──",

        # FAQ
        "btn_faq": "📖 FAQ",
        "faq_title": "<b>📖 FAQ — Частые вопросы</b>\n\nВыбери тему:",
        "faq_btn_signals": "📊 Сигналы",
        "faq_btn_whales": "🐋 Киты",
        "faq_btn_options": "📈 Опционы",
        "faq_btn_orderflow": "📊 Order Flow",
        "faq_btn_liquidity": "🔥 Ликвидации",
        "faq_btn_structure": "🏗 Структура",
        "faq_btn_ai": "🤖 AI-анализ",
        "faq_btn_data": "📡 Данные",
        "faq_btn_plans": "💰 Тарифы",

        "faq_signals": """<b>📊 Что такое сигналы?</b>

Сигнал — это итог анализа 30+ показателей рынка через 3-слойный pipeline.

<b>Слой 1: Направление</b> — куда давят деньги (вверх / вниз / боковик). Учитывает 9 факторов: funding rate, long/short, CVD, киты, MACD и др.

<b>Слой 2: Состояние рынка</b> — фаза: short squeeze, паника, накопление, распределение, боковик.

<b>Слой 3: Качество сетапа</b> — насколько хороший момент для входа (сильный / средний / слабый / плохой).

<b>Сила сигнала:</b>
🟩⬜⬜⬜⬜ слабый — мало подтверждений
🟩🟩🟩⬜⬜ средний — большинство совпадает
🟩🟩🟩🟩🟩 сильный — всё в одну сторону

<b>📊 Market Pressure Gauge</b> — визуальная шкала давления рынка. Показывает кто сейчас сильнее — быки или медведи. Рассчитывается из всех бычьих/медвежьих факторов + качества сетапа.
🐂 ████████░░ 72% = быки доминируют
🐻 ██████████ 85% = сильное медвежье давление

<b>🤖 AI Score</b> — итоговая оценка рынка от 0 до 100:
• 70-100 🟢 STRONG BUY — всё указывает на рост
• 60-69 🟡 BUY — большинство факторов бычьи
• 45-59 ⚪ NEUTRAL — равновесие или неопределённость
• 30-44 🟡 SELL — большинство факторов медвежьи
• 0-29 🔴 STRONG SELL — всё указывает на падение
Показывает топ-факторы: за рост и за падение.

<b>⚠️ Ловушки</b>
«Шорты в ловушке» — шортисты переплатили (funding отрицательный), их ликвидируют. Возможен short squeeze — резкий рост из-за каскадных ликвидаций шортов.
«Лонги в ловушке» — лонгисты перегружены, возможен long squeeze — резкое падение.

<b>OI Structure (OI Delta Trap)</b>
Комбинация изменения цены и Open Interest — показывает настоящий ли это тренд:
• Цена ↑ + OI ↑ = новые лонги, тренд подтверждён
• Цена ↑ + OI ↓ = short squeeze, рост на ликвидациях
• Цена ↓ + OI ↑ = новые шорты, давление вниз
• Цена ↓ + OI ↓ = капитуляция лонгов, возможен отскок

<b>Liquidation Pressure</b>
Показывает кого сейчас выносят с рынка — лонгов или шортов, в процентах.

⚠️ Это не финансовый совет. Всегда проверяй сам.""",

        "faq_whales": """<b>🐋 Что такое киты?</b>

Киты — это крупные держатели крипты (кошельки с $500K+ в одной транзакции).

<b>Whale Alert</b> — отслеживает ВСЕ крупные переводы на 11 блокчейнах в реальном времени.

Что показываем:
⬆️ <b>На биржи</b> — кит заводит крипту на биржу. Скорее всего готовится продавать. Медвежий сигнал.
⬇️ <b>С бирж</b> — кит выводит крипту на холодный кошелёк. Накопление. Бычий сигнал.

<b>Exchange Netflow</b> (только BTC) — суммарный поток BTC на/с бирж. Если отрицательный — больше выводят, бычий знак.

Киты не всегда правы, но когда $50M уходит с биржи — это сигнал.""",

        "faq_options": """<b>📈 Что такое опционы?</b>

Опционы — это контракты на покупку (Call) или продажу (Put) крипты по фиксированной цене в будущем. Доступны для BTC и ETH.

<b>PCR (Put/Call Ratio)</b> — соотношение путов к колам.
• &lt; 0.7 — больше колов, рынок ждёт рост
• &gt; 1.0 — больше путов, рынок ждёт падение

<b>Max Pain</b> — цена, при которой БОЛЬШИНСТВО опционов истекают без прибыли. Цена тянется к ней перед экспирацией как магнит.

<b>IV (Implied Volatility)</b> — ожидаемая волатильность:
• &lt; 30% — тихо, спокойный рынок
• 30-60% — нормально
• &gt; 60% — ожидают резкое движение

<b>Экспирации</b> — даты истечения опционов. Крупная экспирация = повышенная волатильность.

<b>Option Bias</b> — наш индикатор настроения опционного рынка (Bullish/Bearish %). Считается из PCR + положения цены относительно Max Pain.

<b>🎯 Магнит цены (Target Magnet)</b> — уровень Max Pain. Перед экспирацией маркетмейкеры заинтересованы сдвинуть цену к Max Pain, чтобы максимум опционов сгорел.""",

        "faq_orderflow": """<b>📊 Order Flow — поток ордеров</b>

Показывает РЕАЛЬНОЕ давление покупателей и продавцов, а не просто движение цены.

<b>CVD (Cumulative Volume Delta)</b> — разница между рыночными покупками и продажами за 1 час.
• CVD +42M — покупатели агрессивно купили на $42M больше, чем продали продавцы
• CVD -55M — продавцы давят
• Цена растёт, но CVD падает = ложный рост, возможна ловушка
• Цена стоит, но CVD растёт = кто-то тихо набирает позицию

<b>Стакан (Order Book)</b>
🟢 Покупки / 🔴 Продажи — объём лимитных ордеров в стакане.

<b>Стены (Walls)</b>
🟩 Стена покупок — крупный лимитный ордер на покупку. Поддержка цены.
🟥 Стена продаж — крупный лимитный ордер на продажу. Сопротивление.

Если стена покупок на $82,400 ($4.2M) — маркетмейкер не хочет, чтобы цена упала ниже этого уровня.

Данные: Binance Futures (бесплатно, обновление каждые 2-5 мин).""",

        "faq_liquidity": """<b>🔥 Карта ликвидаций</b>

Показывает КУДА маркетмейкеры могут двинуть цену за ликвидностью.

<b>Как это работает:</b>
Большинство трейдеров используют плечи 10x-25x. При падении на 5% все лонги с 20x плечом ликвидируются. Эти ликвидации — как магнит для цены.

<b>Что показываем:</b>
🔥 $86,200 — стопы шортов (выше цены). Если цена дойдёт сюда, шорты начнут ликвидироваться каскадно, толкая цену ещё выше.
🔥 $78,100 — стопы лонгов (ниже цены). Если цена упадёт сюда, лонги начнут ликвидироваться, толкая цену ещё ниже.

<b>Почему это важно:</b>
Маркетмейкеры знают, где стоят стопы. Часто цена "ходит за ликвидностью" — сначала снимает стопы с одной стороны, потом разворачивается. Это называется "ликвидационный магнит".

<b>Также показываем:</b>
💥 Ликвидации за 1ч — сколько денег было ликвидировано у лонгов vs шортов. Если ликвидируют больше шортов — давление вверх, и наоборот.

Расчёт: на основе текущей цены, популярных плечей (10x-25x) и соотношения лонг/шорт.""",

        "faq_structure": """<b>🏗 Структура рынка — Spot vs Perp</b>

Показывает КТО двигает рынок: реальные покупатели или деривативные трейдеры.

<b>Spot Volume</b> — объём реальных покупок/продаж на спотовом рынке. Это когда кто-то реально покупает BTC за доллары.

<b>Perp Volume</b> — объём торгов на фьючерсах (perpetual). Это когда трейдеры торгуют с плечом, но не покупают реальный BTC.

<b>Spot Dominance</b> — какой % от общего объёма составляет спот.
• Spot &gt; 50% — рост поддержан реальными покупками, тренд устойчивый
• Perp &gt; 70% — рынок двигают деривативы, движение нестабильное

<b>Почему это важно:</b>
Если BTC растёт на $3K, но 80% объёма — это perp, значит рост основан на плечах и ликвидациях. Это хрупкий рост, который может развернуться.

Если BTC растёт и 60% объёма — спот, значит реальные покупатели заходят в рынок. Это устойчивый рост.

<b>Правило фондов:</b> "Spot leads, perps follow." Если спот покупает — тренд настоящий.

Данные: Binance Spot + Binance Futures (бесплатно, обновление каждые 10 мин).""",

        "faq_ai": """<b>🤖 Как работает AI-анализ?</b>

Бот использует Claude AI (модель от Anthropic) для анализа каждой монеты.

<b>Как это работает:</b>
1. Алгоритм собирает 30+ метрик (цена, OI, funding, ликвидации, CVD, киты, опционы)
2. 3-слойный pipeline считает направление, состояние рынка, качество сетапа
3. AI получает ВСЕ данные + готовое решение алгоритма
4. AI формулирует объяснение простым языком + уровни входа/стопа/цели

<b>Важно:</b> AI НЕ принимает решение сам — он только объясняет то, что посчитал алгоритм. Решения rule-based (по правилам), а не по "мнению" нейросети.

temperature = 0 — ответы детерминированные, без рандома.

<b>🎯 AI ОЦЕНКА (AI Score)</b>

Числовая оценка от 0 до 100 — единый показатель, который объединяет ВСЕ данные pipeline в одно число.

<b>Как считается:</b>
• Базовый score алгоритма (направление, качество сетапа) нормализуется в шкалу 0–100
• Добавляется корректировка по соотношению бычьих/медвежьих факторов (±15)
• Чем больше метрик "за" — тем выше оценка

<b>Шкала:</b>
🟢 70–100 — СИЛЬНАЯ ПОКУПКА (3+ бычьих фактора, сильный сетап)
🟡 60–69 — ПОКУПКА (преимущество быков)
⚪ 45–59 — НЕЙТРАЛЬНО (факторы противоречат или слабый сигнал)
🟡 30–44 — ПРОДАЖА (преимущество медведей)
🔴 0–29 — СИЛЬНАЯ ПРОДАЖА (3+ медвежьих фактора)

<b>Драйверы:</b> под AI оценкой показаны конкретные причины — какие метрики толкают "за рост" (⬆️) и "за падение" (⬇️). Например: "BUY давление сильное, шорты переплачивают" или "лонгов ликвидируют, киты заводят на биржи".

<b>📊 ДАВЛЕНИЕ РЫНКА (Market Pressure)</b>

Визуальная шкала 🐂 ████████░░ 72% — показывает баланс сил быков и медведей.

• Считается из вероятностей бычьего/медвежьего движения
• 🐂 больше 60% — быки доминируют
• 🐻 меньше 40% — медведи доминируют
• 40–60% — силы примерно равны""",

        "faq_data": """<b>📡 Откуда данные?</b>

Бот агрегирует данные из 15+ источников:

<b>Платные:</b>
• Coinglass ($95/мес) — OI, Funding, L/S, ликвидации
• Whale Alert ($30/мес) — крупные транзакции китов

<b>Бесплатные:</b>
• CoinGecko / CryptoCompare — цены
• Deribit — опционы (PCR, IV, Max Pain)
• Binance Futures — CVD, стакан
• Bitget, Kraken, dYdX — cross-exchange данные
• BGeometrics — SOPR, RSI, MACD, Exchange Netflow
• Alternative.me — Fear &amp; Greed Index
• DeFiLlama — TVL, стейблкоины
• Blockchain.info — active addresses

Обновление: каждые 5 мин (коллектор) + алерты по расписанию.""",

        "faq_plans": """<b>💰 Тарифы</b>

🆓 <b>Free</b> — бесплатно
1 монета · AI-анализ · сигналы · алерты · 15 мин обновление

🟢 <b>Basic $14/мес</b>
Топ-20 монет · AI-анализ · сила сигнала · 5/15/60 мин

🟡 <b>Pro $29/мес</b>
Все метрики · Pro-дашборд · 3 темы · RU/EN · кастомизация

🔴 <b>Pro+ $49/мес</b>
Алерты 1-2 мин · сканер 200 монет · доп. языки

Трейдер экономит $200-800/мес на подписках, получая всё в одном месте.

💳 Оплата скоро — через Telegram Payments прямо в боте.

⚠️ <i>Zender Terminal не является финансовым советом. Вся информация предоставляется для ознакомления. Решения о сделках вы принимаете самостоятельно.</i>""",
    },

    "en": {
        "welcome": """<b>⚡ ZENDER TERMINAL</b>

Crypto data aggregator from 30+ services + LLM analysis.
Trader pays $14/mo instead of $200–800+ separately.

<b>Plans:</b>
🆓 <b>Free</b> — 1 coin · LLM analysis · 15 min refresh
🟢 <b>Basic $14</b> — top-20 coins · LLM analysis · 5/15/60 min
🟡 <b>Pro $29</b> — all metrics · dashboard · 3 themes · no LLM
🔴 <b>Pro+ $49</b> — 1-2 min alerts · 200 coin scanner

Use the buttons below 👇""",

        "help": """<b>⚡ ZENDER TERMINAL — Help</b>

<b>Commands:</b>
/start — main menu
/summary — market overview
/settings — settings
/status — subscription status

<b>How it works:</b>
• Summaries arrive automatically on schedule
• Tap a coin — get full analysis
• ◀ Back button — return to overview

<b>Data updated from:</b>
Coinglass · Glassnode · Hyblock · CryptoQuant
Santiment · Deribit · Nansen · and 20+ more

⚡ t.me/ZenderTerminal_bot""",

        # Buttons
        "btn_radar": "📡 Radar",
        "btn_scanner": "⚡ Scanner",
        "btn_danger": "🚨 Danger",
        "btn_settings": "⚙️ Settings",
        "btn_subscription": "💳 Subscription",
        "btn_help": "❓ Help",
        "btn_refresh": "🔄 Refresh",
        "btn_back": "◀ Back",
        "btn_back_radar": "◀ Back to radar",
        "btn_language": "🌐 Lang: English",
        "btn_view_basic": "📋 Basic",
        "btn_view_basic_on": "📋 Basic ✓",
        "btn_view_pro": "📊 Pro",
        "btn_view_pro_on": "📊 Pro ✓",
        "pro_promo": """📊 <b>PRO View</b> — extended analysis

What PRO view gives you:
• Full AI breakdown (detailed analysis, not 2-3 sentences)
• Options data in the card
• Extended order flow
• On-chain details
• Historical comparisons

🔓 Subscribe to <b>Pro $29/mo</b> to unlock""",

        # Settings
        "settings_title": "⚙️ Settings",
        "settings_plan": "Plan",
        "settings_interval": "Refresh",
        "settings_every": "every {interval} min",
        "settings_alerts": "Alerts",
        "settings_choose_interval": "Choose refresh interval:",
        "alerts_on": "🔔 Alerts: ON",
        "alerts_off": "🔕 Alerts: OFF",
        "alerts_enabled": "🔔 Enabled",
        "alerts_disabled": "🔕 Disabled",
        "alerts_on_short": "🔔 Alerts enabled",
        "alerts_off_short": "🔕 Alerts disabled",
        "interval_set": "✅ Interval: {interval} min",
        "refreshed": "🔄 Refreshed!",

        # Subscription
        "sub_title": "💳 Choose a plan",
        "sub_free": '🆓 <b>Free</b> — 1 coin, LLM analysis, 15 min',
        "sub_basic": '🟢 <b>Basic $14/mo</b> — top-20, LLM analysis, 5/15/60 min',
        "sub_pro": '🟡 <b>Pro $29/mo</b> — all metrics, dashboard, 3 themes',
        "sub_pro_plus": '🔴 <b>Pro+ $49/mo</b> — 1-2 min alerts, 200 coin scanner',
        "payment_soon": "💳 Payment {name} {price} — coming soon!",

        # Status
        "status_title": "📋 Your status",
        "status_plan": "Plan",
        "status_coins": "Coins tracked",
        "status_interval": "Refresh interval",
        "not_registered": "You're not registered yet. Send /start",

        # Radar
        "radar_title": "📡 MARKET RADAR",
        "market_mood": "Market mood",
        "press_coin": "Tap a coin for analysis ⬇",

        # Coin analysis
        "what_happening": "WHAT'S HAPPENING",
        "trap": "TRAP",
        "signal": "SIGNAL",
        "trend_up": "📈 Trend: up",
        "trend_down": "📉 Trend: down",
        "ls_bulls": "bulls pushing",
        "ls_bears": "bears pushing",
        "ls_balance": "balanced",
        "ls_label": "Long/Short",
        "funding_longs_pay": "longs pay shorts",
        "funding_shorts_pay": "shorts pay longs",
        "funding_balance": "balanced",
        "funding_label": "Funding",
        "oi_rising": "rising",
        "oi_falling": "falling",
        "oi_stable": "stable",
        "oi_label": "Open Interest",
        "rsi_overbought": "overbought",
        "rsi_heated": "heated",
        "rsi_oversold": "oversold",
        "rsi_cooling": "cooling",
        "rsi_normal": "normal",
        "state_label": "State",
        "mood_panic": "panic",
        "mood_fear": "fear",
        "mood_calm": "calm",
        "mood_greed": "greed",
        "mood_euphoria": "euphoria",
        "mood_label": "Market mood",
        "whales_vs_crowd": "🐋 WHALES vs CROWD",
        "whales_buying": "whales buying (withdrawing from exchanges)",
        "whales_selling": "whales selling (depositing to exchanges)",
        "whales_waiting": "whales waiting",
        "whale_alert_title": "🐋 Whale Alert (1h)",
        "whale_txs": "{n} large transfers",
        "whale_to_exchange": "⬆️ To exchanges: {usd}",
        "whale_from_exchange": "⬇️ From exchanges: {usd}",
        "whale_bullish": "accumulation — bullish",
        "whale_bearish": "preparing to sell — bearish",
        "whale_neutral": "neutral",
        "probability_title": "PROBABILITY",
        "prob_up": "Up",
        "prob_down": "Down",
        "crowd_overlong": "crowd overleveraged long ({pct}%)",
        "crowd_long": "crowd in longs ({pct}%)",
        "crowd_overshort": "crowd overleveraged short ({pct}%)",
        "crowd_short": "crowd in shorts ({pct}%)",
        "crowd_balance": "crowd balanced",
        "gas_high": "high load",
        "gas_medium": "moderate",
        "gas_low": "low",
        "section_market": "── MARKET ──",
        "section_liquidity": "── LIQUIDITY ──",
        "section_levels": "── LEVELS ──",
        "liq_1h": "Liquidations (1h)",
        "liq_shorts": "shorts",
        "liq_longs": "longs",
        "shorts_stops": "short stops",
        "longs_stops": "long stops",
        "entry_label": "🎯 Entry",
        "stop_label": "🛑 Stop",
        "target_label": "✅ Target",
        "buy_label": "🎯 Buy",
        "sell_label": "✅ Sell",

        # Navigation
        "page_label": "Pg. {page}/{total}",
        "next_page": "▶",
        "prev_page": "◀",

        # Options
        "btn_options": "📊 Options {coin}",
        "btn_back_coin": "◀ Back to {coin}",
        "options_title": "⚡ ZENDER TERMINAL · {coin} OPTIONS",
        "options_pcr_label": "📊 Put/Call Ratio",
        "options_pcr_bullish": "More calls — market expects growth",
        "options_pcr_bearish": "More puts — market expects decline",
        "options_pcr_neutral": "Balanced calls and puts — uncertainty",
        "options_maxpain_label": "🎯 Max Pain",
        "options_maxpain_above": "Pulling up to Max Pain ({pct})",
        "options_maxpain_below": "Pulling down to Max Pain ({pct})",
        "options_maxpain_at": "Price at Max Pain — magnet active",
        "options_iv_label": "📈 IV (volatility)",
        "options_iv_low": "quiet market",
        "options_iv_normal": "normal",
        "options_iv_high": "elevated — expect a move",
        "options_iv_extreme": "storm — sharp move incoming",
        "options_oi_title": "── OPEN INTEREST ──",
        "options_oi_bulls": "Bulls dominate in options",
        "options_oi_bears": "Bears dominate in options",
        "options_oi_balanced": "Bulls and bears balanced",
        "options_exp_title": "── EXPIRATIONS ──",
        "options_exp_days": "in {days} d.",
        "options_exp_warning": "Possible volatility",
        "options_exp_max": "Large expiry = price magnet",
        "options_ai_title": "── 🤖 AI ANALYSIS ──",
        "options_teaser": "── OPTIONS {coin} ──",

        # FAQ
        "btn_faq": "📖 FAQ",
        "faq_title": "<b>📖 FAQ — Frequently Asked</b>\n\nChoose a topic:",
        "faq_btn_signals": "📊 Signals",
        "faq_btn_whales": "🐋 Whales",
        "faq_btn_options": "📈 Options",
        "faq_btn_orderflow": "📊 Order Flow",
        "faq_btn_liquidity": "🔥 Liquidations",
        "faq_btn_structure": "🏗 Structure",
        "faq_btn_ai": "🤖 AI Analysis",
        "faq_btn_data": "📡 Data Sources",
        "faq_btn_plans": "💰 Plans",

        "faq_signals": """<b>📊 What are Signals?</b>

A signal is the result of analyzing 30+ market indicators through a 3-layer pipeline.

<b>Layer 1: Direction</b> — where money flows (up / down / sideways). Uses 9 factors: funding rate, long/short, CVD, whales, MACD, etc.

<b>Layer 2: Market State</b> — phase: short squeeze, panic, accumulation, distribution, sideways.

<b>Layer 3: Setup Quality</b> — how good is the entry (strong / medium / weak / poor).

<b>Signal strength:</b>
🟩⬜⬜⬜⬜ weak — few confirmations
🟩🟩🟩⬜⬜ medium — most agree
🟩🟩🟩🟩🟩 strong — all point one way

<b>📊 Market Pressure Gauge</b> — visual market pressure scale. Shows who's stronger — bulls or bears. Calculated from all bull/bear factors + setup quality.
🐂 ████████░░ 72% = bulls dominate
🐻 ██████████ 85% = strong bearish pressure

<b>🤖 AI Score</b> — overall market score from 0 to 100:
• 70-100 🟢 STRONG BUY — everything points to growth
• 60-69 🟡 BUY — most factors are bullish
• 45-59 ⚪ NEUTRAL — equilibrium or uncertainty
• 30-44 🟡 SELL — most factors are bearish
• 0-29 🔴 STRONG SELL — everything points to decline
Shows top drivers: bullish and bearish factors.

<b>⚠️ Traps</b>
"Shorts trapped" — shorters overpaid (negative funding), getting liquidated. Possible short squeeze — rapid rise from cascading short liquidations.
"Longs trapped" — longs overleveraged, possible long squeeze — rapid drop.

<b>OI Structure (OI Delta Trap)</b>
Combines price and Open Interest changes to reveal true trend:
• Price ↑ + OI ↑ = new longs, trend confirmed
• Price ↑ + OI ↓ = short squeeze, growth on liquidations
• Price ↓ + OI ↑ = new shorts, pressure down
• Price ↓ + OI ↓ = long capitulation, bounce possible

<b>Liquidation Pressure</b>
Shows who is being liquidated right now — longs or shorts, in percentages.

⚠️ Not financial advice. Always DYOR.""",

        "faq_whales": """<b>🐋 What are Whales?</b>

Whales are large crypto holders (wallets with $500K+ in a single transaction).

<b>Whale Alert</b> tracks ALL large transfers across 11 blockchains in real time.

What we show:
⬆️ <b>To exchanges</b> — a whale deposits crypto to exchange. Likely preparing to sell. Bearish signal.
⬇️ <b>From exchanges</b> — a whale withdraws to cold wallet. Accumulation. Bullish signal.

<b>Exchange Netflow</b> (BTC only) — net BTC flow to/from exchanges. Negative = more withdrawals = bullish.

Whales aren't always right, but when $50M leaves an exchange — it's a signal.""",

        "faq_options": """<b>📈 What are Options?</b>

Options are contracts to buy (Call) or sell (Put) crypto at a fixed price in the future. Available for BTC and ETH.

<b>PCR (Put/Call Ratio)</b> — ratio of puts to calls.
• &lt; 0.7 — more calls, market expects growth
• &gt; 1.0 — more puts, market expects decline

<b>Max Pain</b> — the price where MOST options expire worthless. Price is pulled toward it before expiration like a magnet.

<b>IV (Implied Volatility)</b> — expected volatility:
• &lt; 30% — quiet market
• 30-60% — normal
• &gt; 60% — big move expected

<b>Expirations</b> — dates when options expire. Large expiry = increased volatility.

<b>Option Bias</b> — our options sentiment indicator (Bullish/Bearish %). Calculated from PCR + price position relative to Max Pain.

<b>🎯 Target Magnet</b> — the Max Pain level. Before expiry, market makers push price toward Max Pain so most options expire worthless.""",

        "faq_orderflow": """<b>📊 Order Flow</b>

Shows REAL buy/sell pressure, not just price movement.

<b>CVD (Cumulative Volume Delta)</b> — difference between market buys and sells over 1 hour.
• CVD +42M — buyers aggressively bought $42M more than sellers sold
• CVD -55M — sellers dominating
• Price rises but CVD falls = fake rally, possible trap
• Price flat but CVD rises = someone quietly accumulating

<b>Order Book</b>
🟢 Buys / 🔴 Sells — volume of limit orders in the book.

<b>Walls</b>
🟩 Buy wall — large limit buy order. Price support.
🟥 Sell wall — large limit sell order. Resistance.

A buy wall at $82,400 ($4.2M) means a market maker doesn't want price to drop below that level.

Data: Binance Futures (free, updates every 2-5 min).""",

        "faq_liquidity": """<b>🔥 Liquidation Map</b>

Shows WHERE market makers may push price to grab liquidity.

<b>How it works:</b>
Most traders use 10x-25x leverage. A 5% drop liquidates all 20x longs. These liquidations act as price magnets.

<b>What we show:</b>
🔥 $86,200 — short stops (above price). If price reaches here, shorts cascade-liquidate, pushing price higher.
🔥 $78,100 — long stops (below price). If price drops here, longs cascade-liquidate, pushing price lower.

<b>Why it matters:</b>
Market makers know where stops are. Price often "hunts liquidity" — sweeps stops on one side, then reverses. This is called a "liquidation magnet."

<b>Also shown:</b>
💥 1h Liquidations — how much money was liquidated (longs vs shorts). More short liquidations = upward pressure, and vice versa.

Calculation: based on current price, popular leverage (10x-25x), and long/short ratio.""",

        "faq_structure": """<b>🏗 Market Structure — Spot vs Perp</b>

Shows WHO is driving the market: real buyers or derivative traders.

<b>Spot Volume</b> — real buy/sell volume. Someone actually buying BTC with dollars.

<b>Perp Volume</b> — perpetual futures trading volume. Leveraged trading without owning actual BTC.

<b>Spot Dominance</b> — what % of total volume is spot.
• Spot &gt; 50% — growth backed by real buys, trend is stable
• Perp &gt; 70% — derivatives driving market, movement is fragile

<b>Why it matters:</b>
If BTC rises $3K but 80% volume is perps — the rally is built on leverage and liquidations. Fragile, can reverse.

If BTC rises and 60% is spot — real buyers entering. Sustainable rally.

<b>Fund rule:</b> "Spot leads, perps follow." If spot is buying — the trend is real.

Data: Binance Spot + Futures (free, updates every 10 min).""",

        "faq_ai": """<b>🤖 How does AI Analysis work?</b>

The bot uses Claude AI (by Anthropic) to analyze each coin.

<b>How it works:</b>
1. Algorithm collects 30+ metrics (price, OI, funding, liquidations, CVD, whales, options)
2. 3-layer pipeline calculates direction, market state, setup quality
3. AI receives ALL data + algorithm's decision
4. AI formulates explanation in simple language + entry/stop/target levels

<b>Important:</b> AI does NOT make decisions — it only explains what the algorithm calculated. Decisions are rule-based, not neural network "opinions."

temperature = 0 — deterministic responses, no randomness.

<b>🎯 AI SCORE</b>

A numerical score from 0 to 100 — a single metric that combines ALL pipeline data into one number.

<b>How it's calculated:</b>
• Base algorithm score (direction, setup quality) is normalized to 0–100
• Adjusted by bull/bear factor ratio (±15)
• More metrics "for" = higher score

<b>Scale:</b>
🟢 70–100 — STRONG BUY (3+ bullish factors, strong setup)
🟡 60–69 — BUY (bulls have advantage)
⚪ 45–59 — NEUTRAL (conflicting factors or weak signal)
🟡 30–44 — SELL (bears have advantage)
🔴 0–29 — STRONG SELL (3+ bearish factors)

<b>Drivers:</b> below AI score you'll see specific reasons — which metrics push "bullish" (⬆️) and "bearish" (⬇️). For example: "BUY pressure strong, shorts overpaying" or "longs liquidated, whales depositing to exchanges."

<b>📊 MARKET PRESSURE</b>

Visual gauge 🐂 ████████░░ 72% — shows bull/bear balance of power.

• Calculated from bull/bear movement probabilities
• 🐂 above 60% — bulls dominate
• 🐻 below 40% — bears dominate
• 40–60% — forces roughly equal""",

        "faq_data": """<b>📡 Where does data come from?</b>

The bot aggregates data from 15+ sources:

<b>Paid:</b>
• Coinglass ($95/mo) — OI, Funding, L/S, liquidations
• Whale Alert ($30/mo) — large whale transactions

<b>Free:</b>
• CoinGecko / CryptoCompare — prices
• Deribit — options (PCR, IV, Max Pain)
• Binance Futures — CVD, order book
• Bitget, Kraken, dYdX — cross-exchange data
• BGeometrics — SOPR, RSI, MACD, Exchange Netflow
• Alternative.me — Fear &amp; Greed Index
• DeFiLlama — TVL, stablecoins
• Blockchain.info — active addresses

Updates: every 5 min (collector) + alerts on schedule.""",

        "faq_plans": """<b>💰 Plans</b>

🆓 <b>Free</b> — free
1 coin · AI analysis · signals · alerts · 15 min refresh

🟢 <b>Basic $14/mo</b>
Top-20 coins · AI analysis · signal strength · 5/15/60 min

🟡 <b>Pro $29/mo</b>
All metrics · Pro dashboard · 3 themes · RU/EN · customization

🔴 <b>Pro+ $49/mo</b>
1-2 min alerts · 200 coin scanner · extra languages

Traders save $200-800/mo on subscriptions, getting everything in one place.

💳 Payments coming soon — via Telegram Payments directly in the bot.

⚠️ <i>Zender Terminal is not financial advice. All information is provided for informational purposes only. You make trading decisions at your own risk.</i>""",
    },
}


def t(key: str, lang: str = "ru", **kwargs) -> str:
    """Получить текст по ключу и языку"""
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, TEXTS["ru"].get(key, key))
    if kwargs:
        text = text.format(**kwargs)
    return text


async def get_user_lang(user_id: int) -> str:
    """Получить язык пользователя из БД"""
    user = await db.get_user(user_id)
    if user:
        return user.get("language", "ru") or "ru"
    return "ru"


def detect_language(language_code: str | None) -> str:
    """Определить язык по TG language_code"""
    if not language_code:
        return "ru"
    lc = language_code.lower()
    if lc.startswith("en"):
        return "en"
    return "ru"


# ══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ (с пагинацией для 20 монет)
# ══════════════════════════════════════════════════════════════════════════════

COINS_PER_PAGE = 10  # 2 ряда по 5 монет на странице


def _coin_page_buttons(page: int = 0, prefix: str = "coin_", data: dict = None) -> list:
    """Кнопки монет для текущей страницы (2 ряда по 5)."""
    start = page * COINS_PER_PAGE
    end = start + COINS_PER_PAGE
    page_coins = COINS[start:end]

    rows = []
    for i in range(0, len(page_coins), 5):
        chunk = page_coins[i:i+5]
        rows.append([
            InlineKeyboardButton(text=c, callback_data=f"{prefix}{c}")
            for c in chunk
        ])
    return rows


def _page_nav_buttons(page: int, total_pages: int, lang: str = "ru") -> list:
    """Кнопки навигации между страницами — показывают какие монеты на другой странице"""
    if total_pages <= 1:
        return []
    buttons = []
    if page > 0:
        # Показываем первые монеты предыдущей страницы
        prev_start = (page - 1) * COINS_PER_PAGE
        prev_coins = COINS[prev_start:prev_start + 3]
        prev_label = f"◀ {', '.join(prev_coins)}..."
        buttons.append(InlineKeyboardButton(text=prev_label, callback_data=f"page_{page - 1}"))
    if page < total_pages - 1:
        # Показываем первые монеты следующей страницы
        next_start = (page + 1) * COINS_PER_PAGE
        next_coins = COINS[next_start:next_start + 3]
        _more = "Ещё" if lang == "ru" else "More"
        next_label = f"{_more}: {', '.join(next_coins)}... ▶"
        buttons.append(InlineKeyboardButton(text=next_label, callback_data=f"page_{page + 1}"))
    return buttons


TOTAL_PAGES = (len(COINS) + COINS_PER_PAGE - 1) // COINS_PER_PAGE


def kb_main(lang: str = "ru"):
    """Главная клавиатура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_radar", lang), callback_data="radar"),
            InlineKeyboardButton(text=t("btn_scanner", lang), callback_data="scanner"),
            InlineKeyboardButton(text=t("btn_danger", lang), callback_data="danger"),
        ],
        [
            InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
            InlineKeyboardButton(text=t("btn_subscription", lang), callback_data="subscription"),
            InlineKeyboardButton(text=t("btn_help", lang), callback_data="help"),
        ],
    ])


def kb_radar(page: int = 0, lang: str = "ru", data: dict = None):
    """Кнопки под радаром: монеты + пагинация + обновить + настройки"""
    rows = _coin_page_buttons(page, data=data)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton(text="📡 Радар", callback_data="radar"),
        InlineKeyboardButton(text="🔎 Сканер", callback_data="scanner"),
        InlineKeyboardButton(text="🚨 Danger", callback_data="danger"),
    ])
    rows.append([
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_coin_detail(coin: str, page: int = 0, lang: str = "ru", view_mode: str = "basic", data: dict = None):
    """Кнопки под анализом монеты"""
    rows = _coin_page_buttons(page, data=data)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    # Кнопка Опционы — в обоих режимах (Basic и Pro)
    if coin in COINS_WITH_OPTIONS:
        rows.append([InlineKeyboardButton(
            text=t("btn_options", lang, coin=coin), callback_data=f"options_{coin}"
        )])
    # Переключатель Basic / Pro — для ВСЕХ пользователей
    if view_mode == "pro":
        view_row = [
            InlineKeyboardButton(text=t("btn_view_basic", lang), callback_data=f"viewmode_basic_{coin}"),
            InlineKeyboardButton(text=t("btn_view_pro_on", lang), callback_data="noop"),
        ]
    else:
        view_row = [
            InlineKeyboardButton(text=t("btn_view_basic_on", lang), callback_data="noop"),
            InlineKeyboardButton(text=t("btn_view_pro", lang), callback_data=f"viewmode_pro_{coin}"),
        ]
    rows.append(view_row)
    action_row = [
        InlineKeyboardButton(text=t("btn_refresh", lang), callback_data=f"coin_{coin}"),
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ]
    rows.append(action_row)
    rows.append([
        InlineKeyboardButton(text=t("btn_back_radar", lang), callback_data="radar"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_coin_buttons(page: int = 0, lang: str = "ru", data: dict = None):
    """Кнопки монет + обновить + радар + настройки"""
    rows = _coin_page_buttons(page, data=data)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text=t("btn_refresh", lang), callback_data="refresh")])
    rows.append([
        InlineKeyboardButton(text=t("btn_radar", lang), callback_data="radar"),
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_back_to_summary(lang: str = "ru"):
    """Кнопка назад к сводке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("btn_back_radar", lang), callback_data="radar")]
    ])


def kb_settings(alerts_on: bool = True, lang: str = "ru"):
    """Настройки: интервал + алерты + язык"""
    alert_text = t("alerts_on", lang) if alerts_on else t("alerts_off", lang)
    alert_cb = "toggle_alerts_off" if alerts_on else "toggle_alerts_on"
    lang_text = t("btn_language", lang)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин",  callback_data="interval_5"),
            InlineKeyboardButton(text="15 мин", callback_data="interval_15"),
            InlineKeyboardButton(text="1 час",  callback_data="interval_60"),
        ],
        [InlineKeyboardButton(text=alert_text, callback_data=alert_cb)],
        [InlineKeyboardButton(text=lang_text, callback_data="toggle_lang")],
        [InlineKeyboardButton(text=t("btn_back", lang), callback_data="back_main")],
    ])


def kb_subscription(lang: str = "ru"):
    """Тарифы"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Basic — $14/мес",  callback_data="plan_basic")],
        [InlineKeyboardButton(text="🟡 Pro — $29/мес",    callback_data="plan_pro")],
        [InlineKeyboardButton(text="🔴 Pro+ — $49/мес",   callback_data="plan_pro_plus")],
        [InlineKeyboardButton(text=t("btn_back", lang), callback_data="back_main")],
    ])


# ══════════════════════════════════════════════════════════════════════════════
# УТИЛИТЫ ТЕКСТА
# ══════════════════════════════════════════════════════════════════════════════

def _arrow(change_str: str) -> str:
    s = str(change_str).strip()
    if s.startswith("+") and s != "+0.00%":
        return "🔺"
    elif s.startswith("-") and s != "-0.00%":
        return "🔻"
    return "▸"


def _has(val) -> bool:
    if val is None:
        return False
    s = str(val).strip()
    return s != "" and s != "—" and s != "0"


def _rec_icon(rec: str) -> str:
    r = str(rec).lower()
    if "покупать" in r or "buy" in r:
        return "🟢"
    elif "продавать" in r or "sell" in r:
        return "🔴"
    return "🟡"


def _rec_label(rec: str, lang: str = "ru") -> str:
    r = str(rec).lower()
    if lang == "en":
        if "покупать" in r or "buy" in r:
            return "BUY"
        elif "продавать" in r or "sell" in r:
            return "SELL"
        return "HOLD"
    else:
        if "покупать" in r:
            return "ПОКУПАТЬ"
        elif "продавать" in r:
            return "ПРОДАВАТЬ"
        return "ДЕРЖАТЬ"


def _change_icon(change_str: str) -> str:
    s = str(change_str).strip()
    if s.startswith("+") and s != "+0.00%":
        return "🟢"
    elif s.startswith("-") and s != "-0.00%":
        return "🔴"
    return "⚪"


# ══════════════════════════════════════════════════════════════════════════════
# ТЕКСТЫ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════

def text_radar(coins: list[str], data: dict, lang: str = "ru") -> str:
    """📡 РАДАР РЫНКА — ультра-компактный обзор для мобильных."""
    lines = [
        f"<b>{t('radar_title', lang)}</b>",
        "",
    ]

    for coin in coins:
        d = data.get(coin, {})
        price  = d.get("price",  "—")
        change = d.get("change", "—")
        rec    = d.get("recommendation", "")

        # Сигнал: 🟢BUY / 🔴SELL / ⚪HOLD (справа как на скрине)
        r = str(rec).lower()
        if "покупать" in r or "buy" in r:
            sig = "🟢BUY "
        elif "продавать" in r or "sell" in r:
            sig = "🔴SELL"
        else:
            sig = "⚪HOLD"

        # Формат: COIN  $PRICE  CHANGE%  🟢BUY — всё в <code>
        _p = str(price)
        if _p != "—" and not _p.startswith("$"):
            _p = f"${_p}"
        _coin_s = f"{coin:<5}"
        _price_s = f"{_p:>9}"
        _chg_s = f"{change:>7}"
        lines.append(f"<code>{_coin_s}{_price_s} {_chg_s} {sig}</code>")

    # Fear & Greed из BTC данных
    btc = data.get("BTC", {})
    fg = btc.get("fear_greed", "")
    fg_label = btc.get("fear_greed_label", "")
    if fg:
        lines.append("")
        try:
            fg_val = int(fg)
            if fg_val <= 25:
                fg_emoji = "😱"
            elif fg_val <= 45:
                fg_emoji = "😰"
            elif fg_val <= 55:
                fg_emoji = "😐"
            elif fg_val <= 75:
                fg_emoji = "😏"
            else:
                fg_emoji = "🤑"
        except (ValueError, TypeError):
            fg_emoji = ""
        lines.append(f"{fg_emoji} {fg_label} ({fg}/100)")

    lines.append("")
    _tap = "⬇ Нажми монету ниже" if lang == "ru" else "⬇ Tap coin below"
    lines.append(_tap)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# SCANNER — анализ давления и возможностей по всем монетам
# ══════════════════════════════════════════════════════════════════════════════

def _calc_coin_pressure(d: dict) -> dict:
    """Рассчитывает давление рынка для одной монеты.
    Возвращает: bull_pct, bear_pct, signal_type, signal_icon, reason_ru, reason_en, score."""
    signals = []  # (direction, weight)
    reasons_ru = []
    reasons_en = []

    # 1. Funding
    try:
        _fr = float(str(d.get("funding_rate", "0")).replace("%", "").replace("+", ""))
        if _fr > 0.02:
            signals.append((-1, 2.0))
            reasons_ru.append("лонги перегружены")
            reasons_en.append("longs crowded")
        elif _fr > 0.01:
            signals.append((-1, 1.0))
            reasons_ru.append("лонги платят")
            reasons_en.append("longs paying")
        elif _fr < -0.01:
            signals.append((1, 2.0))
            reasons_ru.append("шорты перегружены")
            reasons_en.append("shorts crowded")
        elif _fr < -0.005:
            signals.append((1, 1.0))
            reasons_ru.append("шорты платят")
            reasons_en.append("shorts paying")
    except (ValueError, TypeError):
        pass

    # 2. CVD
    try:
        _cv = float(str(d.get("cvd_value", "0")))
        if _cv > 1.0:
            signals.append((1, 1.5))
            reasons_ru.append("CVD: покупатели")
            reasons_en.append("CVD: buyers")
        elif _cv < -1.0:
            signals.append((-1, 1.5))
            reasons_ru.append("CVD: продавцы")
            reasons_en.append("CVD: sellers")
    except (ValueError, TypeError):
        pass

    # 3. OI change
    try:
        _oi = float(str(d.get("oi_change", "0")).replace("%", "").replace("+", ""))
        if _oi > 2.0:
            signals.append((1 if len([s for s in signals if s[0] > 0]) > len([s for s in signals if s[0] < 0]) else -1, 1.0))
            reasons_ru.append(f"OI +{_oi:.1f}%")
            reasons_en.append(f"OI +{_oi:.1f}%")
        elif _oi < -2.0:
            reasons_ru.append(f"OI {_oi:.1f}%")
            reasons_en.append(f"OI {_oi:.1f}%")
    except (ValueError, TypeError):
        pass

    # 4. Liquidations
    try:
        _lu = float(str(d.get("liq_up", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
        _ld = float(str(d.get("liq_dn", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
        if _lu + _ld > 0:
            _liq_ratio = _lu / (_lu + _ld)
            if _liq_ratio > 0.65:
                signals.append((1, 1.5))
                reasons_ru.append("шортов ликвидируют")
                reasons_en.append("shorts liquidated")
            elif _liq_ratio < 0.35:
                signals.append((-1, 1.5))
                reasons_ru.append("лонгов ликвидируют")
                reasons_en.append("longs liquidated")
    except (ValueError, TypeError):
        pass

    # 5. Whales
    _wd = d.get("whale_direction", "")
    if _wd == "bullish":
        signals.append((1, 1.5))
        reasons_ru.append("киты выводят")
        reasons_en.append("whales withdrawing")
    elif _wd == "bearish":
        signals.append((-1, 1.5))
        reasons_ru.append("киты заводят")
        reasons_en.append("whales depositing")

    # 6. OBI
    try:
        _obi = float(str(d.get("obi_value", "0")))
        if _obi > 0.15:
            signals.append((1, 0.8))
        elif _obi < -0.15:
            signals.append((-1, 0.8))
    except (ValueError, TypeError):
        pass

    # Рассчитываем итоговое давление
    if not signals:
        return {"bull_pct": 50, "bear_pct": 50, "signal_type": "neutral",
                "signal_icon": "🟡", "reason_ru": "нет данных", "reason_en": "no data", "score": 0}

    _total_weight = sum(w for _, w in signals)
    _weighted_sum = sum(d_ * w for d_, w in signals)
    _raw_score = _weighted_sum / _total_weight if _total_weight > 0 else 0
    # Сжатая шкала: max ~80% для сильного сигнала (вместо 95%)
    # Чтобы трейдеры доверяли — экстремальные значения только при ОЧЕНЬ сильном сетапе
    _bull_pct = int(50 + _raw_score * 30)
    _bull_pct = max(15, min(85, _bull_pct))
    _bear_pct = 100 - _bull_pct

    # Определяем тип сигнала
    try:
        _fr_val = float(str(d.get("funding_rate", "0")).replace("%", "").replace("+", ""))
    except (ValueError, TypeError):
        _fr_val = 0
    try:
        _oi_val = float(str(d.get("oi_change", "0")).replace("%", "").replace("+", ""))
    except (ValueError, TypeError):
        _oi_val = 0
    try:
        _price_chg = abs(float(str(d.get("change", "0")).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-")))
    except (ValueError, TypeError):
        _price_chg = 0

    # Squeeze detection (ужесточено — нужно более экстремальное значение)
    if _fr_val < -0.02 and _bull_pct > 65:
        signal_type = "squeeze"
        signal_icon = "🧨"
        reason_ru = "short squeeze"
        reason_en = "short squeeze"
    elif _fr_val > 0.04 and _bear_pct > 65:
        signal_type = "dump_risk"
        signal_icon = "⚠️"
        reason_ru = "dump risk"
        reason_en = "dump risk"
    elif _oi_val > 3.0 and _price_chg < 1.5:
        signal_type = "pre_move"
        signal_icon = "🟠"
        reason_ru = "pre-move"
        reason_en = "pre-move"
    elif _bull_pct >= 65:
        signal_type = "bullish"
        signal_icon = "🟢"
        reason_ru = reasons_ru[0] if reasons_ru else "бычье давление"
        reason_en = reasons_en[0] if reasons_en else "bull pressure"
    elif _bear_pct >= 65:
        signal_type = "bearish"
        signal_icon = "🔴"
        reason_ru = reasons_ru[0] if reasons_ru else "медвежье давление"
        reason_en = reasons_en[0] if reasons_en else "bear pressure"
    else:
        signal_type = "neutral"
        signal_icon = "🟡"
        reason_ru = "нейтрально"
        reason_en = "neutral"

    return {
        "bull_pct": _bull_pct, "bear_pct": _bear_pct,
        "signal_type": signal_type, "signal_icon": signal_icon,
        "reason_ru": reason_ru, "reason_en": reason_en,
        "score": abs(_bull_pct - 50),
    }


def _signal_type_label(sig_type: str, lang: str) -> str:
    labels = {
        "squeeze": ("Short Squeeze", "Short Squeeze"),
        "dump_risk": ("Dump Risk", "Dump Risk"),
        "pre_move": ("Pre-Move", "Pre-Move"),
        "bullish": ("Bull Pressure", "Бычье давление"),
        "bearish": ("Bear Pressure", "Медвежье давление"),
        "neutral": ("Neutral", "Нейтрально"),
    }
    en, ru = labels.get(sig_type, ("—", "—"))
    return ru if lang == "ru" else en


def text_scanner(coins: list[str], data: dict, lang: str = "ru", filter_type: str = "all") -> str:
    """⚡ ZENDER MARKET SCANNER — топ возможностей рынка с категориями.
    filter_type: 'all', 'bullish', 'bearish', 'squeeze', 'pre_move', 'dump_risk'
    """
    lines = [
        "<b>⚡ ZENDER MARKET SCANNER</b>",
        "",
    ]

    # Рассчитываем давление для каждой монеты
    scored = []
    for coin in coins:
        d = data.get(coin, {})
        if not d.get("price"):
            continue
        pressure = _calc_coin_pressure(d)
        pressure["coin"] = coin
        pressure["price"] = d.get("price", "—")
        pressure["change"] = d.get("change", "—")
        pressure["ai_score"] = d.get("ai_score", "—")
        scored.append(pressure)

    # Фильтр по категории
    if filter_type != "all":
        scored = [s for s in scored if s["signal_type"] == filter_type]

    # Сортируем: экстремальные сигналы первыми, потом по силе
    _priority = {"squeeze": 0, "dump_risk": 1, "pre_move": 2, "bullish": 3, "bearish": 3, "neutral": 4}
    scored.sort(key=lambda x: (_priority.get(x["signal_type"], 5), -x["score"]))

    # Заголовок фильтра
    _filter_labels = {
        "all": ("Top Opportunities" if lang == "en" else "Топ возможностей"),
        "bullish": ("🟢 Bull Pressure" if lang == "en" else "🟢 Бычье давление"),
        "bearish": ("🔴 Bear Pressure" if lang == "en" else "🔴 Медвежье давление"),
        "squeeze": ("🧨 Short Squeeze" if lang == "en" else "🧨 Short Squeeze"),
        "dump_risk": ("⚠️ Dump Risk" if lang == "en" else "⚠️ Dump Risk"),
        "pre_move": ("🟠 Pre-Move" if lang == "en" else "🟠 Pre-Move"),
    }
    _filter_title = _filter_labels.get(filter_type, _filter_labels["all"])
    lines.append(f"<b>{_filter_title}</b>")
    lines.append("")

    # Категории — счётчики
    if filter_type == "all":
        _cats = {"bullish": 0, "bearish": 0, "squeeze": 0, "dump_risk": 0, "pre_move": 0, "neutral": 0}
        for s in scored:
            _cats[s["signal_type"]] = _cats.get(s["signal_type"], 0) + 1
        _cat_parts = []
        if _cats["squeeze"] > 0:
            _cat_parts.append(f"🧨{_cats['squeeze']}")
        if _cats["dump_risk"] > 0:
            _cat_parts.append(f"⚠️{_cats['dump_risk']}")
        if _cats["pre_move"] > 0:
            _cat_parts.append(f"🟠{_cats['pre_move']}")
        if _cats["bullish"] > 0:
            _cat_parts.append(f"🟢{_cats['bullish']}")
        if _cats["bearish"] > 0:
            _cat_parts.append(f"🔴{_cats['bearish']}")
        if _cats["neutral"] > 0:
            _cat_parts.append(f"🟡{_cats['neutral']}")
        if _cat_parts:
            lines.append("  ".join(_cat_parts))
            lines.append("")

    # Показываем топ монет
    shown = 0
    for p in scored:
        if shown >= 10:
            break
        if filter_type == "all" and p["signal_type"] == "neutral":
            continue  # В режиме "all" пропускаем нейтральные
        coin = p["coin"]
        pct = max(p["bull_pct"], p["bear_pct"])
        sig_label = _signal_type_label(p["signal_type"], lang)
        reason = p["reason_ru"] if lang == "ru" else p["reason_en"]
        ai_sc = p.get("ai_score", "—")

        lines.append(f"{p['signal_icon']} <b>{coin}</b>  {p['price']}  {p['change']}")
        _ai_str = f"AI: {ai_sc}" if ai_sc and ai_sc != "—" else ""
        lines.append(f"   {sig_label} <b>{pct}%</b>  {_ai_str}")
        lines.append(f"   → {reason}")
        lines.append("")
        shown += 1

    if not shown:
        _no = "Нет активных сигналов в этой категории" if lang == "ru" else "No active signals in this category"
        lines.append(_no)
        lines.append("")

    # Fear & Greed
    btc = data.get("BTC", {})
    fg = btc.get("fear_greed", "")
    fg_label = btc.get("fear_greed_label", "")
    if fg:
        try:
            fg_val = int(fg)
            if fg_val <= 25:
                fg_emoji = "😱"
            elif fg_val <= 45:
                fg_emoji = "😰"
            elif fg_val <= 55:
                fg_emoji = "😐"
            elif fg_val <= 75:
                fg_emoji = "😏"
            else:
                fg_emoji = "🤑"
        except (ValueError, TypeError):
            fg_emoji = "😐"
        lines.append(f"{fg_emoji} F&G: {fg}/100 — {fg_label}")

    lines.append("")
    _tap = "⬇ Нажми монету для полного анализа" if lang == "ru" else "⬇ Tap coin for full analysis"
    lines.append(_tap)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# DANGER CENTER — оценка рисков по рынку
# ══════════════════════════════════════════════════════════════════════════════

def _calc_danger_scores(data: dict) -> dict:
    """Рассчитывает Danger Score для BTC (и рынка в целом).
    Returns: dump_risk, squeeze_risk, volatility_risk, total_danger, drivers_ru, drivers_en."""

    btc = data.get("BTC", {})
    dump_score = 0
    squeeze_score = 0
    vol_score = 0
    dump_drivers_ru = []
    dump_drivers_en = []
    squeeze_drivers_ru = []
    squeeze_drivers_en = []
    vol_drivers_ru = []
    vol_drivers_en = []

    # === DUMP RISK ===
    # 1. Whale inflows
    if btc.get("whale_direction") == "bearish":
        dump_score += 20
        dump_drivers_ru.append("киты заводят на биржи")
        dump_drivers_en.append("whale inflows to exchanges")

    # 2. Longs crowded (funding high)
    try:
        _fr = float(str(btc.get("funding_rate", "0")).replace("%", "").replace("+", ""))
        if _fr > 0.03:
            dump_score += 20
            dump_drivers_ru.append("лонги сильно перегружены")
            dump_drivers_en.append("longs heavily crowded")
        elif _fr > 0.01:
            dump_score += 10
            dump_drivers_ru.append("лонги платят")
            dump_drivers_en.append("longs paying")
    except (ValueError, TypeError):
        pass

    # 3. CVD negative
    try:
        _cv = float(str(btc.get("cvd_value", "0")))
        if _cv < -2.0:
            dump_score += 15
            dump_drivers_ru.append("CVD отрицательный")
            dump_drivers_en.append("CVD negative")
        elif _cv < -0.5:
            dump_score += 8
    except (ValueError, TypeError):
        pass

    # 4. OBI asks stronger
    try:
        _obi = float(str(btc.get("obi_value", "0")))
        if _obi < -0.15:
            dump_score += 12
            dump_drivers_ru.append("стена продаж давит")
            dump_drivers_en.append("sell wall pressure")
    except (ValueError, TypeError):
        pass

    # 5. Crowd in longs (Bitget)
    try:
        _bg = float(str(btc.get("bitget_long_acc", "50")).replace("%", ""))
        if _bg > 70:
            dump_score += 15
            dump_drivers_ru.append(f"толпа в лонгах {_bg:.0f}%")
            dump_drivers_en.append(f"crowd long {_bg:.0f}%")
    except (ValueError, TypeError):
        pass

    # 6. Fear greed extreme greed
    try:
        _fg = int(btc.get("fear_greed", "50"))
        if _fg > 75:
            dump_score += 10
            dump_drivers_ru.append("жадность на рынке")
            dump_drivers_en.append("market greed")
    except (ValueError, TypeError):
        pass

    # === SQUEEZE RISK (short squeeze up) ===
    try:
        _fr = float(str(btc.get("funding_rate", "0")).replace("%", "").replace("+", ""))
        if _fr < -0.02:
            squeeze_score += 25
            squeeze_drivers_ru.append("фандинг сильно отрицательный")
            squeeze_drivers_en.append("deeply negative funding")
        elif _fr < -0.005:
            squeeze_score += 12
            squeeze_drivers_ru.append("шорты платят")
            squeeze_drivers_en.append("shorts paying")
    except (ValueError, TypeError):
        pass

    try:
        _cv = float(str(btc.get("cvd_value", "0")))
        if _cv > 1.0:
            squeeze_score += 15
            squeeze_drivers_ru.append("покупатели давят (CVD+)")
            squeeze_drivers_en.append("buyers pressing (CVD+)")
    except (ValueError, TypeError):
        pass

    try:
        _bg = float(str(btc.get("bitget_long_acc", "50")).replace("%", ""))
        if _bg < 35:
            squeeze_score += 20
            squeeze_drivers_ru.append(f"шортов много ({100 - _bg:.0f}%)")
            squeeze_drivers_en.append(f"shorts crowded ({100 - _bg:.0f}%)")
    except (ValueError, TypeError):
        pass

    try:
        _obi = float(str(btc.get("obi_value", "0")))
        if _obi > 0.15:
            squeeze_score += 12
            squeeze_drivers_ru.append("стена покупок сильная")
            squeeze_drivers_en.append("strong bid wall")
    except (ValueError, TypeError):
        pass

    try:
        _fg = int(btc.get("fear_greed", "50"))
        if _fg < 25:
            squeeze_score += 15
            squeeze_drivers_ru.append("страх на рынке")
            squeeze_drivers_en.append("market fear")
    except (ValueError, TypeError):
        pass

    # === VOLATILITY EXPANSION ===
    try:
        _price_chg = abs(float(str(btc.get("change", "0")).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-")))
        _oi_chg = abs(float(str(btc.get("oi_change", "0")).replace("%", "").replace("+", "")))

        if _oi_chg > 3.0 and _price_chg < 1.5:
            vol_score += 25
            vol_drivers_ru.append(f"OI spike +{_oi_chg:.1f}% при плоской цене")
            vol_drivers_en.append(f"OI spike +{_oi_chg:.1f}% with flat price")

        if _price_chg > 4:
            vol_score += 20
            vol_drivers_ru.append(f"резкое движение {_price_chg:.1f}%")
            vol_drivers_en.append(f"sharp move {_price_chg:.1f}%")
    except (ValueError, TypeError):
        pass

    try:
        _iv = float(str(btc.get("options_iv", "0")).replace("%", ""))
        if _iv > 70:
            vol_score += 20
            vol_drivers_ru.append(f"IV высокая: {_iv:.0f}%")
            vol_drivers_en.append(f"high IV: {_iv:.0f}%")
        elif _iv > 50:
            vol_score += 10
    except (ValueError, TypeError):
        pass

    # Считаем агрессивные сделки через рыночные ликвидации
    try:
        _ml = float(str(btc.get("mkt_liq_long", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
        _ms = float(str(btc.get("mkt_liq_short", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
        if _ml + _ms > 5_000_000:
            vol_score += 15
            vol_drivers_ru.append("крупные ликвидации по рынку")
            vol_drivers_en.append("large market liquidations")
    except (ValueError, TypeError):
        pass

    # Ограничиваем до 100
    dump_score = min(100, dump_score)
    squeeze_score = min(100, squeeze_score)
    vol_score = min(100, vol_score)
    total = max(dump_score, squeeze_score, vol_score)

    return {
        "dump_risk": dump_score,
        "squeeze_risk": squeeze_score,
        "volatility_risk": vol_score,
        "total_danger": total,
        "dump_drivers_ru": dump_drivers_ru,
        "dump_drivers_en": dump_drivers_en,
        "squeeze_drivers_ru": squeeze_drivers_ru,
        "squeeze_drivers_en": squeeze_drivers_en,
        "vol_drivers_ru": vol_drivers_ru,
        "vol_drivers_en": vol_drivers_en,
    }


def _danger_bar(score: int) -> str:
    """Визуальный бар для danger score."""
    filled = round(score / 10)
    return "█" * filled + "░" * (10 - filled)


def _danger_level(score: int, lang: str) -> str:
    if score >= 70:
        return "🔴 HIGH" if lang == "en" else "🔴 ВЫСОКИЙ"
    elif score >= 45:
        return "🟠 MEDIUM" if lang == "en" else "🟠 СРЕДНИЙ"
    else:
        return "🟢 LOW" if lang == "en" else "🟢 НИЗКИЙ"


def text_danger_center(data: dict, lang: str = "ru") -> str:
    """🚨 DANGER CENTER — анализ рисков по рынку."""
    danger = _calc_danger_scores(data)
    lines = [
        "<b>🚨 ZENDER DANGER CENTER</b>",
        "",
    ]

    btc = data.get("BTC", {})
    btc_price = btc.get("price", "—")
    btc_chg = btc.get("change", "—")
    lines.append(f"BTC {btc_price}  {btc_chg}")
    lines.append("")

    total = danger["total_danger"]
    _total_label = "DANGER LEVEL" if lang == "en" else "УРОВЕНЬ ОПАСНОСТИ"
    lines.append(f"<b>{_total_label}: {total}%  {_danger_level(total, lang)}</b>")
    lines.append(f"<code>{_danger_bar(total)}</code>")
    lines.append("")

    # DUMP RISK
    ds = danger["dump_risk"]
    lines.append(f"📉 <b>Dump Risk: {ds}%</b>  {_danger_level(ds, lang)}")
    lines.append(f"<code>{_danger_bar(ds)}</code>")
    drivers = danger["dump_drivers_ru"] if lang == "ru" else danger["dump_drivers_en"]
    if drivers:
        for dr in drivers[:4]:
            lines.append(f"  • {dr}")
    lines.append("")

    # SQUEEZE RISK
    ss = danger["squeeze_risk"]
    lines.append(f"🧨 <b>Squeeze Risk: {ss}%</b>  {_danger_level(ss, lang)}")
    lines.append(f"<code>{_danger_bar(ss)}</code>")
    drivers = danger["squeeze_drivers_ru"] if lang == "ru" else danger["squeeze_drivers_en"]
    if drivers:
        for dr in drivers[:4]:
            lines.append(f"  • {dr}")
    lines.append("")

    # VOLATILITY EXPANSION
    vs = danger["volatility_risk"]
    _vol_label = "Volatility" if lang == "en" else "Волатильность"
    lines.append(f"⚡ <b>{_vol_label}: {vs}%</b>  {_danger_level(vs, lang)}")
    lines.append(f"<code>{_danger_bar(vs)}</code>")
    drivers = danger["vol_drivers_ru"] if lang == "ru" else danger["vol_drivers_en"]
    if drivers:
        for dr in drivers[:4]:
            lines.append(f"  • {dr}")
    lines.append("")

    # What to avoid
    if total >= 50:
        lines.append("")
        _avoid_title = "⚠️ <b>ЧТО ИЗБЕГАТЬ:</b>" if lang == "ru" else "⚠️ <b>WHAT TO AVOID:</b>"
        lines.append(_avoid_title)
        if ds >= 50:
            if lang == "ru":
                lines.append("  • агрессивные лонги без стопа")
                lines.append("  • усреднение вниз")
            else:
                lines.append("  • aggressive longs without stop")
                lines.append("  • averaging down")
        if ss >= 50:
            if lang == "ru":
                lines.append("  • новые шорты под сопротивлением")
                lines.append("  • вход против squeeze")
            else:
                lines.append("  • new shorts near resistance")
                lines.append("  • fighting the squeeze")
        if vs >= 50:
            if lang == "ru":
                lines.append("  • крупные позиции с высоким плечом")
            else:
                lines.append("  • large leveraged positions")

    # Per-coin danger
    lines.append("")
    _per_coin = "── МОНЕТЫ ──" if lang == "ru" else "── COINS ──"
    lines.append(_per_coin)
    for coin in ["BTC", "ETH", "SOL", "XRP", "DOGE"]:
        d = data.get(coin, {})
        if not d.get("price"):
            continue
        p = _calc_coin_pressure(d)
        if p["signal_type"] in ("squeeze", "dump_risk"):
            lines.append(f"{p['signal_icon']} <b>{coin}</b>: {_signal_type_label(p['signal_type'], lang)} {max(p['bull_pct'], p['bear_pct'])}%")
        elif p["score"] > 15:
            lines.append(f"{p['signal_icon']} <b>{coin}</b>: {_signal_type_label(p['signal_type'], lang)} {max(p['bull_pct'], p['bear_pct'])}%")

    return "\n".join(lines)


def kb_scanner(page: int = 0, lang: str = "ru", data: dict = None, filter_type: str = "all"):
    """Кнопки под сканером с фильтрами категорий."""
    rows = _coin_page_buttons(page, data=data)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    # Фильтры категорий
    _filters = [
        ("all", "🔍 ALL" if filter_type != "all" else "▶ ALL"),
        ("bullish", "🟢 BULL" if filter_type != "bullish" else "▶ BULL"),
        ("bearish", "🔴 BEAR" if filter_type != "bearish" else "▶ BEAR"),
        ("squeeze", "🧨 SQZ" if filter_type != "squeeze" else "▶ SQZ"),
        ("pre_move", "🟠 PRE" if filter_type != "pre_move" else "▶ PRE"),
    ]
    rows.append([
        InlineKeyboardButton(text=label, callback_data=f"scan_filter:{ftype}")
        for ftype, label in _filters
    ])
    rows.append([
        InlineKeyboardButton(text="📡 Радар", callback_data="radar"),
        InlineKeyboardButton(text="🚨 Danger", callback_data="danger"),
    ])
    rows.append([
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_danger(lang: str = "ru"):
    """Кнопки под Danger Center."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📡 Радар", callback_data="radar"),
            InlineKeyboardButton(text="🔎 Сканер", callback_data="scanner"),
            InlineKeyboardButton(text="🚨 Danger", callback_data="danger"),
        ],
        [
            InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
        ],
    ])


def text_coin_analysis(coin: str, data: dict, lang: str = "ru", view_mode: str = "basic") -> str:
    """Анализ монеты. view_mode='basic' — компактный, 'pro' — развёрнутый."""
    is_pro = (view_mode == "pro")
    d = data.get(coin, {})
    price  = d.get("price",  "—")
    change = d.get("change", "—")
    ch_icon = _change_icon(change)

    def _clean(v):
        return str(v).replace("**", "").replace("*", "").strip() if v else ""

    what_happening = _clean(d.get("what_happening", ""))
    if what_happening and len(what_happening) > 120:
        what_happening = what_happening[:117] + "..."
    trap           = _clean(d.get("trap_display", d.get("trap", "")))
    recommendation = _clean(d.get("recommendation", ""))
    strength       = _clean(d.get("strength", ""))
    entry          = _clean(d.get("entry", ""))
    stop           = _clean(d.get("stop", ""))
    target         = _clean(d.get("target", ""))
    buy_zone       = _clean(d.get("buy_zone", ""))
    sell_zone      = _clean(d.get("sell_zone", ""))

    rec_icon = _rec_icon(recommendation)
    rec_label = _rec_label(recommendation, lang)
    strength_label = strength.upper() if strength else ""

    # ══════════════════════════════════════════════════════════════
    # ВИРУСНАЯ КАРТОЧКА (первые строки — видны при пересылке)
    # ══════════════════════════════════════════════════════════════
    _pro_badge = "  📊 PRO" if is_pro else ""
    lines = [
        f"<b>⚡ ZENDER TERMINAL</b>{_pro_badge}",
        "",
        f"<b>{coin}</b>  {price}  {ch_icon} {change}",
    ]

    # ── COMPACT HEADER: AI Score + Signal + Confidence ──
    ai_score_str = d.get("ai_score", "")
    ai_sc = None
    score_part = ""
    if ai_score_str and ai_score_str not in ("", "—"):
        try:
            ai_sc = int(float(str(ai_score_str)))
            _ai_title = "AI SCORE" if lang == "en" else "AI SCORE"
            # Mood label — пояснение что значит число
            if lang == "en":
                if ai_sc >= 80: _mood = "strong bullish"
                elif ai_sc >= 60: _mood = "bullish"
                elif ai_sc >= 45: _mood = "neutral"
                elif ai_sc >= 20: _mood = "bearish"
                else: _mood = "strong bearish"
            else:
                if ai_sc >= 80: _mood = "сильный бычий"
                elif ai_sc >= 60: _mood = "бычий"
                elif ai_sc >= 45: _mood = "нейтральный"
                elif ai_sc >= 20: _mood = "медвежий"
                else: _mood = "сильный медвежий"
            score_part = f"<b>{_ai_title}: {ai_sc}</b> · {_mood}"
        except (ValueError, TypeError):
            pass

    sig_part = ""
    signal_reason = _clean(d.get("signal_reason", ""))
    if recommendation:
        _sig_title = "SIGNAL" if lang == "en" else "СИГНАЛ"
        sig_part = f"{rec_icon} {_sig_title}: <b>{rec_label}</b>"
        if signal_reason:
            sig_part += f"\n→ {signal_reason}"

    conf_part = ""
    if is_pro:
        conf_bar = d.get("confidence_bar", "")
        conf_label = d.get("confidence_label", "")
        if conf_bar and conf_label:
            _conf_title = "CONFIDENCE" if lang == "en" else "УВЕРЕННОСТЬ"
            conf_part = f"{_conf_title}: {conf_bar}"

    # Вывод компактным блоком
    if score_part or sig_part:
        lines.append("")
    if score_part:
        lines.append(score_part)
    if sig_part:
        lines.append(sig_part)
    if conf_part:
        lines.append(conf_part)

    # ── TRADE SETUP (единственный блок с уровнями) ──
    horizon = _clean(d.get("horizon", ""))
    if entry or target or stop:
        lines.append("")
        _ts_title = "TRADE SETUP" if lang == "en" else "СДЕЛКА"
        lines.append(f"🎯 <b>{_ts_title}</b>")
        if entry:
            _ent_lbl = "Entry" if lang == "en" else "Вход"
            lines.append(f"  {_ent_lbl}: <b>{html_lib.escape(entry)}</b>")
        if target:
            _tgt_lbl = "Target" if lang == "en" else "Цель"
            lines.append(f"  {_tgt_lbl}: <b>{html_lib.escape(target)}</b>")
        if stop:
            _stp_lbl = "Stop" if lang == "en" else "Стоп"
            lines.append(f"  {_stp_lbl}: <b>{html_lib.escape(stop)}</b>")
        # Risk/Reward ratio
        try:
            _e = float(str(entry).replace("$", "").replace(",", ""))
            _t = float(str(target).replace("$", "").replace(",", ""))
            _s = float(str(stop).replace("$", "").replace(",", ""))
            _risk = abs(_e - _s)
            _reward = abs(_t - _e)
            if _risk > 0:
                _rr = _reward / _risk
                lines.append(f"  R/R: <b>1 : {_rr:.1f}</b>")
        except (ValueError, TypeError):
            pass
        if horizon:
            _hor_lbl = "Timeframe" if lang == "en" else "Горизонт"
            lines.append(f"  ⏱ {_hor_lbl}: {html_lib.escape(horizon)}")
    elif target or stop:
        # Fallback если нет entry
        lines.append("")
        if target:
            _tgt_lbl = "Target" if lang == "en" else "Цель"
            lines.append(f"🎯 {_tgt_lbl}: <b>{html_lib.escape(target)}</b>")
        if stop:
            _stp_lbl = "Stop" if lang == "en" else "Стоп"
            lines.append(f"🛑 {_stp_lbl}: <b>{html_lib.escape(stop)}</b>")
        if horizon:
            _hor_lbl = "Timeframe" if lang == "en" else "Горизонт"
            lines.append(f"⏱ {_hor_lbl}: {html_lib.escape(horizon)}")

    # ══════════════════════════════════════════════════════════════
    # РЫНОЧНЫЕ ДАННЫЕ (сначала все данные)
    # ══════════════════════════════════════════════════════════════
    lines.append("")
    lines.append(t("section_market", lang))

    # Данные
    sma50_val = d.get("sma50", "—")
    sma200_val = d.get("sma200", "—")
    rsi_val = d.get("rsi", "—")
    fr_val = d.get("funding_rate", "—")
    oi_chg = d.get("oi_change", "—")
    long_p = str(d.get("long_pct", "—")).replace("%", "")
    short_p = str(d.get("short_pct", "—")).replace("%", "")
    fg = d.get("fear_greed", "—")
    fg_lbl = d.get("fear_greed_label", "—")
    bg_long_acc = str(d.get("bitget_long_acc", "—")).replace("%", "")
    bg_short_acc = str(d.get("bitget_short_acc", "—")).replace("%", "")

    # Тренд
    if _has(sma50_val) and _has(sma200_val):
        try:
            s50 = float(str(sma50_val).replace("$", "").replace(",", ""))
            s200 = float(str(sma200_val).replace("$", "").replace(",", ""))
            if s50 > s200:
                lines.append(t("trend_up", lang))
            else:
                lines.append(t("trend_down", lang))
            # PRO: точные значения SMA (умное форматирование для дешёвых монет)
            if is_pro:
                def _fmt_sma(v):
                    if v >= 1000: return f"${v:,.0f}"
                    elif v >= 1: return f"${v:.2f}"
                    elif v >= 0.01: return f"${v:.4f}"
                    else: return f"${v:.6f}"
                lines.append(f"  SMA50: <b>{_fmt_sma(s50)}</b> · SMA200: <b>{_fmt_sma(s200)}</b>")
        except (ValueError, TypeError):
            pass

    # Давление рынка
    if _has(long_p) and _has(short_p):
        try:
            lp = float(long_p)
            sp = float(short_p)
            if lp > 55:
                hint = t("ls_bulls", lang)
            elif sp > 55:
                hint = t("ls_bears", lang)
            else:
                hint = t("ls_balance", lang)
            lines.append(f"⚖️ {t('ls_label', lang)}: <b>{lp:.0f}%</b> / <b>{sp:.0f}%</b> — {hint}")
        except (ValueError, TypeError):
            lines.append(f"⚖️ {t('ls_label', lang)}: <b>{long_p}%</b> / <b>{short_p}%</b>")

    # Фандинг
    if _has(fr_val):
        try:
            fv = float(str(fr_val).replace("%", "").replace("+", ""))
            if fv > 0.01:
                fr_hint = t("funding_longs_pay", lang)
            elif fv < -0.005:
                fr_hint = t("funding_shorts_pay", lang)
            else:
                fr_hint = t("funding_balance", lang)
            lines.append(f"💰 {t('funding_label', lang)}: <b>{fr_val}</b> — {fr_hint}")
        except (ValueError, TypeError):
            lines.append(f"💰 {t('funding_label', lang)}: <b>{fr_val}</b>")

    # OI
    if _has(oi_chg):
        try:
            oi_v = float(str(oi_chg).replace("%", "").replace("+", ""))
            if oi_v > 0.5:
                oi_hint = t("oi_rising", lang)
            elif oi_v < -0.5:
                oi_hint = t("oi_falling", lang)
            else:
                oi_hint = t("oi_stable", lang)
            lines.append(f"📊 {t('oi_label', lang)}: <b>{oi_chg}</b> ({oi_hint})")

            # ── OI STRUCTURE (OI Delta Trap) ──
            # Цена↑ + OI↑ = тренд подтверждён, Цена↑ + OI↓ = short squeeze
            # Цена↓ + OI↑ = давление продавцов, Цена↓ + OI↓ = капитуляция лонгов
            try:
                price_chg = float(str(change).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-"))
                price_up = price_chg > 0.3
                price_dn = price_chg < -0.3
                oi_up = oi_v > 0.3
                oi_dn = oi_v < -0.3

                if price_up and oi_up:
                    _oi_trap_ru = "новые лонги заходят — тренд подтверждён"
                    _oi_trap_en = "new longs entering — trend confirmed"
                elif price_up and oi_dn:
                    _oi_trap_ru = "short squeeze — рост на ликвидациях"
                    _oi_trap_en = "short squeeze — growth on liquidations"
                elif price_dn and oi_up:
                    _oi_trap_ru = "новые шорты заходят — давление вниз"
                    _oi_trap_en = "new shorts entering — pressure down"
                elif price_dn and oi_dn:
                    _oi_trap_ru = "капитуляция лонгов — возможен отскок"
                    _oi_trap_en = "long capitulation — bounce possible"
                else:
                    _oi_trap_ru = None
                    _oi_trap_en = None

                if _oi_trap_ru:
                    _price_arrow = "↑" if price_up else "↓"
                    _oi_arrow = "↑" if oi_up else "↓"
                    lines.append(f"→ Price {_price_arrow} + OI {_oi_arrow}: {_oi_trap_ru if lang == 'ru' else _oi_trap_en}")
            except (ValueError, TypeError):
                pass
        except (ValueError, TypeError):
            lines.append(f"📊 {t('oi_label', lang)}: <b>{oi_chg}</b>")

    # RSI
    if _has(rsi_val):
        try:
            rv = float(rsi_val)
            if rv > 70:
                rsi_hint = t("rsi_overbought", lang)
            elif rv > 60:
                rsi_hint = t("rsi_heated", lang)
            elif rv < 30:
                rsi_hint = t("rsi_oversold", lang)
            elif rv < 40:
                rsi_hint = t("rsi_cooling", lang)
            else:
                rsi_hint = t("rsi_normal", lang)
            lines.append(f"🌡 {t('state_label', lang)}: {rsi_hint} (RSI <b>{rv:.0f}</b>)")
        except (ValueError, TypeError):
            pass

    # Fear & Greed
    if _has(fg):
        try:
            fg_val = int(fg)
            if fg_val <= 25:
                fg_hint = t("mood_panic", lang)
            elif fg_val <= 45:
                fg_hint = t("mood_fear", lang)
            elif fg_val <= 55:
                fg_hint = t("mood_calm", lang)
            elif fg_val <= 75:
                fg_hint = t("mood_greed", lang)
            else:
                fg_hint = t("mood_euphoria", lang)
            lines.append(f"😰 {t('mood_label', lang)}: {fg_hint} (<b>{fg}</b>/100)")
        except (ValueError, TypeError):
            pass

    # ══════════════════════════════════════════════════════════════
    # PRO-СЕКЦИИ (видны только в Pro режиме)
    # ══════════════════════════════════════════════════════════════

    # ── МУЛЬТИ-БИРЖА (PRO) ──
    if is_pro:
        okx_long = d.get("okx_top_long", "—")
        okx_short = d.get("okx_top_short", "—")
        bg_long_pos = d.get("bitget_long_pos", "—")
        bg_short_pos = d.get("bitget_short_pos", "—")
        bg_oi = d.get("bitget_oi_usd", "—")
        kraken_fr = d.get("kraken_funding", "—")
        kraken_oi_val = d.get("kraken_oi", "—")
        dydx_fr = d.get("dydx_funding", "—")
        dydx_oi_val = d.get("dydx_oi", "—")

        has_multi = (_has(okx_long) or _has(bg_long_pos) or _has(kraken_fr) or _has(kraken_oi_val) or _has(dydx_fr))
        if has_multi:
            lines.append("")
            _me_title = "── МУЛЬТИ-БИРЖА (PRO) ──" if lang == "ru" else "── MULTI-EXCHANGE (PRO) ──"
            lines.append(_me_title)

            # OKX Top Traders
            if _has(okx_long) and _has(okx_short):
                try:
                    ol = float(str(okx_long).replace("%", ""))
                    os_ = float(str(okx_short).replace("%", ""))
                    _okx_hint = "быки" if ol > 55 else ("медведи" if os_ > 55 else "баланс")
                    if lang != "ru":
                        _okx_hint = "bulls" if ol > 55 else ("bears" if os_ > 55 else "balanced")
                    lines.append(f"🏛 OKX Top: <b>{ol:.0f}%</b>L / <b>{os_:.0f}%</b>S — {_okx_hint}")
                except (ValueError, TypeError):
                    pass

            # Bitget позиции + OI
            _has_bg_ls = False
            if _has(bg_long_pos) and _has(bg_short_pos):
                try:
                    bl = float(str(bg_long_pos).replace("%", ""))
                    bs = float(str(bg_short_pos).replace("%", ""))
                    lines.append(f"🔶 Bitget: <b>{bl:.0f}%</b>L / <b>{bs:.0f}%</b>S")
                    _has_bg_ls = True
                except (ValueError, TypeError):
                    pass
            if _has(bg_oi):
                try:
                    boi = float(str(bg_oi).replace("$", "").replace(",", ""))
                    _oi_str = ""
                    if boi >= 1e9:
                        _oi_str = f"${boi/1e9:.2f}B"
                    elif boi >= 1e6:
                        _oi_str = f"${boi/1e6:.1f}M"
                    elif boi >= 1e3:
                        _oi_str = f"${boi/1e3:.0f}K"
                    elif boi > 0:
                        _oi_str = f"${boi:,.0f}"
                    if _oi_str:
                        if _has_bg_ls:
                            lines.append(f"  OI: <b>{_oi_str}</b>")
                        else:
                            lines.append(f"🔶 Bitget: OI <b>{_oi_str}</b>")
                except (ValueError, TypeError):
                    pass

            # Kraken (funding не собирается — annualized, ненадёжный; показываем OI)
            if _has(kraken_fr) or _has(kraken_oi_val):
                _kr_parts = []
                if _has(kraken_fr):
                    _kr_parts.append(f"funding <b>{kraken_fr}</b>")
                if _has(kraken_oi_val):
                    try:
                        _koi = float(str(kraken_oi_val).replace("$", "").replace(",", ""))
                        if _koi >= 1e9:
                            _kr_parts.append(f"OI <b>${_koi/1e9:.2f}B</b>")
                        elif _koi >= 1e6:
                            _kr_parts.append(f"OI <b>${_koi/1e6:.1f}M</b>")
                        elif _koi >= 1e3:
                            _kr_parts.append(f"OI <b>${_koi/1e3:.0f}K</b>")
                        else:
                            _kr_parts.append(f"OI <b>{kraken_oi_val}</b>")
                    except (ValueError, TypeError):
                        _kr_parts.append(f"OI <b>{kraken_oi_val}</b>")
                lines.append(f"🦑 Kraken: {' · '.join(_kr_parts)}")

            # dYdX
            if _has(dydx_fr):
                _dx_line = f"🟣 dYdX: funding <b>{dydx_fr}</b>"
                if _has(dydx_oi_val):
                    _dx_line += f" · OI {dydx_oi_val}"
                lines.append(_dx_line)

    # ── ГЛУБИНА СТАКАНА (PRO) ──
    # Приоритет: Kraken OB → fallback на Binance OBI
    if is_pro:
        bid_depth = d.get("bid_depth_usd", "—")
        ask_depth = d.get("ask_depth_usd", "—")
        ba_ratio = d.get("bid_ask_ratio", "—")
        # Fallback: если Kraken не вернул данные, берём из Binance OBI
        _bd_val = 0
        _ad_val = 0
        try:
            _bd_val = float(str(bid_depth).replace("$", "").replace(",", "").replace("M", "e6").replace("B", "e9"))
            _ad_val = float(str(ask_depth).replace("$", "").replace(",", "").replace("M", "e6").replace("B", "e9"))
        except (ValueError, TypeError):
            pass
        if _bd_val < 1000 or _ad_val < 1000:
            # Kraken пусто — используем Binance OBI данные
            obi_bid = d.get("obi_bid_vol", "—")
            obi_ask = d.get("obi_ask_vol", "—")
            try:
                _bd_val = float(str(obi_bid).replace("$", "").replace(",", ""))
                _ad_val = float(str(obi_ask).replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                _bd_val = 0
                _ad_val = 0
        if _bd_val > 1000 and _ad_val > 1000:
            lines.append("")
            _ob_title = "── СТАКАН (PRO) ──" if lang == "ru" else "── ORDER BOOK (PRO) ──"
            lines.append(_ob_title)
            _bid_lbl = "Bid liquidity" if lang == "en" else "Bid ликвидность"
            _ask_lbl = "Ask liquidity" if lang == "en" else "Ask ликвидность"
            def _fmt_depth(v):
                if v >= 1e6: return f"${v/1e6:.1f}M"
                elif v >= 1e3: return f"${v/1e3:.0f}K"
                else: return f"${v:.0f}"
            lines.append(f"🟢 {_bid_lbl}: <b>{_fmt_depth(_bd_val)}</b>")
            lines.append(f"🔴 {_ask_lbl}: <b>{_fmt_depth(_ad_val)}</b>")
            if _bd_val > _ad_val * 1.3:
                _ob_hint = "стена покупок — поддержка сильнее" if lang == "ru" else "bid wall — support is stronger"
            elif _ad_val > _bd_val * 1.3:
                _ob_hint = "стена продаж — сопротивление сильнее" if lang == "ru" else "ask wall — resistance is stronger"
            else:
                _ob_hint = "стакан сбалансирован" if lang == "ru" else "order book balanced"
            lines.append(f"→ {_ob_hint}")

    # ── КИТЫ vs ТОЛПА (Whale Alert + Netflow + Crowd) ──
    # Whale Alert данные (реальные транзакции за 1ч) — приоритет над netflow
    whale_txs = d.get("whale_txs", "0")
    whale_to_ex = d.get("whale_to_exchange", "—")
    whale_from_ex = d.get("whale_from_exchange", "—")
    whale_dir = d.get("whale_direction", "—")

    has_whale_alert = False
    try:
        has_whale_alert = int(str(whale_txs)) > 0
    except (ValueError, TypeError):
        pass

    # BGeometrics netflow (суточный) — fallback если нет Whale Alert
    netflow = d.get("exchange_netflow_btc", "—")
    has_netflow = (coin == "BTC" and _has(netflow) and not has_whale_alert)
    has_crowd = _has(bg_long_acc)

    if has_whale_alert or has_netflow or has_crowd:
        lines.append("")
        lines.append(t("whales_vs_crowd", lang))

        # Whale Alert — свежие данные за 1ч (приоритет)
        if has_whale_alert:
            lines.append(t("whale_txs", lang, n=whale_txs))
            if _has(whale_to_ex):
                lines.append(t("whale_to_exchange", lang, usd=whale_to_ex))
            if _has(whale_from_ex):
                lines.append(t("whale_from_exchange", lang, usd=whale_from_ex))
            if whale_dir == "bullish":
                lines.append(f"→ {t('whale_bullish', lang)}")
            elif whale_dir == "bearish":
                lines.append(f"→ {t('whale_bearish', lang)}")
        elif has_netflow:
            # Fallback: BGeometrics netflow (суточный)
            try:
                nf = float(str(netflow).replace(",", "").replace("+", ""))
                if nf < -100:
                    lines.append(t("whales_buying", lang))
                elif nf > 100:
                    lines.append(t("whales_selling", lang))
                else:
                    lines.append(t("whales_waiting", lang))
            except (ValueError, TypeError):
                pass

        if has_crowd:
            try:
                bg_l = float(bg_long_acc)
                bg_s = float(bg_short_acc)
                if bg_l > 70:
                    lines.append(t("crowd_overlong", lang, pct=f"{bg_l:.0f}"))
                elif bg_l > 60:
                    lines.append(t("crowd_long", lang, pct=f"{bg_l:.0f}"))
                elif bg_s > 70:
                    lines.append(t("crowd_overshort", lang, pct=f"{bg_s:.0f}"))
                elif bg_s > 60:
                    lines.append(t("crowd_short", lang, pct=f"{bg_s:.0f}"))
                else:
                    lines.append(t("crowd_balance", lang))
            except (ValueError, TypeError):
                pass

    # ETH Gas
    if coin == "ETH" and _has(d.get("eth_gas_avg", "—")):
        eth_gas = d.get("eth_gas_avg", "—")
        try:
            gas_v = int(float(str(eth_gas)))
            if gas_v > 50:
                gas_hint = t("gas_high", lang)
            elif gas_v > 20:
                gas_hint = t("gas_medium", lang)
            else:
                gas_hint = t("gas_low", lang)
            lines.append(f"⛽ Gas: <b>{gas_v} Gwei</b> — {gas_hint}")
        except (ValueError, TypeError):
            pass

    # ── ORDER FLOW (CVD + стены ликвидности) ──
    cvd_val = d.get("cvd_value", "—")
    cvd_trend = d.get("cvd_trend", "—")
    obi_bid = d.get("obi_bid_vol", "—")
    obi_ask = d.get("obi_ask_vol", "—")

    has_cvd = _has(cvd_val) and cvd_val != "—"
    has_obi = _has(obi_bid) and obi_bid != "—" and _has(obi_ask) and obi_ask != "—"

    if has_cvd or has_obi:
        lines.append("")
        _of_title = "── ПОТОК ОРДЕРОВ ──" if lang == "ru" else "── ORDER FLOW ──"
        lines.append(_of_title)

        if has_cvd:
            try:
                cv = float(cvd_val)
                if abs(cv) < 0.1:
                    _trend_icon = "➖"
                    _cvd_hint_ru = "нейтрально"
                    _cvd_hint_en = "neutral"
                elif cv > 0:
                    _trend_icon = "📈"
                    _cvd_hint_ru = "покупатели давят" if cv > 1.0 else "покупатели активны"
                    _cvd_hint_en = "buyers dominate" if cv > 1.0 else "buyers active"
                else:
                    _trend_icon = "📉"
                    _cvd_hint_ru = "продавцы давят" if cv < -1.0 else "продавцы активны"
                    _cvd_hint_en = "sellers dominate" if cv < -1.0 else "sellers active"
                _cvd_hint = _cvd_hint_ru if lang == "ru" else _cvd_hint_en
                lines.append(f"{_trend_icon} CVD (1ч): <b>{cv:+.1f}M</b>")
                lines.append(f"→ {_cvd_hint}")
            except (ValueError, TypeError):
                pass

        # Bid/Ask ликвидность (только если СТАКАН PRO НЕ показан, т.е. не is_pro)
        if has_obi and not is_pro:
            try:
                bid_v = float(obi_bid)
                ask_v = float(obi_ask)
                if bid_v > 0 or ask_v > 0:
                    _bid_lbl = "Bid liquidity" if lang == "en" else "Bid ликвидность"
                    _ask_lbl = "Ask liquidity" if lang == "en" else "Ask ликвидность"
                    lines.append(f"🟢 {_bid_lbl}: <b>${bid_v/1e6:.1f}M</b>")
                    lines.append(f"🔴 {_ask_lbl}: <b>${ask_v/1e6:.1f}M</b>")
            except (ValueError, TypeError):
                pass

        # Support/Resistance walls
        obi_sup_price = d.get("obi_support_price", "")
        obi_res_price = d.get("obi_resistance_price", "")
        obi_sup_vol = d.get("obi_support_vol", "")
        obi_res_vol = d.get("obi_resistance_vol", "")
        if _has(obi_sup_price) and _has(obi_res_price):
            try:
                sp = float(obi_sup_price)
                rp = float(obi_res_price)
                sv = float(obi_sup_vol) if _has(obi_sup_vol) else 0
                rv = float(obi_res_vol) if _has(obi_res_vol) else 0
                _wall_label = "Buy wall" if lang == "en" else "Стена покупок"
                _rwall_label = "Sell wall" if lang == "en" else "Стена продаж"
                def _fmt_wall_price(v):
                    if v >= 1000: return f"${v:,.0f}"
                    elif v >= 1: return f"${v:.2f}"
                    elif v >= 0.01: return f"${v:.4f}"
                    else: return f"${v:.6f}"
                def _fmt_wall_vol(v):
                    if v >= 1e6: return f"${v/1e6:.1f}M"
                    elif v >= 1e3: return f"${v/1e3:.0f}K"
                    else: return f"${v:.0f}"
                if sv > 0:
                    lines.append(f"🟩 {_wall_label}: <b>{_fmt_wall_price(sp)}</b> (<b>{_fmt_wall_vol(sv)}</b>)")
                if rv > 0:
                    lines.append(f"🟥 {_rwall_label}: <b>{_fmt_wall_price(rp)}</b> (<b>{_fmt_wall_vol(rv)}</b>)")
            except (ValueError, TypeError):
                pass

    # ── LIQUIDITY MAP ──
    liq_lvl_shorts = d.get("liq_level_shorts", "")
    liq_lvl_longs = d.get("liq_level_longs", "")
    if liq_lvl_shorts or liq_lvl_longs:
        lines.append("")
        _liq_title = "── КАРТА ЛИКВИДАЦИЙ ──" if lang == "ru" else "── LIQUIDITY MAP ──"
        lines.append(_liq_title)
        if liq_lvl_shorts:
            _sl = "стопы шортов" if lang == "ru" else "short stops"
            lines.append(f"🔥 <b>{html_lib.escape(liq_lvl_shorts)}</b> — {_sl}")
        if liq_lvl_longs:
            _ll = "стопы лонгов" if lang == "ru" else "long stops"
            lines.append(f"🔥 <b>{html_lib.escape(liq_lvl_longs)}</b> — {_ll}")
        _between_ru = "→ цена между кластерами ликвидаций"
        _between_en = "→ price between liquidation clusters"
        lines.append(_between_ru if lang == "ru" else _between_en)

    # ── MARKET STRUCTURE (Spot vs Perp) ──
    spot_vol = d.get("spot_volume", "")
    perp_vol = d.get("perp_volume", "")
    if _has(spot_vol) and _has(perp_vol):
        try:
            sv = float(str(spot_vol).replace(",", ""))
            pv = float(str(perp_vol).replace(",", ""))
            total_v = sv + pv
            if total_v > 0:
                spot_pct = round(sv / total_v * 100)
                lines.append("")
                _ms_title = "── СТРУКТУРА РЫНКА ──" if lang == "ru" else "── MARKET STRUCTURE ──"
                lines.append(_ms_title)
                def _fmt_vol(v):
                    if v >= 1e9: return f"${v/1e9:.1f}B"
                    elif v >= 1e6: return f"${v/1e6:.0f}M"
                    elif v >= 1e3: return f"${v/1e3:.0f}K"
                    else: return f"${v:.0f}"
                lines.append(f"Spot: <b>{_fmt_vol(sv)}</b>  |  Perp: <b>{_fmt_vol(pv)}</b>")
                if spot_pct >= 50:
                    _hint = "рост поддержан реальными покупками" if lang == "ru" else "growth supported by real buys"
                    lines.append(f"→ Spot Dominance: <b>{spot_pct}%</b>")
                    lines.append(f"→ {_hint}")
                else:
                    _hint = "рынок двигают деривативы" if lang == "ru" else "derivatives driving market"
                    lines.append(f"→ Perp Dominance: <b>{100-spot_pct}%</b>")
                    lines.append(f"→ {_hint}")
        except (ValueError, TypeError):
            pass

    # ── УРОВНИ ──
    liq_up = d.get("liq_up", "—")
    liq_dn = d.get("liq_dn", "—")
    if _has(liq_up) or _has(liq_dn):
        lines.append("")
        lines.append(t("section_levels", lang))
        lines.append(f"💥 {t('liq_1h', lang)}")
        if _has(liq_up) and _has(liq_dn):
            try:
                lu = float(str(liq_up).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
                ld = float(str(liq_dn).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
                up_arrow = " ↑" if lu > ld else ""
                dn_arrow = " ↑" if ld > lu else ""
                lines.append(f"🟢 {t('liq_shorts', lang)}: <b>{liq_up}</b>{up_arrow}")
                lines.append(f"🔴 {t('liq_longs', lang)}: <b>{liq_dn}</b>{dn_arrow}")

                # ── Liquidation Pressure ──
                total_liq = lu + ld
                if total_liq > 0:
                    short_pressure = round(lu / total_liq * 100)
                    long_pressure = 100 - short_pressure
                    if short_pressure > 60:
                        _press_hint = "давление вверх (шортов выносят)" if lang == "ru" else "upward pressure (shorts squeezed)"
                        lines.append(f"→ Short pressure: <b>{short_pressure}%</b> — {_press_hint}")
                    elif long_pressure > 60:
                        _press_hint = "давление вниз (лонгов выносят)" if lang == "ru" else "downward pressure (longs liquidated)"
                        lines.append(f"→ Long pressure: <b>{long_pressure}%</b> — {_press_hint}")
            except (ValueError, TypeError):
                lines.append(f"🟢 {t('liq_shorts', lang)}: <b>{liq_up}</b>")
                lines.append(f"🔴 {t('liq_longs', lang)}: <b>{liq_dn}</b>")
        else:
            if _has(liq_up):
                lines.append(f"🟢 {t('liq_shorts', lang)}: <b>{liq_up}</b>")
            if _has(liq_dn):
                lines.append(f"🔴 {t('liq_longs', lang)}: <b>{liq_dn}</b>")

    # ── МАКРО (PRO, все монеты — данные глобальные, берём из BTC) ──
    if is_pro:
        # Глобальные данные хранятся в BTC записи, берём оттуда
        _btc_d = data.get("BTC", {}) if coin != "BTC" else d
        ahr999 = _btc_d.get("ahr999", "—")
        bull_peak = _btc_d.get("bull_peak_ratio", "—")
        btc_bubble = _btc_d.get("bitcoin_bubble", "—")
        etf_flow = _btc_d.get("etf_netflow", "—")
        defi_tvl = _btc_d.get("defi_tvl", "—")
        defi_tvl_chg = _btc_d.get("defi_tvl_change", "—")
        stbl_mcap = _btc_d.get("stablecoin_mcap", "—")

        has_macro = (_has(defi_tvl) or _has(stbl_mcap) or _has(etf_flow))
        has_btc_onchain = coin == "BTC" and (_has(ahr999) or _has(bull_peak) or _has(btc_bubble))
        if has_macro or has_btc_onchain:
            lines.append("")
            if coin == "BTC":
                _oc_title = "── ОН-ЧЕЙН + МАКРО (PRO) ──" if lang == "ru" else "── ON-CHAIN + MACRO (PRO) ──"
            else:
                _oc_title = "── МАКРО (PRO) ──" if lang == "ru" else "── MACRO (PRO) ──"
            lines.append(_oc_title)

            # BTC-only: AHR999, Bull Peak, Bubble
            if coin == "BTC":
                if _has(ahr999):
                    try:
                        a = float(ahr999)
                        if a < 0.45:
                            _a_hint = "дно — идеальная зона покупки" if lang == "ru" else "bottom — ideal buy zone"
                        elif a < 1.2:
                            _a_hint = "зона накопления" if lang == "ru" else "accumulation zone"
                        else:
                            _a_hint = "перегрет — осторожно" if lang == "ru" else "overheated — caution"
                        lines.append(f"📏 AHR999: <b>{a:.2f}</b> — {_a_hint}")
                    except (ValueError, TypeError):
                        pass

                if _has(bull_peak):
                    try:
                        bp = float(bull_peak)
                        if bp > 0.8:
                            _bp_hint = "пик цикла близко" if lang == "ru" else "cycle peak near"
                        elif bp > 0.5:
                            _bp_hint = "средний цикл" if lang == "ru" else "mid-cycle"
                        else:
                            _bp_hint = "ранняя стадия" if lang == "ru" else "early stage"
                        lines.append(f"📊 Bull Peak: <b>{bp:.2f}</b> — {_bp_hint}")
                    except (ValueError, TypeError):
                        pass

                if _has(btc_bubble):
                    try:
                        bb = float(btc_bubble)
                        if bb > 80:
                            _bb_hint = "пузырь — осторожно!" if lang == "ru" else "bubble — caution!"
                        elif bb > 50:
                            _bb_hint = "разогрет" if lang == "ru" else "heated"
                        else:
                            _bb_hint = "норма" if lang == "ru" else "normal"
                        lines.append(f"🫧 Bubble: <b>{bb:.0f}</b>/100 — {_bb_hint}")
                    except (ValueError, TypeError):
                        pass

            # Глобальные: ETF, DeFi, Stablecoins (для всех монет)
            if _has(etf_flow):
                try:
                    ef = float(str(etf_flow).replace("$", "").replace(",", "").replace(" млрд", "e9").replace("млрд", "e9").replace("M", "e6").replace("B", "e9"))
                    _ef_sign = "+" if ef > 0 else ""
                    if ef > 0:
                        _ef_hint = "деньги заходят" if lang == "ru" else "inflow"
                    else:
                        _ef_hint = "деньги уходят" if lang == "ru" else "outflow"
                    lines.append(f"🏦 BTC ETF: <b>{_ef_sign}${abs(ef)/1e6:.0f}M</b> — {_ef_hint}")
                except (ValueError, TypeError):
                    pass

            if _has(defi_tvl):
                try:
                    tvl = float(str(defi_tvl).replace("$", "").replace(",", "").replace(" млрд", "e9").replace("млрд", "e9").replace("B", "e9").replace("M", "e6"))
                    tvl_line = f"🔗 DeFi TVL: <b>${tvl/1e9:.1f}B</b>"
                    if _has(defi_tvl_chg):
                        tvl_line += f" ({defi_tvl_chg})"
                    lines.append(tvl_line)
                except (ValueError, TypeError):
                    pass

            if _has(stbl_mcap):
                try:
                    sm = float(str(stbl_mcap).replace("$", "").replace(",", "").replace(" млрд", "e9").replace("млрд", "e9").replace("B", "e9").replace("T", "e12"))
                    lines.append(f"💵 Stablecoins: <b>${sm/1e9:.0f}B</b>")
                except (ValueError, TypeError):
                    pass

    # ── SOLANA DEFI (PRO, только SOL) ──
    if is_pro and coin == "SOL":
        sol_dex_vol = d.get("sol_dex_volume", "—")
        sol_dex_chg = d.get("sol_dex_volume_change", "—")
        sol_tvl = d.get("sol_tvl", "—")
        sol_tvl_chg = d.get("sol_tvl_change", "—")
        if _has(sol_dex_vol) or _has(sol_tvl):
            lines.append("")
            _sol_title = "── SOLANA DEFI (PRO) ──" if lang == "ru" else "── SOLANA DEFI (PRO) ──"
            lines.append(_sol_title)
            if _has(sol_dex_vol):
                _vol_line = f"📊 DEX Volume (24ч): <b>{sol_dex_vol}</b>"
                if _has(sol_dex_chg) and sol_dex_chg != "—":
                    _vol_line += f" ({sol_dex_chg})"
                lines.append(_vol_line)
            if _has(sol_tvl):
                _tvl_line = f"🔒 TVL Solana: <b>{sol_tvl}</b>"
                if _has(sol_tvl_chg) and sol_tvl_chg != "—":
                    _tvl_line += f" ({sol_tvl_chg})"
                lines.append(_tvl_line)
            # Подсказка по активности
            try:
                _dex_chg_val = float(str(sol_dex_chg).replace("%", "").replace("+", "")) if _has(sol_dex_chg) and sol_dex_chg != "—" else 0
                _tvl_chg_val = float(str(sol_tvl_chg).replace("%", "").replace("+", "")) if _has(sol_tvl_chg) and sol_tvl_chg != "—" else 0
                if _dex_chg_val > 10 or _tvl_chg_val > 2:
                    _hint = "DeFi активность растёт" if lang == "ru" else "DeFi activity growing"
                    lines.append(f"→ {_hint}")
                elif _dex_chg_val < -10 or _tvl_chg_val < -2:
                    _hint = "DeFi активность падает" if lang == "ru" else "DeFi activity declining"
                    lines.append(f"→ {_hint}")
                else:
                    _hint = "DeFi активность стабильна" if lang == "ru" else "DeFi activity stable"
                    lines.append(f"→ {_hint}")
            except (ValueError, TypeError):
                pass

    # Зоны покупки/продажи (если нет конкретных уровней)
    if not entry and not target and not stop:
        if buy_zone or sell_zone:
            lines.append("")
            if buy_zone:
                lines.append(f"{t('buy_label', lang)}: <b>{html_lib.escape(buy_zone)}</b>")
            if sell_zone:
                lines.append(f"{t('sell_label', lang)}: <b>{html_lib.escape(sell_zone)}</b>")

    # ── ОПЦИОНЫ (BTC/ETH) ──
    if coin in ("BTC", "ETH"):
        pcr_val = d.get("options_pcr", "—")
        mp_val = d.get("options_max_pain", "—")
        iv_val = d.get("options_iv", "—")
        oi_calls = d.get("options_oi_calls", "—")
        oi_puts = d.get("options_oi_puts", "—")
        exp_raw = d.get("options_expiries", "—")

        # Парсим экспирации
        parsed_exps = []
        if exp_raw and exp_raw != "—" and isinstance(exp_raw, str) and exp_raw.startswith("["):
            try:
                import ast
                parsed_exps = ast.literal_eval(exp_raw)
                if not isinstance(parsed_exps, list):
                    parsed_exps = []
            except Exception:
                parsed_exps = []

        if _has(pcr_val) and pcr_val != "—":
            lines.append("")
            lines.append(f"<b>{t('options_teaser', lang, coin=coin)}</b>")
            try:
                pcr_f = float(pcr_val)
                if pcr_f < 0.7:
                    pcr_hint = "бычий" if lang == "ru" else "bullish"
                elif pcr_f > 1.0:
                    pcr_hint = "медвежий" if lang == "ru" else "bearish"
                else:
                    pcr_hint = "нейтр." if lang == "ru" else "neutral"
                mp_str = ""
                if _has(mp_val) and mp_val != "—":
                    try:
                        mp_f = float(str(mp_val).replace("$", "").replace(",", ""))
                        mp_str = f" · Max Pain: <b>${mp_f:,.0f}</b>"
                    except (ValueError, TypeError):
                        pass
                lines.append(f"📊 PCR: <b>{pcr_f:.2f}</b> {pcr_hint}{mp_str}")
            except (ValueError, TypeError):
                lines.append(f"📊 PCR: <b>{pcr_val}</b>")

            # Basic: только ближайшая экспирация
            # Pro: все 3 экспирации
            _exp_limit = 3 if is_pro else 1
            for i, e in enumerate(parsed_exps[:_exp_limit]):
                _oi_k = e.get('oi', 0)
                try:
                    _oi_k = float(_oi_k)
                    _oi_str = f"{_oi_k/1000:.1f}K" if _oi_k >= 1000 else f"{_oi_k:,.0f}"
                except (ValueError, TypeError):
                    _oi_str = str(_oi_k)
                _days = e.get('days', '?')
                _warn = " ⚠️" if isinstance(_days, int) and _days <= 3 else ""
                _fire = "🔥" if i == 0 else "📅"
                lines.append(f"{_fire} {e.get('date', '')} · {_oi_str} OI · {_days}д{_warn}")

            # IV
            if _has(iv_val) and iv_val != "—":
                try:
                    iv_f = float(iv_val)
                    if iv_f < 30:
                        iv_hint = "тихо" if lang == "ru" else "quiet"
                    elif iv_f < 60:
                        iv_hint = "норма" if lang == "ru" else "normal"
                    elif iv_f < 80:
                        iv_hint = "повышенная" if lang == "ru" else "elevated"
                    else:
                        iv_hint = "шторм" if lang == "ru" else "extreme"
                    lines.append(f"📈 IV: <b>{iv_f:.0f}%</b> — {iv_hint}")
                except (ValueError, TypeError):
                    pass

            # OI Calls/Puts
            if _has(oi_calls) and oi_calls != "—" and _has(oi_puts) and oi_puts != "—":
                try:
                    oi_c = float(oi_calls)
                    oi_p = float(oi_puts)
                    total_oi = oi_c + oi_p
                    if total_oi > 0:
                        call_pct = round(oi_c / total_oi * 100)
                        lines.append(f"🟢 Calls: <b>{oi_c/1000:,.0f}K</b> ({call_pct}%) · 🔴 Puts: <b>{oi_p/1000:,.0f}K</b> ({100-call_pct}%)")
                except (ValueError, TypeError):
                    pass

    # ══════════════════════════════════════════════════════════════
    # AI-АНАЛИЗ (внизу после всех данных)
    # ══════════════════════════════════════════════════════════════

    # ── ПРИЧИНЫ (AI Score Drivers) ──
    top_bull = d.get("top_factors_bull", "")
    top_bear = d.get("top_factors_bear", "")
    if top_bull or top_bear:
        lines.append("")
        drv_title = "ПРИЧИНЫ" if lang == "ru" else "DRIVERS"
        lines.append(f"<b>🤖 {drv_title}</b>")
        if top_bull:
            bull_lbl = "За рост" if lang == "ru" else "Bullish"
            lines.append(f"  ⬆️ {bull_lbl}: {html_lib.escape(top_bull)}")
        if top_bear:
            bear_lbl = "За падение" if lang == "ru" else "Bearish"
            lines.append(f"  ⬇️ {bear_lbl}: {html_lib.escape(top_bear)}")

    # ── ЧТО ПРОИСХОДИТ (короткий summary) ──
    llm_text = _clean(d.get("llm_text", ""))
    if what_happening:
        # Ограничиваем до 100 символов для компактности
        _wh = what_happening if len(what_happening) <= 100 else what_happening[:97] + "..."
        lines.append("")
        _wh_title = "SUMMARY" if lang == "en" else "ИТОГО"
        lines.append(f"<b>📋 {_wh_title}</b>")
        lines.append(html_lib.escape(_wh))

    # ── ЛОВУШКА ──
    if trap:
        lines.append("")
        lines.append(f"⚠️ <b>{t('trap', lang)}</b>")
        lines.append(html_lib.escape(trap))

    # ── ДАВЛЕНИЕ РЫНКА (визуальная шкала) ──
    prob_bull = d.get("prob_bull")
    prob_bear = d.get("prob_bear")
    if prob_bull is not None and prob_bear is not None:
        try:
            pb = int(float(str(prob_bull)))
            pr = int(float(str(prob_bear)))
            if pb > 0 or pr > 0:
                lines.append("")
                gauge_title = "ДАВЛЕНИЕ РЫНКА" if lang == "ru" else "MARKET PRESSURE"
                lines.append(f"<b>📊 {gauge_title}</b>")
                bull_blocks = round(pb / 10)
                bear_blocks = 10 - bull_blocks
                bar = "█" * bull_blocks + "░" * bear_blocks
                if pb >= pr:
                    lines.append(f"<code>🐂 {bar} {pb}%</code>")
                    if lang == "ru":
                        hint = "быки доминируют" if pb >= 65 else "лёгкое преимущество быков"
                    else:
                        hint = "bulls dominate" if pb >= 65 else "slight bull advantage"
                else:
                    lines.append(f"<code>🐻 {bar} {pr}%</code>")
                    if lang == "ru":
                        hint = "медведи доминируют" if pr >= 65 else "лёгкое преимущество медведей"
                    else:
                        hint = "bears dominate" if pr >= 65 else "slight bear advantage"
                lines.append(f"<i>{hint}</i>")

                # Если давление конфликтует с сигналом — объяснение
                _rec_lower = recommendation.lower() if recommendation else ""
                _is_sell_signal = any(x in _rec_lower for x in ["продав", "sell", "short"])
                _is_buy_signal = any(x in _rec_lower for x in ["покуп", "buy", "long"])
                if _is_sell_signal and pb > pr:
                    _conflict_note = "давление бычье, но тренд и структура медвежьи → сигнал SELL" if lang == "ru" else "pressure bullish, but trend structure bearish → SELL signal"
                    lines.append(f"ℹ️ {_conflict_note}")
                elif _is_buy_signal and pr > pb:
                    _conflict_note = "давление медвежье, но структура разворачивается → сигнал BUY" if lang == "ru" else "pressure bearish, but structure reversing → BUY signal"
                    lines.append(f"ℹ️ {_conflict_note}")
        except (ValueError, TypeError):
            pass

    # ── КОНФЛИКТ FUNDING ──
    funding_conflict = _clean(d.get("funding_conflict", ""))
    if funding_conflict:
        lines.append(f"ℹ️ {html_lib.escape(funding_conflict)}")

    # ══════════════════════════════════════════════════════════════
    # PRO-АНАЛИТИКА (AI Engine — только в Pro)
    # ══════════════════════════════════════════════════════════════

    # ══════════════════════════════════════════════════════════════
    # ⚡ ZENDER MARKET ENGINE (PRO) — единый индекс давления рынка
    # Комбинирует 7 источников: funding, CVD, OI, liquidations,
    # whales, options PCR, order book в один индекс
    # ══════════════════════════════════════════════════════════════
    if is_pro:
        _pressure_signals = []  # (direction, weight) — direction: 1=bullish, -1=bearish
        _pressure_reasons_ru = []
        _pressure_reasons_en = []

        # 1. Funding (вес 1.5)
        try:
            _fr = float(str(d.get("funding_rate", "0")).replace("%", "").replace("+", ""))
            if _fr > 0.01:
                _pressure_signals.append((-1, 1.5))
                _pressure_reasons_ru.append("funding: лонги перегружены")
                _pressure_reasons_en.append("funding: longs overloaded")
            elif _fr < -0.005:
                _pressure_signals.append((1, 1.5))
                _pressure_reasons_ru.append("funding: шорты платят")
                _pressure_reasons_en.append("funding: shorts paying")
        except (ValueError, TypeError):
            pass

        # 2. CVD (вес 1.0)
        try:
            _cv = float(str(d.get("cvd_value", "0")))
            if _cv > 0.5:
                _pressure_signals.append((1, 1.0))
                _pressure_reasons_ru.append("CVD: покупатели")
                _pressure_reasons_en.append("CVD: buyers")
            elif _cv < -0.5:
                _pressure_signals.append((-1, 1.0))
                _pressure_reasons_ru.append("CVD: продавцы")
                _pressure_reasons_en.append("CVD: sellers")
        except (ValueError, TypeError):
            pass

        # 3. OI change (вес 0.8)
        try:
            _oi = float(str(d.get("oi_change", "0")).replace("%", "").replace("+", ""))
            if _oi > 1.0:
                _pressure_signals.append((1, 0.8))
                _pressure_reasons_ru.append("OI растёт")
                _pressure_reasons_en.append("OI rising")
            elif _oi < -1.0:
                _pressure_signals.append((-1, 0.8))
                _pressure_reasons_ru.append("OI падает")
                _pressure_reasons_en.append("OI falling")
        except (ValueError, TypeError):
            pass

        # 4. Liquidations (вес 1.2)
        try:
            _lu = float(str(d.get("liq_up", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
            _ld = float(str(d.get("liq_dn", "0")).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6").replace(" млрд", "e9").replace("млрд", "e9"))
            if _lu + _ld > 0:
                _liq_ratio = _lu / (_lu + _ld)
                if _liq_ratio > 0.6:
                    _pressure_signals.append((1, 1.2))
                    _pressure_reasons_ru.append("liq: шорты")
                    _pressure_reasons_en.append("liq: shorts")
                elif _liq_ratio < 0.4:
                    _pressure_signals.append((-1, 1.2))
                    _pressure_reasons_ru.append("liq: лонги")
                    _pressure_reasons_en.append("liq: longs")
        except (ValueError, TypeError):
            pass

        # 5. Whales (вес 1.5)
        _wd = d.get("whale_direction", "")
        if _wd == "bullish":
            _pressure_signals.append((1, 1.5))
            _pressure_reasons_ru.append("whales: вывод")
            _pressure_reasons_en.append("whales: withdrawing")
        elif _wd == "bearish":
            _pressure_signals.append((-1, 1.5))
            _pressure_reasons_ru.append("whales: ввод")
            _pressure_reasons_en.append("whales: depositing")

        # 6. Options PCR (вес 1.0) — если есть
        try:
            _pcr = float(str(d.get("options_pcr", "0")))
            if _pcr > 0:
                if _pcr > 1.0:
                    _pressure_signals.append((-1, 1.0))
                    _pressure_reasons_ru.append("options: puts > calls")
                    _pressure_reasons_en.append("options: puts > calls")
                elif _pcr < 0.6:
                    _pressure_signals.append((1, 1.0))
                    _pressure_reasons_ru.append("options: calls > puts")
                    _pressure_reasons_en.append("options: calls > puts")
        except (ValueError, TypeError):
            pass

        # 7. Order Book Imbalance (вес 0.8)
        try:
            _obi_v = float(str(d.get("obi_value", "0")))
            if _obi_v > 0.2:
                _pressure_signals.append((1, 0.8))
                _pressure_reasons_ru.append("OB: bids")
                _pressure_reasons_en.append("OB: bids")
            elif _obi_v < -0.2:
                _pressure_signals.append((-1, 0.8))
                _pressure_reasons_ru.append("OB: asks")
                _pressure_reasons_en.append("OB: asks")
        except (ValueError, TypeError):
            pass

        # ── Считаем итоговый индекс ──
        _engine_bull_pct = 50  # default
        if _pressure_signals:
            _total_weight = sum(w for _, w in _pressure_signals)
            _weighted_sum = sum(d_ * w for d_, w in _pressure_signals)
            if _total_weight > 0:
                _raw_score = _weighted_sum / _total_weight  # от -1 до 1
                # Сжатая шкала: max ~80% для сильного сигнала (трейдеры доверяют)
                _engine_bull_pct = int(50 + _raw_score * 30)
                _engine_bull_pct = max(15, min(85, _engine_bull_pct))
                _engine_bear_pct = 100 - _engine_bull_pct

                lines.append("")
                lines.append("⚡ <b>ZENDER MARKET ENGINE</b>")

                # Один главный бар — доминирующая сторона
                if _engine_bull_pct >= _engine_bear_pct:
                    _dom_blocks = round(_engine_bull_pct / 10)
                    _dom_bar = "█" * _dom_blocks + "░" * (10 - _dom_blocks)
                    lines.append(f"<code>🐂 {_dom_bar} {_engine_bull_pct}%</code>")
                    if _engine_bull_pct >= 70:
                        _regime = "STRONG BULL" if lang == "en" else "СИЛЬНЫЕ БЫКИ"
                    elif _engine_bull_pct >= 55:
                        _regime = "MILD BULL" if lang == "en" else "УМЕРЕННЫЕ БЫКИ"
                    else:
                        _regime = "NEUTRAL" if lang == "en" else "НЕЙТРАЛЬНО"
                else:
                    _dom_blocks = round(_engine_bear_pct / 10)
                    _dom_bar = "█" * _dom_blocks + "░" * (10 - _dom_blocks)
                    lines.append(f"<code>🐻 {_dom_bar} {_engine_bear_pct}%</code>")
                    if _engine_bear_pct >= 70:
                        _regime = "STRONG BEAR" if lang == "en" else "СИЛЬНЫЕ МЕДВЕДИ"
                    elif _engine_bear_pct >= 55:
                        _regime = "MILD BEAR" if lang == "en" else "УМЕРЕННЫЕ МЕДВЕДИ"
                    else:
                        _regime = "NEUTRAL" if lang == "en" else "НЕЙТРАЛЬНО"
                lines.append(f"→ <b>{_regime}</b>")

                # Signal confirmation
                _rec_lower_eng = recommendation.lower() if recommendation else ""
                _is_sell = any(x in _rec_lower_eng for x in ["продав", "sell", "short"])
                _is_buy = any(x in _rec_lower_eng for x in ["покуп", "buy", "long"])
                if _is_buy and _engine_bull_pct >= 55:
                    _conf = "✅ signal confirmed by market pressure" if lang == "en" else "✅ сигнал подтверждён давлением рынка"
                    lines.append(_conf)
                elif _is_sell and _engine_bear_pct >= 55:
                    _conf = "✅ signal confirmed by market pressure" if lang == "en" else "✅ сигнал подтверждён давлением рынка"
                    lines.append(_conf)
                elif _is_buy and _engine_bear_pct >= 55:
                    _conf = "⚠️ signal conflicts with market pressure" if lang == "en" else "⚠️ сигнал конфликтует с давлением рынка"
                    lines.append(_conf)
                elif _is_sell and _engine_bull_pct >= 55:
                    _conf = "⚠️ signal conflicts with market pressure" if lang == "en" else "⚠️ сигнал конфликтует с давлением рынка"
                    lines.append(_conf)

                # Топ причины (компактные)
                _reasons = _pressure_reasons_ru if lang == "ru" else _pressure_reasons_en
                if _reasons:
                    lines.append(f"<i>{' · '.join(_reasons[:4])}</i>")

    # ── PRE-MOVE DETECTOR (PRO) ──
    # Если OI spike + volume/funding shift + цена почти стоит = движение готовится
    if is_pro:
        _premove_flags = []
        _premove_hints_ru = []
        _premove_hints_en = []
        try:
            _price_chg = abs(float(str(change).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-")))
            _oi_chg_val = abs(float(str(d.get("oi_change", "0")).replace("%", "").replace("+", "")))
            _fr_abs = abs(float(str(d.get("funding_rate", "0")).replace("%", "").replace("+", "")))

            # Цена почти стоит но OI растёт
            if _price_chg < 1.0 and _oi_chg_val > 2.0:
                _premove_flags.append("oi_spike")
                _premove_hints_ru.append(f"OI: +{_oi_chg_val:.1f}% при цене {_price_chg:.1f}%")
                _premove_hints_en.append(f"OI: +{_oi_chg_val:.1f}% while price {_price_chg:.1f}%")

            # Funding перегружен
            if _fr_abs > 0.03:
                _premove_flags.append("funding_extreme")
                _premove_hints_ru.append(f"фандинг экстремальный: {d.get('funding_rate', '')}")
                _premove_hints_en.append(f"extreme funding: {d.get('funding_rate', '')}")

            # CVD сменил направление при плоской цене
            _cv_val = float(str(d.get("cvd_value", "0")))
            if _price_chg < 1.0 and abs(_cv_val) > 2.0:
                _premove_flags.append("cvd_divergence")
                _dir = "покупатели" if _cv_val > 0 else "продавцы"
                _dir_en = "buyers" if _cv_val > 0 else "sellers"
                _premove_hints_ru.append(f"CVD: {_dir} давят, цена стоит")
                _premove_hints_en.append(f"CVD: {_dir_en} pressing, price flat")

        except (ValueError, TypeError):
            pass

        if len(_premove_flags) >= 2:
            lines.append("")
            _pm_title = "── ⚡ EARLY SIGNAL (PRO) ──" if lang == "ru" else "── ⚡ EARLY SIGNAL (PRO) ──"
            lines.append(_pm_title)
            _pm_label = "движение готовится" if lang == "ru" else "move building"
            lines.append(f"🔮 <b>PRE-MOVE: {_pm_label}</b>")
            _hints = _premove_hints_ru if lang == "ru" else _premove_hints_en
            for h in _hints:
                lines.append(f"  → {h}")
            # Направление из pressure
            if _pressure_signals:
                _ws = sum(d_ * w for d_, w in _pressure_signals)
                if _ws > 0.3:
                    _pm_dir = "вероятен рост (long squeeze шортов)" if lang == "ru" else "likely up (short squeeze)"
                elif _ws < -0.3:
                    _pm_dir = "вероятно падение (слив лонгов)" if lang == "ru" else "likely down (long liquidation)"
                else:
                    _pm_dir = "направление неясно" if lang == "ru" else "direction unclear"
                lines.append(f"  ⚡ {_pm_dir}")

    # ── MARKET REGIME (PRO) — Trend + Volatility + Liquidity ──
    if is_pro:
        try:
            _rsi = float(d.get("rsi", "50"))
            _price_chg_regime = abs(float(str(change).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-")))

            # TREND
            if _rsi > 65:
                _trend = "BULLISH"
                _trend_icon = "📈"
            elif _rsi < 35:
                _trend = "BEARISH"
                _trend_icon = "📉"
            elif _rsi > 55:
                _trend = "MILDLY BULLISH"
                _trend_icon = "↗️"
            elif _rsi < 45:
                _trend = "MILDLY BEARISH"
                _trend_icon = "↘️"
            else:
                _trend = "SIDEWAYS"
                _trend_icon = "➡️"

            # VOLATILITY
            if _price_chg_regime > 5:
                _vol = "EXTREME"
                _vol_icon = "🔥"
            elif _price_chg_regime > 3:
                _vol = "HIGH"
                _vol_icon = "⚡"
            elif _price_chg_regime > 1:
                _vol = "MEDIUM"
                _vol_icon = "📊"
            else:
                _vol = "LOW"
                _vol_icon = "😴"

            # LIQUIDITY (из OBI — стакан)
            try:
                _obi_v = float(str(d.get("obi_value", "0")))
                if _obi_v > 0.2:
                    _liq = "STRONG BIDS"
                    _liq_icon = "🟢"
                elif _obi_v < -0.2:
                    _liq = "STRONG ASKS"
                    _liq_icon = "🔴"
                else:
                    _liq = "BALANCED"
                    _liq_icon = "⚖️"
            except (ValueError, TypeError):
                _liq = "NORMAL"
                _liq_icon = "⚖️"

            lines.append("")
            _mr_title = "── MARKET REGIME (PRO) ──" if lang == "ru" else "── MARKET REGIME (PRO) ──"
            lines.append(_mr_title)
            lines.append(f"{_trend_icon} Trend: <b>{_trend}</b>")
            lines.append(f"{_vol_icon} Volatility: <b>{_vol}</b>")
            lines.append(f"{_liq_icon} Liquidity: <b>{_liq}</b>")
        except (ValueError, TypeError):
            pass

    # ── FUNDING HEAT (PRO) ──
    if is_pro:
        try:
            _fr_heat = float(str(d.get("funding_rate", "0")).replace("%", "").replace("+", ""))
            # Funding percentile approximation: |FR| < 0.01 = normal, 0.01-0.03 = elevated, >0.03 = extreme
            if abs(_fr_heat) > 0.005:
                _fr_pct = min(99, int(50 + abs(_fr_heat) * 1500))  # rough percentile
                lines.append("")
                _fh_title = "── FUNDING HEAT (PRO) ──" if lang == "ru" else "── FUNDING HEAT (PRO) ──"
                lines.append(_fh_title)
                _fh_bar = "█" * round(_fr_pct / 10) + "░" * (10 - round(_fr_pct / 10))
                lines.append(f"<code>HEAT {_fh_bar} {_fr_pct}%</code>")
                if _fr_heat > 0.03:
                    _fh_hint = "extreme long crowding — ликвидации возможны" if lang == "ru" else "extreme long crowding — liquidations likely"
                elif _fr_heat > 0.01:
                    _fh_hint = "лонги перегружены — повышенный риск" if lang == "ru" else "longs overleveraged — elevated risk"
                elif _fr_heat < -0.02:
                    _fh_hint = "extreme short crowding — squeeze вероятен" if lang == "ru" else "extreme short crowding — squeeze likely"
                elif _fr_heat < -0.005:
                    _fh_hint = "шорты перегружены — squeeze возможен" if lang == "ru" else "shorts crowded — squeeze possible"
                else:
                    _fh_hint = ""
                if _fh_hint:
                    lines.append(f"→ {_fh_hint}")
                # Контекст конфликта с сигналом
                _rec_lower2 = recommendation.lower() if recommendation else ""
                if _fr_heat < -0.01 and any(x in _rec_lower2 for x in ["продав", "sell", "short"]):
                    _fh_ctx = "funding squeeze risk, но тренд медвежий" if lang == "ru" else "funding squeeze risk, but trend bearish"
                    lines.append(f"ℹ️ {_fh_ctx}")
        except (ValueError, TypeError):
            pass

    # ── MARKET CONTEXT (PRO) — BTC/ETH/Alts overview ──
    if is_pro and coin != "BTC":
        btc_d = data.get("BTC", {})
        eth_d = data.get("ETH", {})
        _btc_chg = _clean(btc_d.get("change", ""))
        _eth_chg = _clean(eth_d.get("change", ""))
        # Фикс: "—" и пустые строки не считаются валидными
        if not _has(_btc_chg): _btc_chg = ""
        if not _has(_eth_chg): _eth_chg = ""
        if _btc_chg or _eth_chg:
            lines.append("")
            _mc_title = "── КОНТЕКСТ РЫНКА (PRO) ──" if lang == "ru" else "── MARKET CONTEXT (PRO) ──"
            lines.append(_mc_title)
            # BTC trend
            try:
                _btc_rsi = float(btc_d.get("rsi", "50"))
                if _btc_rsi > 60:
                    _btc_trend = "бычий" if lang == "ru" else "bullish"
                elif _btc_rsi < 40:
                    _btc_trend = "медвежий" if lang == "ru" else "bearish"
                else:
                    _btc_trend = "нейтральный" if lang == "ru" else "neutral"
            except (ValueError, TypeError):
                _btc_trend = "—"
            _btc_suffix = f" ({_btc_chg})" if _btc_chg else ""
            lines.append(f"BTC: <b>{_btc_trend}</b>{_btc_suffix}")
            # ETH trend
            if coin != "ETH":
                try:
                    _eth_rsi = float(eth_d.get("rsi", "50"))
                    if _eth_rsi > 60:
                        _eth_trend = "бычий" if lang == "ru" else "bullish"
                    elif _eth_rsi < 40:
                        _eth_trend = "медвежий" if lang == "ru" else "bearish"
                    else:
                        _eth_trend = "нейтральный" if lang == "ru" else "neutral"
                except (ValueError, TypeError):
                    _eth_trend = "—"
                _eth_suffix = f" ({_eth_chg})" if _eth_chg else ""
                lines.append(f"ETH: <b>{_eth_trend}</b>{_eth_suffix}")
            # Alts strength (используем Fear & Greed)
            try:
                _fg = int(float(str(d.get("fear_greed", "50"))))
                if _fg > 60:
                    _alt_str = "сильные" if lang == "ru" else "strong"
                elif _fg > 40:
                    _alt_str = "умеренные" if lang == "ru" else "moderate"
                else:
                    _alt_str = "слабые" if lang == "ru" else "weak"
            except (ValueError, TypeError):
                _alt_str = "—"
            _alt_lbl = "Alts strength" if lang == "en" else "Сила альтов"
            lines.append(f"{_alt_lbl}: <b>{_alt_str}</b>")

    # ── MARKET SPEED (PRO) — моментум ──
    if is_pro:
        _chg_str = str(change).replace("%", "").replace("+", "").replace("−", "-").replace("–", "-")
        try:
            _chg_val = float(_chg_str)
            _oi_chg_str = str(d.get("oi_change", "0")).replace("%", "").replace("+", "")
            _oi_chg_f = float(_oi_chg_str)
            # Показываем если движение заметное
            if abs(_chg_val) > 0.3 or abs(_oi_chg_f) > 1.0:
                lines.append("")
                _ms_title = "── MARKET SPEED (PRO) ──"
                lines.append(_ms_title)
                _price_dir = "↗️" if _chg_val > 0 else "↘️"
                lines.append(f"{_price_dir} Price move: <b>{change}</b>")
                _oi_dir = "↗️" if _oi_chg_f > 0 else "↘️"
                lines.append(f"{_oi_dir} OI change: <b>{d.get('oi_change', '—')}</b>")
                # Momentum interpretation
                if _chg_val < -1 and _oi_chg_f > 1:
                    _mom = "downside momentum + new shorts" if lang == "en" else "нисходящий импульс + новые шорты"
                elif _chg_val > 1 and _oi_chg_f > 1:
                    _mom = "upside momentum + new longs" if lang == "en" else "восходящий импульс + новые лонги"
                elif _chg_val < -1 and _oi_chg_f < -1:
                    _mom = "long liquidation cascade" if lang == "en" else "каскад ликвидаций лонгов"
                elif _chg_val > 1 and _oi_chg_f < -1:
                    _mom = "short squeeze" if lang == "en" else "сквиз шортов"
                elif abs(_chg_val) < 0.5:
                    _mom = "consolidation" if lang == "en" else "консолидация"
                else:
                    _mom = ""
                if _mom:
                    lines.append(f"→ {_mom}")
        except (ValueError, TypeError):
            pass

    # ── PRO: полный LLM-анализ ──
    if is_pro:
        _pro_text = _clean(d.get("llm_text_pro", ""))
        if _pro_text:
            lines.append("")
            _ai_full_title = "🤖 AI-АНАЛИЗ (ПОЛНЫЙ)" if lang == "ru" else "🤖 AI ANALYSIS (FULL)"
            lines.append(f"<b>{_ai_full_title}</b>")
            lines.append(html_lib.escape(_pro_text))
        elif llm_text:
            lines.append("")
            _ai_full_title = "🤖 AI-АНАЛИЗ" if lang == "ru" else "🤖 AI ANALYSIS"
            lines.append(f"<b>{_ai_full_title}</b>")
            lines.append(html_lib.escape(llm_text))

    lines.append("")
    lines.append("⚡ <b>Zender Terminal</b>")
    lines.append("t.me/ZenderTerminal_bot")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# ОПЦИОНЫ — ПОЛНЫЙ ЭКРАН
# ══════════════════════════════════════════════════════════════════════════════

def text_options_detail(coin: str, data: dict, lang: str = "ru", ai_text: str = "") -> str:
    """Полный экран опционов для BTC/ETH — терминальный стиль."""
    d = data.get(coin, {})
    price = d.get("price", "—")

    lines = [
        f"⚡️ <b>ZENDER TERMINAL · {coin} OPTIONS</b>",
        "",
    ]

    # ── PCR ──
    pcr_val = d.get("options_pcr", "—")
    pcr_f = None
    if _has(pcr_val) and pcr_val != "—":
        try:
            pcr_f = float(pcr_val)
            lines.append(f"📊 <b>Put/Call Ratio:</b> <b>{pcr_f:.2f}</b>")
            if pcr_f < 0.7:
                lines.append(f"→ {t('options_pcr_bullish', lang)}")
            elif pcr_f > 1.0:
                lines.append(f"→ {t('options_pcr_bearish', lang)}")
            else:
                lines.append(f"→ {t('options_pcr_neutral', lang)}")
        except (ValueError, TypeError):
            pass

    # ── Max Pain ──
    mp_val = d.get("options_max_pain", "—")
    mp_f = None
    diff_pct = 0
    if _has(mp_val) and mp_val != "—":
        try:
            mp_f = float(str(mp_val).replace("$", "").replace(",", ""))
            price_f = float(str(price).replace("$", "").replace(",", ""))
            diff_pct = ((mp_f - price_f) / price_f) * 100
            lines.append("")
            lines.append(f"🎯 <b>Max Pain:</b> <b>${mp_f:,.0f}</b>")
            _price_label = "текущая цена" if lang == "ru" else "current price"
            lines.append(f"{_price_label}: {price}")
            lines.append("")
            if abs(diff_pct) < 0.5:
                lines.append(f"→ {t('options_maxpain_at', lang)}")
            elif diff_pct > 0:
                lines.append(f"→ цена ниже Max Pain на {abs(diff_pct):.1f}%" if lang == "ru" else f"→ price below Max Pain by {abs(diff_pct):.1f}%")
                lines.append(f"→ рынок часто тянется к Max Pain перед экспирацией" if lang == "ru" else f"→ market often gravitates to Max Pain before expiry")
            else:
                lines.append(f"→ цена выше Max Pain на {abs(diff_pct):.1f}%" if lang == "ru" else f"→ price above Max Pain by {abs(diff_pct):.1f}%")
        except (ValueError, TypeError):
            pass

    # ── IV ──
    iv_val = d.get("options_iv", "—")
    if _has(iv_val) and iv_val != "—":
        try:
            iv_f = float(iv_val)
            lines.append("")
            if iv_f < 30:
                iv_hint = t("options_iv_low", lang)
            elif iv_f < 60:
                iv_hint = t("options_iv_normal", lang)
            elif iv_f < 80:
                iv_hint = t("options_iv_high", lang)
            else:
                iv_hint = t("options_iv_extreme", lang)
            lines.append(f"📈 <b>Implied Volatility:</b> <b>{iv_f:.0f}%</b>")
            lines.append(f"→ {iv_hint}")
        except (ValueError, TypeError):
            pass

    # ── OPEN INTEREST ──
    oi_calls = d.get("options_oi_calls", "—")
    oi_puts = d.get("options_oi_puts", "—")
    oi_c = None
    oi_p = None
    if _has(oi_calls) and _has(oi_puts) and oi_calls != "—" and oi_puts != "—":
        try:
            oi_c = float(oi_calls)
            oi_p = float(oi_puts)
            lines.append("")
            lines.append(f"<b>📊 OPEN INTEREST</b>")
            lines.append("")
            lines.append(f"🟢 Calls: <b>{oi_c/1000:,.0f}K</b>")
            lines.append(f"🔴 Puts: <b>{oi_p/1000:,.0f}K</b>")
            lines.append("")
            total_oi = oi_c + oi_p
            if total_oi > 0:
                call_pct = round(oi_c / total_oi * 100)
                put_pct = 100 - call_pct
                lines.append(f"<code>Calls: {call_pct}%  |  Puts: {put_pct}%</code>")
            if oi_c > oi_p * 1.3:
                lines.append(f"→ {t('options_oi_bulls', lang)}")
            elif oi_p > oi_c * 1.3:
                lines.append(f"→ {t('options_oi_bears', lang)}")
            else:
                lines.append(f"→ {t('options_oi_balanced', lang)}")
        except (ValueError, TypeError):
            pass

    # ── ЭКСПИРАЦИИ ──
    exp_raw = d.get("options_expiries", "—")
    if exp_raw and exp_raw != "—":
        exps = None
        if isinstance(exp_raw, list):
            exps = exp_raw
        elif isinstance(exp_raw, str) and exp_raw.startswith("["):
            try:
                import ast
                exps = ast.literal_eval(exp_raw)
            except Exception:
                pass
        if exps and isinstance(exps, list):
            lines.append("")
            _exp_title = "⏳ ЭКСПИРАЦИИ" if lang == "ru" else "⏳ EXPIRATIONS"
            lines.append(f"<b>{_exp_title}</b>")
            lines.append("")
            for e in exps:
                date_str = e.get("date", "")
                oi_val = e.get("oi", 0)
                days = e.get("days", 99)
                is_max = e.get("is_max", False)

                icon = "🔥" if is_max else ("⚠️" if days <= 3 else "📅")
                line = f"{icon} {date_str}"
                line += f"\nOI: {oi_val:,} · {t('options_exp_days', lang, days=days)}"
                lines.append(line)

                if days <= 3:
                    lines.append(f"→ {t('options_exp_warning', lang)}")
                elif is_max:
                    lines.append(f"→ {t('options_exp_max', lang)}")
                lines.append("")

    # ── OPTION BIAS ──
    if pcr_f is not None:
        lines.append(f"<b>🎯 OPTION BIAS</b>")
        lines.append("")
        if pcr_f < 0.5:
            bull_pct = 75
        elif pcr_f < 0.7:
            bull_pct = 65
        elif pcr_f < 0.85:
            bull_pct = 58
        elif pcr_f <= 1.0:
            bull_pct = 50
        elif pcr_f < 1.3:
            bull_pct = 40
        else:
            bull_pct = 30
        # Boost if Max Pain above price
        if mp_f and diff_pct > 2:
            bull_pct = min(85, bull_pct + 5)
        elif mp_f and diff_pct < -2:
            bull_pct = max(15, bull_pct - 5)
        bear_pct = 100 - bull_pct
        lines.append(f"📈 Bullish: {bull_pct}%")
        lines.append(f"📉 Bearish: {bear_pct}%")
        if mp_f:
            lines.append("")
            _target_label = "🎯 TARGET MAGNET" if lang == "en" else "🎯 МАГНИТ ЦЕНЫ"
            lines.append(f"{_target_label}: ${mp_f:,.0f} (Max Pain)")
        lines.append("")

    # ── AI-АНАЛИЗ ──
    if ai_text:
        _ai_title = "🤖 AI-АНАЛИЗ ОПЦИОНОВ" if lang == "ru" else "🤖 AI OPTIONS ANALYSIS"
        lines.append(f"<b>{_ai_title}</b>")
        lines.append(html_lib.escape(ai_text))
        lines.append("")

    lines.append("⚡ <b>Zender Terminal</b>")
    lines.append("@ZenderTerminalBot")

    return "\n".join(lines)


def kb_options(coin: str, lang: str = "ru"):
    """Кнопки под экраном опционов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_back_coin", lang, coin=coin), callback_data=f"coin_{coin}"),
            InlineKeyboardButton(text=t("btn_refresh", lang), callback_data=f"options_{coin}"),
        ],
    ])


# ══════════════════════════════════════════════════════════════════════════════
# ХЕНДЛЕРЫ КОМАНД
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Приветствие и регистрация пользователя"""
    user = message.from_user
    # Определяем язык из TG
    detected_lang = detect_language(user.language_code)
    await db.upsert_user(
        telegram_id=user.id,
        username=user.username or "",
        first_name=user.first_name or "",
        language=detected_lang,
    )
    log.info(f"New user: {user.id} @{user.username} lang={detected_lang}")
    await message.answer(
        t("welcome", detected_lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_main(detected_lang),
    )


@dp.message(Command("summary"))
async def cmd_summary(message: Message):
    lang = await get_user_lang(message.from_user.id)
    coins = COINS
    data  = await db.get_market_data(coins)
    await message.answer(
        text_radar(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_radar(page=0, lang=lang, data=data)
    )


# Команды-тикеры: /BTC, /ETH, /RUNE и т.д. — быстрый вход в карточку монеты
@dp.message(F.text.regexp(r"^/([A-Za-z]{2,5})$"))
async def cmd_coin_shortcut(message: Message):
    """Быстрый вход в карточку монеты по команде /BTC, /ETH и т.д."""
    try:
        # Извлекаем тикер из команды: /btc → BTC, /rune → RUNE
        coin = message.text.strip("/").split()[0].upper()
        if coin not in COINS:
            return
        lang = await get_user_lang(message.from_user.id)
        view_mode = await db.get_view_mode(message.from_user.id)
        # Определяем страницу этой монеты
        try:
            idx = COINS.index(coin)
            page = idx // COINS_PER_PAGE
        except ValueError:
            page = 0
        coins_to_load = [coin] if (coin == "BTC" or view_mode != "pro") else [coin, "BTC"]
        data = await db.get_market_data(coins_to_load)
        await message.answer(
            text_coin_analysis(coin, data, lang, view_mode=view_mode),
            parse_mode=ParseMode.HTML,
            link_preview_options=NO_PREVIEW,
            reply_markup=kb_coin_detail(coin, page=page, lang=lang, view_mode=view_mode, data=data)
        )
    except Exception as e:
        log.error(f"cmd_coin_shortcut error for '{message.text}': {e}")
        await message.answer(f"Ошибка при загрузке карточки. Попробуйте через кнопку.")


@dp.message(Command("scanner"))
async def cmd_scanner(message: Message):
    lang = await get_user_lang(message.from_user.id)
    coins = COINS
    data = await db.get_market_data(coins)
    await message.answer(
        text_scanner(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_scanner(page=0, lang=lang, data=data)
    )


@dp.message(Command("danger"))
async def cmd_danger(message: Message):
    lang = await get_user_lang(message.from_user.id)
    coins = COINS
    data = await db.get_market_data(coins)
    await message.answer(
        text_danger_center(data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_danger(lang=lang)
    )


@dp.message(Command("settings"))
async def cmd_settings(message: Message):
    user_id = message.from_user.id
    lang = await get_user_lang(user_id)
    user    = await db.get_user(user_id)
    plan    = user.get("plan", "free") if user else "free"
    interval= user.get("interval", 60) if user else 60
    alerts  = user.get("alerts_enabled", True) if user else True
    alert_status = t("alerts_enabled", lang) if alerts else t("alerts_disabled", lang)
    await message.answer(
        f"<b>{t('settings_title', lang)}</b>\n\n"
        f"{t('settings_plan', lang)}: <b>{plan.upper()}</b>\n"
        f"{t('settings_interval', lang)}: <b>{t('settings_every', lang, interval=interval)}</b>\n"
        f"{t('settings_alerts', lang)}: <b>{alert_status}</b>\n\n"
        f"{t('settings_choose_interval', lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_settings(alerts, lang)
    )


@dp.message(Command("status"))
async def cmd_status(message: Message):
    user_id = message.from_user.id
    lang = await get_user_lang(user_id)
    user = await db.get_user(user_id)
    if not user:
        await message.answer(t("not_registered", lang))
        return
    plan     = user.get("plan", "free")
    coins    = user.get("coins", [])
    interval = user.get("interval", 60)
    await message.answer(
        f"<b>{t('status_title', lang)}</b>\n\n"
        f"{t('status_plan', lang)}: <b>{plan.upper()}</b>\n"
        f"{t('status_coins', lang)}: <b>{len(coins)}</b>\n"
        f"{t('status_interval', lang)}: <b>{interval} мин</b>",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_main(lang)
    )


# ══════════════════════════════════════════════════════════════════════════════
# ХЕНДЛЕРЫ CALLBACK
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data == "summary")
async def cb_summary(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_radar(page=0, lang=lang, data=data)
    )
    await call.answer()


@dp.callback_query(F.data == "radar")
async def cb_radar(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_radar(page=0, lang=lang, data=data)
    )
    await call.answer()


@dp.callback_query(F.data == "refresh")
async def cb_refresh(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_radar(page=0, lang=lang, data=data)
    )
    await call.answer(t("refreshed", lang))


@dp.callback_query(F.data == "scanner")
async def cb_scanner(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coins = COINS
    data = await db.get_market_data(coins)
    await call.message.edit_text(
        text_scanner(coins, data, lang, filter_type="all"),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_scanner(page=0, lang=lang, data=data, filter_type="all")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("scan_filter:"))
async def cb_scan_filter(call: CallbackQuery):
    """Фильтр категорий в сканере."""
    lang = await get_user_lang(call.from_user.id)
    filter_type = call.data.split(":")[1]
    coins = COINS
    data = await db.get_market_data(coins)
    await call.message.edit_text(
        text_scanner(coins, data, lang, filter_type=filter_type),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_scanner(page=0, lang=lang, data=data, filter_type=filter_type)
    )
    await call.answer()


@dp.callback_query(F.data == "danger")
async def cb_danger(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coins = COINS
    data = await db.get_market_data(coins)
    await call.message.edit_text(
        text_danger_center(data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_danger(lang=lang)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("page_"))
async def cb_page(call: CallbackQuery):
    """Пагинация монет"""
    lang = await get_user_lang(call.from_user.id)
    page = int(call.data.replace("page_", ""))
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data, lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_radar(page=page, lang=lang, data=data)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("coin_"))
async def cb_coin(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    view_mode = await db.get_view_mode(call.from_user.id)
    coin = call.data.replace("coin_", "")
    # Определяем страницу этой монеты
    try:
        idx = COINS.index(coin)
        page = idx // COINS_PER_PAGE
    except ValueError:
        page = 0
    # Pro: загружаем и BTC для глобальных макро-данных
    coins_to_load = [coin] if (coin == "BTC" or view_mode != "pro") else [coin, "BTC"]
    data = await db.get_market_data(coins_to_load)
    await call.message.edit_text(
        text_coin_analysis(coin, data, lang, view_mode=view_mode),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_coin_detail(coin, page=page, lang=lang, view_mode=view_mode, data=data)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("viewmode_"))
async def cb_viewmode(call: CallbackQuery):
    """Переключение Basic/Pro вида"""
    lang = await get_user_lang(call.from_user.id)
    # viewmode_pro_BTC или viewmode_basic_BTC
    parts = call.data.split("_", 2)  # ['viewmode', 'pro', 'BTC']
    new_mode = parts[1]  # 'pro' или 'basic'
    coin = parts[2] if len(parts) > 2 else "BTC"

    # ТЕСТОВЫЙ РЕЖИМ: все подписки бесплатны — сразу переключаем
    # В будущем: проверять план пользователя
    # plan = await db.get_plan(call.from_user.id)
    # if new_mode == "pro" and plan not in ("pro", "pro_plus"):
    #     await call.answer()
    #     await call.message.answer(
    #         t("pro_promo", lang),
    #         parse_mode=ParseMode.HTML,
    #         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
    #             [InlineKeyboardButton(text=t("btn_subscription", lang), callback_data="subscription")],
    #         ])
    #     )
    #     return

    await db.set_view_mode(call.from_user.id, new_mode)

    # Перерисовать карточку с новым режимом
    try:
        idx = COINS.index(coin)
        page = idx // COINS_PER_PAGE
    except ValueError:
        page = 0
    coins_to_load = [coin] if (coin == "BTC" or new_mode != "pro") else [coin, "BTC"]
    data = await db.get_market_data(coins_to_load)
    await call.message.edit_text(
        text_coin_analysis(coin, data, lang, view_mode=new_mode),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_coin_detail(coin, page=page, lang=lang, view_mode=new_mode, data=data)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("options_"))
async def cb_options(call: CallbackQuery):
    """📊 Полный экран опционов для BTC/ETH"""
    lang = await get_user_lang(call.from_user.id)
    coin = call.data.replace("options_", "")
    if coin not in COINS_WITH_OPTIONS:
        await call.answer("No options data")
        return
    data = await db.get_market_data([coin])

    # Генерируем AI-анализ опционов
    ai_text = ""
    d = data.get(coin, {})
    pcr = d.get("options_pcr", "—")
    mp = d.get("options_max_pain", "—")
    iv = d.get("options_iv", "—")
    oi_c = d.get("options_oi_calls", "—")
    oi_p = d.get("options_oi_puts", "—")
    price = d.get("price", "—")

    if _has(pcr) and pcr != "—":
        try:
            from collector import generate_options_ai
            ai_text = await generate_options_ai(coin, {
                "pcr": pcr, "max_pain": mp, "iv": iv,
                "oi_calls": oi_c, "oi_puts": oi_p, "price": price,
                "expiries": d.get("options_expiries", "—"),
            }, lang)
        except Exception as e:
            log.warning(f"Options AI error: {e}")

    await call.message.edit_text(
        text_options_detail(coin, data, lang, ai_text),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_options(coin, lang)
    )
    await call.answer()


@dp.callback_query(F.data == "settings")
async def cb_settings(call: CallbackQuery):
    user_id = call.from_user.id
    lang = await get_user_lang(user_id)
    user    = await db.get_user(user_id)
    plan    = user.get("plan", "free") if user else "free"
    interval= user.get("interval", 60) if user else 60
    alerts  = user.get("alerts_enabled", True) if user else True
    alert_status = t("alerts_enabled", lang) if alerts else t("alerts_disabled", lang)
    await call.message.edit_text(
        f"<b>{t('settings_title', lang)}</b>\n\n"
        f"{t('settings_plan', lang)}: <b>{plan.upper()}</b>\n"
        f"{t('settings_interval', lang)}: <b>{t('settings_every', lang, interval=interval)}</b>\n"
        f"{t('settings_alerts', lang)}: <b>{alert_status}</b>\n\n"
        f"{t('settings_choose_interval', lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_settings(alerts, lang)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("interval_"))
async def cb_interval(call: CallbackQuery):
    interval = int(call.data.replace("interval_", ""))
    user_id  = call.from_user.id
    await db.update_user(user_id, {"interval": interval})
    lang = await get_user_lang(user_id)
    user = await db.get_user(user_id)
    alerts = user.get("alerts_enabled", True) if user else True
    plan = user.get("plan", "free") if user else "free"
    alert_status = t("alerts_enabled", lang) if alerts else t("alerts_disabled", lang)
    await call.message.edit_text(
        f"<b>{t('settings_title', lang)}</b>\n\n"
        f"{t('settings_plan', lang)}: <b>{plan.upper()}</b>\n"
        f"{t('settings_interval', lang)}: <b>{t('settings_every', lang, interval=interval)}</b>\n"
        f"{t('settings_alerts', lang)}: <b>{alert_status}</b>\n\n"
        f"{t('settings_choose_interval', lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_settings(alerts, lang)
    )
    await call.answer(t("interval_set", lang, interval=interval))


@dp.callback_query(F.data.startswith("toggle_alerts_"))
async def cb_toggle_alerts(call: CallbackQuery):
    user_id = call.from_user.id
    enable = call.data == "toggle_alerts_on"
    await db.update_user(user_id, {"alerts_enabled": enable})
    lang = await get_user_lang(user_id)
    user = await db.get_user(user_id)
    plan = user.get("plan", "free") if user else "free"
    interval = user.get("interval", 60) if user else 60
    alert_status = t("alerts_enabled", lang) if enable else t("alerts_disabled", lang)
    await call.message.edit_text(
        f"<b>{t('settings_title', lang)}</b>\n\n"
        f"{t('settings_plan', lang)}: <b>{plan.upper()}</b>\n"
        f"{t('settings_interval', lang)}: <b>{t('settings_every', lang, interval=interval)}</b>\n"
        f"{t('settings_alerts', lang)}: <b>{alert_status}</b>\n\n"
        f"{t('settings_choose_interval', lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_settings(enable, lang)
    )
    status_text = t("alerts_on_short", lang) if enable else t("alerts_off_short", lang)
    await call.answer(status_text)


@dp.callback_query(F.data == "toggle_lang")
async def cb_toggle_lang(call: CallbackQuery):
    """Переключение языка RU ↔ EN"""
    user_id = call.from_user.id
    current_lang = await get_user_lang(user_id)
    new_lang = "en" if current_lang == "ru" else "ru"
    await db.update_user(user_id, {"language": new_lang})

    # Обновляем экран настроек на новом языке
    user = await db.get_user(user_id)
    plan = user.get("plan", "free") if user else "free"
    interval = user.get("interval", 60) if user else 60
    alerts = user.get("alerts_enabled", True) if user else True
    alert_status = t("alerts_enabled", new_lang) if alerts else t("alerts_disabled", new_lang)
    await call.message.edit_text(
        f"<b>{t('settings_title', new_lang)}</b>\n\n"
        f"{t('settings_plan', new_lang)}: <b>{plan.upper()}</b>\n"
        f"{t('settings_interval', new_lang)}: <b>{t('settings_every', new_lang, interval=interval)}</b>\n"
        f"{t('settings_alerts', new_lang)}: <b>{alert_status}</b>\n\n"
        f"{t('settings_choose_interval', new_lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_settings(alerts, new_lang)
    )
    await call.answer(f"🌐 {'English' if new_lang == 'en' else 'Русский'}")


@dp.callback_query(F.data == "subscription")
async def cb_subscription(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    await call.message.edit_text(
        f"<b>{t('sub_title', lang)}</b>\n\n"
        f"{t('sub_free', lang)}\n"
        f"{t('sub_basic', lang)}\n"
        f"{t('sub_pro', lang)}\n"
        f"{t('sub_pro_plus', lang)}",
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_subscription(lang)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("plan_"))
async def cb_plan(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    plan_map = {
        "plan_basic":    ("Basic", "$14/мес"),
        "plan_pro":      ("Pro",   "$29/мес"),
        "plan_pro_plus": ("Pro+",  "$49/мес"),
    }
    plan_key  = call.data
    plan_name, plan_price = plan_map.get(plan_key, ("?", "?"))
    await call.answer(
        t("payment_soon", lang, name=plan_name, price=plan_price),
        show_alert=True
    )


@dp.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    await call.message.edit_text(
        t("help", lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t("btn_back", lang), callback_data="back_main")]
        ])
    )
    await call.answer()


@dp.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    await call.message.edit_text(
        t("welcome", lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_main(lang)
    )
    await call.answer()


# ══════════════════════════════════════════════════════════════════════════════
# FAQ
# ══════════════════════════════════════════════════════════════════════════════

def kb_faq(lang: str = "ru"):
    """Кнопки FAQ — категории"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("faq_btn_signals", lang), callback_data="faq_signals"),
            InlineKeyboardButton(text=t("faq_btn_whales", lang), callback_data="faq_whales"),
        ],
        [
            InlineKeyboardButton(text=t("faq_btn_options", lang), callback_data="faq_options"),
            InlineKeyboardButton(text=t("faq_btn_orderflow", lang), callback_data="faq_orderflow"),
        ],
        [
            InlineKeyboardButton(text=t("faq_btn_liquidity", lang), callback_data="faq_liquidity"),
            InlineKeyboardButton(text=t("faq_btn_structure", lang), callback_data="faq_structure"),
        ],
        [
            InlineKeyboardButton(text=t("faq_btn_ai", lang), callback_data="faq_ai"),
            InlineKeyboardButton(text=t("faq_btn_data", lang), callback_data="faq_data"),
        ],
        [
            InlineKeyboardButton(text=t("faq_btn_plans", lang), callback_data="faq_plans"),
        ],
        [
            InlineKeyboardButton(text=t("btn_back", lang), callback_data="back_main"),
        ],
    ])


def kb_faq_back(lang: str = "ru"):
    """Кнопка назад из ответа FAQ"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◀ FAQ", callback_data="faq"),
            InlineKeyboardButton(text=t("btn_back", lang), callback_data="back_main"),
        ],
    ])


@dp.callback_query(F.data == "faq")
async def cb_faq(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    await call.message.edit_text(
        t("faq_title", lang),
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_faq(lang),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("faq_"))
async def cb_faq_item(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    topic = call.data  # faq_signals, faq_whales, etc.
    text = t(topic, lang)
    if text == topic:
        text = "—"  # fallback если ключ не найден
    await call.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        link_preview_options=NO_PREVIEW,
        reply_markup=kb_faq_back(lang),
    )
    await call.answer()


# ══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════════════════════

async def send_alerts():
    """Рассылка алертов пользователям по их расписанию."""
    try:
        users = await db.get_users_for_alerts()
        if not users:
            return

        log.info(f"📨 Алерты: {len(users)} пользователей в очереди")
        coins = COINS
        data = await db.get_market_data(coins)

        sent = 0
        for user in users:
            tid = user.get("telegram_id")
            if not tid:
                continue
            try:
                lang = user.get("language", "ru") or "ru"
                text = text_radar(coins, data, lang)
                await bot.send_message(
                    chat_id=tid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=NO_PREVIEW,
                    reply_markup=kb_radar(page=0, lang=lang, data=data)
                )
                await db.update_last_alert(tid)
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                err_msg = str(e)
                log.warning(f"  ⚠️ Алерт {tid}: {err_msg}")
                # Авто-отключение алертов если юзер заблокировал бота
                if "blocked by the user" in err_msg or "user is deactivated" in err_msg:
                    log.info(f"  🚫 Юзер {tid} заблокировал бота — отключаю алерты")
                    await db.disable_alerts(tid)

        if sent:
            log.info(f"📨 Алерты отправлены: {sent}/{len(users)}")

    except Exception as e:
        log.error(f"send_alerts error: {e}")


async def alert_loop():
    """Цикл рассылки алертов."""
    log.info("📨 Алерт-цикл запущен (проверка каждую минуту)")
    await asyncio.sleep(120)

    while True:
        try:
            await send_alerts()
        except Exception as e:
            log.error(f"Alert loop error: {e}")
        await asyncio.sleep(60)


async def main():
    log.info("⚡ Zender Terminal Bot — starting...")
    asyncio.create_task(collector_loop(interval_minutes=5))
    asyncio.create_task(alert_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
