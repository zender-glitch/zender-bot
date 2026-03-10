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
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode

from config import BOT_TOKEN
from database import db
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
    "MATIC", "TRX", "SHIB", "UNI", "LTC",
    "ATOM", "NEAR", "APT", "ARB", "OP",
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
        "btn_radar": "📡 Радар рынка",
        "btn_settings": "⚙️ Настройки",
        "btn_subscription": "💳 Подписка",
        "btn_help": "❓ Помощь",
        "btn_refresh": "🔄 Обновить",
        "btn_back": "◀ Назад",
        "btn_back_radar": "◀ Назад к радару",
        "btn_language": "🌐 Язык: Русский",

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
        "crowd_overlong": "толпа перегружена лонгами ({pct}%)",
        "crowd_long": "толпа в лонгах ({pct}%)",
        "crowd_overshort": "толпа перегружена шортами ({pct}%)",
        "crowd_short": "толпа в шортах ({pct}%)",
        "crowd_balance": "толпа в балансе",
        "gas_high": "высокая нагрузка",
        "gas_medium": "умеренная",
        "gas_low": "низкая",
        "section_market": "━━━ РЫНОК ━━━",
        "section_liquidity": "━━━ ЛИКВИДНОСТЬ ━━━",
        "section_levels": "━━━ УРОВНИ ━━━",
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
        "options_oi_title": "━━━ OPEN INTEREST ━━━",
        "options_oi_bulls": "Быки доминируют в опционах",
        "options_oi_bears": "Медведи доминируют в опционах",
        "options_oi_balanced": "Баланс быков и медведей",
        "options_exp_title": "━━━ ЭКСПИРАЦИИ ━━━",
        "options_exp_days": "через {days} д.",
        "options_exp_warning": "Возможна волатильность",
        "options_exp_max": "Крупная экспирация = магнит цены",
        "options_ai_title": "━━━ 🤖 AI-АНАЛИЗ ━━━",
        "options_teaser": "━━━ ОПЦИОНЫ {coin} ━━━",
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
        "btn_radar": "📡 Market Radar",
        "btn_settings": "⚙️ Settings",
        "btn_subscription": "💳 Subscription",
        "btn_help": "❓ Help",
        "btn_refresh": "🔄 Refresh",
        "btn_back": "◀ Back",
        "btn_back_radar": "◀ Back to radar",
        "btn_language": "🌐 Lang: English",

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
        "crowd_overlong": "crowd overleveraged long ({pct}%)",
        "crowd_long": "crowd in longs ({pct}%)",
        "crowd_overshort": "crowd overleveraged short ({pct}%)",
        "crowd_short": "crowd in shorts ({pct}%)",
        "crowd_balance": "crowd balanced",
        "gas_high": "high load",
        "gas_medium": "moderate",
        "gas_low": "low",
        "section_market": "━━━ MARKET ━━━",
        "section_liquidity": "━━━ LIQUIDITY ━━━",
        "section_levels": "━━━ LEVELS ━━━",
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
        "options_oi_title": "━━━ OPEN INTEREST ━━━",
        "options_oi_bulls": "Bulls dominate in options",
        "options_oi_bears": "Bears dominate in options",
        "options_oi_balanced": "Bulls and bears balanced",
        "options_exp_title": "━━━ EXPIRATIONS ━━━",
        "options_exp_days": "in {days} d.",
        "options_exp_warning": "Possible volatility",
        "options_exp_max": "Large expiry = price magnet",
        "options_ai_title": "━━━ 🤖 AI ANALYSIS ━━━",
        "options_teaser": "━━━ OPTIONS {coin} ━━━",
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


def _coin_page_buttons(page: int = 0, prefix: str = "coin_") -> list:
    """Кнопки монет для текущей страницы (2 ряда по 5)"""
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
    """Кнопки навигации между страницами"""
    if total_pages <= 1:
        return []
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton(text="◀", callback_data=f"page_{page - 1}"))
    buttons.append(InlineKeyboardButton(
        text=t("page_label", lang, page=page + 1, total=total_pages),
        callback_data="noop"
    ))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton(text="▶", callback_data=f"page_{page + 1}"))
    return buttons


TOTAL_PAGES = (len(COINS) + COINS_PER_PAGE - 1) // COINS_PER_PAGE


def kb_main(lang: str = "ru"):
    """Главная клавиатура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_radar", lang), callback_data="radar"),
            InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text=t("btn_subscription", lang), callback_data="subscription"),
            InlineKeyboardButton(text=t("btn_help", lang), callback_data="help"),
        ],
    ])


def kb_radar(page: int = 0, lang: str = "ru"):
    """Кнопки под радаром: монеты + пагинация + обновить + настройки"""
    rows = _coin_page_buttons(page)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton(text=t("btn_refresh", lang), callback_data="radar"),
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_coin_detail(coin: str, page: int = 0, lang: str = "ru"):
    """Кнопки под анализом монеты"""
    rows = _coin_page_buttons(page)
    nav = _page_nav_buttons(page, TOTAL_PAGES, lang)
    if nav:
        rows.append(nav)
    # Кнопка Опционы только для BTC/ETH
    action_row = [
        InlineKeyboardButton(text=t("btn_refresh", lang), callback_data=f"coin_{coin}"),
        InlineKeyboardButton(text=t("btn_settings", lang), callback_data="settings"),
    ]
    if coin in COINS_WITH_OPTIONS:
        action_row.insert(0, InlineKeyboardButton(
            text=t("btn_options", lang, coin=coin), callback_data=f"options_{coin}"
        ))
    rows.append(action_row)
    rows.append([
        InlineKeyboardButton(text=t("btn_back_radar", lang), callback_data="radar"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_coin_buttons(page: int = 0, lang: str = "ru"):
    """Кнопки монет + обновить + радар + настройки"""
    rows = _coin_page_buttons(page)
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
    """📡 РАДАР РЫНКА — компактный обзор всех монет."""
    lines = [
        f"<b>{t('radar_title', lang)}</b>",
        "",
    ]

    for coin in coins:
        d = data.get(coin, {})
        price  = d.get("price",  "—")
        change = d.get("change", "—")
        rec    = d.get("recommendation", "")
        ch_icon = _change_icon(change)
        r_icon  = _rec_icon(rec)
        r_label = _rec_label(rec, lang)

        lines.append(f"<code>{coin:<5}</code> {str(price):>10}   {ch_icon} <code>{change:<8}</code> {r_icon} {r_label}")

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
        lines.append(f"{fg_emoji} <b>{t('market_mood', lang)}</b>")
        lines.append(f"{fg_label} ({fg}/100)")

    lines.append("")
    lines.append(t("press_coin", lang))

    return "\n".join(lines)


def text_coin_analysis(coin: str, data: dict, lang: str = "ru") -> str:
    """Компактный анализ монеты."""
    d = data.get(coin, {})
    price  = d.get("price",  "—")
    change = d.get("change", "—")
    ch_icon = _change_icon(change)

    def _clean(v):
        return str(v).replace("**", "").replace("*", "").strip() if v else ""

    what_happening = _clean(d.get("what_happening", ""))
    if what_happening and len(what_happening) > 80:
        what_happening = what_happening[:77] + "..."
    trap           = _clean(d.get("trap", ""))
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

    lines = [
        f"<b>ZENDER TERMINAL · {coin}</b>",
        "",
        f"💰 <b>{price}</b>   {ch_icon} {change}",
    ]

    # ── ЧТО ПРОИСХОДИТ ──
    if what_happening:
        lines.append("")
        lines.append(f"<b>{t('what_happening', lang)}</b>")
        lines.append(html_lib.escape(what_happening))

    # ── ЛОВУШКА ──
    if trap:
        lines.append("")
        lines.append(f"⚠️ <b>{t('trap', lang)}</b>")
        lines.append(html_lib.escape(trap))

    # ── СИГНАЛ ──
    if recommendation:
        lines.append("")
        sig_text = f"📊 <b>{t('signal', lang)}:</b> {rec_icon} {rec_label}"
        if strength_label:
            sig_text += f" ({strength_label})"
        lines.append(sig_text)

    # ── УВЕРЕННОСТЬ СИГНАЛА ──
    conf_bar = d.get("confidence_bar", "")
    conf_label = d.get("confidence_label", "")
    if conf_bar and conf_label:
        lines.append(f"🎯 {conf_bar} {html_lib.escape(conf_label)}")

    # ── ГОРИЗОНТ ──
    horizon = _clean(d.get("horizon", ""))
    if horizon:
        lines.append(f"⏱ {html_lib.escape(horizon)}")

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
            lines.append(f"⚖️ {t('ls_label', lang)}: {lp:.0f}% / {sp:.0f}% — {hint}")
        except (ValueError, TypeError):
            lines.append(f"⚖️ {t('ls_label', lang)}: {long_p}% / {short_p}%")

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
            lines.append(f"💰 {t('funding_label', lang)}: {fr_val} — {fr_hint}")
        except (ValueError, TypeError):
            lines.append(f"💰 {t('funding_label', lang)}: {fr_val}")

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
            lines.append(f"📊 {t('oi_label', lang)}: {oi_chg} ({oi_hint})")
        except (ValueError, TypeError):
            lines.append(f"📊 {t('oi_label', lang)}: {oi_chg}")

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
            lines.append(f"🌡 {t('state_label', lang)}: {rsi_hint} (RSI {rv:.0f})")
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
            lines.append(f"😰 {t('mood_label', lang)}: {fg_hint} ({fg}/100)")
        except (ValueError, TypeError):
            pass

    # ── КИТЫ vs ТОЛПА ──
    netflow = d.get("exchange_netflow_btc", "—")
    has_whale = (coin == "BTC" and _has(netflow))
    has_crowd = _has(bg_long_acc)

    if has_whale or has_crowd:
        lines.append("")
        lines.append(t("whales_vs_crowd", lang))

        if has_whale:
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
            lines.append(f"⛽ Gas: {gas_v} Gwei — {gas_hint}")
        except (ValueError, TypeError):
            pass

    # ── ЛИКВИДНОСТЬ ──
    liq_lvl_shorts = d.get("liq_level_shorts", "")
    liq_lvl_longs = d.get("liq_level_longs", "")
    if liq_lvl_shorts or liq_lvl_longs:
        lines.append("")
        lines.append(t("section_liquidity", lang))
        if liq_lvl_shorts:
            lines.append(f"{html_lib.escape(liq_lvl_shorts)} — {t('shorts_stops', lang)}")
        if liq_lvl_longs:
            lines.append(f"{html_lib.escape(liq_lvl_longs)} — {t('longs_stops', lang)}")

    # ── УРОВНИ ──
    lines.append("")
    lines.append(t("section_levels", lang))

    liq_up = d.get("liq_up", "—")
    liq_dn = d.get("liq_dn", "—")
    if _has(liq_up) or _has(liq_dn):
        lines.append(f"💥 {t('liq_1h', lang)}")
        if _has(liq_up) and _has(liq_dn):
            try:
                lu = float(str(liq_up).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6"))
                ld = float(str(liq_dn).replace("$", "").replace(",", "").replace("K", "e3").replace("M", "e6"))
                up_arrow = " ↑" if lu > ld else ""
                dn_arrow = " ↑" if ld > lu else ""
                lines.append(f"{t('liq_shorts', lang)}: {liq_up}{up_arrow}")
                lines.append(f"{t('liq_longs', lang)}: {liq_dn}{dn_arrow}")
            except (ValueError, TypeError):
                lines.append(f"{t('liq_shorts', lang)}: {liq_up}")
                lines.append(f"{t('liq_longs', lang)}: {liq_dn}")
        else:
            if _has(liq_up):
                lines.append(f"{t('liq_shorts', lang)}: {liq_up}")
            if _has(liq_dn):
                lines.append(f"{t('liq_longs', lang)}: {liq_dn}")

    # Вход / Стоп / Цель
    if entry or stop or target:
        lines.append("")
        if entry:
            lines.append(f"{t('entry_label', lang)}: {html_lib.escape(entry)}")
        if stop:
            lines.append(f"{t('stop_label', lang)}: {html_lib.escape(stop)}")
        if target:
            lines.append(f"{t('target_label', lang)}: {html_lib.escape(target)}")
    elif buy_zone or sell_zone:
        lines.append("")
        if buy_zone:
            lines.append(f"{t('buy_label', lang)}: {html_lib.escape(buy_zone)}")
        if sell_zone:
            lines.append(f"{t('sell_label', lang)}: {html_lib.escape(sell_zone)}")

    # ── ТИЗЕР ОПЦИОНОВ (только BTC/ETH) ──
    if coin in ("BTC", "ETH"):
        pcr_val = d.get("options_pcr", "—")
        mp_val = d.get("options_max_pain", "—")
        # Ближайшая экспирация
        exp_raw = d.get("options_expiries", "—")
        nearest_exp = ""
        if exp_raw and exp_raw != "—" and isinstance(exp_raw, str) and exp_raw.startswith("["):
            try:
                import ast
                exps = ast.literal_eval(exp_raw)
                if exps and isinstance(exps, list):
                    e = exps[0]
                    nearest_exp = f"🔥 {e.get('date', '')} — {e.get('oi', 0):,}K OI · {t('options_exp_days', lang, days=e.get('days', '?'))}"
                    if e.get("days", 99) <= 3:
                        nearest_exp += " ⚠️"
            except Exception:
                pass

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
                        mp_str = f" · Max Pain: ${mp_f:,.0f}"
                    except (ValueError, TypeError):
                        pass
                lines.append(f"📊 PCR: {pcr_f:.2f} {pcr_hint}{mp_str}")
            except (ValueError, TypeError):
                lines.append(f"📊 PCR: {pcr_val}")
            if nearest_exp:
                lines.append(nearest_exp)

    lines.append("")
    lines.append("⚡ <b>Zender Terminal</b>")
    lines.append("t.me/ZenderTerminal_bot")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# ОПЦИОНЫ — ПОЛНЫЙ ЭКРАН
# ══════════════════════════════════════════════════════════════════════════════

def text_options_detail(coin: str, data: dict, lang: str = "ru", ai_text: str = "") -> str:
    """Полный экран опционов для BTC/ETH."""
    d = data.get(coin, {})
    price = d.get("price", "—")

    lines = [
        f"<b>{t('options_title', lang, coin=coin)}</b>",
        "",
    ]

    # PCR
    pcr_val = d.get("options_pcr", "—")
    if _has(pcr_val) and pcr_val != "—":
        try:
            pcr_f = float(pcr_val)
            lines.append(f"{t('options_pcr_label', lang)}: {pcr_f:.2f}")
            if pcr_f < 0.7:
                lines.append(f"→ {t('options_pcr_bullish', lang)}")
            elif pcr_f > 1.0:
                lines.append(f"→ {t('options_pcr_bearish', lang)}")
            else:
                lines.append(f"→ {t('options_pcr_neutral', lang)}")
        except (ValueError, TypeError):
            pass

    # Max Pain
    mp_val = d.get("options_max_pain", "—")
    if _has(mp_val) and mp_val != "—":
        try:
            mp_f = float(str(mp_val).replace("$", "").replace(",", ""))
            price_f = float(str(price).replace("$", "").replace(",", ""))
            diff_pct = ((mp_f - price_f) / price_f) * 100
            lines.append("")
            lines.append(f"{t('options_maxpain_label', lang)}: ${mp_f:,.0f} (цена {price})" if lang == "ru" else f"{t('options_maxpain_label', lang)}: ${mp_f:,.0f} (price {price})")
            if abs(diff_pct) < 0.5:
                lines.append(f"→ {t('options_maxpain_at', lang)}")
            elif diff_pct > 0:
                lines.append(f"→ {t('options_maxpain_above', lang, pct=f'+{diff_pct:.1f}%')}")
            else:
                lines.append(f"→ {t('options_maxpain_below', lang, pct=f'{diff_pct:.1f}%')}")
        except (ValueError, TypeError):
            pass

    # IV
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
            lines.append(f"{t('options_iv_label', lang)}: {iv_f:.0f}% — {iv_hint}")
        except (ValueError, TypeError):
            pass

    # OI
    oi_calls = d.get("options_oi_calls", "—")
    oi_puts = d.get("options_oi_puts", "—")
    if _has(oi_calls) and _has(oi_puts) and oi_calls != "—" and oi_puts != "—":
        try:
            c = float(oi_calls)
            p = float(oi_puts)
            lines.append("")
            lines.append(f"<b>{t('options_oi_title', lang)}</b>")
            lines.append(f"🟢 Calls: {c/1000:,.0f}K     🔴 Puts: {p/1000:,.0f}K")
            if c > p * 1.3:
                lines.append(f"→ {t('options_oi_bulls', lang)}")
            elif p > c * 1.3:
                lines.append(f"→ {t('options_oi_bears', lang)}")
            else:
                lines.append(f"→ {t('options_oi_balanced', lang)}")
        except (ValueError, TypeError):
            pass

    # Экспирации
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
            lines.append(f"<b>{t('options_exp_title', lang)}</b>")
            for e in exps:
                date_str = e.get("date", "")
                oi_val = e.get("oi", 0)
                days = e.get("days", 99)
                is_max = e.get("is_max", False)

                line = f"{date_str} — {oi_val:,} OI · {t('options_exp_days', lang, days=days)}"
                if days <= 3:
                    line += " ⚠️"
                if is_max:
                    line += " 🔥"
                lines.append(line)

                # Подсказка
                if days <= 3:
                    lines.append(f"→ {t('options_exp_warning', lang)}")
                elif is_max:
                    lines.append(f"→ {t('options_exp_max', lang)}")
                lines.append("")

    # AI-анализ
    if ai_text:
        lines.append(f"<b>{t('options_ai_title', lang)}</b>")
        lines.append(html_lib.escape(ai_text))

    lines.append("")
    lines.append("⚡ <b>Zender Terminal</b>")

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
        reply_markup=kb_radar(page=0, lang=lang)
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
        reply_markup=kb_radar(page=0, lang=lang)
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
        reply_markup=kb_radar(page=0, lang=lang)
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
        reply_markup=kb_radar(page=0, lang=lang)
    )
    await call.answer(t("refreshed", lang))


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
        reply_markup=kb_radar(page=page, lang=lang)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("coin_"))
async def cb_coin(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id)
    coin = call.data.replace("coin_", "")
    # Определяем страницу этой монеты
    try:
        idx = COINS.index(coin)
        page = idx // COINS_PER_PAGE
    except ValueError:
        page = 0
    data = await db.get_market_data([coin])
    await call.message.edit_text(
        text_coin_analysis(coin, data, lang),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_coin_detail(coin, page=page, lang=lang)
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
        reply_markup=kb_main(lang)
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
                    reply_markup=kb_radar(page=0, lang=lang)
                )
                await db.update_last_alert(tid)
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                log.warning(f"  ⚠️ Алерт {tid}: {e}")

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
