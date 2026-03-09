"""
ZENDER TERMINAL — Telegram Bot
Этап 1-2-5: бот с командами, inline-кнопками + коллектор + LLM-анализ.
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

# ── Монеты доступные в боте ───────────────────────────────────────────────────
COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX"]

# ══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════════════════════

def kb_main():
    """Главная клавиатура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📡 Радар рынка",     callback_data="radar"),
            InlineKeyboardButton(text="⚙️ Настройки",       callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text="💳 Подписка",        callback_data="subscription"),
            InlineKeyboardButton(text="❓ Помощь",          callback_data="help"),
        ],
    ])

def kb_coin_buttons():
    """Кнопки монет + обновить + радар"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"coin_{c}") for c in COINS],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
        [InlineKeyboardButton(text="📡 Радар рынка", callback_data="radar")],
    ])

def kb_coin_detail(coin: str):
    """Кнопки под анализом монеты"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"coin_{c}") for c in COINS],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"coin_{coin}")],
        [InlineKeyboardButton(text="📡 Радар рынка", callback_data="radar")],
    ])

def kb_radar():
    """Кнопки под радаром"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"coin_{c}") for c in COINS],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="radar")],
    ])

def kb_back_to_summary():
    """Кнопка назад к сводке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Назад к радару", callback_data="radar")]
    ])

def kb_intervals():
    """Выбор интервала обновления"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин",  callback_data="interval_5"),
            InlineKeyboardButton(text="15 мин", callback_data="interval_15"),
            InlineKeyboardButton(text="1 час",  callback_data="interval_60"),
        ],
        [InlineKeyboardButton(text="◀ Назад", callback_data="settings")],
    ])

def kb_subscription():
    """Тарифы"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Basic — $14/мес",  callback_data="plan_basic")],
        [InlineKeyboardButton(text="🟡 Pro — $29/мес",    callback_data="plan_pro")],
        [InlineKeyboardButton(text="🔴 Pro+ — $49/мес",   callback_data="plan_pro_plus")],
        [InlineKeyboardButton(text="◀ Назад",             callback_data="back_main")],
    ])

# ══════════════════════════════════════════════════════════════════════════════
# ТЕКСТЫ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════

WELCOME = """<b>⚡ ZENDER TERMINAL</b>

Агрегатор крипто-данных с 30+ сервисов + LLM-анализ.
Трейдер платит $14/мес вместо $200–800+ по отдельности.

<b>Тарифы:</b>
🆓 <b>Free</b> — 1 монета · LLM-анализ · обновление 15 мин
🟢 <b>Basic $14</b> — топ-20 монет · LLM-анализ · 5/15/60 мин
🟡 <b>Pro $29</b> — все метрики · дашборд · 3 темы · без LLM
🔴 <b>Pro+ $49</b> — алерты 1-2 мин · сканер 200 монет

Используй кнопки ниже 👇"""

HELP_TEXT = """<b>⚡ ZENDER TERMINAL — Помощь</b>

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

⚡ t.me/ZenderTerminal_bot"""


def _arrow(change_str: str) -> str:
    """Треугольник: 🔺 рост, 🔻 падение, ▸ без изменений"""
    s = str(change_str).strip()
    if s.startswith("+") and s != "+0.00%":
        return "🔺"
    elif s.startswith("-") and s != "-0.00%":
        return "🔻"
    return "▸"


def _has(val) -> bool:
    """Проверка что значение не пустое / не заглушка"""
    if val is None:
        return False
    s = str(val).strip()
    return s != "" and s != "—" and s != "0"


def _rec_icon(rec: str) -> str:
    """Иконка рекомендации"""
    r = str(rec).lower()
    if "покупать" in r:
        return "🟢"
    elif "продавать" in r:
        return "🔴"
    return "🟡"

def _rec_label(rec: str) -> str:
    """Лейбл рекомендации для радара"""
    r = str(rec).lower()
    if "покупать" in r:
        return "ПОКУПАТЬ"
    elif "продавать" in r:
        return "ПРОДАВАТЬ"
    return "ДЕРЖАТЬ"

def _change_icon(change_str: str) -> str:
    """Цветной кружок для изменения цены"""
    s = str(change_str).strip()
    if s.startswith("+") and s != "+0.00%":
        return "🟢"
    elif s.startswith("-") and s != "-0.00%":
        return "🔴"
    return "⚪"

def text_radar(coins: list[str], data: dict) -> str:
    """
    📡 РАДАР РЫНКА — компактный обзор всех монет.
    """
    lines = [
        "<b>📡 РАДАР РЫНКА</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for coin in coins:
        d = data.get(coin, {})
        price  = d.get("price",  "—")
        change = d.get("change", "—")
        rec    = d.get("recommendation", "")
        ch_icon = _change_icon(change)
        r_icon  = _rec_icon(rec)
        r_label = _rec_label(rec)

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
        lines.append(f"{fg_emoji} <b>Настроение рынка</b>")
        lines.append(f"{fg_label} ({fg} из 100)")

    lines.append("")
    lines.append("Нажми монету для анализа ⬇")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


def text_coin_analysis(coin: str, data: dict) -> str:
    """
    Компактный анализ монеты — новый терминальный формат.
    ~20 строк вместо 40+. Понятный русский язык.
    """
    d = data.get(coin, {})
    price  = d.get("price",  "—")
    change = d.get("change", "—")
    ch_icon = _change_icon(change)

    # LLM данные (новый формат v8) — убираем markdown-звёздочки **
    def _clean(v):
        return str(v).replace("**", "").replace("*", "").strip() if v else ""

    what_happening = _clean(d.get("what_happening", "") or d.get("llm_text", ""))
    trap           = _clean(d.get("trap", ""))
    recommendation = _clean(d.get("recommendation", ""))
    strength       = _clean(d.get("strength", ""))
    entry          = _clean(d.get("entry", ""))
    stop           = _clean(d.get("stop", ""))
    target         = _clean(d.get("target", ""))
    # Fallback на старый формат
    buy_zone       = _clean(d.get("buy_zone", ""))
    sell_zone      = _clean(d.get("sell_zone", ""))

    rec_icon = _rec_icon(recommendation)
    rec_label = _rec_label(recommendation)
    strength_label = strength.upper() if strength else ""

    lines = [
        f"<b>ZENDER TERMINAL · {coin}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        f"💰 <b>{price}</b>   {ch_icon} {change}",
    ]

    # ── ЧТО ПРОИСХОДИТ ──
    if what_happening:
        lines.append("")
        lines.append("<b>ЧТО ПРОИСХОДИТ</b>")
        lines.append(html_lib.escape(what_happening))

    # ── ЛОВУШКА ──
    if trap:
        lines.append("")
        lines.append(f"⚠️ <b>ЛОВУШКА</b>")
        lines.append(html_lib.escape(trap))

    # ── СИГНАЛ ──
    if recommendation:
        lines.append("")
        sig_text = f"📊 <b>СИГНАЛ:</b> {rec_icon} {rec_label}"
        if strength_label:
            sig_text += f" ({strength_label})"
        lines.append(sig_text)

    lines.append("")
    lines.append("━━━ РЫНОК ━━━")

    # Данные
    sma50_val = d.get("sma50", "—")
    sma200_val = d.get("sma200", "—")
    rsi_val = d.get("rsi", "—")
    macd_val = d.get("macd", "—")
    fr_val = d.get("funding_rate", "—")
    oi_val = d.get("oi", "—")
    oi_chg = d.get("oi_change", "—")
    ob_ratio = d.get("bid_ask_ratio", "—")
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
                lines.append("📈 Тренд: вверх")
            else:
                lines.append("📉 Тренд: вниз")
        except (ValueError, TypeError):
            pass

    # Лонг/Шорт ratio (Coinglass taker)
    if _has(long_p) and _has(short_p):
        try:
            lp = float(long_p)
            sp = float(short_p)
            if lp > 55:
                ls_hint = "быки давят"
            elif sp > 55:
                ls_hint = "медведи давят"
            else:
                ls_hint = "баланс"
            lines.append(f"⚖️ Лонг/Шорт: {lp:.0f}% / {sp:.0f}% — {ls_hint}")
        except (ValueError, TypeError):
            lines.append(f"⚖️ Лонг/Шорт: {long_p}% / {short_p}%")

    # Фандинг с числом
    if _has(fr_val):
        try:
            fv = float(str(fr_val).replace("%", "").replace("+", ""))
            if fv > 0.01:
                fr_hint = "лонги платят шортам"
            elif fv < -0.005:
                fr_hint = "шорты платят лонгам"
            else:
                fr_hint = "баланс"
            lines.append(f"💰 Фандинг: {fr_val} — {fr_hint}")
        except (ValueError, TypeError):
            lines.append(f"💰 Фандинг: {fr_val}")

    # Открытый интерес (OI)
    if _has(oi_chg):
        try:
            oi_v = float(str(oi_chg).replace("%", "").replace("+", ""))
            if oi_v > 0.5:
                oi_hint = "растёт"
            elif oi_v < -0.5:
                oi_hint = "падает"
            else:
                oi_hint = "стабильно"
            lines.append(f"📊 Открытый интерес: {oi_chg} ({oi_hint})")
        except (ValueError, TypeError):
            lines.append(f"📊 Открытый интерес: {oi_chg}")

    # Состояние рынка (RSI)
    if _has(rsi_val):
        try:
            rv = float(rsi_val)
            if rv > 70:
                rsi_hint = "перекуплен"
            elif rv > 60:
                rsi_hint = "разогрет"
            elif rv < 30:
                rsi_hint = "перепродан"
            elif rv < 40:
                rsi_hint = "охлаждается"
            else:
                rsi_hint = "норма"
            lines.append(f"🌡 Состояние: {rsi_hint} (RSI {rv:.0f})")
        except (ValueError, TypeError):
            pass

    # Эмоции рынка (Fear & Greed)
    if _has(fg):
        try:
            fg_val = int(fg)
            if fg_val <= 25:
                fg_hint = "паника"
            elif fg_val <= 45:
                fg_hint = "страх"
            elif fg_val <= 55:
                fg_hint = "спокойствие"
            elif fg_val <= 75:
                fg_hint = "жадность"
            else:
                fg_hint = "эйфория"
            lines.append(f"😰 Настроение рынка: {fg_hint} ({fg}/100)")
        except (ValueError, TypeError):
            pass

    # Толпа — показываем ТОЛЬКО при сильном перекосе (>65%)
    if _has(bg_long_acc):
        try:
            bg_l = float(bg_long_acc)
            bg_s = float(bg_short_acc)
            if bg_l > 65:
                lines.append(f"👥 Толпа: {bg_l:.0f}% лонг — перекос")
            elif bg_s > 65:
                lines.append(f"👥 Толпа: {bg_s:.0f}% шорт — перекос")
        except (ValueError, TypeError):
            pass

    # ── КИТЫ (Exchange Netflow — куда двигают крупные игроки) ──
    netflow = d.get("exchange_netflow_btc", "—")
    eth_gas = d.get("eth_gas_avg", "—")
    if coin == "BTC" and _has(netflow):
        try:
            nf = float(str(netflow).replace(",", "").replace("+", ""))
            if nf < -100:
                lines.append(f"🐋 Киты: выводят с бирж ({nf:,.0f} BTC) — возможно накопление")
            elif nf > 100:
                lines.append(f"🐋 Киты: заводят на биржи (+{nf:,.0f} BTC) — возможно продажа")
            else:
                lines.append(f"🐋 Киты: без движения ({nf:+,.0f} BTC)")
        except (ValueError, TypeError):
            pass
    # ETH Gas (только для ETH)
    if coin == "ETH" and _has(eth_gas):
        try:
            gas_v = int(float(str(eth_gas)))
            if gas_v > 50:
                gas_hint = "высокая нагрузка"
            elif gas_v > 20:
                gas_hint = "умеренная"
            else:
                gas_hint = "низкая"
            lines.append(f"⛽ Gas: {gas_v} Gwei — {gas_hint}")
        except (ValueError, TypeError):
            pass

    # ── УРОВНИ ──
    lines.append("")
    lines.append("━━━ УРОВНИ ━━━")

    # Ликвидации
    liq_up = d.get("liq_up", "—")
    liq_dn = d.get("liq_dn", "—")
    if _has(liq_up) or _has(liq_dn):
        lines.append("💥 Ликвидации (1ч)")
        if _has(liq_up):
            lines.append(f"шорты: {liq_up}")
        if _has(liq_dn):
            lines.append(f"лонги: {liq_dn}")

    # Вход / Стоп / Цель
    if entry or stop or target:
        lines.append("")
        if entry:
            lines.append(f"🎯 Вход: {html_lib.escape(entry)}")
        if stop:
            lines.append(f"🛑 Стоп: {html_lib.escape(stop)}")
        if target:
            lines.append(f"✅ Цель: {html_lib.escape(target)}")
    elif buy_zone or sell_zone:
        # Fallback на старый формат зон
        lines.append("")
        if buy_zone:
            lines.append(f"🎯 Покупка: {html_lib.escape(buy_zone)}")
        if sell_zone:
            lines.append(f"✅ Продажа: {html_lib.escape(sell_zone)}")

    lines.append("")
    lines.append("⚡ <b>Zender Terminal</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# ХЕНДЛЕРЫ КОМАНД
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Приветствие и регистрация пользователя"""
    user = message.from_user
    await db.upsert_user(
        telegram_id=user.id,
        username=user.username or "",
        first_name=user.first_name or "",
    )
    log.info(f"New user: {user.id} @{user.username}")
    await message.answer(WELCOME, parse_mode=ParseMode.HTML, reply_markup=kb_main())


@dp.message(Command("summary"))
async def cmd_summary(message: Message):
    """Радар рынка"""
    coins = COINS
    data  = await db.get_market_data(coins)
    await message.answer(
        text_radar(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_radar()
    )


@dp.message(Command("settings"))
async def cmd_settings(message: Message):
    user_id = message.from_user.id
    user    = await db.get_user(user_id)
    plan    = user.get("plan", "free") if user else "free"
    interval= user.get("interval", 15) if user else 15
    await message.answer(
        f"<b>⚙️ Настройки</b>\n\n"
        f"Тариф: <b>{plan.upper()}</b>\n"
        f"Обновление: <b>каждые {interval} мин</b>\n\n"
        f"Выбери интервал обновления:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_intervals()
    )


@dp.message(Command("status"))
async def cmd_status(message: Message):
    user_id = message.from_user.id
    user    = await db.get_user(user_id)
    if not user:
        await message.answer("Ты ещё не зарегистрирован. Напиши /start")
        return
    plan    = user.get("plan", "free")
    coins   = user.get("coins", [])
    interval= user.get("interval", 15)
    await message.answer(
        f"<b>📋 Твой статус</b>\n\n"
        f"Тариф: <b>{plan.upper()}</b>\n"
        f"Монет отслеживается: <b>{len(coins)}</b>\n"
        f"Интервал обновления: <b>{interval} мин</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# ХЕНДЛЕРЫ CALLBACK (нажатия на кнопки)
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "summary")
async def cb_summary(call: CallbackQuery):
    """Радар рынка (legacy callback)"""
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_radar()
    )
    await call.answer()


@dp.callback_query(F.data == "radar")
async def cb_radar(call: CallbackQuery):
    """📡 Радар рынка"""
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_radar()
    )
    await call.answer()


@dp.callback_query(F.data == "refresh")
async def cb_refresh(call: CallbackQuery):
    """🔄 Обновить радар"""
    coins = COINS
    data  = await db.get_market_data(coins)
    await call.message.edit_text(
        text_radar(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_radar()
    )
    await call.answer("🔄 Обновлено!")


@dp.callback_query(F.data.startswith("coin_"))
async def cb_coin(call: CallbackQuery):
    """Компактный анализ монеты"""
    coin = call.data.replace("coin_", "")
    data = await db.get_market_data([coin])
    await call.message.edit_text(
        text_coin_analysis(coin, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_coin_detail(coin)
    )
    await call.answer()


@dp.callback_query(F.data == "settings")
async def cb_settings(call: CallbackQuery):
    user_id = call.from_user.id
    user    = await db.get_user(user_id)
    plan    = user.get("plan", "free") if user else "free"
    interval= user.get("interval", 15) if user else 15
    await call.message.edit_text(
        f"<b>⚙️ Настройки</b>\n\n"
        f"Тариф: <b>{plan.upper()}</b>\n"
        f"Обновление: <b>каждые {interval} мин</b>\n\n"
        f"Выбери интервал обновления:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_intervals()
    )
    await call.answer()


@dp.callback_query(F.data.startswith("interval_"))
async def cb_interval(call: CallbackQuery):
    interval = int(call.data.replace("interval_", ""))
    user_id  = call.from_user.id
    await db.update_user(user_id, {"interval": interval})
    await call.answer(f"✅ Интервал установлен: {interval} мин", show_alert=True)


@dp.callback_query(F.data == "subscription")
async def cb_subscription(call: CallbackQuery):
    await call.message.edit_text(
        "<b>💳 Выбери тариф</b>\n\n"
        "🆓 <b>Free</b> — 1 монета, LLM-анализ, 15 мин\n"
        "🟢 <b>Basic $14/мес</b> — топ-20, LLM-анализ, 5/15/60 мин\n"
        "🟡 <b>Pro $29/мес</b> — все метрики, дашборд, 3 темы\n"
        "🔴 <b>Pro+ $49/мес</b> — алерты 1-2 мин, сканер 200 монет",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_subscription()
    )
    await call.answer()


@dp.callback_query(F.data.startswith("plan_"))
async def cb_plan(call: CallbackQuery):
    plan_map = {
        "plan_basic":    ("Basic", "$14/мес"),
        "plan_pro":      ("Pro",   "$29/мес"),
        "plan_pro_plus": ("Pro+",  "$49/мес"),
    }
    plan_key  = call.data
    plan_name, plan_price = plan_map.get(plan_key, ("?", "?"))
    # TODO: здесь будет Telegram Payments
    await call.answer(
        f"💳 Оплата {plan_name} {plan_price} — скоро будет доступно!",
        show_alert=True
    )


@dp.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    await call.message.edit_text(
        HELP_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Назад", callback_data="back_main")]
        ])
    )
    await call.answer()


@dp.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    await call.message.edit_text(
        WELCOME,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main()
    )
    await call.answer()

# ══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("⚡ Zender Terminal Bot — starting...")

    # Запускаем коллектор данных как фоновую задачу
    asyncio.create_task(collector_loop(interval_minutes=15))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
