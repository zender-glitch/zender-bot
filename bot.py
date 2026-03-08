"""
ZENDER COMMANDER TERMINAL — Telegram Bot
Этап 1-2: бот с командами, inline-кнопками + запуск коллектора данных.
"""

import asyncio
import logging
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
            InlineKeyboardButton(text="📊 Сводка монет",    callback_data="summary"),
            InlineKeyboardButton(text="⚙️ Настройки",       callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text="💳 Подписка",        callback_data="subscription"),
            InlineKeyboardButton(text="❓ Помощь",          callback_data="help"),
        ],
    ])

def kb_coins(coins: list[str]):
    """Кнопки монет под сводкой"""
    buttons = [InlineKeyboardButton(text=c, callback_data=f"coin_{c}") for c in coins]
    rows = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back_to_summary():
    """Кнопка назад к сводке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Назад к сводке", callback_data="summary")]
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

WELCOME = """<b>⚡ ZENDER COMMANDER TERMINAL</b>

Агрегатор крипто-данных с 30+ сервисов + LLM-анализ.
Трейдер платит $14/мес вместо $200–800+ по отдельности.

<b>Тарифы:</b>
🆓 <b>Free</b> — 1 монета · LLM-анализ · обновление 15 мин
🟢 <b>Basic $14</b> — топ-20 монет · LLM-анализ · 5/15/60 мин
🟡 <b>Pro $29</b> — все метрики · дашборд · 3 темы · без LLM
🔴 <b>Pro+ $49</b> — алерты 1-2 мин · сканер 200 монет

Используй кнопки ниже 👇"""

HELP_TEXT = """<b>⚡ ZENDER COMMANDER TERMINAL — Помощь</b>

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

⚡ t.me/ZenderCommander_bot"""


def _arrow(change_str: str) -> str:
    """Стрелка направления по строке процента: ↑ ↓ →"""
    s = str(change_str).strip()
    if s.startswith("+") and s != "+0.00%":
        return "↑"
    elif s.startswith("-") and s != "-0.00%":
        return "↓"
    return "→"


def text_summary(user_coins: list[str], data: dict) -> str:
    """
    Компактная сводка по монетам — эталонный формат.
    """
    lines = [
        "<code>┌──────────────────────────────────┐",
        "│    ZENDER COMMANDER TERMINAL     │",
        "└──────────────────────────────────┘</code>",
        "",
        "<b>ВАШИ МОНЕТЫ</b> · обновление каждые 15 мин",
        "",
    ]

    for coin in user_coins:
        d = data.get(coin, {})
        price  = d.get("price",  "—")
        change = d.get("change", "—")
        signal = d.get("signal", "░░░░░")
        label  = d.get("label",  "—")
        arrow  = _arrow(change)

        lines.append(f"<code>{coin:<5} {str(price):>10}  {arrow} {change:<9} {signal} {label}</code>")

    lines.append("")
    lines.append("<code>──────────────────────────────────</code>")
    lines.append("⚡ <b>Zender Commander Terminal</b>")
    lines.append("t.me/ZenderCommander_bot")
    return "\n".join(lines)


def text_coin_analysis(coin: str, data: dict) -> str:
    """
    Полный анализ одной монеты — эталонный формат.
    Показываем только секции с реальными данными (не "—").
    Секции без данных (киты, биржевой поток) скрываются.
    """
    d = data.get(coin, {})
    price   = d.get("price",       "—")
    change  = d.get("change",      "—")
    long_v  = d.get("long_vol",    "—")
    long_p  = d.get("long_pct",    "—")
    short_v = d.get("short_vol",   "—")
    short_p = d.get("short_pct",   "—")
    oi      = d.get("oi",          "—")
    oi_chg  = d.get("oi_change",   "—")
    fr      = d.get("funding_rate","—")
    liq_up  = d.get("liq_up",      "—")
    liq_dn  = d.get("liq_dn",      "—")
    fg      = d.get("fear_greed",  "—")
    fg_lbl  = d.get("fear_greed_label", "—")
    signal  = d.get("signal",      "░░░░░")
    sig_lbl = d.get("signal_label","—")

    arrow = _arrow(change)

    lines = [
        "<code>┌──────────────────────────────────┐",
        "│    ZENDER COMMANDER TERMINAL     │",
        "└──────────────────────────────────┘</code>",
        "",
        f"<code>{coin} / USDT          {price}   {arrow} {change}</code>",
    ]

    # ── ПОЗИЦИИ (buy/sell ratio) ──
    if long_p != "—" or short_p != "—":
        lines.append("")
        lines.append("<b>ПОЗИЦИИ</b>")
        lines.append(f"<code>ставят на рост     {long_v:<14}{long_p}</code>")
        lines.append(f"<code>ставят на падение  {short_v:<14}{short_p}</code>")

    # ── ОТКРЫТЫЙ ИНТЕРЕС ──
    if oi != "—":
        lines.append("")
        lines.append("<b>ОТКРЫТЫЙ ИНТЕРЕС</b>")
        lines.append(f"<code>{oi}   {oi_chg} за 4 часа</code>")

    # ── КОМИССИЯ ЗА УДЕРЖАНИЕ (Funding Rate) ──
    if fr != "—":
        lines.append("")
        lines.append("<b>КОМИССИЯ ЗА УДЕРЖАНИЕ</b>")
        lines.append(f"<code>{fr}</code>")

    # ── ЛИКВИДАЦИИ ──
    if liq_up != "—" or liq_dn != "—":
        lines.append("")
        lines.append("<b>ЛИКВИДАЦИИ (4ч)</b>")
        lines.append(f"<code>↑  позиций на падение  {liq_up}</code>")
        lines.append(f"<code>↓  позиций на рост     {liq_dn}</code>")

    # ── НАСТРОЕНИЕ ──
    if fg != "—":
        lines.append("")
        lines.append("<b>НАСТРОЕНИЕ</b>")
        lines.append(f"<code>страх/жадность   {fg} — {fg_lbl}</code>")

    # ── СИГНАЛ ──
    lines.append("")
    lines.append("<code>──────────────────────────────────</code>")
    lines.append(f"<code>СИГНАЛ   {signal}   {sig_lbl}</code>")
    lines.append("<code>──────────────────────────────────</code>")

    lines.append("")
    lines.append("⚡ <b>Zender Commander Terminal</b> · t.me/ZenderCommander_bot")

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
    """Сводка по монетам"""
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    coins = user.get("coins", COINS[:1]) if user else COINS[:1]
    data  = await db.get_market_data(coins)
    await message.answer(
        text_summary(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_coins(coins)
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
    user_id = call.from_user.id
    user    = await db.get_user(user_id)
    coins   = user.get("coins", COINS[:1]) if user else COINS[:1]
    data    = await db.get_market_data(coins)
    await call.message.edit_text(
        text_summary(coins, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_coins(coins)
    )
    await call.answer()


@dp.callback_query(F.data.startswith("coin_"))
async def cb_coin(call: CallbackQuery):
    """Полный анализ монеты по нажатию кнопки"""
    coin = call.data.replace("coin_", "")
    data = await db.get_market_data([coin])
    await call.message.edit_text(
        text_coin_analysis(coin, data),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_back_to_summary()
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
    # TODO: здесь будет редирект на Stripe
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
    log.info("⚡ Zender Commander Terminal Bot — starting...")

    # Запускаем коллектор данных как фоновую задачу
    asyncio.create_task(collector_loop(interval_minutes=15))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
