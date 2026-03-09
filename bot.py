"""
ZENDER COMMANDER TERMINAL — Telegram Bot
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


def text_summary(user_coins: list[str], data: dict) -> str:
    """
    Компактная сводка по монетам — эталонный формат.
    """
    lines = [
        "⚡ <b>ZENDER COMMANDER TERMINAL</b>",
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

        lines.append(f"{arrow} <code>{coin:<5} {str(price):>10}  {change:<9} {signal} {label}</code>")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚡ <b>Zender Commander Terminal</b>")
    lines.append("t.me/ZenderCommander_bot")
    return "\n".join(lines)


def text_coin_analysis(coin: str, data: dict) -> str:
    """
    Полный анализ одной монеты — эталонный формат.
    Показываем только секции с реальными данными.
    Секции без данных скрываются.
    + LLM-анализ, рекомендация, зоны покупки/продажи
    + Общие рыночные ликвидации рядом с ликвидациями монеты
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
    mkt_liq_long  = d.get("mkt_liq_long",  "—")
    mkt_liq_short = d.get("mkt_liq_short", "—")
    fg      = d.get("fear_greed",  "—")
    fg_lbl  = d.get("fear_greed_label", "—")
    signal  = d.get("signal",      "░░░░░")
    sig_lbl = d.get("signal_label","—")

    # LLM данные
    llm_text       = d.get("llm_text",        "")
    recommendation = d.get("recommendation",   "")
    buy_zone       = d.get("buy_zone",         "")
    sell_zone      = d.get("sell_zone",        "")

    arrow = _arrow(change)

    lines = [
        "⚡ <b>ZENDER COMMANDER TERMINAL</b>",
        "",
        f"<b>{coin} / USDT</b>          <code>{price}</code>   {arrow} <code>{change}</code>",
    ]

    # ── ЛОНГ / ШОРТ (taker buy/sell ratio) ──
    if _has(long_p) or _has(short_p):
        lines.append("")
        lines.append("<b>ЛОНГ / ШОРТ (1ч)</b>")
        lines.append(f"🔺 <code>лонг    {long_p}</code>")
        lines.append(f"🔻 <code>шорт    {short_p}</code>")

    # ── ОТКРЫТЫЙ ИНТЕРЕС ──
    if _has(oi):
        lines.append("")
        lines.append("<b>ОТКРЫТЫЙ ИНТЕРЕС</b>")
        oi_icon = _arrow(oi_chg)
        lines.append(f"<code>{oi}</code>    {oi_icon} <code>{oi_chg} за 1ч</code>")

    # ── FUNDING RATE ──
    if _has(fr):
        lines.append("")
        lines.append("<b>FUNDING RATE</b>")
        try:
            fr_val = float(fr.replace("%", "").replace("+", ""))
            if fr_val > 0.01:
                fr_hint = "лонги платят шортам (бычий перегрев)"
            elif fr_val < -0.01:
                fr_hint = "шорты платят лонгам (медвежий настрой)"
            else:
                fr_hint = "баланс"
        except (ValueError, AttributeError):
            fr_hint = ""
        if fr_hint:
            lines.append(f"<code>{fr}  → {fr_hint}</code>")
        else:
            lines.append(f"<code>{fr}</code>")

    # ── ЛИКВИДАЦИИ (монета + рынок) ──
    has_coin_liq = _has(liq_up) or _has(liq_dn)
    has_mkt_liq  = _has(mkt_liq_long) or _has(mkt_liq_short)
    if has_coin_liq or has_mkt_liq:
        lines.append("")
        lines.append("<b>ЛИКВИДАЦИИ (1ч)</b>")
        if has_coin_liq:
            lines.append(f"<code>  {coin}:</code>")
            lines.append(f"<code>  ↑ шорты   {liq_up}</code>")
            lines.append(f"<code>  ↓ лонги   {liq_dn}</code>")
        if has_mkt_liq:
            lines.append(f"<code>  РЫНОК:</code>")
            lines.append(f"<code>  ↑ шорты   {mkt_liq_short}</code>")
            lines.append(f"<code>  ↓ лонги   {mkt_liq_long}</code>")

    # ── НАСТРОЕНИЕ ──
    if _has(fg):
        lines.append("")
        lines.append("<b>НАСТРОЕНИЕ</b>")
        try:
            fg_val = int(fg)
            if fg_val <= 25:
                fg_icon = "😱"
            elif fg_val <= 45:
                fg_icon = "😟"
            elif fg_val <= 55:
                fg_icon = "😐"
            elif fg_val <= 75:
                fg_icon = "😏"
            else:
                fg_icon = "🤑"
        except (ValueError, TypeError):
            fg_icon = ""
        lines.append(f"{fg_icon} <code>страх/жадность   {fg} — {fg_lbl}</code>")

    # ── ТЕХН. ИНДИКАТОРЫ ──
    rsi_val = d.get("rsi", "—")
    macd_val = d.get("macd", "—")
    sma50_val = d.get("sma50", "—")
    sma200_val = d.get("sma200", "—")

    has_tech = _has(rsi_val) or _has(macd_val)
    if has_tech:
        lines.append("")
        lines.append("<b>ТЕХН. ИНДИКАТОРЫ</b>")
        if _has(rsi_val):
            try:
                rv = float(rsi_val)
                if rv > 70:
                    rsi_hint = "перекуплен ⚠️"
                elif rv < 30:
                    rsi_hint = "перепродан 🔥"
                elif rv > 60:
                    rsi_hint = "бычья зона"
                elif rv < 40:
                    rsi_hint = "медвежья зона"
                else:
                    rsi_hint = "нейтрально"
            except (ValueError, TypeError):
                rsi_hint = ""
            lines.append(f"<code>  📈 RSI       {rsi_val} — {rsi_hint}</code>")
        if _has(macd_val):
            try:
                mv = float(macd_val)
                macd_hint = "бычий" if mv > 0 else "медвежий"
            except (ValueError, TypeError):
                macd_hint = ""
            lines.append(f"<code>  📉 MACD      {macd_val} — {macd_hint}</code>")
        if _has(sma50_val) and _has(sma200_val):
            try:
                s50 = float(str(sma50_val).replace("$", "").replace(",", ""))
                s200 = float(str(sma200_val).replace("$", "").replace(",", ""))
                cross = "golden cross 🔺" if s50 > s200 else "death cross 🔻"
            except (ValueError, TypeError):
                cross = ""
            lines.append(f"<code>  📊 SMA50     {sma50_val}</code>")
            lines.append(f"<code>  📊 SMA200    {sma200_val}</code>")
            if cross:
                lines.append(f"<code>              {cross}</code>")

    # ── ON-CHAIN ──
    active_addr = d.get("active_addresses", "—")
    active_addr_chg = d.get("active_addresses_change", "—")
    exchange_reserve = d.get("exchange_reserve_btc", "—")
    exchange_netflow = d.get("exchange_netflow_btc", "—")
    sopr_val = d.get("sopr", "—")

    has_onchain = _has(active_addr) or _has(exchange_reserve) or _has(sopr_val) or _has(exchange_netflow)
    if has_onchain:
        lines.append("")
        lines.append("<b>ON-CHAIN</b>")
        if _has(active_addr):
            addr_arrow = _arrow(active_addr_chg)
            lines.append(f"<code>  👥 адреса    {active_addr} {addr_arrow} {active_addr_chg}</code>")
        if _has(exchange_reserve):
            lines.append(f"<code>  🏦 резерв    {exchange_reserve} BTC</code>")
        if _has(exchange_netflow):
            try:
                nf = float(str(exchange_netflow).replace(",", "").replace("+", ""))
                if nf < 0:
                    nf_hint = "📤 отток (бычий)"
                elif nf > 0:
                    nf_hint = "📥 приток (медвежий)"
                else:
                    nf_hint = "баланс"
            except (ValueError, TypeError):
                nf_hint = ""
            lines.append(f"<code>  🔄 поток     {exchange_netflow} BTC</code>")
            if nf_hint:
                lines.append(f"<code>              {nf_hint}</code>")
        if _has(sopr_val):
            try:
                sv = float(sopr_val)
                sopr_hint = "прибыль" if sv > 1 else "убыток" if sv < 1 else "безубыток"
            except (ValueError, TypeError):
                sopr_hint = ""
            lines.append(f"<code>  📊 SOPR      {sopr_val} — {sopr_hint}</code>")

    # ── МАКРО ──
    ahr999_val = d.get("ahr999", "—")
    bull_peak = d.get("bull_peak_ratio", "—")
    bubble_val = d.get("bitcoin_bubble", "—")
    etf_val = d.get("etf_netflow", "—")
    stablecoin_mcap_val = d.get("stablecoin_mcap", "—")
    defi_tvl_val = d.get("defi_tvl", "—")
    defi_tvl_chg = d.get("defi_tvl_change", "—")

    has_macro = _has(ahr999_val) or _has(etf_val) or _has(stablecoin_mcap_val) or _has(defi_tvl_val) or _has(bull_peak)
    if has_macro:
        lines.append("")
        lines.append("<b>МАКРО</b>")
        if _has(ahr999_val):
            try:
                av = float(ahr999_val)
                if av < 0.45:
                    ahr_hint = "зона покупки 🔥"
                elif av > 1.2:
                    ahr_hint = "переоценён ⚠️"
                else:
                    ahr_hint = "нормальная зона"
            except (ValueError, TypeError):
                ahr_hint = ""
            lines.append(f"<code>  📊 AHR999    {ahr999_val} — {ahr_hint}</code>")
        if _has(bull_peak):
            lines.append(f"<code>  🔝 Bull Peak {bull_peak} индикаторов</code>")
        if _has(etf_val):
            try:
                ev_str = str(etf_val).replace("$", "").replace(",", "").strip()
                ev = float(ev_str)
                etf_hint = "📥 приток" if ev > 0 else "📤 отток"
            except (ValueError, TypeError):
                etf_hint = ""
            lines.append(f"<code>  💰 BTC ETF   {etf_val} {etf_hint}</code>")
        if _has(stablecoin_mcap_val):
            lines.append(f"<code>  💵 Стейблы   {stablecoin_mcap_val}</code>")
        if _has(defi_tvl_val):
            tvl_arrow = _arrow(defi_tvl_chg)
            lines.append(f"<code>  🏦 DeFi TVL  {defi_tvl_val} {tvl_arrow} {defi_tvl_chg}</code>")

    # ── CROSS-EXCHANGE (Bitget) ──
    bg_long_acc = d.get("bitget_long_acc", "—")
    bg_short_acc = d.get("bitget_short_acc", "—")
    bg_long_pos = d.get("bitget_long_pos", "—")
    bg_short_pos = d.get("bitget_short_pos", "—")
    bg_oi = d.get("bitget_oi_usd", "—")

    has_cross = _has(bg_long_acc) or _has(bg_long_pos)
    if has_cross:
        lines.append("")
        lines.append("<b>CROSS-EXCHANGE</b>")
        if _has(bg_long_acc):
            lines.append(f"<code>  🔵 Bitget Acc L {bg_long_acc} / S {bg_short_acc}</code>")
        if _has(bg_long_pos):
            lines.append(f"<code>  🔵 Bitget Pos L {bg_long_pos} / S {bg_short_pos}</code>")
        if _has(bg_oi):
            lines.append(f"<code>  📈 Bitget OI  {bg_oi}</code>")

    # ══════ ВЕРДИКТ (внизу, после всех метрик) ══════

    # ── СИГНАЛ ──
    lines.append("")
    lines.append(f"⚡ <code>СИГНАЛ   {signal}   {sig_lbl}</code>")

    # ── LLM-АНАЛИЗ ──
    if llm_text:
        lines.append("")
        lines.append(f"🤖 <b>AI-АНАЛИЗ</b>")
        lines.append(html_lib.escape(llm_text))

    # ── РЕКОМЕНДАЦИЯ + ЗОНЫ ──
    if recommendation:
        rec_clean = recommendation.replace("*", "").replace("_", "").strip()
        rec_upper = rec_clean.upper()
        if "ПОКУПАТЬ" in rec_upper:
            rec_icon = "🟢"
        elif "ПРОДАВАТЬ" in rec_upper:
            rec_icon = "🔴"
        else:
            rec_icon = "🟡"
        lines.append("")
        lines.append(f"{rec_icon} <b>РЕКОМЕНДАЦИЯ:</b> {html_lib.escape(rec_clean)}")

    if buy_zone or sell_zone:
        lines.append("")
        if buy_zone:
            lines.append(f"🔺 <code>Зона покупки:  {html_lib.escape(buy_zone)}</code>")
        if sell_zone:
            lines.append(f"🔻 <code>Зона продажи:  {html_lib.escape(sell_zone)}</code>")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚡ <b>Zender Commander Terminal</b>")
    lines.append("t.me/ZenderCommander_bot")

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
    # Тестовый режим: все монеты всем. После Telegram Payments — ограничить по тарифу.
    coins = COINS
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
    # Тестовый режим: все монеты всем
    coins   = COINS
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
    log.info("⚡ Zender Commander Terminal Bot — starting...")

    # Запускаем коллектор данных как фоновую задачу
    asyncio.create_task(collector_loop(interval_minutes=15))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
