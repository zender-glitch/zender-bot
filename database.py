"""
Работа с Supabase — пользователи, подписки, кэш рыночных данных
"""

import logging
from typing import Optional, List
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Пользователи ──────────────────────────────────────────────────────────

    async def upsert_user(self, telegram_id: int, username: str, first_name: str):
        """Создаём или обновляем пользователя"""
        try:
            self.client.table("users").upsert({
                "telegram_id": telegram_id,
                "username":    username,
                "first_name":  first_name,
            }, on_conflict="telegram_id").execute()
        except Exception as e:
            log.error(f"upsert_user error: {e}")

    async def get_user(self, telegram_id: int) -> Optional[dict]:
        """Получаем пользователя по Telegram ID"""
        try:
            res = (
                self.client.table("users")
                .select("*")
                .eq("telegram_id", telegram_id)
                .single()
                .execute()
            )
            return res.data
        except Exception:
            return None

    async def update_user(self, telegram_id: int, data: dict):
        """Обновляем поля пользователя"""
        try:
            self.client.table("users").update(data).eq("telegram_id", telegram_id).execute()
        except Exception as e:
            log.error(f"update_user error: {e}")

    # ── Рыночные данные (кэш от n8n) ─────────────────────────────────────────

    async def get_market_data(self, coins: List[str]) -> dict:
        """
        Читаем последние данные по монетам из кэша.
        n8n кладёт данные в таблицу market_data по расписанию.
        Берём самую свежую запись для каждой монеты.
        """
        result = {}
        try:
            res = (
                self.client.table("market_data")
                .select("*")
                .in_("coin", coins)
                .order("updated_at", desc=True)
                .execute()
            )
            seen = set()
            for row in res.data:
                coin = row["coin"]
                if coin not in seen:
                    seen.add(coin)
                    result[coin] = _format_row(row)
        except Exception as e:
            log.warning(f"get_market_data: {e} — returning stubs")

        # Заглушка для монет без данных
        for coin in coins:
            if coin not in result:
                result[coin] = _stub(coin)

        return result

    # ── Подписки ──────────────────────────────────────────────────────────────

    async def set_plan(self, telegram_id: int, plan: str):
        await self.update_user(telegram_id, {"plan": plan})

    async def get_plan(self, telegram_id: int) -> str:
        user = await self.get_user(telegram_id)
        return user.get("plan", "free") if user else "free"


def _format_row(row: dict) -> dict:
    """Форматируем сырую строку из Supabase для отображения в боте"""

    # --- Изменение OI (change) ---
    try:
        change_val = float(row.get("change", 0) or 0)
    except (ValueError, TypeError):
        change_val = 0.0

    if change_val > 0:
        change_str = f"+{change_val:.2f}%"
    elif change_val < 0:
        change_str = f"{change_val:.2f}%"
    else:
        change_str = "0.00%"

    # --- Визуальный сигнал (полоска из 5 блоков) ---
    abs_chg = abs(change_val)
    if abs_chg >= 4:
        filled = 5
    elif abs_chg >= 3:
        filled = 4
    elif abs_chg >= 2:
        filled = 3
    elif abs_chg >= 1:
        filled = 2
    else:
        filled = 1
    signal = "▓" * filled + "░" * (5 - filled)

    # --- Текстовая метка ---
    if abs_chg >= 3:
        label = "сильный"
    elif abs_chg >= 1.5:
        label = "средний"
    else:
        label = "слабый"

    # Направление сигнала
    if change_val > 0:
        label = "+" + label
    elif change_val < 0:
        label = "-" + label

    # --- Цена ---
    try:
        price_val = float(row.get("price", 0) or 0)
        price_str = f"${price_val:,.0f}" if price_val > 0 else "—"
    except (ValueError, TypeError):
        price_str = "—"

    return {
        "price":  price_str,
        "change": change_str,
        "signal": signal,
        "label":  label,
        "coin":   row.get("coin", ""),
    }


def _stub(coin: str) -> dict:
    """Заглушка если данных нет в базе"""
    stubs = {
        "BTC":  {"price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"},
        "ETH":  {"price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"},
        "SOL":  {"price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"},
        "BNB":  {"price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"},
        "AVAX": {"price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"},
    }
    return stubs.get(coin, {
        "price": "—", "change": "—", "signal": "░░░░░", "label": "нет данных"
    })


# Глобальный экземпляр
db = Database()
