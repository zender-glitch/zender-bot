"""
ZENDER COMMANDER TERMINAL — Database
Работа с Supabase: пользователи + рыночные данные.
"""

import logging
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Пользователи ─────────────────────────────────────────────────────────

    async def upsert_user(self, telegram_id, username, first_name):
        """Создать или обновить пользователя"""
        try:
            self.client.table("users").upsert(
                {
                    "telegram_id": telegram_id,
                    "username": username,
                    "first_name": first_name,
                },
                on_conflict="telegram_id",
            ).execute()
        except Exception as e:
            log.error(f"upsert_user error: {e}")

    async def get_user(self, telegram_id):
        """Получить пользователя по telegram_id"""
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

    async def update_user(self, telegram_id, data):
        """Обновить данные пользователя"""
        try:
            self.client.table("users").update(data).eq(
                "telegram_id", telegram_id
            ).execute()
        except Exception as e:
            log.error(f"update_user error: {e}")

    async def set_plan(self, telegram_id, plan):
        await self.update_user(telegram_id, {"plan": plan})

    async def get_plan(self, telegram_id):
        user = await self.get_user(telegram_id)
        return user.get("plan", "free") if user else "free"

    # ── Рыночные данные ──────────────────────────────────────────────────────

    async def upsert_market_data(self, record: dict):
        """
        Сохранить/обновить рыночные данные для монеты.
        record должен содержать ключ 'coin'.
        """
        try:
            self.client.table("market_data").upsert(
                record,
                on_conflict="coin",
            ).execute()
        except Exception as e:
            log.error(f"upsert_market_data error: {e}")

    async def get_market_data(self, coins: list) -> dict:
        """
        Получить рыночные данные для списка монет.
        Возвращает: {COIN: {price, change, signal, label, ...}}
        Если данных нет — возвращает заглушки.
        """
        result = {}

        try:
            res = (
                self.client.table("market_data")
                .select("*")
                .in_("coin", coins)
                .execute()
            )
            for row in res.data:
                coin = row["coin"]
                if coin not in result:
                    result[coin] = row
        except Exception as e:
            log.warning(f"get_market_data: {e}")

        # Заглушки для монет без данных
        stubs = {
            "BTC":  {"price": "$83,420",  "change": "+0.8%",  "signal": "▓▓░░░", "label": "слабый"},
            "ETH":  {"price": "$3,240",   "change": "+0.1%",  "signal": "▓▓▓▓░", "label": "сильный"},
            "SOL":  {"price": "$142.80",  "change": "-1.4%",  "signal": "▓▓▓░░", "label": "средний"},
            "BNB":  {"price": "$412.00",  "change": "+1.2%",  "signal": "▓▓▓░░", "label": "средний"},
            "AVAX": {"price": "$38.20",   "change": "-2.1%",  "signal": "▓▓░░░", "label": "слабый"},
        }
        for coin in coins:
            if coin not in result:
                result[coin] = stubs.get(coin, {
                    "price": "—", "change": "—",
                    "signal": "░░░░░", "label": "нет данных",
                })

        return result


db = Database()
