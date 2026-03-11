"""
ZENDER TERMINAL — Database
Работа с Supabase: пользователи + рыночные данные.
"""

import logging
from datetime import datetime, timezone
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Пользователи ─────────────────────────────────────────────────────────

    async def upsert_user(self, telegram_id, username, first_name, language=None):
        """Создать или обновить пользователя"""
        try:
            data = {
                "telegram_id": telegram_id,
                "username": username,
                "first_name": first_name,
            }
            if language is not None:
                data["language"] = language
            self.client.table("users").upsert(
                data,
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

    async def get_view_mode(self, telegram_id) -> str:
        user = await self.get_user(telegram_id)
        return user.get("view_mode", "basic") if user else "basic"

    async def set_view_mode(self, telegram_id, mode: str):
        await self.update_user(telegram_id, {"view_mode": mode})

    # ── Алерты ────────────────────────────────────────────────────────────────

    async def get_users_for_alerts(self) -> list:
        """
        Получить пользователей, которым пора слать алерт.
        Условие: alerts_enabled = true И (last_alert_at IS NULL ИЛИ прошло >= interval минут).
        """
        try:
            res = (
                self.client.table("users")
                .select("telegram_id, interval, last_alert_at, alerts_enabled, language")
                .eq("alerts_enabled", True)
                .execute()
            )
            now = datetime.now(timezone.utc)
            users_to_alert = []

            for user in res.data:
                interval = user.get("interval", 60) or 60
                last_alert = user.get("last_alert_at")

                if last_alert is None:
                    users_to_alert.append(user)
                else:
                    try:
                        if isinstance(last_alert, str):
                            last_dt = datetime.fromisoformat(last_alert.replace("Z", "+00:00"))
                        else:
                            last_dt = last_alert
                        diff_minutes = (now - last_dt).total_seconds() / 60
                        if diff_minutes >= interval:
                            users_to_alert.append(user)
                    except (ValueError, TypeError):
                        users_to_alert.append(user)

            return users_to_alert
        except Exception as e:
            log.error(f"get_users_for_alerts error: {e}")
            return []

    async def update_last_alert(self, telegram_id):
        """Обновить время последнего алерта"""
        now = datetime.now(timezone.utc).isoformat()
        await self.update_user(telegram_id, {"last_alert_at": now})

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
        for coin in coins:
            if coin not in result:
                result[coin] = {
                    "price": "—", "change": "—",
                    "signal": "⬜⬜⬜⬜⬜", "label": "no data",
                }

        return result


db = Database()
