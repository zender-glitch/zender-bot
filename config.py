"""
Конфигурация — читаем переменные из .env файла
"""

import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN         = os.getenv("BOT_TOKEN",         "")
SUPABASE_URL      = os.getenv("SUPABASE_URL",      "")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY",      "")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY",  "")
ANTHROPIC_KEY     = os.getenv("ANTHROPIC_KEY",      "")
# On-chain: бесплатные API (blockchain.info + BGeometrics), ключ НЕ нужен

# Проверка при старте
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в .env!")
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL не задан в .env!")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY не задан в .env!")
if not COINGLASS_API_KEY:
    raise ValueError("COINGLASS_API_KEY не задан в .env!")
