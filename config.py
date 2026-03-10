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
ETHERSCAN_KEY     = os.getenv("ETHERSCAN_KEY",      "")
CRYPTOQUANT_KEY   = os.getenv("CRYPTOQUANT_KEY",    "")
# Binance + Bybit: бесплатные public API, ключ НЕ нужен
# On-chain: бесплатные API (blockchain.info + BGeometrics + DeFiLlama), ключ НЕ нужен
# Etherscan: бесплатный план (100k calls/day) — ETH on-chain данные
# CryptoQuant: бесплатный план (50 req/day, daily resolution, 7 days history)

# Проверка при старте
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в .env!")
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL не задан в .env!")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY не задан в .env!")
if not COINGLASS_API_KEY:
    raise ValueError("COINGLASS_API_KEY не задан в .env!")
