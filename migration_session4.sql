-- ZENDER SESSION 4 — Новые источники данных
-- Coinglass Indicators + DeFiLlama + Whale Alert
-- Запустить в Supabase SQL Editor

ALTER TABLE market_data ADD COLUMN IF NOT EXISTS whale_tx_count TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS whale_volume_usd TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS whale_to_exchange TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS whale_from_exchange TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS stablecoin_mcap TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS defi_tvl TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS defi_tvl_change TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS etf_netflow TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS ahr999 TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS bull_peak_ratio TEXT DEFAULT '—';
ALTER TABLE market_data ADD COLUMN IF NOT EXISTS bitcoin_bubble TEXT DEFAULT '—';

-- Готово! Перезапусти бота на Railway после миграции.
