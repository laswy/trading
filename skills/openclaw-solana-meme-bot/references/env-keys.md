# Environment Keys for OpenClaw Solana Meme Bot

## Required to start bot
- `SOLANA_WALLET_ADDRESS`
- `SOLANA_PRIVATE_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Optional for Base chain
- `BASE_PRIVATE_KEY`
- `OKX_API_KEY`
- `OKX_SECRET_KEY`
- `OKX_API_PASSPHRASE`
- `OKX_PROJECT_ID`

If Base keys are missing, bot should still run for Solana-only mode.

## Profile-safe tuning keys
Use profiles to adjust these safely before any code change:
- `MIN_OPPORTUNITY_SCORE`
- `SCAN_INTERVAL_SECONDS`
- `VALIDATOR_WORKERS`
- `TP_CHECK_NEW_S`, `TP_CHECK_MID_S`, `TP_CHECK_OLD_S`
- `SLIPPAGE_PCT`, `SLIPPAGE_NEW_PCT`, `SLIPPAGE_MID_PCT`
- `PRIORITY_FEE_DEFAULT`, `PRIORITY_FEE_HIGH`, `PRIORITY_FEE_VERY_HIGH`
- `TAKE_PROFIT_PCT`, `TRAIL_TRIGGER_PCT`, `TRAIL_TARGET_PCT`
- `FAST_DUMP_PCT`, `FAST_DUMP_WINDOW_S`
- `RISK_MAX_POSITIONS`, `RISK_MAX_DAILY_TRADES`, `RISK_MAX_DAILY_LOSS`

## Keys not in profile by default
Telegram keys are intentionally not included in profile files. Keep notification routing stable unless explicitly requested.
