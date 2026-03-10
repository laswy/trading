---
name: openclaw-solana-meme-bot
description: Operate, configure, and optimize the modular Solana meme trading bot project for OpenClaw usage. Use when users ask to run the bot, switch strategy profiles, validate .env/runtime setup, inspect trading health, or prepare a daily improvement loop without changing core algorithm logic.
---

# OpenClaw Solana Meme Bot

Operate the bot in `solana-meme-bot/` with safe, repeatable steps. Prefer profile/config tuning and operational checks over editing `core/engine_legacy.py`.

## Quick workflow

1. Validate runtime prerequisites and `.env`.
2. Select profile (`early_sniper` or `safe_trend`) and apply it.
3. Run bot from project root (`python main.py`).
4. Observe logs and key risk metrics.
5. Do daily tuning by updating profile parameters, not core trading logic.

## Execution rules

- Keep algorithm DNA intact: avoid changing scoring/entry/exit logic in `core/engine_legacy.py` unless explicitly requested.
- Use `scripts/run_bot.sh` to start consistently.
- Use `scripts/switch_profile.sh <profile>` to switch between tested presets.
- If `.env` misses mandatory keys, stop and report exactly which keys are missing.
- For improvements, change profile values first; only propose code changes after profile A/B evidence.

## Commands

From repository root:

```bash
# 1) Validate environment and files
bash skills/openclaw-solana-meme-bot/scripts/check_setup.sh

# 2) Switch profile
bash skills/openclaw-solana-meme-bot/scripts/switch_profile.sh early_sniper
# or
bash skills/openclaw-solana-meme-bot/scripts/switch_profile.sh safe_trend

# 3) Start bot
bash skills/openclaw-solana-meme-bot/scripts/run_bot.sh
```

## Daily optimization loop (without changing core logic)

- Use `references/daily-ops.md` for the checklist.
- Compare KPI between profiles over equal market windows.
- Update only profile knobs first (scan interval, slippage, TP/trailing, risk caps).
- Keep rollback path: preserve yesterday’s `.env` before applying new profile.

## References

- Use `references/env-keys.md` for required environment keys and profile-safe keys.
- Use `references/daily-ops.md` for A/B routine and profitability guardrails.
