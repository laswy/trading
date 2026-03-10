---
name: openclaw-solana-meme-bot
description: Operate, configure, and continuously improve the modular Solana meme trading bot for OpenClaw. Use when users ask to run the bot, switch profiles, validate runtime setup, or build a full self-learning loop (data collection, training, evaluation, promotion, canary, rollback).
---

# OpenClaw Solana Meme Bot

Run and improve the bot in `solana-meme-bot/` using safe workflows.

## Workflow selector

1. **Ops mode (fast)**: run bot, switch profile, validate `.env`, monitor KPIs.
2. **Self-learning mode (full)**: bootstrap ML pipeline, train daily offline, promote model only when guardrails pass.

## Ops mode

### Commands

Run from repository root:

```bash
# validate setup
bash skills/openclaw-solana-meme-bot/scripts/check_setup.sh

# switch profile
bash skills/openclaw-solana-meme-bot/scripts/switch_profile.sh early_sniper
# or
bash skills/openclaw-solana-meme-bot/scripts/switch_profile.sh safe_trend

# run bot
bash skills/openclaw-solana-meme-bot/scripts/run_bot.sh
```

### Rules

- Keep core trading logic unchanged unless user explicitly asks.
- Tune profiles first; change code only after A/B evidence.
- Keep required env keys present before every run.

## Self-learning mode (full)

### 1) Bootstrap ML scaffold

```bash
python skills/openclaw-solana-meme-bot/scripts/bootstrap_self_learning.py
```

This creates a minimal pipeline in `solana-meme-bot/ml/` and appends optional ML env knobs.

### 2) Daily loop

1. Export latest trading events/features.
2. Build training dataset with clear labels and reward definition.
3. Train candidate model offline.
4. Evaluate candidate vs champion.
5. Promote only if all guardrails pass.
6. Deploy in shadow/canary before full traffic.
7. Auto-rollback on degradation.

Use `references/self-learning-full.md` for thresholds and promotion policy.

### 3) Safety guardrails

- Never remove hard risk limits.
- Never hot-swap to full allocation directly.
- Always maintain previous champion artifact for rollback.

## References

- `references/env-keys.md`
- `references/daily-ops.md`
- `references/self-learning-full.md`
