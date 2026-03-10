# Daily Ops and Improvement Loop

## Goal
Improve consistency and profitability without touching core trading logic.

## Procedure
1. Run one profile per time block (e.g., 24h) with fixed capital settings.
2. Record:
   - Fill rate
   - Win rate
   - Net PnL/day
   - Max intraday drawdown
   - Fast-dump exit count
3. Compare profile blocks over similar volatility periods.
4. Tune only 2-3 parameters/day to avoid confounded results.
5. If PnL degrades for 2 consecutive windows, rollback to last stable profile.

## Guardrails
- Do not widen slippage and lower score threshold aggressively at the same time.
- Keep risk limits active at all times.
- Preserve previous `.env` snapshot before applying a new profile.
