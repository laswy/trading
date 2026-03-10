# Full Self-Learning Blueprint

## Objective
Build a continuous learning loop that updates model behavior safely and measurably.

## Architecture

- **Data layer**: collect decisions, market context, execution quality, outcomes.
- **Feature layer**: deterministic feature builder with versioning.
- **Training layer**: scheduled offline training (daily/weekly).
- **Evaluation layer**: walk-forward and out-of-sample checks.
- **Registry layer**: champion/challenger model metadata and artifacts.
- **Serving layer**: inference endpoint or local inference module.
- **Safety layer**: canary, drift checks, rollback triggers.

## Minimum metrics

Promote only if candidate beats champion on:
- Net PnL (after fees/slippage)
- Max drawdown
- Profit factor
- Win rate stability
- Tail risk (worst N trades)

## Deployment policy

1. **Shadow**: 3-7 days, no real capital impact.
2. **Canary**: 5-10% capital allocation.
3. **Ramp-up**: 25% -> 50% -> 100% only if stable.
4. **Rollback**: immediate if DD threshold or KPI floor is breached.

## Data contract (recommended)

Each row should include:
- timestamp, chain, token, regime labels
- pre-trade features
- model decision/action
- fill details (price, slippage, latency)
- realized outcome over fixed horizons

## Practical recommendation

Start with contextual bandit or supervised ranking before full RL.
