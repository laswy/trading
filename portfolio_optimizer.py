"""Portfolio optimizer that balances concentration and confidence-adjusted allocation."""

from __future__ import annotations

from typing import Dict, List


class PortfolioOptimizer:
    def rebalance_targets(self, opportunities: List[Dict], capital_usd: float) -> List[Dict]:
        if not opportunities:
            return []

        weighted = []
        for opp in opportunities:
            confidence = opp.get("ai_probability", 0.5)
            liquidity = min(opp.get("liquidity", 0) / 1_000_000, 1)
            volatility = 1 - min(opp.get("volatility", 0.5), 1)
            weight = max(0.01, confidence * 0.5 + liquidity * 0.3 + volatility * 0.2)
            weighted.append((opp, weight))

        total_weight = sum(w for _, w in weighted)
        allocations = []
        for opp, weight in weighted:
            pct = weight / total_weight
            allocations.append({
                "token_address": opp["token_address"],
                "target_usd": capital_usd * pct,
                "target_pct": pct,
                "chain": opp["chain"],
            })
        return allocations
