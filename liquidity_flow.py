"""Liquidity flow engine for LP additions/removals and pool depth shifts."""

from __future__ import annotations

from typing import Dict, List


class LiquidityFlowEngine:
    def analyze(self, events: List[Dict]) -> Dict[str, float | bool | int]:
        lp_add = sum(e.get("usd_delta", 0) for e in events if e.get("event") == "lp_add")
        lp_remove = sum(abs(e.get("usd_delta", 0)) for e in events if e.get("event") == "lp_remove")
        migration = any(e.get("event") == "dex_migration" for e in events)
        depth_change = sum(e.get("depth_delta_pct", 0) for e in events)

        inflow = lp_add > lp_remove
        shift = migration or abs(depth_change) > 10

        score = 0
        score += 8 if inflow and lp_add > 200_000 else 0
        score += 4 if shift else 0
        score += 3 if depth_change > 0 else 0

        return {
            "lp_add_usd": lp_add,
            "lp_remove_usd": lp_remove,
            "liquidity_inflow": inflow,
            "liquidity_shift": shift,
            "pool_depth_delta_pct": depth_change,
            "liquidity_score": min(score, 15),
        }
