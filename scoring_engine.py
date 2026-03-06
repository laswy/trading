"""Unified opportunity scoring engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ScoringWeights:
    trend_score: int = 20
    momentum_score: int = 15
    liquidity_score: int = 15
    smart_money_score: int = 20
    wallet_cluster_score: int = 15
    ai_prediction_score: int = 15


class OpportunityScoringEngine:
    def __init__(self, weights: ScoringWeights | None = None) -> None:
        self.weights = weights or ScoringWeights()

    def score(self, signals: Dict[str, float], ai_probability: float) -> Dict[str, float | str]:
        trend = min(signals.get("trend_score", 0), self.weights.trend_score)
        momentum = min(signals.get("momentum_score", 0), self.weights.momentum_score)
        liquidity = min(signals.get("liquidity_score", 0), self.weights.liquidity_score)
        smart = min(signals.get("smart_money_score", 0), self.weights.smart_money_score)
        cluster = min(signals.get("wallet_cluster_score", 0), self.weights.wallet_cluster_score)
        ai = min(ai_probability * 100 * self.weights.ai_prediction_score / 100, self.weights.ai_prediction_score)

        total = trend + momentum + liquidity + smart + cluster + ai
        label = "hold"
        if total >= 90:
            label = "strong_buy"
        elif total >= 75:
            label = "buy"

        return {
            "score": round(total, 2),
            "decision": label,
            "components": {
                "trend": trend,
                "momentum": momentum,
                "liquidity": liquidity,
                "smart_money": smart,
                "wallet_cluster": cluster,
                "ai_prediction": round(ai, 2),
            },
        }
