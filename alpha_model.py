"""AI alpha model that predicts price increase probability in the next 6-24h."""

from __future__ import annotations

from typing import Dict, Iterable, List

try:
    from sklearn.ensemble import GradientBoostingClassifier
except Exception:  # pragma: no cover
    GradientBoostingClassifier = None


class AlphaModel:
    def __init__(self) -> None:
        self.model = GradientBoostingClassifier(random_state=42) if GradientBoostingClassifier else None
        self._is_fitted = False

    @staticmethod
    def _vectorize(feature: Dict[str, float]) -> List[float]:
        ordered_keys = [
            "trend_score",
            "momentum_score",
            "liquidity_score",
            "smart_money_score",
            "wallet_cluster_score",
            "volatility",
            "rsi",
            "macd",
            "liquidity_inflow",
        ]
        return [float(feature.get(k, 0)) for k in ordered_keys]

    def train(self, features: Iterable[Dict[str, float]], labels: Iterable[int]) -> None:
        if not self.model:
            return
        x = [self._vectorize(row) for row in features]
        y = list(labels)
        if len(x) < 20 or len(set(y)) < 2:
            return
        self.model.fit(x, y)
        self._is_fitted = True

    def predict_probability(self, feature: Dict[str, float]) -> float:
        if self.model and self._is_fitted:
            x = [self._vectorize(feature)]
            prob = float(self.model.predict_proba(x)[0][1])
            return max(0.0, min(1.0, prob))

        heuristic = (
            feature.get("trend_score", 0) / 20 * 0.25
            + feature.get("momentum_score", 0) / 15 * 0.2
            + feature.get("liquidity_score", 0) / 15 * 0.2
            + feature.get("smart_money_score", 0) / 20 * 0.2
            + feature.get("wallet_cluster_score", 0) / 15 * 0.15
        )
        return max(0.0, min(1.0, heuristic))
