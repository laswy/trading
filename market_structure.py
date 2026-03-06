"""Market structure and momentum analysis based on technical indicators."""

from __future__ import annotations

from typing import Dict, List


def _ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    ema = values[0]
    for val in values[1:]:
        ema = val * k + ema * (1 - k)
    return ema


def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(-period, 0):
        delta = values[i] - values[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses -= delta
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))


class MarketStructureEngine:
    def analyze(self, candles: List[Dict[str, float]]) -> Dict[str, float | str | bool]:
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema20 = _ema(closes[-50:], 20)
        ema50 = _ema(closes[-200:], 50)
        ema200 = _ema(closes[-500:], 200)
        rsi = _rsi(closes)

        price = closes[-1] if closes else 0
        vwap = sum(c["close"] * c["volume"] for c in candles[-100:]) / max(sum(volumes[-100:]), 1)
        macd = _ema(closes[-60:], 12) - _ema(closes[-60:], 26)

        bb_mid = sum(closes[-20:]) / max(len(closes[-20:]), 1)
        bb_std = (sum((x - bb_mid) ** 2 for x in closes[-20:]) / max(len(closes[-20:]), 1)) ** 0.5
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std

        bullish = price > ema50 and ema50 > ema200
        volume_expansion = (volumes[-1] > (sum(volumes[-24:]) / max(len(volumes[-24:]), 1)) * 2) if volumes else False
        breakout = price > bb_upper and volume_expansion
        accumulation = bb_lower <= price <= bb_upper and rsi > 45

        trend_score = 0
        trend_score += 8 if bullish else 0
        trend_score += 4 if price > vwap else 0
        trend_score += 4 if macd > 0 else 0
        trend_score += 4 if breakout or accumulation else 0

        return {
            "price": price,
            "ema20": ema20,
            "ema50": ema50,
            "ema200": ema200,
            "vwap": vwap,
            "rsi": rsi,
            "macd": macd,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bullish": bullish,
            "breakout": breakout,
            "accumulation": accumulation,
            "volume_expansion": volume_expansion,
            "trend_score": min(trend_score, 20),
            "trend_label": "bullish" if bullish else "neutral",
        }

    def momentum_score(self, volumes_1h: float, volumes_24h: float, recent_returns: List[float]) -> int:
        avg_hour = volumes_24h / 24 if volumes_24h else 0
        volume_spike = volumes_1h > avg_hour * 2 if avg_hour else False
        acceleration = sum(recent_returns[-3:]) > 0.08 if recent_returns else False
        volatility_expansion = max(recent_returns[-8:], default=0) - min(recent_returns[-8:], default=0) > 0.12

        score = 0
        score += 7 if volume_spike else 0
        score += 4 if acceleration else 0
        score += 4 if volatility_expansion else 0
        return min(score, 15)
