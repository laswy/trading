"""Position sizing, trailing stop, and profit ladder management."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from config import TRADING_CONFIG


@dataclass
class PositionState:
    token_address: str
    entry_price: float
    size_usd: float
    peak_price: float
    sold_30: bool = False
    sold_60: bool = False
    closed: bool = False


class PositionManager:
    def size_position(self, capital_usd: float, confidence: float, volatility: float, liquidity: float) -> float:
        base = capital_usd * TRADING_CONFIG.max_position_pct
        confidence_mult = min(max(confidence, 0.2), 1.0)
        volatility_mult = max(0.4, 1.0 - volatility)
        liquidity_mult = 1.0 if liquidity > 800_000 else 0.8
        return max(100.0, base * confidence_mult * volatility_mult * liquidity_mult)

    def apply_profit_ladder(self, pos: PositionState, current_price: float) -> Dict:
        pnl = (current_price - pos.entry_price) / max(pos.entry_price, 1e-9)
        actions: List[Dict] = []
        if pnl >= 0.30 and not pos.sold_30:
            pos.sold_30 = True
            actions.append({"action": "sell", "fraction": 0.30, "reason": "tp_30"})
        if pnl >= 0.60 and not pos.sold_60:
            pos.sold_60 = True
            actions.append({"action": "sell", "fraction": 0.30, "reason": "tp_60"})
        if pnl >= 1.00 and not pos.closed:
            pos.closed = True
            actions.append({"action": "sell", "fraction": 1.0, "reason": "tp_100"})
        return {"pnl": pnl, "actions": actions}

    def trailing_stop_triggered(self, pos: PositionState, current_price: float) -> bool:
        pos.peak_price = max(pos.peak_price, current_price)
        if pos.peak_price <= pos.entry_price:
            return False
        drop_from_peak = (pos.peak_price - current_price) / pos.peak_price
        return drop_from_peak >= TRADING_CONFIG.trailing_stop_pct
