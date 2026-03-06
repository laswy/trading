"""Global and position-level risk controls."""

from __future__ import annotations

from dataclasses import dataclass

from config import TRADING_CONFIG


@dataclass
class RiskState:
    start_equity: float
    current_equity: float
    halted: bool = False


class RiskEngine:
    def daily_loss_exceeded(self, state: RiskState) -> bool:
        if state.start_equity <= 0:
            return False
        daily_drawdown = (state.start_equity - state.current_equity) / state.start_equity
        return daily_drawdown >= TRADING_CONFIG.max_daily_loss_pct

    def position_loss_exceeded(self, entry_price: float, current_price: float) -> bool:
        if entry_price <= 0:
            return False
        drawdown = (entry_price - current_price) / entry_price
        return drawdown >= TRADING_CONFIG.max_position_loss_pct

    def evaluate_system_state(self, state: RiskState) -> str:
        if self.daily_loss_exceeded(state):
            state.halted = True
            return "halt"
        return "active"
