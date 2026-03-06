"""Validation engine to reject unsafe or low-quality tokens."""

from __future__ import annotations

from typing import Dict, List, Tuple

from config import TRADING_CONFIG


class TokenValidator:
    def __init__(self) -> None:
        self.rules = [
            ("liquidity", lambda x: x >= TRADING_CONFIG.min_liquidity_usd, "Liquidity below threshold"),
            ("volume_24h", lambda x: x >= TRADING_CONFIG.min_volume_24h_usd, "24h volume below threshold"),
            ("contract_verified", lambda x: bool(x), "Contract not verified"),
            ("honeypot_risk", lambda x: not bool(x), "Honeypot risk detected"),
            ("buy_tax", lambda x: x <= TRADING_CONFIG.max_buy_tax_pct, "Buy tax too high"),
            ("sell_tax", lambda x: x <= TRADING_CONFIG.max_sell_tax_pct, "Sell tax too high"),
            ("top10_holder_pct", lambda x: x <= TRADING_CONFIG.max_top10_holder_pct, "Holder concentration too high"),
            ("lp_locked", lambda x: bool(x), "LP unlocked"),
            ("owner_suspicious", lambda x: not bool(x), "Owner wallet suspicious"),
        ]

    def validate(self, token: Dict) -> Tuple[bool, List[str]]:
        issues: List[str] = []
        for field, check, message in self.rules:
            value = token.get(field)
            if value is None:
                issues.append(f"Missing required field: {field}")
                continue
            if not check(value):
                issues.append(message)
        return len(issues) == 0, issues
