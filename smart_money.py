"""Smart money engine for detecting high-conviction wallet flows."""

from __future__ import annotations

from statistics import mean
from typing import Dict, List


class SmartMoneyEngine:
    def __init__(self) -> None:
        self.smart_wallets: Dict[str, Dict] = {}
        self.whale_wallets: Dict[str, Dict] = {}
        self.profitable_traders: Dict[str, Dict] = {}

    def update_wallet_registry(self, wallets: List[Dict]) -> None:
        for wallet in wallets:
            address = wallet["address"]
            roi = wallet.get("roi_90d", 0)
            avg_trade = wallet.get("avg_trade_size_usd", 0)
            record = {"roi_90d": roi, "avg_trade_size_usd": avg_trade, "win_rate": wallet.get("win_rate", 0)}
            if roi >= 0.4 and record["win_rate"] >= 0.55:
                self.smart_wallets[address] = record
            if avg_trade >= 250_000:
                self.whale_wallets[address] = record
            if roi >= 0.7:
                self.profitable_traders[address] = record

    def analyze_flows(self, flows: List[Dict]) -> Dict[str, float | int | bool]:
        smart_buys = [f for f in flows if f.get("wallet") in self.smart_wallets and f.get("side") == "buy"]
        whales = [f for f in flows if f.get("wallet") in self.whale_wallets and f.get("side") == "buy"]
        unique_buyers = len({f["wallet"] for f in smart_buys})
        cluster_buying = unique_buyers >= 3
        whale_acc = sum(f.get("usd_value", 0) for f in whales)

        smart_score = 0
        smart_score += 8 if cluster_buying else 0
        smart_score += 8 if whale_acc >= 500_000 else 0
        if smart_buys and mean([f.get("usd_value", 0) for f in smart_buys]) > 75_000:
            smart_score += 4

        return {
            "smart_wallet_buys": len(smart_buys),
            "cluster_buying": cluster_buying,
            "whale_accumulation_usd": whale_acc,
            "smart_money_score": min(smart_score, 20),
        }
