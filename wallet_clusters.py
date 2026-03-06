"""Wallet clustering engine based on behavior similarity and fund-flow topology."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List


class WalletClusterEngine:
    def cluster(self, wallet_events: List[Dict]) -> Dict[str, Dict]:
        groups = defaultdict(list)
        for event in wallet_events:
            key = (
                event.get("shared_contract", "na"),
                event.get("time_bucket", "na"),
                event.get("pattern", "na"),
            )
            groups[key].append(event)

        clusters: Dict[str, Dict] = {}
        for i, (_key, events) in enumerate(groups.items(), start=1):
            addresses = {e["wallet"] for e in events}
            total_flow = sum(e.get("usd_value", 0) for e in events)
            cluster_type = "insider" if len(addresses) <= 3 and total_flow > 300_000 else "whale"
            if len(addresses) >= 4 and total_flow > 500_000:
                cluster_type = "coordinated_accumulation"
            clusters[f"cluster_{i}"] = {
                "wallet_count": len(addresses),
                "total_flow_usd": total_flow,
                "cluster_type": cluster_type,
                "wallets": sorted(addresses),
            }

        return clusters

    def score(self, clusters: Dict[str, Dict]) -> int:
        score = 0
        for data in clusters.values():
            if data["cluster_type"] == "coordinated_accumulation":
                score += 7
            elif data["cluster_type"] == "whale":
                score += 5
            elif data["cluster_type"] == "insider":
                score += 3
        return min(score, 15)
