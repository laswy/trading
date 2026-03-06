"""Continuous monitoring for exits and risk-aware position maintenance."""

from __future__ import annotations

from typing import Dict


class MonitorEngine:
    def exit_signal(self, telemetry: Dict) -> str | None:
        if telemetry.get("trend_breakdown"):
            return "trend_breakdown"
        if telemetry.get("volume_collapse"):
            return "volume_collapse"
        if telemetry.get("whale_selling"):
            return "whale_selling"
        if telemetry.get("liquidity_removal"):
            return "liquidity_removal"
        return None
