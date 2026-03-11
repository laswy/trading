#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
TRADE_LOG_PATH = ROOT / "data" / "trade_log.json"

BASELINE = {
    "MIN_OPPORTUNITY_SCORE": "83",
    "MIN_HOLDER_COUNT": "5",
    "MAX_TOP10_HOLDER_PCT": "20",
    "ENTRY_DIP_WINDOW_S": "5",
    "ENTRY_BBL_WINDOW_S": "15",
    "ENTRY_BBL_PERIOD": "14",
    "ENTRY_BBL_STD": "1.8",
    "TP1_PCT": "25",
    "TP2_PCT": "60",
    "TRAIL_TRIGGER_PCT": "20",
    "TRAILING_START_PCT": "20",
    "TRAILING_STOP_PCT": "10",
    "VOLUME_SPIKE_MULTIPLIER": "2",
    "MIN_LP_SOL": "5",
    "MIN_TOKEN_AGE_S": "90",
}


def parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def write_env(path: Path, values: dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in sorted(values.items())]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_trades(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _pnl_pct(tr: dict) -> float | None:
    for key in ("pnl_pct", "profit_pct", "roi_pct"):
        val = tr.get(key)
        if isinstance(val, (int, float)):
            return float(val)
    buy = tr.get("buy_price")
    sell = tr.get("sell_price")
    if isinstance(buy, (int, float)) and isinstance(sell, (int, float)) and buy > 0:
        return (float(sell) / float(buy) - 1) * 100
    return None


def optimize_params(trades: list[dict]) -> dict[str, str]:
    cfg = dict(BASELINE)
    pnls = [x for t in trades if (x := _pnl_pct(t)) is not None]
    wins = [x for x in pnls if x > 0]

    if not pnls:
        return cfg

    winrate = len(wins) / len(pnls)
    avg_profit = mean(wins) if wins else 0.0

    if winrate >= 0.70:
        cfg["MIN_OPPORTUNITY_SCORE"] = "80"
        cfg["TRAILING_STOP_PCT"] = "8"
    elif winrate < 0.55:
        cfg["MIN_OPPORTUNITY_SCORE"] = "86"
        cfg["VOLUME_SPIKE_MULTIPLIER"] = "2.3"
        cfg["MAX_TOP10_HOLDER_PCT"] = "18"

    if avg_profit >= 60:
        cfg["TP2_PCT"] = "80"
    elif avg_profit < 20:
        cfg["TP2_PCT"] = "50"
        cfg["TRAIL_TRIGGER_PCT"] = "18"
        cfg["TRAILING_START_PCT"] = "18"

    # Keep alias keys in sync (TRAIL_TRIGGER_PCT <-> TRAILING_START_PCT)
    cfg["TRAILING_START_PCT"] = cfg.get("TRAIL_TRIGGER_PCT", cfg.get("TRAILING_START_PCT", "20"))

    return cfg


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto optimize trading parameters from trade log")
    parser.add_argument("--apply", action="store_true", help="Write optimized params directly to .env")
    parser.add_argument("--log", type=Path, default=TRADE_LOG_PATH, help="Path to trade_log.json")
    args = parser.parse_args()

    trades = load_trades(args.log)
    optimized = optimize_params(trades)

    print("=== AUTO PARAMETER OPTIMIZER ===")
    print(f"Trades analyzed: {len(trades)}")
    for k, v in optimized.items():
        print(f"{k}={v}")

    if args.apply:
        env = parse_env(ENV_PATH)
        env.update(optimized)
        write_env(ENV_PATH, env)
        print(f"\nApplied optimized params to: {ENV_PATH}")


if __name__ == "__main__":
    main()
