"""Configuration and runtime constants for the multi-chain trading system."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class ChainConfig:
    name: str
    chain_id: int
    rpc_url: str


@dataclass(frozen=True)
class TradingConfig:
    min_token_age_days: int = 1
    max_token_age_days: int = 365
    min_liquidity_usd: float = 150_000
    min_volume_24h_usd: float = 75_000
    max_buy_tax_pct: float = 10.0
    max_sell_tax_pct: float = 10.0
    max_top10_holder_pct: float = 35.0
    max_position_pct: float = 0.05
    max_open_positions: int = 12
    trailing_stop_pct: float = 0.15
    max_daily_loss_pct: float = 0.05
    max_position_loss_pct: float = 0.20
    buy_score_threshold: int = 75
    strong_buy_score_threshold: int = 90


@dataclass(frozen=True)
class RuntimeConfig:
    scan_interval_s: int = 20
    monitor_interval_s: int = 15
    max_parallel_workers: int = 20
    http_timeout_s: int = 15
    retry_attempts: int = 3
    retry_backoff_s: float = 1.0
    cache_ttl_s: int = 30


CHAIN_CONFIGS: Dict[str, ChainConfig] = {
    "ethereum": ChainConfig("ethereum", 1, os.getenv("RPC_ETHEREUM", "https://rpc.ankr.com/eth")),
    "base": ChainConfig("base", 8453, os.getenv("RPC_BASE", "https://mainnet.base.org")),
    "bnb": ChainConfig("bnb", 56, os.getenv("RPC_BNB", "https://bsc-dataseed.binance.org")),
    "arbitrum": ChainConfig("arbitrum", 42161, os.getenv("RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc")),
    "optimism": ChainConfig("optimism", 10, os.getenv("RPC_OPTIMISM", "https://mainnet.optimism.io")),
}

SUPPORTED_CHAINS: List[str] = list(CHAIN_CONFIGS.keys())

TRADING_CONFIG = TradingConfig()
RUNTIME_CONFIG = RuntimeConfig()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///trading.db")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/search"
ZEROX_BASE_URL = "https://api.0x.org"
ONE_INCH_BASE_URL = "https://api.1inch.dev"
OPENOCEAN_BASE_URL = "https://open-api.openocean.finance/v3"
