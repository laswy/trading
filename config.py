"""Centralized environment loading for the bot."""

import os
from pathlib import Path
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SOLANA_WALLET_ADDRESS = os.getenv("SOLANA_WALLET_ADDRESS", "")
SOLANA_PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY", "")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "")
BASE_PRIVATE_KEY = os.getenv("BASE_PRIVATE_KEY", "")
BASE_RPC_URL = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")
JUP_API_URL = os.getenv("JUP_API_URL", "https://quote-api.jup.ag/v6")
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.1"))
BUY_AMOUNT_USDC = float(os.getenv("BUY_AMOUNT_USDC", "10"))
LANG = os.getenv("LANG", "en")
SELL_MODE = os.getenv("SELL_MODE", "sell")
HOLD_MIN_SCORE = int(os.getenv("HOLD_MIN_SCORE", "95"))
SIGNAL_ALERT_MIN_SCORE = int(os.getenv("SIGNAL_ALERT_MIN_SCORE", "70"))
