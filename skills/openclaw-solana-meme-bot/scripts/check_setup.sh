#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BOT_DIR="$ROOT/solana-meme-bot"
ENV_FILE="$BOT_DIR/.env"

missing=0

if [[ ! -d "$BOT_DIR" ]]; then
  echo "[ERR] Missing bot directory: $BOT_DIR"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERR] Missing .env file: $ENV_FILE"
  exit 1
fi

required=(SOLANA_WALLET_ADDRESS SOLANA_PRIVATE_KEY TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID)
for key in "${required[@]}"; do
  if ! grep -qE "^${key}=" "$ENV_FILE"; then
    echo "[ERR] Missing key in .env: $key"
    missing=1
  fi
done

if [[ $missing -ne 0 ]]; then
  exit 1
fi

echo "[OK] Basic setup is present."
echo "[OK] Bot dir: $BOT_DIR"
