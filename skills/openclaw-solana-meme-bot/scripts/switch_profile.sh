#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <early_sniper|safe_trend>"
  exit 1
fi

PROFILE="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BOT_DIR="$ROOT/solana-meme-bot"

cd "$BOT_DIR"
python scripts/apply_profile.py "$PROFILE"
