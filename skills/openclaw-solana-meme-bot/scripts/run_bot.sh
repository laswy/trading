#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BOT_DIR="$ROOT/solana-meme-bot"

cd "$BOT_DIR"
python main.py
