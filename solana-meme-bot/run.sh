#!/usr/bin/env bash
# Chạy bot 1 lệnh trên Linux: tự tạo venv (nếu chưa có), cài thư viện, rồi chạy bot.
#
# Cách dùng:
#   ./run.sh
# hoặc:
#   bash run.sh
#
# Lần đầu chạy sẽ tự tạo file .env từ .env.example nếu chưa có — nhớ dừng lại
# (Ctrl+C) và điền secrets vào .env trước khi chạy thật.

set -euo pipefail

# Luôn chạy từ đúng thư mục chứa script này, bất kể gọi từ đâu.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "=================================================================="
echo "  Solana Meme Bot — one-shot launcher"
echo "=================================================================="

# ── 1) Kiểm tra python3 ────────────────────────────────────────────
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "❌ Không tìm thấy '$PYTHON_BIN'. Cài Python 3.9+ trước (vd: sudo apt install python3 python3-venv)." >&2
    exit 1
fi

# ── 2) Tạo venv nếu chưa có ─────────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Tạo virtualenv tại $VENV_DIR ..."
    "$PYTHON_BIN" -m venv "$VENV_DIR"
else
    echo "📦 Đã có virtualenv tại $VENV_DIR — bỏ qua bước tạo."
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# ── 3) Cài/cập nhật thư viện ─────────────────────────────────────────
echo "📥 Cài đặt thư viện (requirements.txt) ..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✅ Thư viện đã sẵn sàng."

# ── 4) Tạo .env từ mẫu nếu chưa có ──────────────────────────────────
if [ ! -f ".env" ]; then
    echo ""
    echo "⚠️  Chưa có file .env — tạo từ .env.example."
    cp .env.example .env
    echo "⚠️  Điền secrets vào .env (SOLANA_PRIVATE_KEY, TELEGRAM_BOT_TOKEN, ...) rồi chạy lại ./run.sh"
    echo "    (xem docs/USAGE_GUIDE.md mục 2 để biết các biến bắt buộc tối thiểu)"
    exit 1
fi

# ── 5) Chạy bot ──────────────────────────────────────────────────────
echo ""
echo "🚀 Khởi động bot ..."
echo "=================================================================="
exec python bot.py
