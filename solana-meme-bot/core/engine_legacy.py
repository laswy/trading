"""
╔══════════════════════════════════════════════════════════════════╗
║  MULTI-CHAIN AUTO TRADER v4.1  🟣 Solana  🔵 Base               ║
║  Solana: Jupiter API v6 (0 phí platform)                         ║
║  Base:   OKX DEX API                                             ║
║                                                                  ║
║  KIẾN TRÚC:                                                      ║
║  Thread 1 → Scanner    (3s, DexScreener: Solana + Base)          ║
║  Thread 2 → Validator  (GoPlus parallel x20, chain-aware)        ║
║  Thread 3 → Buyer      (Jupiter/OKX swap, non-blocking)          ║
║  Thread 4 → Monitor    (TP + Rug + Fast Dump + EMA, adaptive)    ║
║                                                                  ║
║  TÍNH NĂNG v4.1:                                                 ║
║  ✅ Jupiter API v6 cho Solana — không phí platform               ║
║  ✅ Dynamic slippage BPS + priority fee tự động                  ║
║  ✅ Signal Alert: score ≥ 70 → thông báo bạn bè mua thủ công    ║
║  ✅ Adaptive TP Check: 1s/5s/10s theo tuổi token                 ║
║  ✅ PriceTracker + EMA: phát hiện downtrend real-time            ║
║  ✅ Fast Dump Detection: -15% trong 10s → bán ngay lập tức       ║
║  ✅ Scoring v4: weight cao hơn cho early signals (<5p)           ║
║  ✅ Risk Manager: daily limits + position caps + PnL tracking    ║
║  ✅ EntryGuard v2: 4 chiến lược theo tuổi token                  ║
║     <1p  Dip 1s + Volume Check  (chống pump-dump siêu nhanh)    ║
║     1-5p Dip 3s + RSI Filter    (tránh mua khi overbought)      ║
║     5-30p BBL 45s + MACD Cross  (bắt momentum đảo chiều)        ║
║     >30p VWAP 60s               (fair value có volume hỗ trợ)  ║
║                                                                  ║
║  LUỒNG THÔNG BÁO:                                                ║
║  score ≥ 70  → 🚨 Signal Alert → TELEGRAM_SIGNAL_CHANNELS       ║
║  score ≥ MIN → 🤖 Auto Buy    → TELEGRAM_CHAT_ID (owner)        ║
║  TP/Rug      → 💰/🚨 Alert    → TELEGRAM_CHAT_ID (owner)        ║
║  < 2 phút    → ⚡ Early Alert → TELEGRAM_SIGNAL_CHANNELS        ║
║                                                                  ║
║  DB:    SQLite (positions + blacklist + permanent_ban)            ║
╚══════════════════════════════════════════════════════════════════╝

Cài đặt:
    pip install requests python-dotenv base58 PyNaCl web3

.env cần có:
    SOLANA_WALLET_ADDRESS / SOLANA_PRIVATE_KEY  (base58)
    SOLANA_RPC_URL
    BASE_PRIVATE_KEY  (hex, 0x-prefixed optional)  ← chỉ cần nếu dùng Base
    BASE_RPC_URL  (optional, default: https://mainnet.base.org)
    OKX_API_KEY / OKX_SECRET_KEY / OKX_API_PASSPHRASE / OKX_PROJECT_ID  ← Base chain only
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
    TELEGRAM_SIGNAL_CHANNEL  (kênh riêng cho bạn bè, có thể nhiều kênh phân cách dấu phẩy)
    SIGNAL_ALERT_MIN_SCORE   (mặc định: 70)
    JUP_API_URL  (optional, default: https://quote-api.jup.ag/v6)
    BUY_AMOUNT_SOL  (default: 0.1 SOL per Solana trade)
    BUY_AMOUNT_USDC (default: 10 USDC per Base trade)
    LANG            (default: en — set to vi for Vietnamese notifications)
    SELL_MODE       (default: sell — "sell" | "hold" | "smart")
    HOLD_MIN_SCORE  (default: 95  — score threshold for smart hold mode)
"""

import os, json, time, hmac, hashlib, base64, threading, sqlite3, re, sys, subprocess
import requests, urllib.parse
import numpy as _np
from datetime import datetime, timezone
from queue import Queue, Empty
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv
import builtins as _builtins

# ── Auto-install PyNaCl nếu chưa có (Termux-friendly) ────────────
def _ensure_nacl():
    try:
        import nacl.signing  # noqa
        return True
    except ImportError:
        pass
    print("[Boot] PyNaCl chưa cài — đang tự cài...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "PyNaCl"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        import nacl.signing  # noqa
        print("[Boot] ✅ PyNaCl đã cài xong.")
        return True
    except Exception as e:
        print(f"[Boot] ⚠️  Không cài được PyNaCl: {e}")
        return False

_NACL_OK = _ensure_nacl()

# ── Pure-Python Ed25519 fallback (không cần cryptography/PyNaCl) ──
# Dùng khi cả nacl lẫn cryptography đều thiếu (Termux bare-minimum)
def _ed25519_sign_pure(secret_seed_32: bytes, message: bytes) -> bytes:
    """
    Ed25519 ký thuần Python — RFC 8032.
    Chỉ dùng stdlib: hashlib.  Không cần thư viện ngoài.
    """
    import hashlib

    def _clamp(n):
        n &= ~7
        n &= ~(128 << 8 * 31)
        n |= 64 << 8 * 31
        return n

    p = 2**255 - 19
    q = 2**252 + 27742317777372353535851937790883648493

    def _mod(a, n=p): return a % n
    def _inv(x): return pow(x, p - 2, p)

    d = -121665 * _inv(121666) % p
    Gx = 15112221349535807912866137220509078750507884956996801861223382522073
    Gy = 46316835694926478169428394003475163141307993866256225615783033011972
    B  = (Gx % p, Gy % p, 1, Gx * Gy % p)

    def _pt_add(P, Q):
        A = (P[1]-P[0]) * (Q[1]-Q[0]) % p
        B_ = (P[1]+P[0]) * (Q[1]+Q[0]) % p
        C = 2 * P[3] * Q[3] * d % p
        D = 2 * P[2] * Q[2] % p
        E, F, G_, H = B_-A, D-C, D+C, B_+A
        return (E*F%p, G_*H%p, F*G_%p, E*H%p)

    def _pt_mul(s, P):
        Q = (0, 1, 1, 0)
        while s:
            if s & 1: Q = _pt_add(Q, P)
            P = _pt_add(P, P)
            s >>= 1
        return Q

    def _encode_pt(P):
        zi = _inv(P[2])
        x  = P[0] * zi % p
        y  = P[1] * zi % p
        return (y | ((x & 1) << 255)).to_bytes(32, "little")

    h = hashlib.sha512(secret_seed_32).digest()
    a = int.from_bytes(h[:32], "little")
    a = _clamp(a) % q
    A = _encode_pt(_pt_mul(a, B))

    r = int.from_bytes(hashlib.sha512(h[32:] + message).digest(), "little") % q
    R = _encode_pt(_pt_mul(r, B))

    k = int.from_bytes(
        hashlib.sha512(R + A + message).digest(), "little"
    ) % q
    S = (r + k * a) % q

    return R + S.to_bytes(32, "little")


def _ed25519_sign(secret_seed_32: bytes, message: bytes) -> bytes:
    """Ký Ed25519 — thử nacl → cryptography → pure-Python."""
    try:
        import nacl.signing as _nacl
        sk = _nacl.SigningKey(secret_seed_32)
        return bytes(sk.sign(message).signature)
    except ImportError:
        pass
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        priv = Ed25519PrivateKey.from_private_bytes(secret_seed_32)
        return priv.sign(message)
    except ImportError:
        pass
    return _ed25519_sign_pure(secret_seed_32, message)


def _ed25519_pubkey(secret_seed_32: bytes) -> bytes:
    """Lấy public key từ seed — thử nacl → cryptography → pure-Python."""
    try:
        import nacl.signing as _nacl
        return bytes(_nacl.SigningKey(secret_seed_32).verify_key)
    except ImportError:
        pass
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        priv = Ed25519PrivateKey.from_private_bytes(secret_seed_32)
        return priv.public_key().public_bytes_raw()
    except ImportError:
        pass
    # pure-python: derive từ _ed25519_sign (dùng nonce cố định để lấy A)
    import hashlib
    h = hashlib.sha512(secret_seed_32).digest()

    p = 2**255 - 19
    q = 2**252 + 27742317777372353535851937790883648493
    d = -121665 * pow(121666, p - 2, p) % p
    Gx = 15112221349535807912866137220509078750507884956996801861223382522073
    Gy = 46316835694926478169428394003475163141307993866256225615783033011972
    B  = (Gx % p, Gy % p, 1, Gx * Gy % p)

    def _pt_add(P, Q):
        A = (P[1]-P[0]) * (Q[1]-Q[0]) % p
        B_ = (P[1]+P[0]) * (Q[1]+Q[0]) % p
        C = 2 * P[3] * Q[3] * d % p
        D = 2 * P[2] * Q[2] % p
        E, F, G_, H = B_-A, D-C, D+C, B_+A
        return (E*F%p, G_*H%p, F*G_%p, E*H%p)

    def _pt_mul(s, P):
        Q = (0, 1, 1, 0)
        while s:
            if s & 1: Q = _pt_add(Q, P)
            P = _pt_add(P, P)
            s >>= 1
        return Q

    a = int.from_bytes(h[:32], "little")
    a &= ~7; a &= ~(128 << 8*31); a |= 64 << 8*31
    a %= q

    pt = _pt_mul(a, B)
    zi = pow(pt[2], p - 2, p)
    x  = pt[0] * zi % p
    y  = pt[1] * zi % p
    return (y | ((x & 1) << 255)).to_bytes(32, "little")

# ── Timestamp tự động cho mọi print() ────────────────────────────
_real_print = _builtins.print
def _ts_print(*args, sep=" ", end="\n", file=None, flush=False):
    msg = sep.join(str(a) for a in args)
    ts  = __import__('datetime').datetime.now().strftime("%H:%M:%S")
    if msg.strip():
        _real_print(f"[{ts}] {msg}", end=end, file=file, flush=flush)
    else:
        _real_print(msg, end=end, file=file, flush=flush)
_builtins.print = _ts_print

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# ================================================================
# LANGUAGE / I18N
# ================================================================
# Default: English. Set LANG=vi in .env to use Vietnamese.
_LANG = os.getenv("LANG", "en").strip().lower()

def _t(en: str, vi: str) -> str:
    """Return Vietnamese string if LANG=vi, else English."""
    return vi if _LANG == "vi" else en

# ================================================================
# CONFIG
# ================================================================

TELEGRAM_BOT_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID", "")
_raw_ch                  = os.getenv("TELEGRAM_SIGNAL_CHANNEL", "")
TELEGRAM_SIGNAL_CHANNELS: List[str] = [c.strip() for c in _raw_ch.split(",") if c.strip()]

OKX_API_KEY        = os.getenv("OKX_API_KEY", "")
OKX_SECRET_KEY     = os.getenv("OKX_SECRET_KEY", "")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
OKX_PROJECT_ID     = os.getenv("OKX_PROJECT_ID", "")
OKX_BASE_URL       = "https://web3.okx.com"
# ↑ OKX chỉ dùng cho Base chain. Solana dùng Jupiter API (miễn phí).

# ── Jupiter API (Solana swap) ─────────────────────────────────────
# Jupiter Lite API — miễn phí, không cần API key
# quote-api.jup.ag/v6 đã deprecated → dùng lite-api.jup.ag/swap/v1
JUP_QUOTE_URL      = os.getenv("JUP_QUOTE_URL", "https://lite-api.jup.ag/swap/v1/quote")
JUP_SWAP_URL       = os.getenv("JUP_SWAP_URL",  "https://lite-api.jup.ag/swap/v1/swap")
JUP_TIMEOUT        = int(os.getenv("JUP_TIMEOUT_S", "12"))
JUP_RETRIES        = int(os.getenv("JUP_RETRIES",   "3"))

# ── Helius (RPC + Sender) ─────────────────────────────────────────
# HELIUS_API_KEY dùng cho RPC URL, KHÔNG phải cho Jupiter API
# Đăng ký free: https://dashboard.helius.dev
HELIUS_API_KEY     = os.getenv("HELIUS_API_KEY", "")
HELIUS_SENDER_URL  = os.getenv("HELIUS_SENDER_URL", "http://ewr-sender.helius-rpc.com/fast")
HELIUS_USE_SENDER  = os.getenv("HELIUS_USE_SENDER", "false").lower() == "true"
HELIUS_TIP_SOL     = float(os.getenv("HELIUS_TIP_SOL", "0.0002"))  # tip Jito (SOL)
_JITO_TIP_ACCOUNTS = [
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
    "2nyhqdwKcJZR2vcqCyrYnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
]

# ── Jupiter priority fee (đúng format theo Helius docs) ───────────
# Dùng priorityLevelWithMaxLamports thay vì flat lamports số
JUP_PRIORITY_LEVEL    = os.getenv("JUP_PRIORITY_LEVEL",    "veryHigh")
JUP_MAX_PRIORITY_LAMS = int(os.getenv("JUP_MAX_PRIORITY_LAMS", "1000000"))

WALLET_ADDRESS = os.getenv("SOLANA_WALLET_ADDRESS", "")
WALLET_PRIVKEY = os.getenv("SOLANA_PRIVATE_KEY", "")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

BUY_AMOUNT_SOL        = float(os.getenv("BUY_AMOUNT_SOL",        "0.1"))   # SOL per trade
BUY_AMOUNT_USDC       = float(os.getenv("BUY_AMOUNT_USDC",       "10"))    # kept for Base chain

# ── SELL / HOLD MODE ──────────────────────────────────────────────
# SELL_MODE controls what the bot does after buying a token:
#   "sell"  → normal auto-sell (TP + RUG + Fast Dump)         [default]
#   "hold"  → NEVER sell — buy-and-hold, monitor only sends alerts
#   "smart" → sell normally UNLESS opp_score >= HOLD_MIN_SCORE,
#             in which case the token is held forever (diamond hands)
#
# HOLD_MIN_SCORE: score threshold for "smart" mode — tokens scoring
# at or above this value are treated as hold-forever positions.
# Default: 95 (top-tier signals only).
SELL_MODE      = os.getenv("SELL_MODE",      "sell").strip().lower()   # "sell" | "hold" | "smart"
HOLD_MIN_SCORE = int(os.getenv("HOLD_MIN_SCORE", "95"))                # used when SELL_MODE=smart
MIN_SCORE             = int(  os.getenv("MIN_OPPORTUNITY_SCORE",  "83"))
TAKE_PROFIT_PCT       = float(os.getenv("TAKE_PROFIT_PCT",        "30"))
# ── Trailing Stop config ───────────────────────────────────────────
TP1_PCT               = float(os.getenv("TP1_PCT",                "25"))
TP2_PCT               = float(os.getenv("TP2_PCT",                "60"))
TRAIL_TRIGGER_PCT     = float(os.getenv("TRAIL_TRIGGER_PCT",      os.getenv("TRAILING_START_PCT", "20")))
TRAILING_STOP_PCT     = float(os.getenv("TRAILING_STOP_PCT",      "10"))
TRAIL_TARGET_PCT      = float(os.getenv("TRAIL_TARGET_PCT",       os.getenv("TP2_PCT", "60")))
SLIPPAGE_PCT          = float(os.getenv("SLIPPAGE_PCT",           "10"))
SCAN_INTERVAL         = float(os.getenv("SCAN_INTERVAL_SECONDS",  "3"))
TP_CHECK_INTERVAL     = float(os.getenv("TP_CHECK_INTERVAL_SECONDS", "5"))
MIN_LIQUIDITY_USD     = float(os.getenv("MIN_LIQUIDITY_USD",      "5000"))
MAX_AGE_MIN           = float(os.getenv("MAX_TOKEN_AGE_MINUTES",  "120"))
MIN_TOKEN_AGE_S       = float(os.getenv("MIN_TOKEN_AGE_S",        "90"))
MIN_HOLDER_COUNT      = int(  os.getenv("MIN_HOLDER_COUNT",       "5"))
MAX_TOP10_HOLDER_PCT  = float(os.getenv("MAX_TOP10_HOLDER_PCT",   "20"))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "2"))
MIN_LP_SOL            = float(os.getenv("MIN_LP_SOL",             "5"))

# ── Adaptive TP Check intervals (theo tuổi token) ─────────────────
TP_CHECK_INTERVAL_NEW   = float(os.getenv("TP_CHECK_NEW_S",    "1"))   # token < 5p
TP_CHECK_INTERVAL_MID   = float(os.getenv("TP_CHECK_MID_S",    "5"))   # token 5-30p
TP_CHECK_INTERVAL_OLD   = float(os.getenv("TP_CHECK_OLD_S",   "10"))   # token > 30p

# ── Fast Dump Detection ────────────────────────────────────────────
FAST_DUMP_PCT           = float(os.getenv("FAST_DUMP_PCT",    "-15"))  # % drop trigger bán ngay
FAST_DUMP_WINDOW_S      = float(os.getenv("FAST_DUMP_WINDOW_S", "10")) # cửa sổ theo dõi (giây)

# ── Dynamic Slippage theo tuổi token ──────────────────────────────
SLIPPAGE_TOKEN_NEW      = float(os.getenv("SLIPPAGE_NEW_PCT",  "30"))  # token < 5p
SLIPPAGE_TOKEN_MID      = float(os.getenv("SLIPPAGE_MID_PCT",  "15"))  # token 5-30p
# token > 30p → dùng SLIPPAGE_PCT mặc định (10%)

# ── Dynamic Priority Fee (Solana) ──────────────────────────────────
PRIORITY_FEE_DEFAULT    = int(  os.getenv("PRIORITY_FEE_DEFAULT",  "100000"))  # microlamports
PRIORITY_FEE_HIGH       = int(  os.getenv("PRIORITY_FEE_HIGH",     "500000"))
PRIORITY_FEE_VERY_HIGH  = int(  os.getenv("PRIORITY_FEE_VERY_HIGH","1000000"))

# ── Risk Manager ───────────────────────────────────────────────────
RISK_MAX_POSITIONS      = int(  os.getenv("RISK_MAX_POSITIONS",    "5"))   # tối đa vị thế cùng lúc
RISK_MAX_DAILY_TRADES   = int(  os.getenv("RISK_MAX_DAILY_TRADES", "20"))  # tối đa lệnh/ngày
RISK_MAX_DAILY_LOSS_USD = float(os.getenv("RISK_MAX_DAILY_LOSS",  "50"))   # stop khi lỗ > $X/ngày
VALIDATOR_WORKERS       = int(  os.getenv("VALIDATOR_WORKERS",         "20"))
GOPLUS_RELAX_UNDER      = int(  os.getenv("GOPLUS_RELAX_UNDER_MIN",    "5"))
SOL_MAX_PROFILE_ADDRS   = int(  os.getenv("SOL_MAX_PROFILE_ADDRS",     "40"))
BASE_MAX_PROFILE_ADDRS  = int(  os.getenv("BASE_MAX_PROFILE_ADDRS",    "40"))  # FIX: quota riêng cho Base
SOL_PREFILTER_MIN_SCORE = int(  os.getenv("SOL_PREFILTER_MIN_SCORE",   "25"))
SCORE_GREEN           = int(  os.getenv("SCORE_GREEN_MIN",        "70"))
SCORE_YELLOW          = int(  os.getenv("SCORE_YELLOW_MIN",       "40"))

BUY_TXN_BY_AGE = [
    (10,   int(os.getenv("BUY_TXN_AGE_10MIN",  "2"))),
    (30,   int(os.getenv("BUY_TXN_AGE_30MIN",  "5"))),
    (60,   int(os.getenv("BUY_TXN_AGE_60MIN",  "15"))),
    (None, int(os.getenv("BUY_TXN_AGE_OVER60", "30"))),
]


PROFILE_PRESETS: Dict[str, Dict[str, str]] = {
    "early_sniper": {
        "MIN_SCORE": "83",
        "SCAN_INTERVAL": "2",
        "TP_CHECK_INTERVAL_NEW": "1",
        "TP_CHECK_INTERVAL_MID": "3",
        "TP_CHECK_INTERVAL_OLD": "8",
        "SLIPPAGE_PCT": "12",
        "SLIPPAGE_TOKEN_NEW": "35",
        "SLIPPAGE_TOKEN_MID": "18",
        "PRIORITY_FEE_DEFAULT": "150000",
        "PRIORITY_FEE_HIGH": "650000",
        "PRIORITY_FEE_VERY_HIGH": "1200000",
        "FAST_DUMP_PCT": "-12",
        "FAST_DUMP_WINDOW_S": "8",
        "RISK_MAX_POSITIONS": "4",
        "RISK_MAX_DAILY_TRADES": "25",
        "RISK_MAX_DAILY_LOSS": "40",
    },
    "safe_trend": {
        "MIN_SCORE": "83",
        "SCAN_INTERVAL": "4",
        "TP_CHECK_INTERVAL_NEW": "2",
        "TP_CHECK_INTERVAL_MID": "6",
        "TP_CHECK_INTERVAL_OLD": "12",
        "SLIPPAGE_PCT": "8",
        "SLIPPAGE_TOKEN_NEW": "25",
        "SLIPPAGE_TOKEN_MID": "12",
        "PRIORITY_FEE_DEFAULT": "100000",
        "PRIORITY_FEE_HIGH": "450000",
        "PRIORITY_FEE_VERY_HIGH": "900000",
        "FAST_DUMP_PCT": "-15",
        "FAST_DUMP_WINDOW_S": "10",
        "RISK_MAX_POSITIONS": "3",
        "RISK_MAX_DAILY_TRADES": "15",
        "RISK_MAX_DAILY_LOSS": "30",
    },
}

# ================================================================
# DYNAMIC CONFIG  — chỉnh runtime qua Telegram /set
# ================================================================
# Tất cả thông số có thể chỉnh runtime đều đọc qua cfg() thay vì
# dùng biến global trực tiếp. File .env chỉ là giá trị mặc định lúc
# khởi động — thay đổi qua /set KHÔNG ghi lại vào .env.
#
# Để đọc 1 thông số:  cfg("BUY_AMOUNT_SOL")
# Để ghi:             cfg_set("BUY_AMOUNT_SOL", 0.2)
# ================================================================

_cfg_lock = threading.Lock()
_cfg_store: Dict[str, float] = {}   # key → giá trị hiện tại (float/int)

# Schema: tên key → (kiểu, min, max, mô tả tiếng Việt, env_var gốc)
# Dùng để validate input và hiển thị /config
_CFG_SCHEMA: Dict[str, dict] = {
    # ── Trading ───────────────────────────────────────────────────
    "BUY_AMOUNT_SOL":      {"type": float, "min": 0.001, "max": 10.0,
                            "env": "BUY_AMOUNT_SOL",
                            "desc": "SOL mỗi lệnh mua (Solana)"},
    "BUY_AMOUNT_USDC":     {"type": float, "min": 1.0,   "max": 10000.0,
                            "env": "BUY_AMOUNT_USDC",
                            "desc": "USDC mỗi lệnh mua (Base)"},
    "MIN_SCORE":           {"type": int,   "min": 1,     "max": 100,
                            "env": "MIN_OPPORTUNITY_SCORE",
                            "desc": "Điểm tối thiểu để auto-buy (1-100)"},
    "TAKE_PROFIT_PCT":     {"type": float, "min": 1.0,   "max": 10000.0,
                            "env": "TAKE_PROFIT_PCT",
                            "desc": "% lãi chốt lời"},
    "MIN_LIQUIDITY_USD":   {"type": float, "min": 100.0, "max": 10_000_000.0,
                            "env": "MIN_LIQUIDITY_USD",
                            "desc": "Liquidity tối thiểu (USD)"},
    # ── Risk ──────────────────────────────────────────────────────
    "RISK_MAX_POSITIONS":  {"type": int,   "min": 1,     "max": 50,
                            "env": "RISK_MAX_POSITIONS",
                            "desc": "Số vị thế tối đa cùng lúc"},
    "RISK_MAX_DAILY_TRADES":{"type": int,  "min": 1,     "max": 200,
                            "env": "RISK_MAX_DAILY_TRADES",
                            "desc": "Số lệnh tối đa mỗi ngày"},
    "RISK_MAX_DAILY_LOSS": {"type": float, "min": 1.0,   "max": 100_000.0,
                            "env": "RISK_MAX_DAILY_LOSS",
                            "desc": "Dừng mua khi lỗ vượt mức này (USD)"},
    # ── Timing ────────────────────────────────────────────────────
    "SCAN_INTERVAL":       {"type": float, "min": 1.0,   "max": 60.0,
                            "env": "SCAN_INTERVAL_SECONDS",
                            "desc": "Tần suất scan token (giây)"},
    "TP_CHECK_INTERVAL_NEW":{"type": float,"min": 0.5,   "max": 30.0,
                            "env": "TP_CHECK_NEW_S",
                            "desc": "Kiểm tra TP mỗi N giây (token < 5p)"},
    "TP_CHECK_INTERVAL_MID":{"type": float,"min": 1.0,   "max": 60.0,
                            "env": "TP_CHECK_MID_S",
                            "desc": "Kiểm tra TP mỗi N giây (token 5-30p)"},
    "TP_CHECK_INTERVAL_OLD":{"type": float,"min": 1.0,   "max": 120.0,
                            "env": "TP_CHECK_OLD_S",
                            "desc": "Kiểm tra TP mỗi N giây (token > 30p)"},
    "FAST_DUMP_PCT":       {"type": float, "min": -99.0, "max": -1.0,
                            "env": "FAST_DUMP_PCT",
                            "desc": "% giảm kích hoạt Fast Dump (số âm, vd -15)"},
    "FAST_DUMP_WINDOW_S":  {"type": float, "min": 1.0,   "max": 300.0,
                            "env": "FAST_DUMP_WINDOW_S",
                            "desc": "Cửa sổ Fast Dump (giây)"},
    # ── Slippage & Fee ────────────────────────────────────────────
    "SLIPPAGE_PCT":        {"type": float, "min": 0.1,   "max": 50.0,
                            "env": "SLIPPAGE_PCT",
                            "desc": "Slippage mặc định (token > 30p)"},
    "SLIPPAGE_TOKEN_NEW":  {"type": float, "min": 1.0,   "max": 50.0,
                            "env": "SLIPPAGE_NEW_PCT",
                            "desc": "Slippage token < 5p"},
    "SLIPPAGE_TOKEN_MID":  {"type": float, "min": 1.0,   "max": 50.0,
                            "env": "SLIPPAGE_MID_PCT",
                            "desc": "Slippage token 5-30p"},
    "PRIORITY_FEE_DEFAULT":{"type": int,   "min": 1000,  "max": 10_000_000,
                            "env": "PRIORITY_FEE_DEFAULT",
                            "desc": "Priority fee mặc định (microlamports)"},
    "PRIORITY_FEE_HIGH":   {"type": int,   "min": 1000,  "max": 10_000_000,
                            "env": "PRIORITY_FEE_HIGH",
                            "desc": "Priority fee cao (microlamports)"},
    "PRIORITY_FEE_VERY_HIGH":{"type": int, "min": 1000,  "max": 10_000_000,
                            "env": "PRIORITY_FEE_VERY_HIGH",
                            "desc": "Priority fee rất cao (microlamports)"},
}

def _cfg_init():
    """Khởi tạo _cfg_store từ biến global hiện tại (đã load từ .env)."""
    defaults = {
        "BUY_AMOUNT_SOL":       BUY_AMOUNT_SOL,
        "BUY_AMOUNT_USDC":      BUY_AMOUNT_USDC,
        "MIN_SCORE":            MIN_SCORE,
        "TAKE_PROFIT_PCT":      TAKE_PROFIT_PCT,
        "MIN_LIQUIDITY_USD":    MIN_LIQUIDITY_USD,
        "RISK_MAX_POSITIONS":   RISK_MAX_POSITIONS,
        "RISK_MAX_DAILY_TRADES":RISK_MAX_DAILY_TRADES,
        "RISK_MAX_DAILY_LOSS":  RISK_MAX_DAILY_LOSS_USD,
        "SCAN_INTERVAL":        SCAN_INTERVAL,
        "TP_CHECK_INTERVAL_NEW":TP_CHECK_INTERVAL_NEW,
        "TP_CHECK_INTERVAL_MID":TP_CHECK_INTERVAL_MID,
        "TP_CHECK_INTERVAL_OLD":TP_CHECK_INTERVAL_OLD,
        "FAST_DUMP_PCT":        FAST_DUMP_PCT,
        "FAST_DUMP_WINDOW_S":   FAST_DUMP_WINDOW_S,
        "SLIPPAGE_PCT":         SLIPPAGE_PCT,
        "SLIPPAGE_TOKEN_NEW":   SLIPPAGE_TOKEN_NEW,
        "SLIPPAGE_TOKEN_MID":   SLIPPAGE_TOKEN_MID,
        "PRIORITY_FEE_DEFAULT": PRIORITY_FEE_DEFAULT,
        "PRIORITY_FEE_HIGH":    PRIORITY_FEE_HIGH,
        "PRIORITY_FEE_VERY_HIGH": PRIORITY_FEE_VERY_HIGH,
    }
    with _cfg_lock:
        _cfg_store.update(defaults)

def cfg(key: str):
    """Đọc thông số dynamic. Fallback về global nếu chưa init."""
    with _cfg_lock:
        return _cfg_store.get(key)

def cfg_set(key: str, value) -> Tuple[bool, str]:
    """
    Ghi thông số runtime. Validate theo schema.
    Trả về (True, msg_ok) hoặc (False, msg_lỗi).
    """
    schema = _CFG_SCHEMA.get(key)
    if not schema:
        return False, f"Không tìm thấy thông số: <code>{key}</code>"

    # Ép kiểu — strip dấu = và khoảng trắng thừa (vd: user gõ "=70" hay "= 70")
    if isinstance(value, str):
        value = value.strip().lstrip("= ").strip()
    try:
        typed_val = schema["type"](value)
    except (ValueError, TypeError):
        return False, f"Giá trị không hợp lệ: <code>{value}</code> (cần {schema['type'].__name__})"

    # Validate range
    mn, mx = schema.get("min"), schema.get("max")
    if mn is not None and typed_val < mn:
        return False, f"Giá trị quá nhỏ: {typed_val} (min={mn})"
    if mx is not None and typed_val > mx:
        return False, f"Giá trị quá lớn: {typed_val} (max={mx})"

    with _cfg_lock:
        old_val = _cfg_store.get(key)
        _cfg_store[key] = typed_val

    # Sync ngay vào RiskManager nếu là thông số risk
    _cfg_sync_risk()

    print(f"[Config] ✏️  {key}: {old_val} → {typed_val}")
    return True, f"✅ <b>{key}</b>: <code>{old_val}</code> → <code>{typed_val}</code>"

def cfg_save_env() -> Tuple[bool, str]:
    """
    Ghi các thông số hiện tại vào file .env để persist qua restart.
    Chỉ cập nhật các dòng đã có — không thêm key mới vào .env nếu chưa có.
    """
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return False, ".env không tồn tại"

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Map env_var → giá trị hiện tại
        env_to_val: Dict[str, str] = {}
        with _cfg_lock:
            for key, schema in _CFG_SCHEMA.items():
                env_key = schema["env"]
                val = _cfg_store.get(key)
                if val is not None:
                    env_to_val[env_key] = str(val)

        new_lines = []
        updated = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                new_lines.append(line)
                continue
            env_key = stripped.split("=", 1)[0].strip()
            if env_key in env_to_val:
                new_lines.append(f"{env_key}={env_to_val[env_key]}\n")
                updated.append(env_key)
            else:
                new_lines.append(line)

        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        return True, f"💾 Đã lưu {len(updated)} thông số vào .env"
    except Exception as e:
        return False, f"Lỗi ghi .env: {e}"

def _cfg_sync_risk():
    """Đồng bộ thông số risk vào RiskManager ngay lập tức."""
    global RISK_MAX_POSITIONS, RISK_MAX_DAILY_TRADES, RISK_MAX_DAILY_LOSS_USD
    try:
        v_pos   = cfg("RISK_MAX_POSITIONS")
        v_trade = cfg("RISK_MAX_DAILY_TRADES")
        v_loss  = cfg("RISK_MAX_DAILY_LOSS")
        if v_pos   is not None: RISK_MAX_POSITIONS    = int(v_pos)
        if v_trade is not None: RISK_MAX_DAILY_TRADES = int(v_trade)
        if v_loss  is not None: RISK_MAX_DAILY_LOSS_USD = float(v_loss)
    except Exception:
        pass

SOL_CHAIN_INDEX     = "501"
SOL_NATIVE          = "So11111111111111111111111111111111111111112"   # Wrapped SOL mint
SOL_USDC            = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_DECIMALS        = 9
USDC_DECIMALS       = 6
SOLANA_VALID_QUOTES = {"SOL", "WSOL", "USDC", "USDT"}

# ── Base (EVM) ────────────────────────────────────────────────────
BASE_CHAIN_INDEX    = "8453"
BASE_USDC           = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
BASE_RPC_URL        = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")
BASE_WALLET_PRIVKEY = os.getenv("BASE_PRIVATE_KEY", "")   # hex, 0x-prefixed optional
BASE_VALID_QUOTES   = {"ETH", "WETH", "USDC", "USDT"}
SUPPORTED_CHAINS    = {"solana", "base"}

# ── Rug detection ─────────────────────────────────────────────────
RUG_PRICE_DROP_1H   = float(os.getenv("RUG_PRICE_DROP_1H", "-60"))   # % ngưỡng rug

# ── Spam domain blocklist (social score) ─────────────────────────
_SPAM_DOMAINS = {
    "t.me", "twitter.com", "x.com", "discord.gg", "discord.com",
    "linktree.com", "linktr.ee", "bit.ly", "tinyurl.com",
}

# ── Fake / redirect patterns to reject for website URLs ──────────
_FAKE_WEBSITE_DOMAINS = {
    # Search engines
    "google.com", "google.co", "bing.com", "yahoo.com", "duckduckgo.com",
    "baidu.com", "yandex.com", "ask.com",
    # Link aggregators / redirectors
    "linktree.com", "linktr.ee", "bit.ly", "tinyurl.com", "shorturl.at",
    "rebrand.ly", "cutt.ly", "t.co", "ow.ly", "buff.ly", "tiny.cc",
    "lnk.to", "snip.ly", "soo.gd", "bc.vc",
    # Generic / useless pages
    "about.me", "carrd.co", "wix.com", "weebly.com", "squarespace.com",
    # Crypto spam aggregators
    "coinmarketcap.com", "coingecko.com", "dexscreener.com",
    "dextools.io", "birdeye.so", "defined.fi",
    # Social networks themselves (not a project website)
    "twitter.com", "x.com", "t.me", "telegram.org", "discord.gg",
    "discord.com", "reddit.com", "medium.com", "substack.com",
    "youtube.com", "youtu.be", "tiktok.com", "instagram.com",
    "facebook.com", "fb.com", "linkedin.com",
    # Encyclopedias / general knowledge
    "wikipedia.org", "wikimedia.org", "wikidata.org",
    "britannica.com", "encyclopedia.com",
    # News / general content sites
    "cnn.com", "bbc.com", "bbc.co.uk", "reuters.com", "apnews.com",
    "nytimes.com", "theguardian.com", "washingtonpost.com",
    "bloomberg.com", "forbes.com", "businessinsider.com",
    "techcrunch.com", "wired.com", "theverge.com",
    # Blog / content platforms
    "blogger.com", "blogspot.com", "wordpress.com", "ghost.io",
    "hubpages.com", "quora.com", "stackexchange.com", "stackoverflow.com",
    # Language learning / education
    "duolingo.com", "coursera.org", "udemy.com", "khan.org",
    "khanacademy.org", "w3schools.com",
}

# ── X/Twitter URL patterns that are NOT real project accounts ─────
# These are search results, trending, community pages, hashtags etc.
_FAKE_TWITTER_PATH_PREFIXES = (
    "/search",
    "/explore",
    "/trending",
    "/i/",           # /i/communities, /i/topics, /i/lists …
    "/hashtag/",
    "/home",
    "/notifications",
    "/messages",
    "/settings",
)

_FAKE_TWITTER_USERNAMES = {
    "search", "explore", "home", "trending", "notifications",
    "messages", "settings", "i", "login", "signup", "compose",
}

# Cache kết quả validate social để tránh gọi HTTP nhiều lần
_social_cache: Dict[str, Optional[bool]] = {}
_social_cache_lock = threading.Lock()


def _validate_website(url: str) -> Tuple[bool, str]:
    """
    Validate a project website URL.
    Returns (is_valid: bool, reason: str).

    Checks (in order):
      1. Domain not in _FAKE_WEBSITE_DOMAINS blocklist
      2. Valid TLD (≥2 chars)
      3. HTTP GET → must return 200, Content-Type HTML
      4. Final redirect domain not blocklisted
      5. Content relevance — page must contain at least one crypto/project
         keyword; generic/unrelated pages (Wikipedia, news, blogs about other
         topics) are rejected even if they respond fine
    """
    if not url:
        return False, "empty"

    # ── Crypto/project relevance keywords ─────────────────────────────
    # Use specific compound phrases to avoid false positives from
    # generic content (Wikipedia "coin" = ancient currency, "mint" = royal mint,
    # "token" = grammar token, "wallet" = leather wallet, etc.)
    _RELEVANCE_KEYWORDS = [
        # Unambiguous crypto terms (very unlikely in non-crypto pages)
        "tokenomics", "defi", "dex ", "web3", "memecoin", "meme coin",
        "airdrop", "whitepaper", "white paper",
        "smart contract", "liquidity pool", "staking rewards",
        "solana", "ethereum", "base chain", "binance smart chain",
        "presale", "pre-sale", "ido ", "ico ",
        # Compound phrases that only appear on crypto project sites
        "buy token", "buy now", "contract address",
        "token launch", "token sale", "connect wallet",
        "total supply", "max supply",
        "ca:", "mint address",
        # Chart/trading specific
        "dexscreener", "birdeye", "raydium", "jupiter swap",
        "uniswap", "pancakeswap",
    ]

    try:
        from urllib.parse import urlparse
        parsed = urlparse(url if url.startswith("http") else "https://" + url)
        domain = parsed.netloc.lower().lstrip("www.").split(":")[0]

        # 1. Domain blocklist
        if not domain or "." not in domain:
            return False, f"invalid domain: {domain}"
        for bad in _FAKE_WEBSITE_DOMAINS:
            if domain == bad or domain.endswith("." + bad):
                return False, f"blocklisted domain: {domain}"

        # 2. TLD sanity
        tld = domain.rsplit(".", 1)[-1]
        if len(tld) < 2:
            return False, f"bad TLD: {tld}"

        # 3–5. HTTP + content check (cache by URL)
        with _social_cache_lock:
            if url in _social_cache:
                cached = _social_cache[url]
                if cached is None:
                    return False, "cached fail"
                return True, "cached ok"

        try:
            resp = requests.get(
                url if url.startswith("http") else "https://" + url,
                timeout=10,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                },
                stream=True,
            )

            # 4. Check final redirect domain
            final_domain = urlparse(resp.url).netloc.lower().lstrip("www.").split(":")[0]
            for bad in _FAKE_WEBSITE_DOMAINS:
                if final_domain == bad or final_domain.endswith("." + bad):
                    with _social_cache_lock:
                        _social_cache[url] = None
                    return False, f"redirects to blocklisted: {final_domain}"

            if resp.status_code not in (200, 201, 202, 301, 302, 303, 307, 308):
                with _social_cache_lock:
                    _social_cache[url] = None
                return False, f"HTTP {resp.status_code}"

            ct = resp.headers.get("Content-Type", "")
            if ct and not any(x in ct for x in ("text/html", "application/xhtml", "text/plain")):
                with _social_cache_lock:
                    _social_cache[url] = None
                return False, f"bad content-type: {ct[:40]}"

            # 5. Content relevance — read first 8KB
            body_bytes = b""
            for chunk in resp.iter_content(chunk_size=1024):
                body_bytes += chunk
                if len(body_bytes) >= 8192:
                    break
            body = body_bytes.decode("utf-8", errors="ignore").lower()

            matched = [kw for kw in _RELEVANCE_KEYWORDS if kw in body]
            if not matched:
                with _social_cache_lock:
                    _social_cache[url] = None
                return False, f"no crypto keywords found on page ({final_domain})"

            with _social_cache_lock:
                _social_cache[url] = True
            return True, f"{final_domain} — matched: {', '.join(matched[:3])}"

        except requests.exceptions.SSLError:
            with _social_cache_lock:
                _social_cache[url] = None
            return False, "SSL error"
        except requests.exceptions.ConnectionError:
            with _social_cache_lock:
                _social_cache[url] = None
            return False, "connection refused"
        except requests.exceptions.Timeout:
            with _social_cache_lock:
                _social_cache[url] = None
            return False, "timeout"

    except Exception as e:
        return False, f"error: {e}"


def _validate_twitter(url: str) -> Tuple[bool, str]:
    """
    Validate an X/Twitter URL — must be a real project account page.
    Returns (is_valid: bool, reason: str).

    Accepts:   x.com/projectname   twitter.com/projectname
    Rejects:   /search?q=...  /explore  /i/communities/...
               /hashtag/...   bare x.com  /home  etc.
    """
    if not url:
        return False, "empty"
    try:
        from urllib.parse import urlparse, unquote
        parsed   = urlparse(url if url.startswith("http") else "https://" + url)
        domain   = parsed.netloc.lower().lstrip("www.")
        path     = unquote(parsed.path).rstrip("/")

        if domain not in ("twitter.com", "x.com"):
            return False, f"not twitter/x: {domain}"

        if not path or path == "":
            return False, "no username path"

        # Check for fake path prefixes
        for bad_prefix in _FAKE_TWITTER_PATH_PREFIXES:
            if path.startswith(bad_prefix):
                return False, f"non-account path: {path[:40]}"

        # Extract username — must be first path segment
        parts    = [p for p in path.split("/") if p]
        if not parts:
            return False, "no path segments"

        username = parts[0].lstrip("@").lower()

        # Must not be a reserved/fake username
        if username in _FAKE_TWITTER_USERNAMES:
            return False, f"reserved username: {username}"

        # Username must match Twitter rules: 4-50 chars, alphanumeric + underscore
        import re as _re
        if not _re.match(r'^[a-zA-Z0-9_]{1,50}$', username):
            return False, f"invalid username format: {username}"

        # If path has more than 1 segment it might be /username/status/... → still valid (it's a real account)
        # But /username/search or /username/media with query params → probably OK (it's still that account)

        # Query params that suggest search/discovery (not a profile)
        qs = parsed.query.lower()
        if any(k in qs for k in ("q=", "src=trend", "f=live", "vertical=trends")):
            return False, f"search/trend query: {qs[:60]}"

        return True, f"@{username}"

    except Exception as e:
        return False, f"error: {e}"


def validate_social_links(token: dict) -> dict:
    """
    Validate website + twitter for a token.
    Updates token in-place with:
      website_valid:  True/False
      twitter_valid:  True/False
      website_reason: human-readable result
      twitter_reason: human-readable result
    Returns the updated token dict.
    """
    website = token.get("website", "")
    twitter = token.get("twitter", "")

    w_ok, w_reason = _validate_website(website) if website else (False, "not provided")
    t_ok, t_reason = _validate_twitter(twitter) if twitter else (False, "not provided")

    token["website_valid"]  = w_ok
    token["twitter_valid"]  = t_ok
    token["website_reason"] = w_reason
    token["twitter_reason"] = t_reason

    sym = token.get("symbol", "?")
    if website:
        icon = "✅" if w_ok else "❌"
        print(f"[Social] {icon} {sym} website: {w_reason}")
    if twitter:
        icon = "✅" if t_ok else "❌"
        print(f"[Social] {icon} {sym} twitter: {t_reason}")

    return token

GOPLUS_CRITICAL = {"is_honeypot": "Honeypot", "is_blacklisted": "Blacklisted"}
GOPLUS_NONCRIT  = {
    # mintable + freeze_authority đã xử lý riêng trong _validate_one (hard reject)
    "transfer_fee_enable": "Phí transfer",
}

DB_PATH = "positions.db"
SCORE_HISTORY_PATH = Path(__file__).resolve().parents[1] / "data" / "score_history.jsonl"

# ── Early alert: token < 2 phút → gửi 1 lần dù chưa đủ score ────
EARLY_ALERT_MAX_AGE_MIN = float(os.getenv("EARLY_ALERT_MAX_AGE_MIN", "2"))
_early_alert_sent: set  = set()   # {mint} đã gửi trong session
_early_alert_lock       = threading.Lock()

# ── Signal Alert: token đạt ngưỡng điểm → thông báo cho bạn bè mua ──
# Gửi đến TELEGRAM_SIGNAL_CHANNELS (tách bạch với TELEGRAM_CHAT_ID của bot)
SIGNAL_ALERT_MIN_SCORE  = int(os.getenv("SIGNAL_ALERT_MIN_SCORE", "70"))   # ngưỡng điểm gửi signal
_signal_alert_sent: set = set()    # {mint} đã gửi signal trong session
_signal_alert_lock      = threading.Lock()

# ================================================================
# QUEUES
# ================================================================

TOKEN_QUEUE: Queue = Queue(maxsize=500)   # scanner → validator
BUY_QUEUE:   Queue = Queue(maxsize=100)   # validator → buyer

# ================================================================
# DEXSCREENER CACHE
# ================================================================

class DexCache:
    def __init__(self, ttl: float = 5.0):
        self._cache: Dict[str, dict] = {}
        self.ttl = ttl
        self._lock = threading.Lock()

    def get(self, key: str, ttl: float = None):
        with self._lock:
            v = self._cache.get(key)
            if not v:
                return None
            effective_ttl = ttl if ttl is not None else self.ttl
            if time.time() - v["time"] > effective_ttl:
                return None
            return v["data"]

    def set(self, key: str, data):
        with self._lock:
            self._cache[key] = {"data": data, "time": time.time()}

_dex_cache = DexCache(ttl=5)

# ================================================================
# PRICE TRACKER + EMA  (Fast Dump Detection)
# ================================================================

class PriceTracker:
    """
    Theo dõi lịch sử giá real-time cho mỗi token đang hold.
    - Lưu circular buffer (timestamp, price) để detect dump nhanh.
    - Tính EMA để phát hiện downtrend trước khi giá sập mạnh.
    EMA_alpha = 2/(N+1), N=5 (EMA5 nhạy với biến động ngắn).
    """
    EMA_ALPHA = 0.333   # EMA5 ≈ alpha=2/(5+1)

    def __init__(self):
        self._data:  Dict[str, list]  = {}   # mint → [(ts, price), ...]
        self._ema:   Dict[str, float] = {}   # mint → ema hiện tại
        self._lock   = threading.Lock()

    def update(self, mint: str, price: float):
        if price <= 0:
            return
        now = time.time()
        with self._lock:
            buf = self._data.setdefault(mint, [])
            buf.append((now, price))
            # Giữ tối đa 60 điểm (đủ cho 10 phút với TP_CHECK=10s)
            if len(buf) > 60:
                self._data[mint] = buf[-60:]
            # Cập nhật EMA
            if mint not in self._ema:
                self._ema[mint] = price
            else:
                self._ema[mint] = self.EMA_ALPHA * price + (1 - self.EMA_ALPHA) * self._ema[mint]

    def get_ema(self, mint: str) -> float:
        with self._lock:
            return self._ema.get(mint, 0.0)

    def fast_dump_pct(self, mint: str, window_s: float = 10.0) -> float:
        """
        Tính % thay đổi giá trong `window_s` giây gần nhất.
        Trả về số âm nếu giá giảm (VD: -18.5 nghĩa là giảm 18.5%).
        Trả về 0.0 nếu chưa đủ dữ liệu.
        """
        with self._lock:
            buf = self._data.get(mint, [])
            if len(buf) < 2:
                return 0.0
            now = time.time()
            cutoff = now - window_s
            # Tìm điểm cũ nhất trong cửa sổ
            old_price = None
            for ts, px in buf:
                if ts >= cutoff:
                    old_price = px
                    break
            if old_price is None or old_price <= 0:
                return 0.0
            latest_price = buf[-1][1]
            return (latest_price / old_price - 1) * 100

    def is_downtrend(self, mint: str) -> bool:
        """
        True nếu EMA đang dốc xuống (3 điểm EMA liên tiếp giảm).
        """
        with self._lock:
            buf = self._data.get(mint, [])
            if len(buf) < 4:
                return False
            # Tính EMA cho 4 điểm cuối
            prices = [p for _, p in buf[-4:]]
            emas = [prices[0]]
            for p in prices[1:]:
                emas.append(self.EMA_ALPHA * p + (1 - self.EMA_ALPHA) * emas[-1])
            # Downtrend nếu 3 EMA liên tiếp giảm dần
            return emas[-1] < emas[-2] < emas[-3]

    def remove(self, mint: str):
        with self._lock:
            self._data.pop(mint, None)
            self._ema.pop(mint, None)

_price_tracker = PriceTracker()
_trailing_peaks: dict = {}  # mint -> peak_price khi đang trailing
_tp1_alerted: set = set()

# ================================================================
# RISK MANAGER  (Daily limits + position caps)
# ================================================================

class RiskManager:
    """
    Quản lý rủi ro toàn cục:
      - Giới hạn số vị thế đang mở đồng thời (RISK_MAX_POSITIONS)
      - Giới hạn số lệnh mua trong ngày (RISK_MAX_DAILY_TRADES)
      - Dừng mua khi lỗ tích lũy trong ngày vượt RISK_MAX_DAILY_LOSS_USD
    Reset daily counter lúc 00:00 UTC mỗi ngày.
    """
    def __init__(self):
        self._lock         = threading.Lock()
        self._trade_count  = 0       # lệnh đã mua hôm nay
        self._daily_pnl    = 0.0     # lãi/lỗ tích lũy hôm nay ($)
        self._day_key      = self._today()
        self._paused       = False   # True = stop mua vì risk limit

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _check_day_reset(self):
        today = self._today()
        if today != self._day_key:
            self._day_key     = today
            self._trade_count = 0
            self._daily_pnl   = 0.0
            self._paused      = False
            print(f"[RiskManager] 🔄 Reset daily counters ({today})")

    def can_buy(self, open_positions: int) -> Tuple[bool, str]:
        """
        Kiểm tra điều kiện trước khi mua.
        Trả về (True, "") nếu được phép, hoặc (False, lý_do).
        """
        with self._lock:
            self._check_day_reset()
            if self._paused:
                return False, f"Risk paused: daily loss > ${RISK_MAX_DAILY_LOSS_USD}"
            if open_positions >= RISK_MAX_POSITIONS:
                return False, f"Đủ {open_positions}/{RISK_MAX_POSITIONS} vị thế rồi"
            if self._trade_count >= RISK_MAX_DAILY_TRADES:
                return False, f"Đủ {self._trade_count}/{RISK_MAX_DAILY_TRADES} lệnh hôm nay"
            return True, ""

    def record_trade(self):
        """Gọi sau khi đặt lệnh mua thành công."""
        with self._lock:
            self._check_day_reset()
            self._trade_count += 1
            print(f"[RiskManager] 📊 Hôm nay: {self._trade_count}/{RISK_MAX_DAILY_TRADES} lệnh | "
                  f"PnL: ${self._daily_pnl:+.2f}")

    def record_close(self, pnl_usd: float):
        """Gọi sau khi đóng vị thế (TP hoặc rug). pnl_usd âm = lỗ."""
        with self._lock:
            self._check_day_reset()
            self._daily_pnl += pnl_usd
            if self._daily_pnl <= -abs(RISK_MAX_DAILY_LOSS_USD):
                self._paused = True
                print(f"[RiskManager] 🛑 Daily loss limit hit: ${self._daily_pnl:.2f} → DỪNG MUA")
            print(f"[RiskManager] 💰 Close PnL: ${pnl_usd:+.2f} | Tổng hôm nay: ${self._daily_pnl:+.2f}")

    def status(self) -> str:
        with self._lock:
            self._check_day_reset()
            status = "🛑 PAUSED" if self._paused else "🟢 OK"
            return (f"{status} | Lệnh: {self._trade_count}/{RISK_MAX_DAILY_TRADES} | "
                    f"PnL hôm nay: ${self._daily_pnl:+.2f} | "
                    f"Daily loss limit: ${RISK_MAX_DAILY_LOSS_USD}")

_risk_manager = RiskManager()

# TTL riêng cho từng loại endpoint (tránh 429)
_CACHE_TTL_PROFILE  = float(os.getenv("CACHE_TTL_PROFILE_S",  "30"))   # Profile/Boost: 30s
_CACHE_TTL_NEWPAIRS = float(os.getenv("CACHE_TTL_NEWPAIRS_S", "10"))   # /new: 10s
_CACHE_TTL_PAIRS    = float(os.getenv("CACHE_TTL_PAIRS_S",    "8"))    # /tokens/{addr}: 8s

def _dex_get_with_retry(url: str, label: str = "", retries: int = 3) -> Optional[requests.Response]:
    """
    GET với retry + exponential backoff khi gặp 429.
    Trả về Response hoặc None nếu thất bại.
    """
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=12,
                             headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 429:
                wait = 2 ** attempt * 2   # 2s, 4s, 8s
                print(f"[DexScreener] ⚠️  429 {label} — chờ {wait}s (lần {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                raise
    return None

# ================================================================
# SQLITE POSITION DB
# ================================================================

_db_lock = threading.Lock()

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _db_lock:
        conn = _get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                mint               TEXT PRIMARY KEY,
                symbol             TEXT NOT NULL,
                name               TEXT,
                pair_address       TEXT,
                chain              TEXT DEFAULT 'solana',
                entry_price        REAL NOT NULL DEFAULT 0,
                actual_entry_price REAL DEFAULT 0,
                token_amount       REAL DEFAULT 0,
                raw_balance        TEXT DEFAULT '0',
                entry_time         INTEGER NOT NULL,
                usdc_spent         REAL NOT NULL,
                buy_sig            TEXT NOT NULL,
                opp_score          INTEGER DEFAULT 0,
                token_decimals     INTEGER DEFAULT 6,
                tp_sent            INTEGER DEFAULT 0,
                price_confirmed    INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS blacklist (
                mint       TEXT PRIMARY KEY,
                added_time INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS permanent_ban (
                mint       TEXT PRIMARY KEY,
                reason     TEXT,
                added_time INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_history (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                -- Token info
                mint                TEXT NOT NULL,
                symbol              TEXT NOT NULL,
                name                TEXT,
                pair_address        TEXT,
                chain               TEXT DEFAULT 'solana',
                -- Buy side
                buy_time            INTEGER NOT NULL,
                buy_sig             TEXT,
                buy_price_est       REAL DEFAULT 0,   -- DexScreener tạm lúc mua
                buy_price_actual    REAL DEFAULT 0,   -- USD/token thực sau slippage
                native_spent        REAL DEFAULT 0,   -- SOL (Solana) hoặc USDC (Base) thực chi
                usd_spent           REAL DEFAULT 0,   -- native_spent × sol_price (Solana) | native_spent (Base)
                token_amount        REAL DEFAULT 0,   -- số token nhận được thực tế
                slippage_pct        REAL DEFAULT 0,   -- % slippage thực tế
                -- Scoring
                opp_score           INTEGER DEFAULT 0,
                score_detail        TEXT,             -- JSON array of score detail strings
                website             TEXT,
                twitter             TEXT,
                website_valid       INTEGER DEFAULT -1,  -- -1=unknown 0=invalid 1=valid
                twitter_valid       INTEGER DEFAULT -1,
                lp_status           TEXT,
                holder_count        INTEGER DEFAULT 0,
                top10_holder_pct    REAL DEFAULT 0,
                -- Sell side (NULL until sold)
                sell_time           INTEGER,
                sell_sig            TEXT,
                sell_reason         TEXT,             -- TP | RUG | FAST_DUMP | MANUAL
                sell_price          REAL DEFAULT 0,   -- USD/token lúc bán
                native_received     REAL DEFAULT 0,   -- SOL/USDC thực nhận về
                net_profit_native   REAL DEFAULT 0,   -- native_received - native_spent
                net_profit_pct      REAL DEFAULT 0,   -- %
                hold_duration_s     INTEGER DEFAULT 0 -- giây từ mua đến bán
            );
        """)
        conn.commit()

        # Migration: thêm cột mới nếu DB cũ chưa có
        _migrate_cols = [
            ("actual_entry_price", "REAL DEFAULT 0"),
            ("token_amount",       "REAL DEFAULT 0"),
            ("raw_balance",        "TEXT DEFAULT '0'"),
            ("price_confirmed",    "INTEGER DEFAULT 0"),
            ("chain",              "TEXT DEFAULT 'solana'"),
        ]
        existing = {row[1] for row in conn.execute("PRAGMA table_info(positions)").fetchall()}
        for col, typedef in _migrate_cols:
            if col not in existing:
                conn.execute(f"ALTER TABLE positions ADD COLUMN {col} {typedef}")
                print(f"[DB] 🔧 Migration: thêm cột positions.{col}")
        conn.commit()
        conn.close()
    print("[DB] ✅ SQLite ready (positions + blacklist + permanent_ban + trade_history)")

def db_is_perm_banned(mint: str) -> bool:
    with _db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT 1 FROM permanent_ban WHERE mint=?", (mint,)).fetchone()
        conn.close()
        return row is not None

def db_add_perm_ban(mint: str, reason: str):
    """Cấm vĩnh viễn — ghi vào cả permanent_ban lẫn blacklist."""
    with _db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT OR IGNORE INTO permanent_ban (mint, reason, added_time) VALUES (?,?,?)",
            (mint, reason, int(time.time()))
        )
        conn.execute(
            "INSERT OR IGNORE INTO blacklist (mint, added_time) VALUES (?,?)",
            (mint, int(time.time()))
        )
        conn.commit()
        conn.close()
    print(f"[DB] 🚫 Permanent ban: {mint[:12]}... | {reason}")

def db_save_position(token: dict, buy_sig: str, usdc_spent: float):
    """
    Lưu position ngay sau khi gửi lệnh mua.
    entry_price = giá DexScreener tạm thời (placeholder).
    actual_entry_price = 0 → sẽ được cập nhật bởi _confirm_entry sau khi TX confirm.
    price_confirmed = 0 → monitor dùng DexScreener price, không trigger TP sớm.
    """
    with _db_lock:
        conn = _get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO positions
            (mint, symbol, name, pair_address, chain,
             entry_price, actual_entry_price, token_amount, raw_balance,
             entry_time, usdc_spent, buy_sig, opp_score, token_decimals,
             tp_sent, price_confirmed)
            VALUES (?,?,?,?,?, ?,0,0,'0', ?,?,?,?,?, 0,0)
        """, (
            token["address"], token["symbol"], token.get("name", ""),
            token.get("pair_address", ""), token.get("chain", "solana"),
            token.get("price_usd", 0),          # placeholder — sẽ được overwrite
            int(time.time()), usdc_spent, buy_sig,
            token.get("_opp_score", 0), token.get("token_decimals", 6),
        ))
        conn.commit()
        conn.close()

def db_close_position(mint: str):
    with _db_lock:
        conn = _get_conn()
        conn.execute("DELETE FROM positions WHERE mint=?", (mint,))
        conn.commit()
        conn.close()

def db_mark_tp_sent(mint: str):
    with _db_lock:
        conn = _get_conn()
        conn.execute("UPDATE positions SET tp_sent=1 WHERE mint=?", (mint,))
        conn.commit()
        conn.close()

def db_update_actual_entry(mint: str, actual_price: float,
                           token_amount: float, raw_balance: str,
                           token_decimals: int, native_spent: float = 0.0):
    """
    Cập nhật giá vào thực tế sau khi xác nhận balance trên RPC.
    actual_entry_price = usd_spent / token_amount  ← giá USD/token thực sau slippage
                         (đã quy đổi SOL → USD bên ngoài trước khi truyền vào)
    native_spent       = SOL thô (Solana) hoặc USDC (Base) — lưu vào usdc_spent để
                         send_tp_signal tính P&L theo đúng native unit.
    entry_price cũng được sync để nhất quán.
    price_confirmed = 1 → monitor bắt đầu tính TP từ giá này.
    """
    with _db_lock:
        conn = _get_conn()
        conn.execute("""
            UPDATE positions
            SET actual_entry_price = ?,
                entry_price        = ?,
                token_amount       = ?,
                raw_balance        = ?,
                token_decimals     = ?,
                price_confirmed    = 1
                {native_update}
            WHERE mint = ?
        """.replace("{native_update}", ", usdc_spent = ?" if native_spent > 0 else ""),
        (actual_price, actual_price, token_amount, raw_balance, token_decimals,
         *([native_spent] if native_spent > 0 else []),
         mint))
        conn.commit()
        conn.close()

def db_get_positions() -> List[dict]:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM positions WHERE tp_sent=0").fetchall()
        conn.close()
        return [dict(r) for r in rows]


def db_log_buy(token: dict, buy_sig: str,
               native_spent: float, usd_spent: float,
               token_amount: float, buy_price_actual: float,
               slippage_pct: float) -> int:
    """
    Ghi lệnh mua vào trade_history.
    Trả về row id để sau này update khi bán.
    """
    import json as _json
    with _db_lock:
        conn = _get_conn()
        cur = conn.execute("""
            INSERT INTO trade_history (
                mint, symbol, name, pair_address, chain,
                buy_time, buy_sig,
                buy_price_est, buy_price_actual,
                native_spent, usd_spent, token_amount, slippage_pct,
                opp_score, score_detail,
                website, twitter, website_valid, twitter_valid,
                lp_status, holder_count, top10_holder_pct
            ) VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?, ?,?,?,?, ?,?,?)
        """, (
            token.get("address",""), token.get("symbol","?"), token.get("name",""),
            token.get("pair_address",""), token.get("chain","solana"),
            int(time.time()), buy_sig,
            token.get("price_usd", 0), buy_price_actual,
            native_spent, usd_spent, token_amount, slippage_pct,
            token.get("_opp_score", 0),
            _json.dumps(token.get("_score_detail", []), ensure_ascii=False),
            token.get("website",""), token.get("twitter",""),
            1 if token.get("website_valid") else (0 if token.get("website_valid") is False else -1),
            1 if token.get("twitter_valid") else (0 if token.get("twitter_valid") is False else -1),
            token.get("lp_status","unknown"),
            token.get("holder_count", 0),
            token.get("top10_holder_pct", 0),
        ))
        row_id = cur.lastrowid
        conn.commit()
        conn.close()
    print(f"[TradeLog] 📝 BUY logged: {token.get('symbol','?')} | row_id={row_id}")
    return row_id


def db_log_sell(mint: str, sell_sig: str, sell_reason: str,
                sell_price: float, native_received: float):
    """
    Cập nhật trade_history với thông tin bán — tìm lệnh mua gần nhất chưa có sell_sig.
    """
    with _db_lock:
        conn = _get_conn()
        # Lấy row mua gần nhất của mint này chưa bán
        row = conn.execute("""
            SELECT id, buy_time, native_spent
            FROM trade_history
            WHERE mint=? AND sell_sig IS NULL
            ORDER BY buy_time DESC LIMIT 1
        """, (mint,)).fetchone()

        if not row:
            conn.close()
            print(f"[TradeLog] ⚠️  db_log_sell: không tìm thấy lệnh mua của {mint[:12]}...")
            return

        row_id       = row["id"]
        buy_time     = row["buy_time"]
        native_spent = float(row["native_spent"] or 0)
        sell_time    = int(time.time())
        hold_s       = sell_time - buy_time
        net_profit   = native_received - native_spent
        net_pct      = (net_profit / native_spent * 100) if native_spent > 0 else 0.0

        conn.execute("""
            UPDATE trade_history
            SET sell_time         = ?,
                sell_sig          = ?,
                sell_reason       = ?,
                sell_price        = ?,
                native_received   = ?,
                net_profit_native = ?,
                net_profit_pct    = ?,
                hold_duration_s   = ?
            WHERE id = ?
        """, (sell_time, sell_sig, sell_reason, sell_price,
              native_received, net_profit, net_pct, hold_s, row_id))
        conn.commit()
        conn.close()
    sign = "+" if net_profit >= 0 else ""
    print(f"[TradeLog] 📝 SELL logged: {mint[:12]}... | "
          f"{sell_reason} | {sign}{net_pct:.1f}% | hold={hold_s}s")


def db_export_csv(filepath: str = "trade_history.csv") -> str:
    """
    Xuất toàn bộ trade_history ra CSV.
    Trả về filepath đã ghi.
    """
    import csv as _csv
    import json as _json

    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT * FROM trade_history ORDER BY buy_time DESC
        """).fetchall()
        conn.close()

    if not rows:
        print("[TradeLog] ⚠️  trade_history trống — không có gì để export")
        return filepath

    def _ts(v):
        if not v: return ""
        import datetime
        return datetime.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")

    def _dur(s):
        if not s: return ""
        s = int(s)
        h, r = divmod(s, 3600)
        m, sec = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = _csv.writer(f)
        writer.writerow([
            "id", "chain", "symbol", "name", "mint", "pair_address",
            # Buy
            "buy_time", "buy_sig",
            "buy_price_est_usd", "buy_price_actual_usd",
            "native_spent", "usd_spent", "token_amount", "slippage_pct",
            # Score
            "opp_score", "score_detail",
            "website", "website_valid",
            "twitter", "twitter_valid",
            "lp_status", "holder_count", "top10_holder_pct",
            # Sell
            "sell_time", "sell_sig", "sell_reason",
            "sell_price_usd",
            "native_received", "net_profit_native", "net_profit_pct",
            "hold_duration",
            # Status
            "status",
        ])
        for r in rows:
            d = dict(r)
            status = "OPEN" if not d.get("sell_sig") else d.get("sell_reason", "SOLD")
            writer.writerow([
                d["id"],
                d.get("chain",""),
                d.get("symbol",""),
                d.get("name",""),
                d.get("mint",""),
                d.get("pair_address",""),
                # Buy
                _ts(d.get("buy_time")),
                d.get("buy_sig",""),
                d.get("buy_price_est",""),
                d.get("buy_price_actual",""),
                d.get("native_spent",""),
                d.get("usd_spent",""),
                d.get("token_amount",""),
                d.get("slippage_pct",""),
                # Score
                d.get("opp_score",""),
                d.get("score_detail",""),
                d.get("website",""),
                {1:"YES", 0:"NO", -1:"?"}.get(d.get("website_valid",-1),"?"),
                d.get("twitter",""),
                {1:"YES", 0:"NO", -1:"?"}.get(d.get("twitter_valid",-1),"?"),
                d.get("lp_status",""),
                d.get("holder_count",""),
                d.get("top10_holder_pct",""),
                # Sell
                _ts(d.get("sell_time")),
                d.get("sell_sig",""),
                d.get("sell_reason",""),
                d.get("sell_price",""),
                d.get("native_received",""),
                d.get("net_profit_native",""),
                d.get("net_profit_pct",""),
                _dur(d.get("hold_duration_s")),
                status,
            ])

    print(f"[TradeLog] ✅ Exported {len(rows)} rows → {filepath}")
    return filepath

def db_is_blacklisted(mint: str) -> bool:
    with _db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT 1 FROM blacklist WHERE mint=?", (mint,)).fetchone()
        conn.close()
        return row is not None

def db_add_blacklist(mint: str):
    with _db_lock:
        conn = _get_conn()
        conn.execute("INSERT OR IGNORE INTO blacklist (mint, added_time) VALUES (?,?)",
                     (mint, int(time.time())))
        conn.commit()
        conn.close()

def db_has_position(mint: str) -> bool:
    with _db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT 1 FROM positions WHERE mint=?", (mint,)).fetchone()
        conn.close()
        return row is not None

# ================================================================
# UTILS
# ================================================================

def _fmt_usd(v: float) -> str:
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:.0f}"

def _fmt_pct(p: float) -> str:
    return f"{'🟢' if p>0 else '🔴'}{p:+.1f}%"

def _age_str(m: Optional[float]) -> str:
    if m is None: return "N/A"
    if m < 60: return f"{m:.0f}p"
    return f"{m/60:.1f}h"

def _duration_str(entry_ts: int) -> str:
    secs = int(time.time()) - entry_ts
    if secs < 60:   return f"{secs}s"
    if secs < 3600: return f"{secs//60}p{secs%60:02d}s"
    return f"{secs//3600}h{(secs%3600)//60}p"

def _get_min_buys(age_min: Optional[float]) -> int:
    for max_age, min_buys in BUY_TXN_BY_AGE:
        if max_age is None or (age_min is not None and age_min <= max_age):
            return min_buys
    return 30

_score_history_lock = threading.Lock()

def _log_score_snapshot(token: dict, score: int, min_score: int) -> None:
    """Lưu lịch sử chấm điểm để theo dõi mặt bằng thị trường theo thời gian."""
    rec = {
        "ts": int(time.time()),
        "chain": token.get("chain", "solana"),
        "symbol": token.get("symbol", "?"),
        "address": token.get("address", ""),
        "score": int(score),
        "min_score": int(min_score),
        "tradable": bool(score >= min_score),
        "age_min": round(float(token.get("token_age_minutes") or 0), 2),
        "liq_usd": round(float(token.get("liquidity_usd") or 0), 2),
        "vol_1h": round(float(token.get("volume_1h") or 0), 2),
    }

    try:
        with _score_history_lock:
            SCORE_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
            with SCORE_HISTORY_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ScoreHistory] ⚠️  Không ghi được score history: {e}")


def _content_score_history(limit: int = 200) -> str:
    """Tóm tắt lịch sử score gần nhất để ước lượng mặt bằng có thể trade."""
    try:
        if not SCORE_HISTORY_PATH.exists():
            return (
                "📊 <b>Lịch sử chấm điểm</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "📭 Chưa có dữ liệu score. Hãy để bot chạy thêm vài phút rồi thử lại."
            )

        lines = SCORE_HISTORY_PATH.read_text(encoding="utf-8").splitlines()
        if not lines:
            return (
                "📊 <b>Lịch sử chấm điểm</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "📭 File score history đang rỗng."
            )

        raw = lines[-max(1, int(limit)):]
        rows = []
        for ln in raw:
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue

        if not rows:
            return "📊 <b>Lịch sử chấm điểm</b>\n━━━━━━━━━━━━━━━━━━━━\n⚠️ Dữ liệu score bị lỗi format."

        scores = [int(r.get("score", 0)) for r in rows]
        scores_sorted = sorted(scores)
        total = len(scores)
        avg = sum(scores) / total
        med = scores_sorted[total // 2] if total % 2 == 1 else (scores_sorted[total // 2 - 1] + scores_sorted[total // 2]) / 2
        p75 = scores_sorted[min(total - 1, int(total * 0.75))]

        cur_min_score = int(cfg("MIN_SCORE") or MIN_SCORE)
        over70 = sum(1 for s in scores if s >= 70)
        tradable = sum(1 for s in scores if s >= cur_min_score)

        recent = sorted(rows, key=lambda x: int(x.get("ts", 0)), reverse=True)[:12]
        token_lines = []
        for r in recent:
            sym = r.get("symbol", "?")
            sc = int(r.get("score", 0))
            ch = "SOL" if r.get("chain") == "solana" else "BASE"
            icon = "✅" if sc >= cur_min_score else "⏭"
            token_lines.append(f"{icon} <b>${sym}</b> [{ch}] — {sc}/100")

        return (
            "📊 <b>Lịch sử chấm điểm (gần nhất)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🧪 Mẫu: <b>{total}</b> token gần nhất\n"
            f"🎯 Ngưỡng trade hiện tại: <b>{cur_min_score}</b>\n"
            f"📈 Trung bình: <b>{avg:.1f}</b> | Median: <b>{med:.1f}</b> | P75: <b>{p75:.1f}</b>\n"
            f"🟡 >=70 điểm: <b>{over70}</b> ({over70/max(total,1)*100:.1f}%)\n"
            f"🟢 >=ngưỡng trade: <b>{tradable}</b> ({tradable/max(total,1)*100:.1f}%)\n\n"
            "<b>12 token quét gần nhất:</b>\n" + "\n".join(token_lines) +
            "\n\n💡 Dùng lệnh: <code>/scores 500</code> để xem mẫu rộng hơn."
        )
    except Exception as ex:
        return f"❌ Lỗi đọc score history: {ex}"

# ================================================================
# TELEGRAM
# ================================================================

def _send_tg(text: str, chat_id: str = None) -> bool:
    """Gửi Telegram message. Return True nếu có ít nhất 1 target gửi thành công."""
    if not TELEGRAM_BOT_TOKEN:
        print("[TG] ⚠️  TELEGRAM_BOT_TOKEN chưa set — bỏ qua gửi Telegram")
        return False

    targets = [chat_id] if chat_id else (TELEGRAM_SIGNAL_CHANNELS or [TELEGRAM_CHAT_ID])
    sent_ok = False
    for cid in targets:
        if not cid:
            continue
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=10
            )
            if resp.ok:
                sent_ok = True
            else:
                print(f"[TG] ⚠️  sendMessage fail chat_id={cid} status={resp.status_code} body={resp.text[:200]}")
        except Exception as e:
            print(f"[TG] ❌ {e}")
    return sent_ok

def _alert(text: str):
    if TELEGRAM_CHAT_ID:
        _send_tg(text, chat_id=TELEGRAM_CHAT_ID)

def send_error_alert(msg: str):
    _alert(f"⚠️ <b>SOLANA TRADER ERROR</b>\n\n{msg}")

# ================================================================
# OKX AUTH
# ================================================================

def _okx_headers(method: str, path: str, query: str = "", body: str = "") -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    prehash = ts + method.upper() + path + (query or body)
    sig = base64.b64encode(
        hmac.new(OKX_SECRET_KEY.encode(), prehash.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "Content-Type":         "application/json",
        "OK-ACCESS-KEY":        OKX_API_KEY,
        "OK-ACCESS-SIGN":       sig,
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": OKX_API_PASSPHRASE,
        "OK-ACCESS-PROJECT":    OKX_PROJECT_ID,
    }

# Semaphore giới hạn số request OKX đồng thời — tránh 429
_okx_semaphore = threading.Semaphore(3)   # tối đa 3 request song song

def okx_get(path: str, params: dict, _retries: int = 4) -> Optional[dict]:
    """Gọi OKX API với retry + exponential backoff khi gặp 429."""
    path_v6 = path.replace("/api/v5/", "/api/v6/")
    qs      = "?" + urllib.parse.urlencode(params)
    hdrs    = _okx_headers("GET", path_v6, query=qs)
    url     = OKX_BASE_URL + path_v6 + qs

    for attempt in range(_retries):
        with _okx_semaphore:   # chờ slot trống trước khi gọi
            try:
                r = requests.get(url, headers=hdrs, timeout=15)

                # 429 → back-off rồi retry
                if r.status_code == 429:
                    wait = 2 ** attempt          # 1s, 2s, 4s, 8s
                    print(f"[OKX] ⏳ 429 rate-limit — chờ {wait}s (lần {attempt+1}/{_retries})")
                    time.sleep(wait)
                    hdrs = _okx_headers("GET", path_v6, query=qs)  # refresh timestamp
                    continue

                r.raise_for_status()
                data = r.json()
                if data.get("code") != "0":
                    print(f"[OKX] ❌ {path_v6}: code={data.get('code')} msg={data.get('msg')}")
                    return None
                return data

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[OKX] ⏳ 429 rate-limit — chờ {wait}s (lần {attempt+1}/{_retries})")
                    time.sleep(wait)
                    hdrs = _okx_headers("GET", path_v6, query=qs)
                    continue
                print(f"[OKX] ❌ GET {path_v6}: {e}")
                return None
            except Exception as e:
                print(f"[OKX] ❌ GET {path_v6}: {e}")
                return None

    print(f"[OKX] ❌ {path_v6}: hết retry sau {_retries} lần")
    return None

# ================================================================
# SOLANA SIGNING
# ================================================================

def _sign_and_send_tx(tx_b64: str) -> Optional[str]:
    """
    Ký và gửi Solana VersionedTransaction.
    Không dùng solders — chỉ cần: base58, PyNaCl (nacl) hoặc cryptography.
    Cài: pip install base58 PyNaCl
    """
    try:
        import base58 as _b58

        # ── 1. Decode private key (base58, 64 bytes: 32 secret + 32 public) ──
        secret_full = _b58.b58decode(WALLET_PRIVKEY)
        if len(secret_full) == 64:
            secret_key = secret_full[:32]   # seed
            public_key = secret_full[32:]
        elif len(secret_full) == 32:
            secret_key = secret_full
            # Derive public key từ seed
            public_key = _ed25519_pubkey(secret_key)
        else:
            raise ValueError(f"Private key không hợp lệ: {len(secret_full)} bytes")

        # ── 2. Decode TX bytes ──────────────────────────────────────────────
        try:
            tx_bytes = _b58.b58decode(tx_b64)
        except Exception:
            tx_bytes = base64.b64decode(tx_b64)

        # ── 3. Parse VersionedTransaction (prefix byte = 0x80 | num_signatures) ──
        #   Layout: [num_sigs (compact-u16)] [sig × num_sigs (64 bytes each)]
        #           [message_prefix (1 byte)] [message...]
        #   OKX trả về TX với 1 chữ ký placeholder (64 zero bytes).
        offset = 0

        # Compact-u16 decode cho num_signatures
        def _read_compact_u16(buf, pos):
            val, shift = 0, 0
            while True:
                b = buf[pos]; pos += 1
                val |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            return val, pos

        num_sigs, offset = _read_compact_u16(tx_bytes, offset)
        sig_section_end  = offset + num_sigs * 64   # mỗi sig = 64 bytes

        # Message bắt đầu ngay sau phần signatures
        message_bytes = tx_bytes[sig_section_end:]

        # ── 4. Ký message ───────────────────────────────────────────────────
        signature = _ed25519_sign(secret_key, message_bytes)

        # ── 5. Ghép lại TX: [compact-u16(1)] [signature 64B] [message] ──────
        signed_tx_bytes = bytes([0x01]) + signature + message_bytes
        encoded         = base64.b64encode(signed_tx_bytes).decode()

        # ── 6. Gửi lên RPC ──────────────────────────────────────────────────
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method":  "sendTransaction",
            "params":  [encoded, {
                "encoding": "base64", "skipPreflight": False,
                "preflightCommitment": "confirmed", "maxRetries": 3,
            }]
        }, timeout=30)
        result = resp.json()
        if "error" in result:
            print(f"[RPC] ❌ {result['error']}")
            return None
        sig = result.get("result")
        print(f"[RPC] ✅ Tx sent: {sig}")
        return sig

    except Exception as e:
        print(f"[Sign] ❌ {e}")
        return None

# ================================================================
# TX CONFIRM + USDC SPENT HELPERS
# ================================================================

def _wait_tx_confirmed(sig: str, chain: str, label: str = "",
                       max_attempts: int = 20, interval: float = 3.0) -> bool:
    """
    Poll đến khi TX confirmed hoặc hết timeout.
    Solana: getSignatureStatuses — trả về confirmed/finalized.
    Base:   eth_getTransactionReceipt — status=1 là thành công.
    Trả về True nếu confirmed OK, False nếu timeout hoặc lỗi.
    """
    for attempt in range(max_attempts):
        time.sleep(interval)
        try:
            if chain == "base":
                from web3 import Web3
                w3   = _get_base_w3()
                rcpt = w3.eth.get_transaction_receipt(sig)
                if rcpt is None:
                    continue
                # FIX: web3.py trả về AttributeDict, dùng [] và int() cast
                status = int(rcpt["status"])
                if status == 1:
                    return True
                if status == 0:
                    print(f"[Confirm] ❌ {label}: TX revert (status=0)")
                    return False
            else:
                resp = requests.post(SOLANA_RPC_URL, json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getSignatureStatuses",
                    "params": [[sig], {"searchTransactionHistory": True}]
                }, timeout=10)
                st = resp.json().get("result", {}).get("value", [None])[0]
                if not st:
                    continue
                if st.get("err"):
                    print(f"[Confirm] ❌ {label}: TX error={st['err']}")
                    return False
                if st.get("confirmationStatus") in ("confirmed", "finalized"):
                    return True
        except Exception as e:
            print(f"[Confirm] ⚠️  {label} poll {attempt+1}: {e}")
    return False


def _get_spend_from_tx(sig: str, chain: str, label: str = "") -> float:
    """
    Reads confirmed TX → returns actual amount spent (SOL for Solana, USDC for Base).
    Returns 0.0 if cannot parse.
    """
    try:
        if chain == "base":
            return _get_usdc_spent_base(sig)
        else:
            return _get_sol_spent_solana(sig)
    except Exception as e:
        print(f"[Confirm] ⚠️  {label} parse spend: {e}")
        return 0.0


def _get_usdc_spent_from_tx(sig: str, chain: str, label: str = "") -> float:
    """Legacy alias — kept for Base chain compatibility."""
    return _get_spend_from_tx(sig, chain, label)


def _get_sol_spent_solana(sig: str) -> float:
    """
    Solana: read TX → compute SOL spent (lamport delta of fee payer account).
    pre_balance - post_balance - fee = SOL used for swap input.
    Returns float SOL. 0.0 if cannot parse.
    """
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }]
        }, timeout=15)
        tx = resp.json().get("result")
        if not tx:
            return 0.0
        meta = tx.get("meta", {})
        fee  = meta.get("fee", 0)
        pre  = meta.get("preBalances",  [])
        post = meta.get("postBalances", [])
        if not pre or not post:
            return 0.0
        # Account 0 = fee payer / wallet
        delta_lam = pre[0] - post[0] - fee
        if delta_lam > 0:
            return delta_lam / 1e9   # lamports → SOL
        return 0.0
    except Exception as e:
        print(f"[Confirm] ⚠️  _get_sol_spent_solana: {e}")
        return 0.0


def _get_usdc_spent_solana(sig: str) -> float:
    """
    Solana: dùng getTransaction với encoding=jsonParsed.
    So sánh preTokenBalances vs postTokenBalances của USDC account.
    Delta âm = ví đã gửi đi (= USDC đã tiêu thực tế).
    """
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }]
        }, timeout=15)
        tx = resp.json().get("result")
        if not tx:
            return 0.0

        meta = tx.get("meta", {})
        pre  = {b["accountIndex"]: b for b in (meta.get("preTokenBalances")  or [])}
        post = {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}

        for idx, pb in pre.items():
            mint = pb.get("mint", "")
            # Chỉ quan tâm USDC
            if mint != SOL_USDC:
                continue
            # Chỉ account thuộc ví của bot
            owner = pb.get("owner", "")
            if owner != WALLET_ADDRESS:
                continue
            pre_amt  = float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
            post_amt = float(post.get(idx, {}).get("uiTokenAmount", {})
                             .get("uiAmount", 0) or 0)
            delta = pre_amt - post_amt   # dương = đã chi ra
            if delta > 0:
                return delta
        return 0.0
    except Exception as e:
        print(f"[Confirm] ⚠️  _get_usdc_spent_solana: {e}")
        return 0.0


def _get_usdc_spent_base(sig: str) -> float:
    """
    Base: decode Transfer log của USDC contract.
    Transfer(from=wallet, to=router, amount) → amount là USDC đã tiêu.
    """
    try:
        from web3 import Web3
        w3   = _get_base_w3()
        rcpt = w3.eth.get_transaction_receipt(sig)
        if not rcpt:
            return 0.0

        wallet = _get_base_wallet_address().lower()
        usdc   = BASE_USDC.lower()
        # ERC-20 Transfer topic: keccak256("Transfer(address,address,uint256)")
        TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        for log in rcpt.get("logs", []):
            if log.get("address", "").lower() != usdc:
                continue
            topics = log.get("topics", [])
            if not topics or topics[0].lower() != TRANSFER_TOPIC:
                continue
            if len(topics) < 3:
                continue
            # topics[1] = from address (padded 32 bytes)
            from_addr = "0x" + topics[1][-40:]
            if from_addr.lower() != wallet:
                continue
            # data = amount (uint256, hex)
            amount_raw = int(log.get("data", "0x0"), 16)
            return amount_raw / (10 ** 6)  # USDC = 6 decimals
        return 0.0
    except Exception as e:
        print(f"[Confirm] ⚠️  _get_usdc_spent_base: {e}")
        return 0.0




def _get_base_w3():
    """Lazy-import web3 và trả về Web3 instance cho Base."""
    try:
        from web3 import Web3
        return Web3(Web3.HTTPProvider(BASE_RPC_URL, request_kwargs={"timeout": 30}))
    except ImportError:
        raise RuntimeError("pip install web3  # cần cho Base chain")

def _get_okx_spender(chain_index: str = BASE_CHAIN_INDEX) -> Optional[str]:
    """
    Lấy địa chỉ spender (OKX router) cần approve USDC trước khi swap.
    Gọi OKX /dex/aggregator/approve-transaction để lấy địa chỉ contract.
    Cache kết quả trong session — spender ít khi thay đổi.
    """
    cache_key = f"_okx_spender_{chain_index}"
    cached = getattr(_get_okx_spender, "_cache", {}).get(cache_key)
    if cached:
        return cached

    data = okx_get("/api/v5/dex/aggregator/approve-transaction", {
        "chainIndex":       chain_index,
        "tokenContractAddress": BASE_USDC,
        "approveAmount":    "1",   # chỉ cần lấy spender address, không cần amount thật
    })
    if not data:
        print("[Base] ⚠️  Không lấy được spender address từ OKX → dùng fallback")
        # Fallback: OKX DEX Router trên Base (địa chỉ cố định)
        return "0x40aA958dd87FC8305b97f2BA922CDdCa374bcD7"

    try:
        spender = data["data"][0].get("dexContractAddress", "")
        if not spender:
            return "0x40aA958dd87FC8305b97f2BA922CDdCa374bcD7"
        if not hasattr(_get_okx_spender, "_cache"):
            _get_okx_spender._cache = {}
        _get_okx_spender._cache[cache_key] = spender
        print(f"[Base] 🔑 OKX Spender: {spender}")
        return spender
    except Exception as e:
        print(f"[Base] ⚠️  Parse spender lỗi: {e} → dùng fallback")
        return "0x40aA958dd87FC8305b97f2BA922CDdCa374bcD7"


def _ensure_usdc_allowance(w3, acct, spender: str, amount_raw: int,
                            privkey: str) -> bool:
    """
    Kiểm tra và approve USDC allowance cho OKX router nếu chưa đủ.

    Flow:
      1. Gọi USDC.allowance(wallet, spender) trên RPC
      2. Nếu allowance >= amount_raw → skip (không tốn gas)
      3. Nếu thiếu → gửi approve(spender, MAX_UINT256)
         - Dùng MAX_UINT256 để chỉ approve 1 lần duy nhất
         - Poll receipt tối đa 30s để chắc chắn approve confirmed
      4. Trả về True nếu allowance OK, False nếu lỗi

    Tại sao MAX_UINT256?
      - Tiết kiệm gas mỗi lần swap (không approve lại)
      - Standard cho DEX (Uniswap, Aerodrome đều làm vậy)
      - Rủi ro thấp vì OKX router là contract đã audit
    """
    from web3 import Web3

    MAX_UINT256 = 2**256 - 1
    USDC_ADDR   = Web3.to_checksum_address(BASE_USDC)
    spender_cs  = Web3.to_checksum_address(spender)

    # ── ABI tối thiểu: allowance + approve ───────────────────────
    ERC20_ABI = [
        {
            "inputs": [
                {"name": "owner",   "type": "address"},
                {"name": "spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [
                {"name": "spender", "type": "address"},
                {"name": "value",   "type": "uint256"},
            ],
            "name": "approve",
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
            "type": "function",
        },
    ]

    try:
        usdc = w3.eth.contract(address=USDC_ADDR, abi=ERC20_ABI)

        # ── Bước 1: Kiểm tra allowance hiện tại ──────────────────
        current = usdc.functions.allowance(acct.address, spender_cs).call()
        print(f"[Base] 🔍 USDC allowance: {current} | cần: {amount_raw}")

        if current >= amount_raw:
            print("[Base] ✅ Allowance đủ → skip approve")
            return True

        # ── Bước 2: Gửi approve(spender, MAX_UINT256) ────────────
        print(f"[Base] 🔓 Approve USDC MAX cho {spender_cs[:12]}...")

        gas_price = w3.eth.gas_price
        approve_tx = usdc.functions.approve(
            spender_cs, MAX_UINT256
        ).build_transaction({
            "from":     acct.address,
            "nonce":    w3.eth.get_transaction_count(acct.address),
            "gas":      80_000,        # approve luôn tốn ~46k gas, 80k dư
            "gasPrice": gas_price,
            "chainId":  int(BASE_CHAIN_INDEX),
        })

        signed_approve = w3.eth.account.sign_transaction(approve_tx, privkey)
        raw_approve    = (getattr(signed_approve, "raw_transaction", None)
                          or getattr(signed_approve, "rawTransaction", None))
        approve_hash   = w3.eth.send_raw_transaction(raw_approve).hex()
        print(f"[Base] ⏳ Approve TX: {approve_hash[:20]}... → chờ confirm")

        # ── Bước 3: Poll confirm tối đa 30s ──────────────────────
        for attempt in range(10):
            time.sleep(3)
            try:
                rcpt = w3.eth.get_transaction_receipt(approve_hash)
                if rcpt is None:
                    print(f"[Base] ⏳ Approve chờ... ({attempt+1}/10)")
                    continue
                status = int(rcpt["status"])
                if status == 1:
                    print(f"[Base] ✅ Approve confirmed! TX: {approve_hash[:20]}...")
                    return True
                else:
                    print(f"[Base] ❌ Approve revert (status=0)")
                    return False
            except Exception as poll_err:
                print(f"[Base] ⚠️  Poll approve: {poll_err}")

        print("[Base] ⏰ Approve timeout (30s) → tiếp tục thử swap")
        return True   # optimistic: có thể confirm chậm, thử swap vẫn được

    except Exception as e:
        print(f"[Base] ❌ _ensure_usdc_allowance: {e}")
        return False


def execute_swap_base(from_token: str, to_token: str, amount_raw: str,
                      label: str = "SWAP",
                      token_age_min: Optional[float] = None) -> Optional[str]:
    """
    Swap EVM trên Base qua OKX DEX API.
    OKX trả về calldata → web3.py ký và gửi qua Base RPC.
    Không block — return tx hash ngay sau khi gửi.
    Dynamic slippage: 10% → 30% cho token < 5p.

    Bước approve USDC tự động:
      - Trước khi gọi OKX swap, kiểm tra allowance USDC → spender (OKX router)
      - Nếu chưa đủ → approve MAX_UINT256 và chờ confirm
      - Chỉ approve 1 lần duy nhất trong lifetime (MAX_UINT256)
    """
    if not BASE_WALLET_PRIVKEY:
        print("[Base] ❌ BASE_PRIVATE_KEY chưa set trong .env")
        return None

    from web3 import Web3
    w3      = _get_base_w3()
    privkey = BASE_WALLET_PRIVKEY if BASE_WALLET_PRIVKEY.startswith("0x") \
              else "0x" + BASE_WALLET_PRIVKEY
    acct    = w3.eth.account.from_key(privkey)

    # ── Bước 0: Approve USDC nếu from_token là USDC (BUY) ────────
    # Sell TX: from_token là địa chỉ token mới → không cần approve USDC
    if from_token.lower() == BASE_USDC.lower():
        spender = _get_okx_spender(BASE_CHAIN_INDEX)
        if spender:
            ok = _ensure_usdc_allowance(
                w3, acct, spender,
                int(amount_raw),   # amount_raw là string số nguyên
                privkey,
            )
            if not ok:
                print("[Base] ❌ Approve USDC thất bại → hủy swap")
                return None
        else:
            print("[Base] ⚠️  Không lấy được spender → bỏ qua approve (rủi ro revert)")

    dyn_slippage = _get_dynamic_slippage(token_age_min)
    print(f"[Base] 📐 Slippage: {dyn_slippage}% (tuổi={token_age_min:.1f}p)" if token_age_min is not None
          else f"[Base] 📐 Slippage: {dyn_slippage}%")

    data = okx_get("/api/v5/dex/aggregator/swap", {
        "chainIndex":        BASE_CHAIN_INDEX,
        "fromTokenAddress":  from_token,
        "toTokenAddress":    to_token,
        "amount":            amount_raw,
        "slippagePercent":   str(dyn_slippage),
        "userWalletAddress": _get_base_wallet_address(),
    })
    if not data:
        return None

    try:
        tx_obj = data["data"][0].get("tx", {})

        # FIX: parse gasPrice/maxFeePerGas đúng cách cho EIP-1559 và legacy
        def _parse_hex_or_int(val, fallback=0):
            if not val:
                return fallback
            if isinstance(val, str) and val.startswith("0x"):
                return int(val, 16)
            try:
                return int(val)
            except Exception:
                return fallback

        # Ưu tiên EIP-1559 fields nếu OKX trả về
        max_fee          = _parse_hex_or_int(tx_obj.get("maxFeePerGas"))
        max_priority_fee = _parse_hex_or_int(tx_obj.get("maxPriorityFeePerGas"))
        legacy_gas_price = _parse_hex_or_int(tx_obj.get("gasPrice"), w3.eth.gas_price)

        tx = {
            "from":  acct.address,
            "to":    Web3.to_checksum_address(tx_obj.get("to", "")),
            "value": _parse_hex_or_int(tx_obj.get("value"), 0),
            "data":  tx_obj.get("data", "0x"),
            "gas":   _parse_hex_or_int(tx_obj.get("gas"), 300000),
            "nonce": w3.eth.get_transaction_count(acct.address),
            "chainId": int(BASE_CHAIN_INDEX),
        }
        if max_fee and max_priority_fee:
            # EIP-1559
            tx["maxFeePerGas"]         = max_fee
            tx["maxPriorityFeePerGas"] = max_priority_fee
        else:
            # Legacy
            tx["gasPrice"] = legacy_gas_price

        print(f"[Base] 📋 TX: to={tx['to'][:12]}... gas={tx['gas']} "
              f"{'EIP-1559' if max_fee else 'legacy'}")

        signed = w3.eth.account.sign_transaction(tx, privkey)
        # FIX: web3.py v7+ đổi rawTransaction → raw_transaction; hỗ trợ cả hai
        raw_tx = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction", None)
        tx_hash = w3.eth.send_raw_transaction(raw_tx).hex()
        print(f"[Base] ✅ {label}: {tx_hash[:20]}...")
        return tx_hash

    except Exception as e:
        print(f"[Base] ❌ execute_swap_base: {e}")
        return None

def _get_base_wallet_address() -> str:
    """Lấy địa chỉ EVM wallet từ private key."""
    if not BASE_WALLET_PRIVKEY:
        return ""
    try:
        from web3 import Web3
        privkey = BASE_WALLET_PRIVKEY if BASE_WALLET_PRIVKEY.startswith("0x") \
                  else "0x" + BASE_WALLET_PRIVKEY
        return Web3().eth.account.from_key(privkey).address
    except Exception as e:
        print(f"[Base] ⚠️  Không lấy được wallet address: {e}")
        return ""

def get_token_raw_balance_base(token_addr: str) -> str:
    """Lấy số dư ERC-20 trên Base (raw wei)."""
    try:
        from web3 import Web3
        w3     = _get_base_w3()
        wallet = _get_base_wallet_address()
        if not wallet:
            return "0"
        # ERC-20 balanceOf ABI minimal
        abi = [{"inputs":[{"name":"_owner","type":"address"}],
                "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
                "stateMutability":"view","type":"function"}]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token_addr), abi=abi)
        bal = contract.functions.balanceOf(
            Web3.to_checksum_address(wallet)).call()
        return str(bal)
    except Exception as e:
        print(f"[Base] ❌ get_balance: {e}")
        return "0"

# ================================================================
# SWAP (chain-agnostic dispatcher)
# ================================================================

def _sol_raw(sol: float) -> str:
    """Convert SOL amount to lamports (raw)."""
    return str(int(sol * 10 ** SOL_DECIMALS))

def _usdc_raw(usdc: float, chain: str = "solana") -> str:
    """Convert USDC to raw amount. Base USDC also 6 decimals."""
    return str(int(usdc * 10 ** USDC_DECIMALS))

def _buy_token_and_raw(chain: str) -> tuple:
    """
    Returns (from_token_address, raw_amount) for a buy swap.
    Solana → buys with SOL (native WSOL mint), amount in lamports.
    Base   → buys with USDC, amount in USDC raw units.
    Đọc BUY_AMOUNT từ cfg() để phản ánh thay đổi runtime.
    """
    if chain == "base":
        amount = cfg("BUY_AMOUNT_USDC") or BUY_AMOUNT_USDC
        return BASE_USDC, _usdc_raw(float(amount))
    amount = cfg("BUY_AMOUNT_SOL") or BUY_AMOUNT_SOL
    return SOL_NATIVE, _sol_raw(float(amount))

def _get_dynamic_slippage(token_age_min: Optional[float]) -> float:
    """
    Dynamic Slippage theo tuổi token — đọc từ cfg() để phản ánh thay đổi runtime.
      < 5p  → SLIPPAGE_TOKEN_NEW (default 30%)
      5-30p → SLIPPAGE_TOKEN_MID (default 15%)
      > 30p → SLIPPAGE_PCT       (default 10%)
    """
    slip_default = cfg("SLIPPAGE_PCT")         or SLIPPAGE_PCT
    slip_new     = cfg("SLIPPAGE_TOKEN_NEW")   or SLIPPAGE_TOKEN_NEW
    slip_mid     = cfg("SLIPPAGE_TOKEN_MID")   or SLIPPAGE_TOKEN_MID
    if token_age_min is None:
        return float(slip_default)
    if token_age_min < 5:
        return float(slip_new)
    if token_age_min < 30:
        return float(slip_mid)
    return float(slip_default)

def _get_dynamic_priority_fee(chain: str = "solana") -> int:
    """
    Dynamic Priority Fee cho Solana dựa trên network congestion.
    Thử đọc recent prioritization fees từ RPC:
      median fee thấp  → PRIORITY_FEE_DEFAULT (100k)
      median fee cao   → PRIORITY_FEE_HIGH (500k)
      rất cao / lỗi   → PRIORITY_FEE_VERY_HIGH (1M)
    """
    if chain != "solana":
        return PRIORITY_FEE_DEFAULT
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getRecentPrioritizationFees",
            "params": []
        }, timeout=5)
        fees = resp.json().get("result", [])
        if not fees:
            return PRIORITY_FEE_DEFAULT
        # Lấy median của 20 fee gần nhất
        recent = sorted([f.get("prioritizationFee", 0) for f in fees[-20:]])
        median_fee = recent[len(recent) // 2]
        if median_fee > 500_000:
            return PRIORITY_FEE_VERY_HIGH
        if median_fee > 100_000:
            return PRIORITY_FEE_HIGH
        return PRIORITY_FEE_DEFAULT
    except Exception:
        return PRIORITY_FEE_DEFAULT

def _jup_get(params: dict) -> Optional[dict]:
    """
    GET Jupiter quote endpoint với retry.
    Dùng JUP_QUOTE_URL (lite-api.jup.ag/swap/v1/quote).
    """
    for attempt in range(JUP_RETRIES):
        try:
            r = requests.get(JUP_QUOTE_URL, params=params,
                             timeout=JUP_TIMEOUT,
                             headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                print(f"[JUP] ❌ Quote error: {data['error']}")
                return None
            return data
        except requests.exceptions.ConnectionError:
            print(f"[JUP] 🔌 Không kết nối được: {JUP_QUOTE_URL}")
        except requests.exceptions.Timeout:
            print(f"[JUP] ⏰ Timeout quote (lần {attempt+1}/{JUP_RETRIES})")
        except Exception as e:
            print(f"[JUP] ❌ Quote: {e}")

        if attempt < JUP_RETRIES - 1:
            time.sleep(1.5 ** attempt)

    print("[JUP] 💀 Quote thất bại sau tất cả retry")
    print("[JUP] 💡 Kiểm tra: ping lite-api.jup.ag | Đổi DNS: 8.8.8.8")
    return None


def _jup_post(body: dict) -> Optional[dict]:
    """
    POST Jupiter swap endpoint với retry.
    Dùng JUP_SWAP_URL (lite-api.jup.ag/swap/v1/swap).
    Priority fee format đúng theo Helius docs:
      prioritizationFeeLamports.priorityLevelWithMaxLamports
    """
    for attempt in range(JUP_RETRIES):
        try:
            r = requests.post(JUP_SWAP_URL, json=body,
                              timeout=JUP_TIMEOUT,
                              headers={"Content-Type": "application/json",
                                       "Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                print(f"[JUP] ❌ Swap error: {data['error']}")
                return None
            return data
        except requests.exceptions.ConnectionError:
            print(f"[JUP] 🔌 Không kết nối được: {JUP_SWAP_URL}")
        except requests.exceptions.Timeout:
            print(f"[JUP] ⏰ Timeout swap (lần {attempt+1}/{JUP_RETRIES})")
        except Exception as e:
            print(f"[JUP] ❌ Swap: {e}")

        if attempt < JUP_RETRIES - 1:
            time.sleep(1.5 ** attempt)

    print("[JUP] 💀 Swap build thất bại sau tất cả retry")
    return None


def _helius_rpc_url() -> str:
    """Trả về Helius RPC URL nếu có key, không thì dùng SOLANA_RPC_URL."""
    if HELIUS_API_KEY:
        return f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    return SOLANA_RPC_URL


def _broadcast_tx(signed_tx_b64: str, sig: str) -> bool:
    """
    Broadcast TX đã ký:
      - HELIUS_USE_SENDER=true  → Helius Sender (Jito, ultra-low latency)
      - HELIUS_USE_SENDER=false → RPC thường (Helius hoặc public)
    Trả về True nếu broadcast thành công.
    """
    if HELIUS_USE_SENDER:
        # Helius Sender: skipPreflight=True bắt buộc, maxRetries=0
        endpoint = HELIUS_SENDER_URL
        label    = "Helius Sender"
    else:
        endpoint = _helius_rpc_url()
        label    = "Helius RPC" if HELIUS_API_KEY else "Solana RPC"

    try:
        resp = requests.post(endpoint, json={
            "jsonrpc": "2.0", "id": 1,
            "method":  "sendTransaction",
            "params":  [signed_tx_b64, {
                "encoding":       "base64",
                "skipPreflight":  HELIUS_USE_SENDER,   # Sender yêu cầu skipPreflight=True
                "maxRetries":     0 if HELIUS_USE_SENDER else 3,
                "preflightCommitment": "confirmed",
            }]
        }, timeout=15)
        result = resp.json()
        if "error" in result:
            print(f"[{label}] ❌ {result['error']}")
            return False
        print(f"[{label}] ✅ Broadcast OK: {sig[:20]}...")
        return True
    except Exception as e:
        print(f"[{label}] ❌ {e}")
        return False


def execute_swap(from_token: str, to_token: str, amount_raw: str,
                 label: str = "SWAP", chain: str = "solana",
                 token_age_min: Optional[float] = None) -> Optional[str]:
    """
    Chain-agnostic swap dispatcher.
    - chain="solana" → Jupiter Lite API v1 (lite-api.jup.ag/swap/v1)
                       Priority fee: priorityLevelWithMaxLamports (đúng format)
                       Broadcast: Helius Sender hoặc RPC tùy HELIUS_USE_SENDER
    - chain="base"   → OKX DEX API + web3.py EVM signing
    """
    if chain == "base":
        return execute_swap_base(from_token, to_token, amount_raw, label, token_age_min)

    # ================================================================
    # SOLANA — Jupiter Lite API v1
    # ================================================================
    dyn_slippage = _get_dynamic_slippage(token_age_min)
    slippage_bps = int(dyn_slippage * 100)
    broadcast_via = "Helius Sender" if HELIUS_USE_SENDER else ("Helius RPC" if HELIUS_API_KEY else "Solana RPC")

    print(f"[JUP] 📐 Slippage: {dyn_slippage}% ({slippage_bps} BPS) | "
          f"Priority: {JUP_PRIORITY_LEVEL} (max {JUP_MAX_PRIORITY_LAMS//1000}k lam) | "
          f"Via: {broadcast_via}")

    # ── Bước 1: Quote ─────────────────────────────────────────────
    quote = _jup_get({
        "inputMint":        from_token,
        "outputMint":       to_token,
        "amount":           amount_raw,
        "slippageBps":      slippage_bps,
        "onlyDirectRoutes": "false",
        "maxAccounts":      "64",
    })
    if not quote:
        return None

    in_ui  = int(quote.get("inAmount",  0)) / (10**SOL_DECIMALS if from_token == SOL_NATIVE else 10**6)
    out_ui = int(quote.get("outAmount", 0))
    routes = quote.get("routePlan", [])
    route_labels = " → ".join(
        r.get("swapInfo", {}).get("label", "?") for r in routes[:3]
    )
    in_sym = "SOL" if from_token == SOL_NATIVE else "USDC"
    print(f"[JUP] 💱 {label}: {in_ui:.6f} {in_sym} → {out_ui} raw | Route: {route_labels}")

    # ── Bước 2: Build swap TX ──────────────────────────────────────
    # priority fee format đúng theo Helius docs (không phải flat số)
    swap_data = _jup_post({
        "quoteResponse":           quote,
        "userPublicKey":           WALLET_ADDRESS,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "maxLamports":  JUP_MAX_PRIORITY_LAMS,
                "priorityLevel": JUP_PRIORITY_LEVEL,
            }
        },
        "wrapAndUnwrapSol": True,
    })
    if not swap_data:
        return None

    tx_b64 = swap_data.get("swapTransaction", "")
    if not tx_b64:
        print("[JUP] ❌ Không nhận được swapTransaction")
        return None

    # ── Bước 3: Ký TX ─────────────────────────────────────────────
    sig = _sign_and_send_tx_jup(tx_b64)
    if not sig:
        return None

    print(f"[JUP] ✅ {label} signed: {sig[:20]}...")
    return sig


def _sign_and_send_tx_jup(tx_b64: str) -> Optional[str]:
    """
    Ký VersionedTransaction từ Jupiter và broadcast.
    Nếu HELIUS_USE_SENDER=true: broadcast qua Helius Sender endpoint.
    Nếu không: dùng _sign_and_send_tx() thông thường (với Helius RPC nếu có key).

    Helius Sender yêu cầu skipPreflight=True và maxRetries=0 — đây là lý do
    cần hàm riêng thay vì dùng _sign_and_send_tx() cũ (vốn set skipPreflight=False).
    """
    try:
        import base58 as _b58

        secret_full = _b58.b58decode(WALLET_PRIVKEY)
        secret_key  = secret_full[:32]

        # Decode TX (Jupiter trả về base64 chuẩn)
        tx_bytes = base64.b64decode(tx_b64)

        # Parse compact-u16 num_signatures
        def _read_compact_u16(buf, pos):
            val, shift = 0, 0
            while True:
                b = buf[pos]; pos += 1
                val |= (b & 0x7F) << shift
                if not (b & 0x80): break
                shift += 7
            return val, pos

        num_sigs, offset = _read_compact_u16(tx_bytes, 0)
        sig_end          = offset + num_sigs * 64
        message_bytes    = tx_bytes[sig_end:]

        # Ký message
        signature = _ed25519_sign(secret_key, message_bytes)

        # Ghép lại: [0x01][64B sig][message]
        signed_bytes = bytes([0x01]) + signature + message_bytes
        signed_b64   = base64.b64encode(signed_bytes).decode()

        import base58 as _b58
        sig_str = _b58.b58encode(signature).decode()

        # Broadcast (Helius Sender hoặc RPC thường)
        ok = _broadcast_tx(signed_b64, sig_str)
        return sig_str if ok else None

    except Exception as e:
        import traceback
        print(f"[Sign] ❌ {e}\n{traceback.format_exc()}")
        return None


def get_token_raw_balance(mint: str, chain: str = "solana") -> str:
    """Lấy raw balance. Tự chọn RPC theo chain."""
    if chain == "base":
        return get_token_raw_balance_base(mint)
    # Solana path
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [WALLET_ADDRESS, {"mint": mint}, {"encoding": "jsonParsed"}]
        }, timeout=10)
        accounts = resp.json().get("result", {}).get("value", [])
        total = sum(
            int(acc.get("account",{}).get("data",{}).get("parsed",{})
                .get("info",{}).get("tokenAmount",{}).get("amount", 0) or 0)
            for acc in accounts
        )
        return str(total)
    except:
        return "0"

def get_token_decimals_rpc(mint: str) -> int:
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}]
        }, timeout=10)
        info = resp.json().get("result",{}).get("value",{})
        return int(info.get("data",{}).get("parsed",{}).get("info",{}).get("decimals", 6))
    except:
        return 6

# ================================================================
# GOPLUS SECURITY
# ================================================================

def check_goplus(addr: str, age_min: Optional[float] = None,
                  chain: str = "solana") -> Tuple[bool, str, dict]:
    """
    GoPlus security check. Trả về (ok, reason, raw_data).
    raw_data chứa đầy đủ: holder info, mint/freeze authority, v.v.
    """
    try:
        if chain == "base":
            addr_q = addr.lower()
            url = f"https://api.gopluslabs.io/api/v1/token_security/8453?contract_addresses={addr_q}"
        else:
            addr_q = addr
            url = f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={addr_q}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        result = r.json().get("result", {})
        res = result.get(addr_q) or result.get(addr) or {}
        if not res:
            return True, "", {}
        bad = [lbl for k, lbl in GOPLUS_CRITICAL.items() if str(res.get(k,"0")) == "1"]
        if bad:
            return False, ", ".join(bad), res
        relax = (age_min is not None and age_min < GOPLUS_RELAX_UNDER)
        if not relax:
            noncrit = [lbl for k, lbl in GOPLUS_NONCRIT.items() if str(res.get(k,"0")) == "1"]
            if noncrit:
                return False, ", ".join(noncrit), res
        return True, "", res
    except Exception as e:
        print(f"[GoPlus] ⚠️ {addr[:12]}: {e} — cho qua")
        return True, "", {}

def check_lp_status(addr: str) -> str:
    try:
        r = requests.get(
            f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={addr}",
            timeout=10
        )
        res = r.json().get("result", {}).get(addr, {})
        if not res:
            return "unknown"
        lock_info = res.get("lockInfo", [])
        lp_holder = res.get("lpHolder", [])
        if lock_info:
            return "locked"
        for h in lp_holder:
            if h.get("is_burned") == "1" or float(h.get("percent","0")) > 0.95:
                return "burned"
        return "unknown"
    except:
        return "unknown"

# ================================================================
# OPPORTUNITY SCORE
# ================================================================

def calculate_score(t: dict) -> Tuple[int, list]:
    """
    Scoring v4 — 5 nhóm tiêu chí (weight cao hơn cho early signals):
      A. Liquidity quality      (max +20)
      B. Momentum  m5/h1        (max +35)  ← tăng cap, thưởng mạnh hơn cho token < 5p
      C. Buy acceleration       (max +20)  ← tăng weight, bắt sớm spike
      D. Holder distribution    (max +20)  ← GoPlus data
      E. Age zone               (max +20)  ← thưởng cao hơn cho token cực mới có momentum
    Total max = 100. Penalty không giới hạn (có thể ra điểm âm → loại).
    """
    score, detail = 0, []
    chain   = t.get("chain", "solana")
    age     = t.get("token_age_minutes")
    liq     = t.get("liquidity_usd", 0)

    # ── A. LIQUIDITY QUALITY ────────────────────────────────────────
    if liq >= 80_000:
        score += 12; detail.append(f"💧 Liq ${liq/1000:.0f}K (deep): +12")
    elif liq >= 30_000:
        score += 20; detail.append(f"💧 Liq ${liq/1000:.0f}K (tốt): +20")
    elif liq >= 10_000:
        score += 14; detail.append(f"💧 Liq ${liq/1000:.0f}K: +14")
    elif liq >= 5_000:
        score += 6;  detail.append(f"💧 Liq ${liq/1000:.1f}K (mỏng): +6")
    # < 3K đã bị hard reject ở _validate_one, không cần penalty ở đây

    # ── B. MOMENTUM — dùng khung thời gian phù hợp tuổi token ─────
    # Token < 30p: dùng m5 + h1 | Token >= 30p: dùng h1 + h6
    vol_m5   = t.get("volume_5m", 0)
    vol_1h   = t.get("volume_1h", 0)
    vol_6h   = t.get("volume_6h", 0)
    buys_m5  = t.get("buys_5m", 0)
    sells_m5 = t.get("sells_5m", 0)
    buys_1h  = t.get("buys_1h", 0)
    sells_1h = t.get("sells_1h", 0)

    is_new = (age is not None and age < 30)

    if is_new:
        # Dùng m5 + h1 cho token mới
        ref_buys  = buys_m5  + buys_1h
        ref_sells = sells_m5 + sells_m5
        ref_vol   = vol_m5 * 12 + vol_1h   # annualize m5 để so sánh
        window_label = "m5+h1"
    else:
        ref_buys  = buys_1h
        ref_sells = sells_1h
        ref_vol   = vol_1h
        window_label = "h1"

    # Vol/Liq ratio
    if liq > 0 and ref_vol > 0:
        vl = ref_vol / liq
        if vl > 25:
            score -= 15; detail.append(f"🚨 Vol/Liq={vl:.0f}x (wash trade?): -15")
        elif vl >= 5:
            # v4: token < 5p FOMO tốt hơn → +35
            bonus = 35 if (age is not None and age < 5) else 30
            score += bonus; detail.append(f"📊 Vol/Liq={vl:.1f}x [{window_label}] (FOMO): +{bonus}")
        elif vl >= 2:
            bonus = 27 if (age is not None and age < 5) else 22
            score += bonus; detail.append(f"📊 Vol/Liq={vl:.1f}x [{window_label}]: +{bonus}")
        elif vl >= 0.8:
            score += 14; detail.append(f"📊 Vol/Liq={vl:.2f}x [{window_label}]: +14")
        elif vl >= 0.3:
            score += 6;  detail.append(f"📊 Vol/Liq={vl:.2f}x [{window_label}]: +6")

    # Buy/Sell pressure
    total_txns = ref_buys + ref_sells
    bp = (ref_buys / total_txns * 100) if total_txns > 0 else 0
    bs = ref_buys / max(ref_sells, 1)

    if ref_sells > ref_buys * 2:
        score -= 15; detail.append(f"🚨 Sells >> Buys [{window_label}] ({ref_sells}/{ref_buys}): -15")
    elif bs >= 5:
        score += 15; detail.append(f"🔥 B/S={bs:.1f}:1 [{window_label}]: +15")
    elif bs >= 3:
        score += 10; detail.append(f"🔥 B/S={bs:.1f}:1 [{window_label}]: +10")
    elif bs >= 1.5:
        score += 5;  detail.append(f"🔄 B/S={bs:.1f}:1 [{window_label}]: +5")
    if bp >= 75:
        score += 5; detail.append(f"💥 {bp:.0f}% là lệnh MUA: +5")

    # ── C. BUY ACCELERATION — vol_m5 tăng nhanh so với h1 average ─
    # v4: weight cao hơn để bắt spike sớm hơn
    if vol_m5 > 0 and vol_1h > 0:
        avg_m5_in_1h = vol_1h / 12   # trung bình mỗi 5 phút trong 1h
        if avg_m5_in_1h > 0:
            accel = vol_m5 / avg_m5_in_1h
            if accel >= 5:
                score += 20; detail.append(f"🚀 Vol 5p = {accel:.1f}x trung bình h1: +20")
            elif accel >= 3:
                score += 14; detail.append(f"📈 Vol 5p = {accel:.1f}x trung bình h1: +14")
            elif accel >= 1.5:
                score += 7;  detail.append(f"📈 Vol 5p = {accel:.1f}x trung bình h1: +7")
            elif accel < 0.3 and vol_1h > 20_000:
                score -= 8; detail.append(f"📉 Vol đang chết ({accel:.2f}x): -8")

    # ── D. HOLDER DISTRIBUTION (từ GoPlus) ─────────────────────────
    top10_pct   = t.get("top10_holder_pct", 0)
    creator_pct = t.get("creator_pct", 0)
    holders     = t.get("holder_count", 0)

    if top10_pct > 0:
        if top10_pct <= 20:
            score += 20; detail.append(f"👥 Top10 chỉ {top10_pct:.0f}% (phân tán tốt): +20")
        elif top10_pct <= 30:
            score += 12; detail.append(f"👥 Top10={top10_pct:.0f}%: +12")
        elif top10_pct <= 40:
            score += 5;  detail.append(f"👥 Top10={top10_pct:.0f}% (chấp nhận): +5")
        else:
            score -= 10; detail.append(f"⚠️  Top10={top10_pct:.0f}% (tập trung cao): -10")
    # > 50% đã bị hard reject

    if creator_pct > 20:
        score -= 10; detail.append(f"🚨 Creator giữ {creator_pct:.0f}%: -10")
    elif creator_pct > 10:
        score -= 5;  detail.append(f"⚠️  Creator giữ {creator_pct:.0f}%: -5")

    if holders >= 500:
        score += 5; detail.append(f"👥 {holders} holders (cộng đồng rộng): +5")
    elif holders >= 100:
        score += 3; detail.append(f"👥 {holders} holders: +3")
    elif 0 < holders < 30:
        score -= 8; detail.append(f"⚠️  Chỉ {holders} holders: -8")

    # ── E. AGE ZONE ─────────────────────────────────────────────────
    # v4: thưởng cao hơn cho token cực mới có momentum — bắt sớm hơn
    if age is not None:
        has_momentum = (ref_vol > 0 and liq > 0 and ref_vol / liq >= 0.5)
        has_buyers   = bp >= 55

        if age < 5:
            if sells_m5 > 0:
                score -= 10; detail.append(f"⚠️  {age:.1f}p + sells ngay ({sells_m5}): -10")
            elif has_momentum and has_buyers:
                # v4: token < 5p có momentum → thưởng mạnh (tín hiệu rất sớm)
                score += 20; detail.append(f"🆕 {age:.1f}p + momentum + buyers: +20")
            elif has_momentum:
                score += 10; detail.append(f"🆕 {age:.1f}p + momentum: +10")
        elif age <= 20:
            if has_momentum and has_buyers:
                score += 15; detail.append(f"🆕 {age:.0f}p + momentum tốt: +15")
            elif has_momentum:
                score += 8;  detail.append(f"⏰ {age:.0f}p + vol OK: +8")
            else:
                score += 2;  detail.append(f"⏰ {age:.0f}p (vol yếu): +2")
        elif age <= 45:
            score += 10; detail.append(f"⏰ {age:.0f}p: +10")
        elif age <= 90:
            score += 5;  detail.append(f"⏰ {age:.0f}p: +5")
        else:
            score -= 5;  detail.append(f"⏳ {age:.0f}p (>90p): -5")

    # ── F. LP STATUS (Solana only) ──────────────────────────────────
    lp = t.get("lp_status", "")
    if chain == "solana":
        if lp == "burned":
            score += 15; detail.append("🔒 LP đốt 100%: +15")
        elif lp == "locked":
            score += 8;  detail.append("🔒 LP khóa: +8")
        else:
            score -= 8;  detail.append("⚠️  LP không rõ: -8")

    # ── G. SOCIAL (validated) ────────────────────────────────────────
    # validate_social_links() đã chạy trong _validate_one trước khi gọi calculate_score
    # Chỉ cộng điểm nếu link được xác nhận hợp lệ
    website = t.get("website", "")
    twitter = t.get("twitter", "")
    w_valid = t.get("website_valid", None)   # None = chưa validate
    t_valid = t.get("twitter_valid", None)

    # Nếu chưa validate (gọi trực tiếp không qua _validate_one) → fallback domain check
    if w_valid is None and website:
        try:
            from urllib.parse import urlparse
            domain = urlparse(website).netloc.lower().lstrip("www.")
            w_valid = bool(domain and domain not in _SPAM_DOMAINS and "." in domain)
        except Exception:
            w_valid = False
    if t_valid is None and twitter:
        t_valid, _ = _validate_twitter(twitter)

    if w_valid:
        score += 5; detail.append(f"🌐 Website verified: +5")
    elif website and w_valid is False:
        score -= 3; detail.append(f"⚠️  Fake/dead website: -3")

    if t_valid:
        score += 5; detail.append(f"🐦 X/Twitter verified: +5")
    elif twitter and t_valid is False:
        score -= 3; detail.append(f"⚠️  Fake X account/search: -3")

    return max(0, min(score, 100)), detail

def _score_bar(score: int) -> Tuple[str, str, str]:
    if score >= SCORE_GREEN:
        return "🟪"*round(score/10)+"⬜"*(10-round(score/10)), "✅ VÀO ĐƯỢC", "🟣"
    elif score >= SCORE_YELLOW:
        return "🟨"*round(score/10)+"⬜"*(10-round(score/10)), "⚠️ CÂN NHẮC", "🟡"
    return "🟥"*round(score/10)+"⬜"*(10-round(score/10)), "❌ RỦI RO", "🔴"

# ================================================================
# DEXSCREENER — 4 nguồn + cache
# ================================================================

PROFILE_SOURCES = [
    "https://api.dexscreener.com/token-profiles/latest/v1",
    "https://api.dexscreener.com/token-boosts/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1",
]
NEW_PAIRS_URLS = [
    "https://api.dexscreener.com/latest/dex/pairs/solana/new",
    "https://api.dexscreener.com/latest/dex/pairs/base/new",
]
GECKO_NEW_POOL_URLS = [
    "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1",
    "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=2",
    "https://api.geckoterminal.com/api/v2/networks/base/new_pools?page=1",
    "https://api.geckoterminal.com/api/v2/networks/base/new_pools?page=2",
]

REQUIRE_SOCIAL_WEB_X = str(os.getenv("REQUIRE_SOCIAL_WEB_X", "1")).strip().lower() in (
    "1", "true", "yes", "on"
)

def _extract_social(pair: dict, kind: str) -> str:
    """Lấy URL social từ DexScreener pair.info block."""
    info = pair.get("info") or {}
    if kind == "website":
        sites = info.get("websites") or []
        return sites[0].get("url", "") if sites else ""
    if kind == "twitter":
        for s in (info.get("socials") or []):
            if (s.get("type") or "").lower() in ("twitter", "x"):
                return s.get("url", "")
    return ""

def _norm_dex(raw: str) -> str:
    d = (raw or "").lower()
    for name in ("raydium","orca","meteora","lifinity","phoenix","pumpfun"):
        if name in d: return name
    return raw or "unknown"

def _quick_score(pair: dict) -> int:
    """Score nhanh từ raw pair object — không cần GoPlus."""
    score = 0
    pcAt = pair.get("pairCreatedAt")
    age  = ((time.time() - pcAt / 1000) / 60) if pcAt else None
    if age is not None:
        if age < 3:           score += 20   # FIX: token cực mới không bị phạt
        elif age <= 15:       score += 30
        elif age <= 30:       score += 18
        elif age <= 60:       score += 8
        else:                 score -= 5
    liq = (pair.get("liquidity") or {}).get("usd", 0) or 0
    if 5_000 <= liq <= 40_000:    score += 20
    elif 40_000 < liq <= 100_000: score += 15
    elif liq > 100_000:           score += 10
    vol1h = (pair.get("volume") or {}).get("h1", 0) or 0
    if liq > 0:
        if vol1h >= liq:          score += 25
        elif vol1h >= liq * 0.5:  score += 18
        elif vol1h >= 20_000:     score += 12
        elif vol1h >= 10_000:     score += 6
    txns  = pair.get("txns", {}) or {}
    buys  = txns.get("h24", {}).get("buys", 0)
    sells = txns.get("h24", {}).get("sells", 1)
    ratio = buys / sells if sells > 0 else 0
    if ratio >= 3.0:   score += 20
    elif ratio >= 2.0: score += 15
    elif ratio >= 1.5: score += 8
    return max(0, min(score, 100))

def _pair_to_token(pair: dict) -> Optional[dict]:
    """
    Chuyển raw pair object → token info dict.
    Tự động nhận dạng chain từ pair.chainId (solana / base).
    Không gọi GoPlus ở đây — để validator làm.
    """
    chain_id = pair.get("chainId", "")
    if chain_id not in SUPPORTED_CHAINS:
        return None

    if chain_id == "base":
        valid_quotes = BASE_VALID_QUOTES
    else:
        valid_quotes = SOLANA_VALID_QUOTES

    quote = (pair.get("quoteToken") or {}).get("symbol", "")
    if quote not in valid_quotes:
        return None
    liq = (pair.get("liquidity") or {}).get("usd", 0) or 0
    if liq < MIN_LIQUIDITY_USD:
        return None
    pcAt    = pair.get("pairCreatedAt")
    age_min = ((time.time() - pcAt / 1000) / 60) if pcAt else None
    if age_min is not None and age_min > MAX_AGE_MIN:
        return None
    addr = (pair.get("baseToken") or {}).get("address", "")
    if not addr:
        return None
    txns  = pair.get("txns") or {}
    buys  = txns.get("h24", {}).get("buys", 0)
    sells = txns.get("h24", {}).get("sells", 0)
    # Dynamic min buys theo tuổi
    # FIX: Base token < 5p: relax min_buys vì data h24 chưa tích lũy đủ
    effective_age = age_min if age_min is not None else 9999
    if chain_id == "base" and effective_age < 5:
        min_buys_required = 1   # chỉ cần có ít nhất 1 giao dịch mua
    else:
        min_buys_required = _get_min_buys(age_min)
    if buys < min_buys_required:
        return None
    vol  = pair.get("volume") or {}
    pc   = pair.get("priceChange") or {}
    website = _extract_social(pair, "website")
    twitter = _extract_social(pair, "twitter")

    # Chỉ scan token có đủ website + X/Twitter thật (nếu bật REQUIRE_SOCIAL_WEB_X)
    if REQUIRE_SOCIAL_WEB_X:
        w_ok, _ = _validate_website(website) if website else (False, "missing")
        t_ok, _ = _validate_twitter(twitter) if twitter else (False, "missing")
        if not (w_ok and t_ok):
            return None

    return {
        "address":          addr,
        "symbol":           (pair.get("baseToken") or {}).get("symbol", ""),
        "name":             (pair.get("baseToken") or {}).get("name", ""),
        "pair_address":     pair.get("pairAddress", ""),
        "price_usd":        float(pair.get("priceUsd", 0) or 0),
        "liquidity_usd":    liq,
        # Volume theo từng khung — dùng m5/h1 cho token mới thay vì h24
        "volume_5m":        float(vol.get("m5", 0) or 0),
        "volume_1h":        float(vol.get("h1", 0) or 0),
        "volume_6h":        float(vol.get("h6", 0) or 0),
        "volume_24h":       float(vol.get("h24", 0) or 0),
        "volume_30s":       float(vol.get("m5", 0) or 0) / 10.0,
        "volume_5m_avg":    float(vol.get("h1", 0) or 0) / 12.0,
        # Txns theo từng khung
        "buys_5m":          txns.get("m5", {}).get("buys", 0),
        "sells_5m":         txns.get("m5", {}).get("sells", 0),
        "buys_1h":          txns.get("h1", {}).get("buys", 0),
        "sells_1h":         txns.get("h1", {}).get("sells", 0),
        "buys_24h":         buys,
        "sells_24h":        sells,
        "price_change_5m":  float(pc.get("m5", 0) or 0),
        "price_change_1h":  float(pc.get("h1", 0) or 0),
        "price_change_6h":  float(pc.get("h6", 0) or 0),
        "price_change_24h": float(pc.get("h24", 0) or 0),
        "fdv":              pair.get("fdv", 0) or 0,
        "market_cap":       pair.get("marketCap", 0) or 0,
        "quote_symbol":     quote,
        "dex_key":          _norm_dex(pair.get("dexId", "")),
        "token_age_minutes": age_min,
        "lp_status":         "unknown",
        "chain":             chain_id,
        "website":           website,
        "twitter":           twitter,
        # GoPlus security data — sẽ được fill bởi _validate_one
        "goplus":            {},
    }

def fetch_profile_addresses() -> List[str]:
    """Nguồn A: Profile/Boost → địa chỉ token. TTL=30s, retry on 429."""
    sol_addrs, base_addrs = [], []
    sol_seen, base_seen   = set(), set()
    for url in PROFILE_SOURCES:
        # TTL dài hơn (30s) — Profile/Boost không thay đổi từng giây
        cached = _dex_cache.get(url, ttl=_CACHE_TTL_PROFILE)
        if cached is not None:
            for item in cached:
                if item.get("chain") == "base" and item["addr"] not in base_seen:
                    base_seen.add(item["addr"]); base_addrs.append(item["addr"])
                elif item.get("chain") == "solana" and item["addr"] not in sol_seen:
                    sol_seen.add(item["addr"]); sol_addrs.append(item["addr"])
            continue
        try:
            r = _dex_get_with_retry(url, label=url.split("/")[-1])
            if r is None:
                continue
            items = r.json() if isinstance(r.json(), list) else []
            batch = []
            for item in items:
                cid  = item.get("chainId")
                addr = item.get("tokenAddress") or item.get("address", "")
                if not addr or cid not in SUPPORTED_CHAINS:
                    continue
                batch.append({"addr": addr, "chain": cid})
                if cid == "base" and addr not in base_seen:
                    base_seen.add(addr); base_addrs.append(addr)
                elif cid == "solana" and addr not in sol_seen:
                    sol_seen.add(addr); sol_addrs.append(addr)
            _dex_cache.set(url, batch)
        except Exception as e:
            print(f"[Scan-A] ⚠️  {url.split('/')[-1]}: {e}")
    return sol_addrs[:SOL_MAX_PROFILE_ADDRS] + base_addrs[:BASE_MAX_PROFILE_ADDRS]

def fetch_token_pairs(addr: str) -> List[dict]:
    """Lấy pairs của 1 token address. Cache 8s, retry on 429."""
    key = f"pairs:{addr}"
    cached = _dex_cache.get(key, ttl=_CACHE_TTL_PAIRS)
    if cached is not None:
        return cached
    try:
        r = _dex_get_with_retry(
            f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
            label=addr[:12])
        if r is None:
            return []
        data   = r.json()
        ps     = data.get("pairs") if isinstance(data, dict) else None
        result = ps if isinstance(ps, list) else []
        _dex_cache.set(key, result)
        return result
    except Exception as e:
        print(f"[Scan-A] ⚠️  fetch_pairs {addr[:12]}: {e}")
        return []

def fetch_new_pairs() -> List[dict]:
    """Nguồn B: /pairs/{chain}/new. Cache riêng 10s/URL, retry on 429."""
    result = []
    for url in NEW_PAIRS_URLS:
        cache_key   = f"new_pairs:{url.split('/')[-2]}"
        chain_label = url.split("/")[-2].upper()
        cached = _dex_cache.get(cache_key, ttl=_CACHE_TTL_NEWPAIRS)
        if cached is not None:
            result.extend(cached)
            continue
        try:
            r = _dex_get_with_retry(url, label=chain_label)
            if r is None:
                continue
            data = r.json()
            if isinstance(data, list):
                pairs = data
            elif isinstance(data, dict):
                pairs = data.get("pairs") or data.get("data") or []
            else:
                pairs = []
            _dex_cache.set(cache_key, pairs)
            result.extend(pairs)
            print(f"  [Scan-B] {chain_label}: {len(pairs)} pairs từ /new")
        except Exception as e:
            print(f"[Scan-B] ⚠️  {chain_label}: {e}")
    return result

def _gecko_token_id_to_address(raw_id: str) -> str:
    """Chuẩn hoá token id/address từ GeckoTerminal về địa chỉ thuần để query DexScreener."""
    rid = str(raw_id or "").strip()
    if not rid:
        return ""

    # Format thường gặp: "solana_<mint>" hoặc "base_0x..."
    if "_" in rid:
        _, tail = rid.split("_", 1)
        rid = tail.strip()

    # Gecko đôi lúc trả id dưới dạng path-like (vd: /tokens/<addr>)
    if "/" in rid:
        rid = rid.rsplit("/", 1)[-1].strip()

    return rid


def fetch_gecko_new_pool_addresses() -> List[str]:
    """
    Nguồn C: GeckoTerminal new_pools → token addresses.
    Dùng để mở rộng discovery ngoài DexScreener profile/boost/new.
    """
    addrs: List[str] = []
    seen: set = set()
    for url in GECKO_NEW_POOL_URLS:
        cache_key = f"gecko_new:{url.split('/networks/')[-1]}"
        cached = _dex_cache.get(cache_key, ttl=20)
        if cached is not None:
            for addr in cached:
                if addr not in seen:
                    seen.add(addr)
                    addrs.append(addr)
            continue
        try:
            r = requests.get(
                url,
                timeout=10,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
            )
            if r.status_code != 200:
                print(f"[Scan-C] ⚠️  gecko status={r.status_code}: {url}")
                continue

            payload = r.json() if isinstance(r.json(), dict) else {}
            items = payload.get("data") or []
            included = payload.get("included") or []

            # Map token id -> token address từ included (json:api)
            included_token_addr: Dict[str, str] = {}
            for inc in included:
                inc_id = str(inc.get("id") or "")
                inc_addr = _gecko_token_id_to_address((inc.get("attributes") or {}).get("address") or inc_id)
                if inc_id and inc_addr:
                    included_token_addr[inc_id] = inc_addr

            batch: List[str] = []
            for item in items:
                attrs = item.get("attributes") or {}
                rels = item.get("relationships") or {}

                # Ưu tiên field address trực tiếp nếu API có
                base_addr = _gecko_token_id_to_address(
                    attrs.get("base_token_address")
                    or attrs.get("token_address")
                    or attrs.get("address")
                    or ""
                )

                # Fallback 1: lấy từ relationships.base_token.data.id
                base_token_id = ((rels.get("base_token") or {}).get("data") or {}).get("id") or ""
                if not base_addr:
                    base_addr = _gecko_token_id_to_address(base_token_id)

                # Fallback 2: map token id -> included.attributes.address
                if not base_addr and base_token_id:
                    base_addr = included_token_addr.get(base_token_id, "")

                if not base_addr:
                    continue

                batch.append(base_addr)
                if base_addr not in seen:
                    seen.add(base_addr)
                    addrs.append(base_addr)

            _dex_cache.set(cache_key, batch)
            print(f"  [Scan-C] {url.split('/networks/')[-1]}: +{len(batch)} token")
        except Exception as e:
            print(f"[Scan-C] ⚠️  gecko new_pools: {e}")
    return addrs

def get_price_usd(addr: str, chain: str = "solana") -> float:
    """Lấy giá USD từ DexScreener. chain-aware: lọc đúng chainId."""
    cached = _dex_cache.get(f"price:{addr}:{chain}")
    if cached is not None:
        return cached
    try:
        r    = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{addr}", timeout=10)
        data = r.json()
        ps   = data.get("pairs", []) if isinstance(data, dict) else []
        # FIX: lọc theo chain của token, không cứng "solana"
        sp   = [p for p in ps if p.get("chainId") == chain]
        p    = sp[0] if sp else (ps[0] if ps else {})
        price = float(p.get("priceUsd", 0) or 0)
        _dex_cache.set(f"price:{addr}:{chain}", price)
        return price
    except:
        return 0.0


_sol_price_cache: Dict[str, float] = {}   # {"ts": timestamp, "price": float}
_sol_price_lock  = threading.Lock()

def get_sol_price_usd(max_age_s: float = 30.0) -> float:
    """
    Lấy giá SOL/USD hiện tại — dùng để quy đổi SOL spent → USD.

    Sources (theo thứ tự ưu tiên):
      1. Cache nội bộ (TTL 30s) — tránh gọi API liên tục
      2. DexScreener: giá SOL/USDC pair trên Solana
      3. Jupiter price API (fallback)
      4. Hardcoded fallback 0.0 nếu tất cả fail (caller tự xử lý)
    """
    with _sol_price_lock:
        cached = _sol_price_cache.get("price", 0.0)
        cached_ts = _sol_price_cache.get("ts", 0.0)
        if cached > 0 and (time.time() - cached_ts) < max_age_s:
            return cached

    price = 0.0

    # Source 1: DexScreener — SOL/USDC pair (Raydium)
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{SOL_NATIVE}",
            timeout=8, headers={"User-Agent": "Mozilla/5.0"}
        )
        pairs = r.json().get("pairs", []) if isinstance(r.json(), dict) else []
        # Lọc pair SOL/USDC trên Solana có liquidity cao nhất
        sol_pairs = [
            p for p in pairs
            if p.get("chainId") == "solana"
            and (p.get("quoteToken") or {}).get("symbol", "").upper() in ("USDC", "USDT")
        ]
        if sol_pairs:
            # Chọn pair có liquidity cao nhất
            best = max(sol_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))
            price = float(best.get("priceUsd", 0) or 0)
    except Exception as e:
        print(f"[SOLPrice] ⚠️  DexScreener: {e}")

    # Source 2: Jupiter price API (fallback)
    if price <= 0:
        try:
            r = requests.get(
                f"https://lite-api.jup.ag/price/v2?ids={SOL_NATIVE}",
                timeout=8
            )
            data  = r.json().get("data", {})
            entry = data.get(SOL_NATIVE) or data.get(SOL_NATIVE.lower()) or {}
            price = float(entry.get("price", 0) or 0)
        except Exception as e:
            print(f"[SOLPrice] ⚠️  Jupiter: {e}")

    if price > 0:
        with _sol_price_lock:
            _sol_price_cache["price"] = price
            _sol_price_cache["ts"]    = time.time()
        print(f"[SOLPrice] 💲 SOL = ${price:.2f}")

    return price
# ================================================================
# TELEGRAM SIGNALS
# ================================================================

def _build_buy_links(token: dict) -> str:
    """
    Tạo deeplink mua token cho 5 ví/DEX — chain-aware (Solana / Base).

    Solana:
      🔶 OKX Web3  — deeplink chính thức mở swap trong OKX Wallet
      🟣 Jupiter   — Jupiter App (DEX aggregator số 1 Solana)
      🟢 Bitget    — Bitget Wallet DApp browser (ref: XnpXRj)
      🟡 Binance   — Binance Web3 Wallet universal link
      📊 DexScreener — chart + swap trực tiếp

    Base:
      🔶 OKX Web3  — deeplink OKX cho Base chain
      🔵 Aerodrome — DEX native Base, volume lớn nhất
      🟢 Bitget    — Bitget Wallet (ref: XnpXRj)
      🟡 Binance   — Binance Web3 Wallet (Base → map ETH)
      📊 DexScreener — chart + swap trực tiếp
    """
    addr      = token["address"]
    chain     = token.get("chain", "solana")
    pair_addr = token.get("pair_address", addr)
    _up       = urllib.parse

    if chain == "solana":
        from_token = SOL_USDC
        chain_id   = "501"

        # OKX Web3 Wallet deeplink chính thức
        dex_url_okx = (
            f"https://web3.okx.com/dex-swap"
            f"#inputChain={chain_id}&inputCurrency={from_token}"
            f"&outputChain={chain_id}&outputCurrency={addr}"
        )
        deep_okx = f"okx://wallet/dapp/url?dappUrl={_up.quote(dex_url_okx, safe='')}"
        okx_link = f"https://web3.okx.com/download?deeplink={_up.quote(deep_okx, safe='')}"

        # Jupiter App
        jup_link = f"https://jup.ag/swap/{from_token}-{addr}"

        # Bitget Wallet — DApp browser (ref: XnpXRj)
        uni_inner = f"https://jup.ag/swap/{from_token}-{addr}"
        bitget_link = (
            f"https://bkcode.vip?action=dapp"
            f"&url={_up.quote(uni_inner, safe='')}"
            f"&referralCode=XnpXRj"
        )

        # Binance Web3 Wallet universal link
        binance_link = (
            f"https://app.binance.com/uni-qr/wwswap"
            f"?fromNetwork=SOL"
            f"&fromTokenAddress={_up.quote(from_token, safe='')}"
            f"&toNetwork=SOL"
            f"&toTokenAddress={_up.quote(addr, safe='')}"
            f"&ref=FY4S1TGI"
        )

        # DexScreener chart/swap
        dex_link = f"https://dexscreener.com/solana/{pair_addr}"

        return (
            f"🛒 <b>Mua ngay:</b>\n"
            f"  ├ <a href='{okx_link}'>🔶 OKX Web3 Wallet</a>\n"
            f"  ├ <a href='{jup_link}'>🟣 Jupiter</a>\n"
            f"  ├ <a href='{bitget_link}'>🟢 Bitget Wallet</a>\n"
            f"  ├ <a href='{binance_link}'>🟡 Binance Web3</a>\n"
            f"  └ <a href='{dex_link}'>📊 DexScreener</a>"
        )

    else:
        # Base chain
        from_token = BASE_USDC
        chain_id   = "8453"

        # OKX Web3 Wallet deeplink Base
        dex_url_okx = (
            f"https://web3.okx.com/dex-swap"
            f"#inputChain={chain_id}&inputCurrency={from_token}"
            f"&outputChain={chain_id}&outputCurrency={addr}"
        )
        deep_okx = f"okx://wallet/dapp/url?dappUrl={_up.quote(dex_url_okx, safe='')}"
        okx_link = f"https://web3.okx.com/download?deeplink={_up.quote(deep_okx, safe='')}"

        # Aerodrome — DEX native của Base
        aero_link = f"https://aerodrome.finance/swap?from={from_token}&to={addr}"

        # Bitget Wallet — mở Aerodrome trong DApp browser (ref: XnpXRj)
        uni_inner_base = f"https://aerodrome.finance/swap?from={from_token}&to={addr}"
        bitget_link = (
            f"https://bkcode.vip?action=dapp"
            f"&url={_up.quote(uni_inner_base, safe='')}"
            f"&referralCode=XnpXRj"
        )

        # Binance Web3 Wallet (Base chưa hỗ trợ riêng → dùng ETH network)
        binance_link = (
            f"https://app.binance.com/uni-qr/wwswap"
            f"?fromNetwork=ETH"
            f"&fromTokenAddress={_up.quote(from_token, safe='')}"
            f"&toNetwork=ETH"
            f"&toTokenAddress={_up.quote(addr, safe='')}"
            f"&ref=FY4S1TGI"
        )

        # DexScreener
        dex_link = f"https://dexscreener.com/base/{pair_addr}"

        return (
            f"🛒 <b>Mua ngay:</b>\n"
            f"  ├ <a href='{okx_link}'>🔶 OKX Web3 Wallet</a>\n"
            f"  ├ <a href='{aero_link}'>🔵 Aerodrome (Base)</a>\n"
            f"  ├ <a href='{bitget_link}'>🟢 Bitget Wallet</a>\n"
            f"  ├ <a href='{binance_link}'>🟡 Binance Web3</a>\n"
            f"  └ <a href='{dex_link}'>📊 DexScreener</a>"
        )


def send_early_alert(token: dict):
    """
    Cảnh báo sớm: token < EARLY_ALERT_MAX_AGE_MIN phút vừa xuất hiện.
    Gửi 1 lần/token/session. Không yêu cầu đủ MIN_SCORE.
    """
    addr = token["address"]
    with _early_alert_lock:
        if addr in _early_alert_sent:
            return
        _early_alert_sent.add(addr)

    chain       = token.get("chain", "solana")
    sym         = token.get("symbol", "?")
    name        = token.get("name", sym)
    age         = token.get("token_age_minutes")
    price       = token.get("price_usd", 0)
    liq         = token.get("liquidity_usd", 0)
    vol1h       = token.get("volume_1h", 0)
    buys        = token.get("buys_24h", 0)
    sells       = token.get("sells_24h", 0)
    pair_addr   = token.get("pair_address", "")
    chain_emoji = "🟣" if chain == "solana" else "🔵"
    chain_name  = "SOLANA" if chain == "solana" else "BASE"

    # Định dạng tuổi
    if age is not None:
        age_secs = int(age * 60)
        age_str  = f"{age_secs}s" if age_secs < 60 else f"{age_secs//60}p{age_secs%60:02d}s"
    else:
        age_str = "< 2p"

    buy_ratio = buys / max(buys + sells, 1) * 100
    if buy_ratio >= 65:
        press_icon, press_text = "🔥", _t("STRONG BUY", "MUA MẠNH")
    elif buy_ratio >= 50:
        press_icon, press_text = "📈", "Bullish"
    else:
        press_icon, press_text = "⚠️", _t("Weak", "Yếu")

    scan_url  = (f"https://basescan.org/token/{addr}" if chain == "base"
                 else f"https://solscan.io/token/{addr}")
    chart_url = f"https://dexscreener.com/{chain}/{pair_addr}"
    buy_links = _build_buy_links(token)

    msg = (
        f"{chain_emoji}⚡ <b>{_t('NEW TOKEN LAUNCH','TOKEN MỚI LAUNCH')}! [{chain_name}]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{name}</b>  (<code>${sym}</code>)\n"
        f"⏱ {_t('Age','Tuổi')}: <b>{age_str}</b>  🆕 {_t('JUST LAUNCHED','VỪA RA MẮT')}\n\n"
        f"📋 <b>{_t('Contract','Contract')}:</b>\n"
        f"<code>{addr}</code>\n"
        f"<i>👆 {_t('Tap to copy','Nhấn để copy')}</i>\n\n"
        f"💰 {_t('Price','Giá')}: <code>${price:.10f}</code>\n"
        f"💧 {_t('Liq','Liq')}: <b>{_fmt_usd(liq)}</b>  |  📊 {_t('Vol 1h','Vol 1h')}: <b>{_fmt_usd(vol1h)}</b>\n"
        f"{press_icon} {_t('Buys','Mua')}: <b>{buys}</b> | {_t('Sells','Bán')}: <b>{sells}</b>  "
        f"— {press_text} ({buy_ratio:.0f}% {_t('buys','mua')})\n\n"
        f"{buy_links}\n\n"
        f"<a href='{chart_url}'>📉 {_t('Chart','Chart')}</a>  |  "
        f"<a href='{scan_url}'>🔍 Explorer</a>\n"
        f"<i>⚠️ {_t('Bot analyzing — no auto-buy yet','Bot đang phân tích — chưa tự động mua')}</i>\n"
        f"#EarlyAlert #{chain_name} ${sym}"
    )
    _send_tg(msg)
    print(f"[Scanner] ⚡ Early alert gửi: {sym} [{chain}] tuổi={age_str}")


def send_signal_alert(token: dict, score: int, detail: list):
    """
    Signal alert cho bạn bè: token đạt ≥ SIGNAL_ALERT_MIN_SCORE (mặc định 70).
    Gửi đến TELEGRAM_SIGNAL_CHANNELS — tách biệt với chat bot.
    Gửi 1 lần / token / session.

    Hiển thị:
      • Điểm + thanh tiến độ màu
      • Phân tích ngắn (top 4 tiêu chí)
      • Toàn bộ deeplink mua (OKX, Jupiter/Aerodrome, Bitget, Binance, DexScreener)
      • Nhãn chiến lược: SNIPE 🎯 hay RECOVERY 🔄
    """
    addr = token["address"]
    with _signal_alert_lock:
        if addr in _signal_alert_sent:
            return

    chain      = token.get("chain", "solana")
    sym        = token.get("symbol", "?")
    name       = token.get("name", sym)
    age        = token.get("token_age_minutes")
    price      = token.get("price_usd", 0)
    liq        = token.get("liquidity_usd", 0)
    vol_1h     = token.get("volume_1h", 0)
    vol_24h    = token.get("volume_24h", 0)
    pc_h1      = token.get("price_change_1h", 0)
    buys_1h    = token.get("buys_1h", 0)
    sells_1h   = token.get("sells_1h", 0)
    buys_24h   = token.get("buys_24h", 0)
    sells_24h  = token.get("sells_24h", 0)
    holders    = token.get("holder_count", 0)
    top10      = token.get("top10_holder_pct", 0)
    lp_status  = token.get("lp_status", "unknown")
    pair_addr  = token.get("pair_address", "")
    website    = token.get("website", "")
    twitter    = token.get("twitter", "")

    chain_emoji = "🟣" if chain == "solana" else "🔵"
    chain_name  = "SOLANA" if chain == "solana" else "BASE"

    # Strategy
    is_recovery  = (age is not None and age >= 60)
    strategy_tag = f"🔄 {_t('RECOVERY','RECOVERY')}" if is_recovery else f"🎯 {_t('SNIPE','SNIPE')}"

    # Score bar
    filled  = round(score / 10)
    bar     = "🟩" * filled + "⬜" * (10 - filled)
    if score >= 85:
        score_label, score_color = f"🔥 {_t('VERY HIGH','RẤT CAO')}", "🟢"
    elif score >= 70:
        score_label, score_color = f"✅ {_t('GOOD','TỐT')}", "🟡"
    else:
        score_label, score_color = f"⚠️ {_t('AVERAGE','TRUNG BÌNH')}", "🟠"

    # Age
    age_str = _age_str(age) if age is not None else "N/A"
    if age is not None and age < 10:
        age_badge = f"🆕 {age_str} — {_t('JUST LAUNCHED','MỚI LAUNCH')}"
    elif age is not None and age < 60:
        age_badge = f"⏱ {age_str}"
    else:
        age_badge = f"⏰ {age_str} — {_t('Recovery wave','Bắt sóng hồi')}"

    # Buy pressure
    ref_b = buys_1h or buys_24h
    ref_s = sells_1h or sells_24h
    total = ref_b + ref_s
    bp    = (ref_b / total * 100) if total > 0 else 0
    bs    = ref_b / max(ref_s, 1)
    if bp >= 70:
        press_str = f"🔥 {_t('STRONG BUY','MUA MẠNH')} ({bp:.0f}%)"
    elif bp >= 55:
        press_str = f"📈 Bullish ({bp:.0f}%)"
    else:
        press_str = f"⚠️ {_t('Weak','Yếu')} ({bp:.0f}% {_t('buys','mua')})"

    # LP status
    lp_map  = {"burned": f"🔥 {_t('LP 100% burned','LP đốt 100%')}",
               "locked": f"🔒 {_t('LP locked','LP khóa')}",
               "n/a": "—",
               "unknown": f"❓ {_t('LP unknown','LP chưa rõ')}"}
    lp_str  = lp_map.get(lp_status, "❓")

    # Social (with validation status)
    social_parts = []
    if website:
        w_ok  = token.get("website_valid", None)
        w_icon = "✅" if w_ok else ("❌" if w_ok is False else "🔗")
        social_parts.append(f"<a href='{website}'>{w_icon} {_t('Web','Web')}</a>")
    if twitter:
        t_ok  = token.get("twitter_valid", None)
        t_icon = "✅" if t_ok else ("❌" if t_ok is False else "🔗")
        social_parts.append(f"<a href='{twitter}'>{t_icon} Twitter</a>")
    social_str = "  ".join(social_parts) if social_parts else f"❌ {_t('None','Chưa có')}"

    # Analysis (top 4 lines)
    top_detail = detail[:4] if detail else []
    analysis   = "\n".join(f"  • {d}" for d in top_detail)
    if len(detail) > 4:
        analysis += f"\n  <i>... +{len(detail)-4} {_t('more signals','tín hiệu nữa')}</i>"

    # Volume spike (Recovery)
    vol_spike_str = ""
    if is_recovery and vol_24h > 0:
        ratio = vol_1h / vol_24h * 100
        vol_spike_str = f"  🔥 Vol h1/h24: <b>{ratio:.0f}%</b> "
        if ratio >= 30:
            vol_spike_str += f"({_t('SPIKE','ĐỘT BIẾN')} ✅)"
    price_h1_str = ""
    if pc_h1 != 0:
        price_h1_str = f"  📈 {_t('Price h1','Price h1')}: <b>{pc_h1:+.1f}%</b>\n"

    scan_url  = (f"https://basescan.org/token/{addr}" if chain == "base"
                 else f"https://solscan.io/token/{addr}")
    chart_url = f"https://dexscreener.com/{chain}/{pair_addr}"
    buy_links = _build_buy_links(token)

    msg = (
        f"{chain_emoji}🚨 <b>SIGNAL [{chain_name}] — {strategy_tag}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{name}</b>  (<code>${sym}</code>)\n"
        f"{age_badge}\n\n"

        f"🎯 <b>{_t('SCORE','ĐIỂM')}: {score}/100</b>  {score_label}\n"
        f"{bar}\n\n"

        f"📋 <b>Contract:</b>\n"
        f"<code>{addr}</code>\n"
        f"<i>👆 {_t('Tap to copy','Nhấn để copy')}</i>\n\n"

        f"💰 {_t('Price','Giá')}: <code>${price:.10f}</code>\n"
        f"💧 {_t('Liq','Liq')}: <b>{_fmt_usd(liq)}</b>  |  📊 {_t('Vol 1h','Vol 1h')}: <b>{_fmt_usd(vol_1h)}</b>\n"
        + (f"  {vol_spike_str}\n" if vol_spike_str else "")
        + price_h1_str
        + f"🔄 B/S: <b>{bs:.1f}:1</b>  —  {press_str}\n"
        f"👥 {_t('Holders','Holders')}: <b>{holders:,}</b>  |  Top10: <b>{top10:.0f}%</b>  |  {lp_str}\n"
        f"🌍 {social_str}\n\n"

        f"📊 <b>{_t('Bot analysis','Phân tích bot')}:</b>\n{analysis}\n\n"

        f"{buy_links}\n\n"

        f"<a href='{chart_url}'>📉 Chart</a>  |  "
        f"<a href='{scan_url}'>🔍 Explorer</a>\n"
        f"<i>⚠️ {_t('Not financial advice. DYOR!','Không phải lời khuyên đầu tư. DYOR!')}</i>\n"
        f"#Signal #{chain_name} ${sym} {strategy_tag.split()[1]}"
    )

    # Gửi đến signal channels (cho bạn bè)
    sent_ok = False
    if TELEGRAM_SIGNAL_CHANNELS:
        for cid in TELEGRAM_SIGNAL_CHANNELS:
            sent_ok = _send_tg(msg, chat_id=cid) or sent_ok
    else:
        # Fallback: gửi vào chat chính nếu không có channel riêng
        sent_ok = _send_tg(msg, chat_id=TELEGRAM_CHAT_ID)

    if sent_ok:
        with _signal_alert_lock:
            _signal_alert_sent.add(addr)
        print(f"[Signal] 🚨 Signal gửi: {sym} [{chain}] score={score} ({strategy_tag})")
    else:
        print(f"[Signal] ⚠️  Không gửi được signal: {sym} [{chain}] score={score}")


def send_buy_signal(token: dict, score: int, detail: list, buy_sig: str):
    """Thông báo bot đã TỰ ĐỘNG MUA — gửi đến TELEGRAM_CHAT_ID (chat riêng của bot)."""
    bar, label, border = _score_bar(score)
    buys_24h  = token.get("buys_24h", 0)
    sells_24h = token.get("sells_24h", 0)
    bp        = buys_24h / max(buys_24h + sells_24h, 1) * 100
    press     = (f"🔥 {_t('STRONG BUY','MUA MẠNH')}" if bp >= 65
                 else (f"📈 Bullish" if bp >= 50 else f"⚠️ {_t('Bearish','Bearish')}"))
    age       = token.get("token_age_minutes")
    age_badge = (f"🆕 {_age_str(age)} — {_t('JUST LAUNCHED','MỚI LAUNCH')}" if age and age <= 10
                 else f"⏰ {_age_str(age)}" if age else "N/A")
    lp_map    = {"burned": f"🔥 {_t('LP 100% burned','LP đốt 100%')}",
                 "locked": f"🔒 {_t('LP locked','LP khóa')}",
                 "n/a": "—",
                 "unknown": f"❓ {_t('Unknown','Không rõ')}"}
    chain       = token.get("chain", "solana")
    chain_emoji = "🟣" if chain == "solana" else "🔵"
    chain_name  = "SOLANA" if chain == "solana" else "BASE"
    spend_label = "SOL" if chain == "solana" else "USDC"
    spend_amount = BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC

    est_price    = token.get("price_usd", 0)
    actual_price = token.get("actual_entry_price", 0)
    slip_actual  = token.get("slippage_actual", 0)
    token_amount = token.get("token_amount", 0)

    if actual_price > 0:
        price_block = (
            f"  💰 {_t('Est. price','Giá ước tính')}:  <code>${est_price:.10f}</code>\n"
            f"  ✅ {_t('Actual price','Giá thực tế')}:   <code>${actual_price:.10f}</code>\n"
            f"  📉 Slippage:      <b>{slip_actual:+.2f}%</b>\n"
            f"  🪙 {_t('Tokens received','Token nhận')}:    <b>{token_amount:.6f}</b>\n"
        )
        tp_price = actual_price * (1 + TAKE_PROFIT_PCT / 100)
        tp_line  = f"🎯 {_t('Take profit','Chốt lời')}: <code>${tp_price:.10f}</code> (+{TAKE_PROFIT_PCT:.0f}% {_t('actual','thực')})"
    else:
        price_block = (
            f"  💰 {_t('Price (est.)','Giá (ước tính)')}: <code>${est_price:.10f}</code>\n"
            f"  ⏳ {_t('Confirming actual price...','Đang xác nhận giá thực...')}\n"
        )
        tp_line = f"🎯 {_t('Take profit','Chốt lời')}: +{TAKE_PROFIT_PCT:.0f}% ({_t('from confirmed price','từ giá thực sau confirm')})"

    explorer  = "https://basescan.org/tx/" if chain == "base" else "https://solscan.io/tx/"
    chart_url = f"https://dexscreener.com/{chain}/{token['pair_address']}"
    scan_url  = (f"https://basescan.org/token/{token['address']}" if chain == "base"
                 else f"https://solscan.io/token/{token['address']}")
    buy_links = _build_buy_links(token)

    msg = (
        f"{chain_emoji}🤖 <b>AUTO BUY [{chain_name}]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>{_t('SCORE','ĐIỂM')}: {score}/100</b>  {label}\n"
        f"{bar}\n\n"
        f"🪙 <b>{token.get('name','?')}</b>  (<code>${token['symbol']}</code>)\n"
        f"{age_badge}  |  {lp_map.get(token.get('lp_status','unknown'),'❓')}\n\n"
        f"📋 <b>Contract:</b>\n"
        f"<code>{token['address']}</code>\n"
        f"<i>👆 {_t('Tap to copy','Nhấn để copy')}</i>\n\n"
        f"{price_block}"
        f"  💵 {_t('Spent','Đã chi')}: <b>{spend_amount} {spend_label}</b>\n"
        f"  {tp_line}\n\n"
        f"💧 {_t('Liq','Liq')}: <b>{_fmt_usd(token['liquidity_usd'])}</b>  |  "
        f"📊 {_t('Vol 1h','Vol 1h')}: <b>{_fmt_usd(token['volume_1h'])}</b>\n"
        f"🔄 {_t('Buys','Mua')}: <b>{buys_24h}</b> | {_t('Sells','Bán')}: <b>{sells_24h}</b>  — {press} ({bp:.0f}% {_t('buys','mua')})\n"
        f"🏊 {token['quote_symbol']} — {token['dex_key'].upper()}\n\n"
        f"{buy_links}\n\n"
        f"<a href='{explorer}{buy_sig}'>✅ {_t('Buy TX','TX mua')}</a>  |  "
        f"<a href='{chart_url}'>📉 Chart</a>  |  "
        f"<a href='{scan_url}'>🔍 Explorer</a>\n"
        f"#AutoBuy #{chain_name} ${token['symbol']}"
    )
    _alert(msg)   # gửi vào TELEGRAM_CHAT_ID (bot owner)

def _get_sol_received_solana(sig: str) -> float:
    """
    Solana sell TX → SOL received = increase in wallet SOL balance (minus fee).
    post_balance[0] - pre_balance[0] + fee = SOL received (if positive).
    """
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }]
        }, timeout=15)
        tx = resp.json().get("result")
        if not tx:
            return 0.0
        meta = tx.get("meta", {})
        fee  = meta.get("fee", 0)
        pre  = meta.get("preBalances",  [])
        post = meta.get("postBalances", [])
        if not pre or not post:
            return 0.0
        # Account 0 = wallet: net SOL in = (post - pre) + fee (fee was taken from pre)
        delta_lam = post[0] - pre[0] + fee
        if delta_lam > 0:
            return delta_lam / 1e9
        return 0.0
    except Exception as e:
        print(f"[TP] ⚠️  _get_sol_received_solana: {e}")
        return 0.0


def _get_usdc_received_from_sell_tx(sig: str, chain: str, label: str = "") -> float:
    """
    Returns amount received from sell TX.
    Solana → SOL received. Base → USDC received.
    """
    try:
        if chain == "base":
            return _get_usdc_received_base(sig)
        else:
            return _get_sol_received_solana(sig)
    except Exception as e:
        print(f"[TP] ⚠️  {label} parse received: {e}")
        return 0.0


def _get_usdc_received_solana(sig: str) -> float:
    """
    Solana: delta USDC tăng lên trong ví = USDC nhận được từ bán.
    post_amount - pre_amount > 0 → ví nhận vào.
    """
    try:
        resp = requests.post(SOLANA_RPC_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }]
        }, timeout=15)
        tx = resp.json().get("result")
        if not tx:
            return 0.0

        meta = tx.get("meta", {})
        pre  = {b["accountIndex"]: b for b in (meta.get("preTokenBalances")  or [])}
        post = {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}

        for idx, pb in pre.items():
            mint  = pb.get("mint", "")
            if mint != SOL_USDC:
                continue
            owner = pb.get("owner", "")
            if owner != WALLET_ADDRESS:
                continue
            pre_amt  = float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
            post_amt = float(post.get(idx, {}).get("uiTokenAmount", {})
                             .get("uiAmount", 0) or 0)
            delta = post_amt - pre_amt   # dương = nhận vào
            if delta > 0:
                return delta
        return 0.0
    except Exception as e:
        print(f"[TP] ⚠️  _get_usdc_received_solana: {e}")
        return 0.0


def _get_usdc_received_base(sig: str) -> float:
    """
    Base: decode Transfer log USDC contract.
    Transfer(from=router, to=wallet, amount) → amount = USDC nhận về.
    """
    try:
        from web3 import Web3
        w3   = _get_base_w3()
        rcpt = w3.eth.get_transaction_receipt(sig)
        if not rcpt:
            return 0.0

        wallet = _get_base_wallet_address().lower()
        usdc   = BASE_USDC.lower()
        TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        for log in rcpt.get("logs", []):
            if log.get("address", "").lower() != usdc:
                continue
            topics = log.get("topics", [])
            if not topics or topics[0].lower() != TRANSFER_TOPIC:
                continue
            if len(topics) < 3:
                continue
            # topics[2] = to address (padded 32 bytes)
            to_addr = "0x" + topics[2][-40:]
            if to_addr.lower() != wallet:
                continue
            amount_raw = int(log.get("data", "0x0"), 16)
            return amount_raw / (10 ** 6)
        return 0.0
    except Exception as e:
        print(f"[TP] ⚠️  _get_usdc_received_base: {e}")
        return 0.0


def send_tp_signal(pos: dict, cur_price: float, pct: float, sell_sig: str):
    """
    Gửi thông báo TAKE PROFIT với lãi thực tế sau trượt giá + phí.

    Luồng tính toán:
      usdc_spent   = USDC thực tế ĐÃ CHI khi mua (từ TX mua, đã lưu DB)
      usdc_received = USDC thực tế NHẬN VỀ sau khi bán (đọc từ TX bán)
      net_profit   = usdc_received - usdc_spent
      net_pct      = net_profit / usdc_spent * 100

    Nếu không đọc được TX bán → fallback ước tính từ giá và slippage.
    """
    chain       = pos.get("chain", "solana")
    sym         = pos["symbol"]
    spend_label = "SOL" if chain == "solana" else "USDC"
    # usdc_spent trong DB luôn là native unit: SOL thô (Solana) hoặc USDC (Base)
    native_spent = float(pos.get("usdc_spent", BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC))
    entry_price = float(pos.get("actual_entry_price") or pos.get("entry_price", 0))
    hold_time   = _duration_str(pos["entry_time"])

    # ── Đọc native token thực tế nhận về từ TX bán ───────────────────
    # Solana → SOL nhận về (lamport delta); Base → USDC nhận về
    # Poll với retry để chờ TX confirmed trước khi đọc log
    native_received = 0.0
    for attempt in range(8):
        native_received = _get_usdc_received_from_sell_tx(sell_sig, chain, sym)
        if native_received > 0:
            break
        if attempt < 7:
            time.sleep(3)

    # ── Tính lãi ròng theo native unit ──────────────────────────────
    if native_received > 0:
        net_profit  = native_received - native_spent
        net_pct     = (net_profit / native_spent) * 100 if native_spent > 0 else 0.0
        data_source = _t("✅ Actual TX data", "✅ Dữ liệu thực tế từ TX")
        profit_line = (
            f"  💵 {_t('Spent','Đã bỏ vào')}:     <b>{native_spent:.6f} {spend_label}</b>\n"
            f"  💰 {_t('Received','Nhận về')}:       <b>{native_received:.6f} {spend_label}</b>\n"
            f"  📊 {_t('Net profit','Lãi ròng')}:      "
            + (f"<b>+{net_profit:.6f} {spend_label} (+{net_pct:.2f}%)</b> 🟢" if net_profit >= 0
               else f"<b>{net_profit:.6f} {spend_label} ({net_pct:.2f}%)</b> 🔴") + "\n"
        )
    else:
        # Fallback estimate từ % thay đổi giá
        sell_slip    = SLIPPAGE_PCT / 100
        net_received = native_spent * (1 + pct / 100) * (1 - sell_slip)
        net_profit   = net_received - native_spent
        net_pct      = (net_profit / native_spent) * 100 if native_spent > 0 else 0.0
        data_source  = _t("⚠️ Estimated (TX not read)", "⚠️ Ước tính (chưa đọc được TX)")
        profit_line = (
            f"  💵 {_t('Spent','Đã bỏ vào')}:     <b>{native_spent:.6f} {spend_label}</b>\n"
            f"  💰 {_t('Received (est)','Nhận về (est)')}: <b>~{net_received:.6f} {spend_label}</b>\n"
            f"  📊 {_t('Net profit est','Lãi ròng est')}:  "
            + (f"<b>~+{net_profit:.6f} {spend_label} (~+{net_pct:.2f}%)</b> 🟢" if net_profit >= 0
               else f"<b>~{net_profit:.6f} {spend_label} (~{net_pct:.2f}%)</b> 🔴") + "\n"
            f"  ℹ️  ({_t('After','Đã trừ')} ~{SLIPPAGE_PCT:.0f}% {_t('sell slippage','slippage bán')})\n"
        )

    # Chọn emoji theo lãi ròng thực tế
    if net_pct >= 100:  emoji = "🤑🚀"
    elif net_pct >= 50: emoji = "💰🔥"
    elif net_pct >= 0:  emoji = "✅📈"
    else:               emoji = "📉❌"

    chain_emoji = "🟣" if chain == "solana" else "🔵"
    chain_name  = "SOLANA" if chain == "solana" else "BASE"
    explorer    = "https://basescan.org/tx/" if chain == "base" else "https://solscan.io/tx/"
    chart_url   = f"https://dexscreener.com/{chain}/{pos.get('pair_address','')}"

    msg = (
        f"{chain_emoji}{emoji} <b>{_t('TAKE PROFIT','TAKE PROFIT')}! [{chain_name}]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>${sym}</b>  (<code>{pos['mint']}</code>)\n\n"
        f"  📈 {_t('Entry price (actual)','Giá vào (thực)')}: <code>${entry_price:.10f}</code>\n"
        f"  📉 {_t('Sell price','Giá bán')}:        <code>${cur_price:.10f}</code>\n"
        f"  📊 {_t('Price change','Biến động giá')}:  <b>{pct:+.2f}%</b>\n\n"
        f"{profit_line}\n"
        f"  ⏱ {_t('Hold time','Thời gian giữ')}:  <b>{hold_time}</b>\n"
        f"  🔎 {data_source}\n\n"
        f"<a href='{explorer}{sell_sig}'>✅ {_t('Sell TX','TX bán')}</a>  |  "
        f"<a href='{chart_url}'>📉 Chart</a>\n"
        f"#TakeProfit #{chain_name} ${sym}"
    )
    _alert(msg)   # gửi vào chat bot owner

# ================================================================
# THREAD 1 — SCANNER (4 nguồn, song song)
# ================================================================

def _fetch_pairs_for_addr(addr: str, now_ts: float) -> List[dict]:
    """
    Worker: fetch pairs của 1 địa chỉ → lọc age + quick_score → trả về token dicts.
    Chạy song song trong ThreadPoolExecutor của scan_once.
    """
    if db_is_perm_banned(addr) or db_is_blacklisted(addr) or db_has_position(addr):
        return []
    pairs = fetch_token_pairs(addr)
    results = []
    for pair in pairs:
        pcAt    = pair.get("pairCreatedAt")
        age_min = ((now_ts - pcAt / 1000) / 60) if pcAt else None
        if age_min is not None and age_min > MAX_AGE_MIN:
            continue
        if _quick_score(pair) < SOL_PREFILTER_MIN_SCORE:
            continue
        token = _pair_to_token(pair)
        if token:
            token["_src_pair"] = pair   # giữ pair gốc để tính early alert
            results.append(token)
    return results


def scan_once() -> List[dict]:
    """
    Quét token song song:
      Nguồn A: Profile/Boost → fetch tất cả địa chỉ SONG SONG (ThreadPool)
      Nguồn B: /pairs/{chain}/new — chạy đồng thời với Nguồn A
      Nguồn C: GeckoTerminal new_pools → bổ sung địa chỉ token mới
    Tổng thời gian scan giảm từ ~30s xuống ~3-5s.
    """
    now_ts    = time.time()
    seen_pair: set = set()
    cands: List[dict] = []

    # ── Nguồn A + C chạy đồng thời ───────────────────────────────
    _profile_addrs_result: List[List[str]] = [[]]
    _gecko_addrs_result: List[List[str]] = [[]]

    def _fetch_a():
        _profile_addrs_result[0] = fetch_profile_addresses()

    def _fetch_c():
        _gecko_addrs_result[0] = fetch_gecko_new_pool_addresses()

    a_thread = threading.Thread(target=_fetch_a, daemon=True)
    c_thread = threading.Thread(target=_fetch_c, daemon=True)
    a_thread.start()
    c_thread.start()

    a_thread.join(timeout=15)
    c_thread.join(timeout=15)

    all_addrs = []
    seen_addr: set = set()
    for addr in (_profile_addrs_result[0] or []) + (_gecko_addrs_result[0] or []):
        if addr and addr not in seen_addr:
            seen_addr.add(addr)
            all_addrs.append(addr)

    sol_cnt   = sum(1 for a in all_addrs if len(a) > 42)
    base_cnt  = len(all_addrs) - sol_cnt
    print(
        f"  [SCAN-A+C] {len(all_addrs)} địa chỉ (A:{len(_profile_addrs_result[0])} + C:{len(_gecko_addrs_result[0])}) "
        f"(🟣 Sol:{sol_cnt} | 🔵 Base:{base_cnt}) — fetch song song"
    )

    # Nguồn B fetch trong thread riêng đồng thời với Nguồn A
    _new_pairs_result: List[list] = [[]]
    def _fetch_b():
        _new_pairs_result[0] = fetch_new_pairs()
    b_thread = threading.Thread(target=_fetch_b, daemon=True)
    b_thread.start()

    # Nguồn A: fetch tất cả địa chỉ song song, tối đa 10 thread đồng thời
    # (DexScreener rate limit ~10 req/s, mỗi thread có cache TTL 5s)
    SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))  # 4 đủ nhanh, không bị 429
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS, thread_name_prefix="scan") as ex:
        futs = {ex.submit(_fetch_pairs_for_addr, addr, now_ts): addr
                for addr in all_addrs}
        for fut in as_completed(futs):
            try:
                for token in fut.result():
                    pa = token.get("pair_address", "")
                    if not pa or pa in seen_pair:
                        continue
                    seen_pair.add(pa)
                    pair = token.pop("_src_pair", {})
                    cands.append(token)
                    # Early alert
                    if (token.get("token_age_minutes") is not None
                            and token["token_age_minutes"] <= EARLY_ALERT_MAX_AGE_MIN
                            and token["address"] not in _early_alert_sent):
                        token["_quick_score"] = _quick_score(pair)
                        threading.Thread(
                            target=send_early_alert, args=(dict(token),),
                            daemon=True, name=f"early-{token['address'][:8]}"
                        ).start()
            except Exception as e:
                print(f"[Scan-A] ⚠️  worker: {e}")

    # ── Nguồn B: đợi fetch xong (thường xong trước Nguồn A) ─────
    b_thread.join(timeout=15)
    new_pairs = _new_pairs_result[0]
    print(f"  [SCAN-B] {len(new_pairs)} pair từ /new")

    for pair in new_pairs:
        pa        = pair.get("pairAddress", "")
        base_addr = (pair.get("baseToken") or {}).get("address", "")
        if not pa or pa in seen_pair:
            continue
        if base_addr and (db_is_perm_banned(base_addr)
                          or db_is_blacklisted(base_addr)
                          or db_has_position(base_addr)):
            continue
        if _quick_score(pair) < SOL_PREFILTER_MIN_SCORE:
            continue
        token = _pair_to_token(pair)
        if token:
            seen_pair.add(pa)
            cands.append(token)
            if (token.get("token_age_minutes") is not None
                    and token["token_age_minutes"] <= EARLY_ALERT_MAX_AGE_MIN
                    and token["address"] not in _early_alert_sent):
                token["_quick_score"] = _quick_score(pair)
                threading.Thread(
                    target=send_early_alert, args=(dict(token),),
                    daemon=True, name=f"early-{token['address'][:8]}"
                ).start()

    print(f"  [SCAN] {len(cands)} candidate đưa vào validate queue")
    return cands


def scanner_thread(stop_event: threading.Event):
    print("[Scanner] 🟢 Bắt đầu scan (đa nguồn: DexScreener + GeckoTerminal)...")
    in_queue: set = set()
    last_clear    = time.time()

    while not stop_event.is_set():
        try:
            ts = time.time()
            print(f"\n[Scanner] 🔄 {datetime.now().strftime('%H:%M:%S')}")
            cands = scan_once()

            new_count = 0
            for t in cands:
                addr = t.get("address", "")
                if not addr or addr in in_queue:
                    continue
                if TOKEN_QUEUE.full():
                    break
                TOKEN_QUEUE.put(t)
                in_queue.add(addr)  # blacklist cứng trong session
                new_count += 1

            elapsed = time.time() - ts
            print(f"[Scanner] ✅ {new_count} token mới vào queue | "
                  f"Queue: {TOKEN_QUEUE.qsize()} | {elapsed:.1f}s")

            # Xóa in_queue mỗi 60s để re-scan token cũ nếu điều kiện thay đổi
            if time.time() - last_clear > 60:
                in_queue.clear()
                last_clear = time.time()

        except Exception as e:
            import traceback
            print(f"[Scanner] ❌ {e}")
            print(traceback.format_exc())

        stop_event.wait(float(cfg("SCAN_INTERVAL") or SCAN_INTERVAL))
# ================================================================
# THREAD 2 — VALIDATOR
# ================================================================

def _validate_one(token: dict) -> Optional[dict]:
    """
    Validate 1 token — 2 lớp:

    LỚP 1 — HARD REJECT (loại ngay, không tính score):
      • Blacklist / perm_ban / đã có position
      • Honeypot / Blacklisted (GoPlus critical)
      • Mint authority chưa revoke  (Solana: có thể in thêm token)
      • Freeze authority chưa revoke (Solana: có thể đóng băng ví)
      • Top holder > 50%             (concentration cực cao)
      • Holder count < 10            (quá ít người giữ)
      • Liq < $3K                    (quá mỏng)

    LỚP 2 — SCORE CƠ HỘI (calculate_score v2):
      • Momentum: vol/liq ratio, buy pressure (dùng m5/h1 cho token mới)
      • Holder distribution (top10 < 30% thưởng điểm)
      • Age zone hợp lý
      • LP status (Solana)
      • Social signals
    """
    addr    = token["address"]
    chain   = token.get("chain", "solana")
    age_min = token.get("token_age_minutes")
    sym     = token.get("symbol", "?")

    # ── LỚP 1A: DB checks ────────────────────────────────────────
    if db_is_perm_banned(addr) or db_is_blacklisted(addr) or db_has_position(addr):
        return None

    # ── LỚP 1B: GoPlus — lấy full data để hard reject + score ───
    ok, reason, gp = check_goplus(addr, age_min, chain=chain)
    if not ok:
        print(f"[Validator] 🚫 {sym}: {reason}")
        return None

    token["goplus"] = gp   # lưu để calculate_score dùng

    # ── Hard reject từ GoPlus data ────────────────────────────────
    def _gp(key, default="0"):
        return str(gp.get(key, default) or default)

    # Solana: mint/freeze authority
    if chain == "solana":
        if _gp("mint_authority") not in ("", "0", "null", "None"):
            print(f"[Validator] 🚫 {sym}: Mint authority chưa revoke ({gp.get('mint_authority','')[:20]})")
            db_add_blacklist(addr)
            return None
        if _gp("freeze_authority") not in ("", "0", "null", "None"):
            print(f"[Validator] 🚫 {sym}: Freeze authority chưa revoke")
            db_add_blacklist(addr)
            return None

    # Holder concentration
    try:
        top10_pct = float(gp.get("top10HolderPercent") or gp.get("top_10_holder_rate") or 0)
        if top10_pct > 0 and top10_pct < 1:
            top10_pct *= 100   # GoPlus đôi khi trả về dạng 0.xx thay vì xx%
    except Exception:
        top10_pct = 0

    try:
        creator_pct = float(gp.get("creatorPercent") or gp.get("creator_percent") or 0)
        if creator_pct > 0 and creator_pct < 1:
            creator_pct *= 100
    except Exception:
        creator_pct = 0

    try:
        holder_count = int(gp.get("holderCount") or gp.get("holder_count") or 0)
    except Exception:
        holder_count = 0

    if top10_pct > MAX_TOP10_HOLDER_PCT:
        print(f"[Validator] 🚫 {sym}: Top10 holder {top10_pct:.0f}% (>{MAX_TOP10_HOLDER_PCT:.0f}%)")
        return None

    if holder_count > 0 and holder_count < MIN_HOLDER_COUNT:
        print(f"[Validator] 🚫 {sym}: Chỉ {holder_count} holders (<{MIN_HOLDER_COUNT})")
        return None

    # Token age filter (anti-rug giai đoạn vừa deploy)
    if age_min is not None and age_min * 60 < MIN_TOKEN_AGE_S:
        print(f"[Validator] 🚫 {sym}: Token age {age_min*60:.0f}s < {MIN_TOKEN_AGE_S:.0f}s")
        return None

    # Volume spike filter: chỉ vào khi có đột biến vol ngắn hạn
    vol_30s = float(token.get("volume_30s", 0) or 0)
    vol_5m_avg = float(token.get("volume_5m_avg", 0) or 0)
    if vol_5m_avg <= 0 or vol_30s <= vol_5m_avg * VOLUME_SPIKE_MULTIPLIER:
        print(f"[Validator] 🚫 {sym}: Volume spike chưa đạt ({vol_30s:.0f} <= {vol_5m_avg:.0f}×{VOLUME_SPIKE_MULTIPLIER:.1f})")
        return None

    # Liq hard floor — dùng MIN_LIQUIDITY_USD dynamic (min 3K tuyệt đối)
    liq_floor = max(3_000.0, cfg("MIN_LIQUIDITY_USD") or MIN_LIQUIDITY_USD)
    liq_usd = float(token.get("liquidity_usd", 0) or 0)
    if liq_usd < liq_floor:
        print(f"[Validator] 🚫 {sym}: Liq ${liq_usd:.0f} < ${liq_floor:.0f}")
        return None

    # Liquidity filter theo SOL: LP >= 5 SOL
    if chain == "solana":
        sol_price = get_sol_price_usd(max_age_s=60.0)
        lp_sol = (liq_usd / sol_price) if sol_price > 0 else 0.0
        token["lp_sol"] = lp_sol
        if lp_sol < MIN_LP_SOL:
            print(f"[Validator] 🚫 {sym}: LP {lp_sol:.2f} SOL < {MIN_LP_SOL:.2f} SOL")
            return None

    # ── LP status (Solana only) ───────────────────────────────────
    token["lp_status"] = check_lp_status(addr) if chain == "solana" else "n/a"

    # ── Enrich token với holder data cho scoring ──────────────────
    token["top10_holder_pct"] = top10_pct
    token["creator_pct"]      = creator_pct
    token["holder_count"]     = holder_count

    # ── Validate social links (website + Twitter) ─────────────────
    # Chạy trước calculate_score để scoring dùng kết quả validated
    validate_social_links(token)

    if REQUIRE_SOCIAL_WEB_X and not (token.get("website_valid") and token.get("twitter_valid")):
        print(f"[Validator] 🚫 {sym}: thiếu Web/X hợp lệ")
        return None

    # ── LỚP 2: Score cơ hội ──────────────────────────────────────
    score, detail = calculate_score(token)
    token["_opp_score"]    = score
    token["_score_detail"] = detail

    _min_score = int(cfg("MIN_SCORE") or MIN_SCORE)
    _log_score_snapshot(token, score, _min_score)
    age_log = f"{age_min:.0f}p" if age_min is not None else "N/A"
    flag = "✅" if score >= _min_score else "⏭ "
    print(f"[Validator] {flag} {sym:<10} | Score:{score}/100 | "
          f"Tuổi:{age_log} | Liq:{_fmt_usd(token['liquidity_usd'])} | "
          f"Top10:{top10_pct:.0f}% | Holders:{holder_count}")

    # ── Signal Alert: gửi thông báo cho bạn bè nếu điểm đủ cao ────
    # Chạy trong thread riêng để không chặn validator pipeline
    if score >= SIGNAL_ALERT_MIN_SCORE:
        def _fire_signal(t=dict(token), s=score, d=list(detail)):
            try:
                send_signal_alert(t, s, d)
            except Exception as _e:
                print(f"[Signal] ⚠️  {_e}")
        threading.Thread(
            target=_fire_signal, daemon=True,
            name=f"signal-{addr[:8]}"
        ).start()

    if score >= _min_score:
        return token
    return None

def validator_thread(stop_event: threading.Event):
    print("[Validator] 🟢 Bắt đầu validate...")
    with ThreadPoolExecutor(max_workers=VALIDATOR_WORKERS, thread_name_prefix="valid") as ex:
        while not stop_event.is_set():
            batch = []
            # Lấy tối đa 20 token một lần
            for _ in range(VALIDATOR_WORKERS):
                try:
                    batch.append(TOKEN_QUEUE.get_nowait())
                except Empty:
                    break

            if not batch:
                stop_event.wait(0.5)
                continue

            futs = {ex.submit(_validate_one, t): t for t in batch}
            for fut in as_completed(futs):
                try:
                    result = fut.result()
                    if result and not BUY_QUEUE.full():
                        BUY_QUEUE.put(result)
                except Exception as e:
                    print(f"[Validator] ❌ {e}")

# ================================================================
# THREAD 3 — BUYER
# ================================================================

# ================================================================
# ENTRY PRICE CONFIRMATION
# ================================================================
# 4 chiến lược theo tuổi token (tất cả có thể override qua .env):
#
#   Tuổi < 1p  → Dip 1s + Volume Check
#                Chờ 1s, kiểm tra giá giảm/bằng VÀ volume tăng >20%
#                Tránh pump-dump nhanh ở token siêu mới
#
#   Tuổi 1-5p  → Dip 3s + RSI Filter
#                Chờ 3s, giá phải giảm/bằng VÀ RSI(14) < 70
#                RSI < 30 (oversold) → ưu tiên mua ngay
#                RSI 30-70 → mua bình thường sau dip
#                RSI > 70  → bỏ qua (overbought)
#
#   Tuổi 5-30p → BBL 45s + MACD Cross
#                Poll mỗi 2s trong 45s, chờ giá chạm BBL(14,2)
#                MACD(12,26,9) cross lên → ưu tiên mua sớm (không cần chạm BBL)
#                Hết 45s → bỏ qua
#
#   Tuổi >30p  → VWAP 60s
#                Poll mỗi 5s trong 60s, chờ giá chạm VWAP lower bound
#                VWAP = Σ(price×volume) / Σ(volume) tính từ snapshot
#                Hết 60s → bỏ qua
# ================================================================

# ── Ngưỡng tuổi phân vùng chiến lược ─────────────────────────────
ENTRY_AGE_ULTRA_MIN  = float(os.getenv("ENTRY_AGE_ULTRA_MIN",  "1"))   # < 1p  → Ultra new
ENTRY_AGE_NEW_MIN    = float(os.getenv("ENTRY_AGE_NEW_MIN",    "5"))   # 1-5p  → New
ENTRY_AGE_MID_MIN    = float(os.getenv("ENTRY_AGE_MID_MIN",   "30"))   # 5-30p → Mid

# ── Chiến lược 1: Dip 1s + Volume Check (<1p) ────────────────────
ENTRY_ULTRA_DIP_S       = float(os.getenv("ENTRY_ULTRA_DIP_S",       "1"))   # giây chờ
ENTRY_ULTRA_VOL_PCT     = float(os.getenv("ENTRY_ULTRA_VOL_PCT",     "20"))  # % volume tăng tối thiểu

# ── Chiến lược 2: Dip 3s + RSI Filter (1-5p) ─────────────────────
ENTRY_DIP_WINDOW_S      = float(os.getenv("ENTRY_DIP_WINDOW_S",      "5"))   # giây chờ dip
ENTRY_RSI_PERIOD        = int(  os.getenv("ENTRY_RSI_PERIOD",        "14"))  # period RSI
ENTRY_RSI_OVERBOUGHT    = float(os.getenv("ENTRY_RSI_OVERBOUGHT",    "70"))  # RSI > 70 → bỏ qua
ENTRY_RSI_OVERSOLD      = float(os.getenv("ENTRY_RSI_OVERSOLD",      "30"))  # RSI < 30 → ưu tiên mua

# ── Chiến lược 3: BBL 45s + MACD Cross (5-30p) ───────────────────
ENTRY_BBL_WINDOW_S      = float(os.getenv("ENTRY_BBL_WINDOW_S",     "15"))  # giây chờ BBL
ENTRY_BBL_PERIOD        = int(  os.getenv("ENTRY_BBL_PERIOD",        "14"))  # số nến BBL
ENTRY_BBL_STD           = float(os.getenv("ENTRY_BBL_STD",           "1.8")) # std dev BBL
ENTRY_BBL_TOUCH_TOL_PCT = float(os.getenv("ENTRY_BBL_TOUCH_TOL_PCT", "1.5"))# tolerance % so BBL
ENTRY_BBL_POLL_S        = float(os.getenv("ENTRY_BBL_POLL_S",        "2"))   # poll mỗi N giây
ENTRY_MACD_FAST         = int(  os.getenv("ENTRY_MACD_FAST",         "12"))  # MACD fast EMA
ENTRY_MACD_SLOW         = int(  os.getenv("ENTRY_MACD_SLOW",         "26"))  # MACD slow EMA
ENTRY_MACD_SIGNAL       = int(  os.getenv("ENTRY_MACD_SIGNAL",        "9"))  # MACD signal line

# ── Chiến lược 4: VWAP 60s (>30p) ────────────────────────────────
ENTRY_VWAP_WINDOW_S     = float(os.getenv("ENTRY_VWAP_WINDOW_S",    "60"))  # giây chờ VWAP
ENTRY_VWAP_POLL_S       = float(os.getenv("ENTRY_VWAP_POLL_S",       "5"))  # poll mỗi N giây
ENTRY_VWAP_TOL_PCT      = float(os.getenv("ENTRY_VWAP_TOL_PCT",      "1.0"))# tolerance % so VWAP


def _fetch_price_dex(addr: str, chain: str) -> float:
    """Lấy giá realtime từ DexScreener (bypass cache riêng cho confirm)."""
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{addr}", timeout=8)
        data = r.json()
        ps   = data.get("pairs", []) if isinstance(data, dict) else []
        sp   = [p for p in ps if p.get("chainId") == chain]
        p    = sp[0] if sp else (ps[0] if ps else {})
        return float(p.get("priceUsd", 0) or 0)
    except Exception:
        return 0.0


def _fetch_price_and_volume_dex(addr: str, chain: str) -> tuple:
    """
    Lấy giá USD và volume 5m từ DexScreener.
    Trả về (price: float, vol_5m: float).
    """
    try:
        r    = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{addr}", timeout=8)
        data = r.json()
        ps   = data.get("pairs", []) if isinstance(data, dict) else []
        sp   = [p for p in ps if p.get("chainId") == chain]
        p    = sp[0] if sp else (ps[0] if ps else {})
        price  = float(p.get("priceUsd", 0) or 0)
        vol_5m = float((p.get("volume") or {}).get("m5", 0) or 0)
        return price, vol_5m
    except Exception:
        return 0.0, 0.0


def _bollinger_lower(prices: list, period: int, std_dev: float) -> float:
    """Tính Bollinger Band Lower từ list giá. Trả về 0 nếu chưa đủ dữ liệu."""
    if len(prices) < period:
        return 0.0
    arr = _np.array(prices[-period:], dtype=float)
    sma = float(_np.mean(arr))
    std = float(_np.std(arr))
    return sma - std_dev * std


def _rsi(prices: list, period: int = 14) -> float:
    """
    Tính RSI(period) từ list giá. Trả về float 0-100.
    Trả về 50 (neutral) nếu chưa đủ dữ liệu.
    Dùng phương pháp Wilder smoothing (EMA variant).
    """
    if len(prices) < period + 1:
        return 50.0
    arr    = _np.array(prices[-(period + 1):], dtype=float)
    deltas = _np.diff(arr)
    gains  = _np.where(deltas > 0, deltas, 0.0)
    losses = _np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(_np.mean(gains))
    avg_loss = float(_np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _ema(prices: list, period: int) -> float:
    """Tính EMA(period) từ list giá. Trả về 0 nếu chưa đủ dữ liệu."""
    if len(prices) < period:
        return 0.0
    k   = 2.0 / (period + 1)
    ema = float(_np.mean(prices[:period]))
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _macd(prices: list,
          fast: int = 12, slow: int = 26, signal: int = 9
          ) -> tuple:
    """
    Tính MACD line, Signal line, Histogram.
    Trả về (macd_line, signal_line, histogram) hoặc (0, 0, 0) nếu chưa đủ dữ liệu.
    MACD cross lên = macd_line vừa cắt qua trên signal_line.
    """
    if len(prices) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast   = _ema(prices, fast)
    ema_slow   = _ema(prices, slow)
    macd_line  = ema_fast - ema_slow
    # Tính signal line = EMA(macd_line) — cần lịch sử MACD
    # Dùng approximation: lấy macd values cho các window trước
    macd_hist_list = []
    for i in range(signal, 0, -1):
        ef = _ema(prices[:-i] if i > 0 else prices, fast)
        es = _ema(prices[:-i] if i > 0 else prices, slow)
        macd_hist_list.append(ef - es)
    macd_hist_list.append(macd_line)
    signal_line = float(_np.mean(macd_hist_list[-signal:])) if len(macd_hist_list) >= signal else macd_line
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram


def _vwap_lower(prices: list, volumes: list, tol_pct: float = 1.0) -> float:
    """
    Tính VWAP lower bound = VWAP × (1 - tol_pct/100).
    Trả về 0 nếu chưa đủ dữ liệu hoặc volume = 0.
    """
    if len(prices) < 2 or len(volumes) < 2:
        return 0.0
    arr_p = _np.array(prices,  dtype=float)
    arr_v = _np.array(volumes, dtype=float)
    total_vol = float(_np.sum(arr_v))
    if total_vol <= 0:
        return 0.0
    vwap = float(_np.dot(arr_p, arr_v) / total_vol)
    return vwap * (1.0 - tol_pct / 100.0)


# ================================================================
# CHIẾN LƯỢC 1 — Dip 1s + Volume Check  (token < 1 phút)
# ================================================================

def confirm_entry_ultra(addr: str, sym: str, chain: str,
                        snapshot_price: float, snapshot_vol: float) -> tuple:
    """
    Token < 1 phút — Dip 1s + Volume Check.

    Logic:
      1. Chờ ENTRY_ULTRA_DIP_S giây (default 1s)
      2. Lấy giá + volume mới
      3. Giá phải giảm hoặc bằng snapshot (dip confirmed)
      4. Volume 5m phải tăng ≥ ENTRY_ULTRA_VOL_PCT% (default 20%)
         → Có người mua thực sự, không phải pump giả
      5. Nếu không lấy được volume → fallback chỉ dùng dip check

    Trả về (should_buy: bool, entry_price: float).
    """
    print(f"[EntryGuard] ⚡ {sym}: ULTRA DIP+VOL CHECK {ENTRY_ULTRA_DIP_S:.0f}s "
          f"(snapshot=${snapshot_price:.10f}, vol5m={snapshot_vol:.0f})")
    time.sleep(ENTRY_ULTRA_DIP_S)

    live_price, live_vol = _fetch_price_and_volume_dex(addr, chain)
    if live_price <= 0:
        print(f"[EntryGuard] ⚠️  {sym}: không lấy được giá live → fallback mua")
        return True, snapshot_price

    change_pct = (live_price - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0

    # ── Check 1: Giá phải giảm hoặc bằng ────────────────────────
    if live_price > snapshot_price:
        print(f"[EntryGuard] ❌ {sym}: Giá tăng {change_pct:+.2f}% sau {ENTRY_ULTRA_DIP_S:.0f}s → BỎ QUA")
        return False, 0.0

    # ── Check 2: Volume tăng ≥ 20% ───────────────────────────────
    if snapshot_vol > 0 and live_vol > 0:
        vol_change_pct = (live_vol - snapshot_vol) / snapshot_vol * 100
        if vol_change_pct >= ENTRY_ULTRA_VOL_PCT:
            print(f"[EntryGuard] ✅ {sym}: Dip {change_pct:+.2f}% + Vol tăng "
                  f"{vol_change_pct:+.1f}% → XÁC NHẬN MUA @ ${live_price:.10f}")
            return True, live_price
        else:
            print(f"[EntryGuard] ❌ {sym}: Giá dip OK nhưng Vol chỉ {vol_change_pct:+.1f}% "
                  f"(cần ≥{ENTRY_ULTRA_VOL_PCT:.0f}%) → BỎ QUA (pump-dump risk)")
            return False, 0.0
    else:
        # Fallback: không có volume data → chỉ dùng dip
        print(f"[EntryGuard] ✅ {sym}: Dip {change_pct:+.2f}% (vol N/A → fallback) "
              f"→ MUA @ ${live_price:.10f}")
        return True, live_price


# ================================================================
# CHIẾN LƯỢC 2 — Dip 3s + RSI Filter  (token 1-5 phút)
# ================================================================

def confirm_entry_dip_rsi(addr: str, sym: str, chain: str,
                          snapshot_price: float) -> tuple:
    """
    Token 1-5 phút — Dip 3s + RSI Filter.

    Logic:
      1. Chờ ENTRY_DIP_WINDOW_S giây (default 3s)
      2. Lấy giá mới
      3. Giá phải giảm hoặc bằng snapshot
      4. Tính RSI(14) từ price history gần nhất:
           RSI < 30 (oversold) → ưu tiên mua ngay
           RSI 30-70           → mua bình thường (dip confirmed là đủ)
           RSI > 70 (overbought) → bỏ qua (đang pump, rủi ro cao)

    Trả về (should_buy: bool, entry_price: float).
    """
    print(f"[EntryGuard] ⏱️  {sym}: DIP+RSI CHECK {ENTRY_DIP_WINDOW_S:.0f}s "
          f"(snapshot=${snapshot_price:.10f})")
    time.sleep(ENTRY_DIP_WINDOW_S)

    live_price = _fetch_price_dex(addr, chain)
    if live_price <= 0:
        print(f"[EntryGuard] ⚠️  {sym}: không lấy được giá → fallback mua")
        return True, snapshot_price

    change_pct = (live_price - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0

    # ── Check 1: Giá phải giảm hoặc bằng ────────────────────────
    if live_price > snapshot_price:
        print(f"[EntryGuard] ❌ {sym}: Giá tăng {change_pct:+.2f}% → BỎ QUA")
        return False, 0.0

    # ── Check 2: RSI filter ───────────────────────────────────────
    # Xây price list từ snapshot → live để tính RSI
    # Với 2 điểm dữ liệu RSI sẽ trả về 50 (neutral) → mua bình thường
    price_list = [snapshot_price, live_price]
    rsi_val    = _rsi(price_list, period=ENTRY_RSI_PERIOD)

    if rsi_val > ENTRY_RSI_OVERBOUGHT:
        print(f"[EntryGuard] ❌ {sym}: Dip OK nhưng RSI={rsi_val:.1f} > {ENTRY_RSI_OVERBOUGHT:.0f} "
              f"(overbought) → BỎ QUA")
        return False, 0.0
    elif rsi_val < ENTRY_RSI_OVERSOLD:
        print(f"[EntryGuard] ✅ {sym}: Dip {change_pct:+.2f}% + RSI={rsi_val:.1f} "
              f"(OVERSOLD 🔥) → ƯU TIÊN MUA @ ${live_price:.10f}")
        return True, live_price
    else:
        print(f"[EntryGuard] ✅ {sym}: Dip {change_pct:+.2f}% + RSI={rsi_val:.1f} "
              f"(neutral) → MUA @ ${live_price:.10f}")
        return True, live_price


# ── Alias backward-compat: giữ tên cũ trỏ sang hàm mới ──────────
def confirm_entry_dip(addr: str, sym: str, chain: str,
                      snapshot_price: float) -> tuple:
    """Alias của confirm_entry_dip_rsi (backward-compat)."""
    return confirm_entry_dip_rsi(addr, sym, chain, snapshot_price)


# ================================================================
# CHIẾN LƯỢC 3 — BBL 45s + MACD Cross  (token 5-30 phút)
# ================================================================

def confirm_entry_bbl(addr: str, sym: str, chain: str,
                      snapshot_price: float) -> tuple:
    """
    Token 5-30 phút — BBL 45s + MACD Cross.

    Logic:
      Poll giá mỗi ENTRY_BBL_POLL_S giây, tối đa ENTRY_BBL_WINDOW_S giây.
      Điều kiện mua (OR):
        A. Giá chạm BBL(14, 2.0) ± ENTRY_BBL_TOUCH_TOL_PCT%
        B. MACD(12,26,9) cross lên (histogram chuyển từ âm sang dương)
           → momentum đảo chiều, ưu tiên mua sớm (bỏ qua BBL)
      Hết 45s không thỏa mãn → bỏ qua.
      Fallback khi chưa đủ nến BBL: dip ≥2% vs snapshot.

    Trả về (should_buy: bool, entry_price: float).
    """
    print(f"[EntryGuard] 📊 {sym}: BBL+MACD CHECK tối đa {ENTRY_BBL_WINDOW_S:.0f}s "
          f"(BBL period={ENTRY_BBL_PERIOD}, std={ENTRY_BBL_STD} | "
          f"MACD {ENTRY_MACD_FAST},{ENTRY_MACD_SLOW},{ENTRY_MACD_SIGNAL})")

    price_history  = [snapshot_price] if snapshot_price > 0 else []
    deadline       = time.time() + ENTRY_BBL_WINDOW_S
    prev_histogram = None   # theo dõi MACD cross

    while time.time() < deadline:
        time.sleep(ENTRY_BBL_POLL_S)
        cur = _fetch_price_dex(addr, chain)
        if cur <= 0:
            continue

        price_history.append(cur)
        remaining = deadline - time.time()

        # ── MACD Check: cross lên → mua ngay (ưu tiên cao hơn BBL) ──
        if len(price_history) >= ENTRY_MACD_SLOW + ENTRY_MACD_SIGNAL:
            macd_line, signal_line, histogram = _macd(
                price_history, ENTRY_MACD_FAST, ENTRY_MACD_SLOW, ENTRY_MACD_SIGNAL)
            if prev_histogram is not None and prev_histogram < 0 and histogram > 0:
                pct_from_snap = (cur - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0
                print(f"[EntryGuard] ✅ {sym}: MACD CROSS LÊN 🚀 "
                      f"(hist {prev_histogram:.6f}→{histogram:.6f}) | "
                      f"${cur:.10f} ({pct_from_snap:+.2f}%) → MUA SỚM | Còn {remaining:.0f}s")
                return True, cur
            prev_histogram = histogram
            macd_str = f"MACD={macd_line:.6f} Sig={signal_line:.6f} Hist={histogram:.6f}"
        else:
            macd_str = f"MACD: chưa đủ {ENTRY_MACD_SLOW + ENTRY_MACD_SIGNAL} nến"

        # ── BBL Check ─────────────────────────────────────────────
        bbl = _bollinger_lower(price_history, ENTRY_BBL_PERIOD, ENTRY_BBL_STD)

        if bbl <= 0:
            if snapshot_price > 0 and cur <= snapshot_price * 0.98:
                print(f"[EntryGuard] ✅ {sym}: Chưa đủ nến BBL, dip ≥2% "
                      f"(${cur:.10f}) → MUA")
                return True, cur
            print(f"[EntryGuard] ⏳ {sym}: BBL chưa đủ nến "
                  f"({len(price_history)}/{ENTRY_BBL_PERIOD}) | {macd_str} | Còn {remaining:.0f}s")
            continue

        touch_threshold = bbl * (1 + ENTRY_BBL_TOUCH_TOL_PCT / 100)
        if cur <= touch_threshold:
            pct_from_snap = (cur - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0
            print(f"[EntryGuard] ✅ {sym}: Giá ${cur:.10f} chạm BBL ${bbl:.10f} "
                  f"({pct_from_snap:+.2f}% vs snapshot) → MUA | Còn {remaining:.0f}s")
            return True, cur

        print(f"[EntryGuard] ⏳ {sym}: ${cur:.10f} | BBL=${bbl:.10f} "
              f"cần ≤${touch_threshold:.10f} | {macd_str} | Còn {remaining:.0f}s")

    # Hết giờ
    final_price = _fetch_price_dex(addr, chain) or snapshot_price
    change_pct  = (final_price - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0
    print(f"[EntryGuard] ❌ {sym}: Hết {ENTRY_BBL_WINDOW_S:.0f}s không chạm BBL/MACD "
          f"(giá {change_pct:+.2f}% vs snapshot) → BỎ QUA")
    return False, 0.0


# ================================================================
# CHIẾN LƯỢC 4 — VWAP 60s  (token > 30 phút)
# ================================================================

def confirm_entry_vwap(addr: str, sym: str, chain: str,
                       snapshot_price: float, snapshot_vol: float) -> tuple:
    """
    Token > 30 phút — VWAP 60s.

    Logic:
      Poll giá + volume 5m mỗi ENTRY_VWAP_POLL_S giây (default 5s),
      tối đa ENTRY_VWAP_WINDOW_S giây (default 60s).
      VWAP = Σ(price × vol) / Σ(vol) — giá trung bình có trọng số volume.
      Mua khi giá ≤ VWAP × (1 - ENTRY_VWAP_TOL_PCT/100).
        → Token ổn định, chờ giá về vùng fair value có volume hỗ trợ.
      Fallback khi không có volume data: dip ≥1% vs snapshot.
      Hết 60s → bỏ qua.

    Trả về (should_buy: bool, entry_price: float).
    """
    print(f"[EntryGuard] 📈 {sym}: VWAP CHECK tối đa {ENTRY_VWAP_WINDOW_S:.0f}s "
          f"(poll {ENTRY_VWAP_POLL_S:.0f}s, tol={ENTRY_VWAP_TOL_PCT:.1f}%)")

    price_history  = [snapshot_price] if snapshot_price > 0 else []
    volume_history = [snapshot_vol]   if snapshot_vol  > 0 else []
    deadline       = time.time() + ENTRY_VWAP_WINDOW_S

    while time.time() < deadline:
        time.sleep(ENTRY_VWAP_POLL_S)
        cur, cur_vol = _fetch_price_and_volume_dex(addr, chain)
        if cur <= 0:
            continue

        remaining = deadline - time.time()

        if cur_vol > 0:
            price_history.append(cur)
            volume_history.append(cur_vol)

        vwap_lower = _vwap_lower(price_history, volume_history, ENTRY_VWAP_TOL_PCT)

        if vwap_lower <= 0:
            # Không có volume data → fallback dip ≥1%
            if snapshot_price > 0 and cur <= snapshot_price * 0.99:
                print(f"[EntryGuard] ✅ {sym}: Không có volume, dip ≥1% "
                      f"(${cur:.10f}) → MUA (VWAP fallback)")
                return True, cur
            print(f"[EntryGuard] ⏳ {sym}: ${cur:.10f} | VWAP: chưa có volume data "
                  f"| Còn {remaining:.0f}s")
            continue

        vwap_val = vwap_lower / (1.0 - ENTRY_VWAP_TOL_PCT / 100.0)   # recover VWAP từ lower
        if cur <= vwap_lower:
            pct_from_snap = (cur - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0
            print(f"[EntryGuard] ✅ {sym}: Giá ${cur:.10f} ≤ VWAP lower ${vwap_lower:.10f} "
                  f"(VWAP=${vwap_val:.10f}, {pct_from_snap:+.2f}% vs snap) → MUA | Còn {remaining:.0f}s")
            return True, cur

        print(f"[EntryGuard] ⏳ {sym}: ${cur:.10f} | VWAP=${vwap_val:.10f} "
              f"| Cần ≤${vwap_lower:.10f} | Còn {remaining:.0f}s")

    # Hết giờ
    final_price = _fetch_price_dex(addr, chain) or snapshot_price
    change_pct  = (final_price - snapshot_price) / snapshot_price * 100 if snapshot_price > 0 else 0
    print(f"[EntryGuard] ❌ {sym}: Hết {ENTRY_VWAP_WINDOW_S:.0f}s không chạm VWAP "
          f"(giá {change_pct:+.2f}% vs snapshot) → BỎ QUA")
    return False, 0.0


# ================================================================
# DISPATCHER CHÍNH — chọn chiến lược theo tuổi token
# ================================================================

def confirm_entry_price(token: dict) -> tuple:
    """
    Dispatcher: chọn chiến lược xác nhận giá vào lệnh theo tuổi token.

      Tuổi < 1p   → Dip 1s + Volume Check  (tránh pump-dump siêu nhanh)
      Tuổi 1-5p   → Dip 3s + RSI Filter    (confirm dip + không overbought)
      Tuổi 5-30p  → BBL 45s + MACD Cross   (chờ dip sâu + momentum đảo chiều)
      Tuổi > 30p  → VWAP 60s              (fair value có volume hỗ trợ)

    Trả về (should_buy: bool, entry_price: float).
    """
    addr           = token["address"]
    sym            = token.get("symbol", "?")
    chain          = token.get("chain", "solana")
    age_min        = token.get("token_age_minutes")
    snapshot_price = token.get("price_usd", 0)

    if snapshot_price <= 0:
        snapshot_price = _fetch_price_dex(addr, chain)

    # Lấy volume hiện tại (cần cho Ultra + VWAP)
    _, snapshot_vol = _fetch_price_and_volume_dex(addr, chain)

    # ── Phân vùng chiến lược ──────────────────────────────────────
    if age_min is not None and age_min < ENTRY_AGE_ULTRA_MIN:
        strategy = f"ULTRA DIP+VOL {ENTRY_ULTRA_DIP_S:.0f}s (token {age_min:.1f}p < {ENTRY_AGE_ULTRA_MIN:.0f}p)"
        print(f"[EntryGuard] 🔀 {sym}: Dùng chiến lược {strategy}")
        return confirm_entry_ultra(addr, sym, chain, snapshot_price, snapshot_vol)

    elif age_min is not None and age_min < ENTRY_AGE_NEW_MIN:
        strategy = f"DIP+RSI {ENTRY_DIP_WINDOW_S:.0f}s (token {age_min:.1f}p, 1-5p zone)"
        print(f"[EntryGuard] 🔀 {sym}: Dùng chiến lược {strategy}")
        return confirm_entry_dip_rsi(addr, sym, chain, snapshot_price)

    elif age_min is not None and age_min < ENTRY_AGE_MID_MIN:
        strategy = f"BBL+MACD {ENTRY_BBL_WINDOW_S:.0f}s (token {age_min:.1f}p, 5-30p zone)"
        print(f"[EntryGuard] 🔀 {sym}: Dùng chiến lược {strategy}")
        return confirm_entry_bbl(addr, sym, chain, snapshot_price)

    else:
        age_str  = f"{age_min:.1f}p" if age_min is not None else "N/A"
        strategy = f"VWAP {ENTRY_VWAP_WINDOW_S:.0f}s (token {age_str} > {ENTRY_AGE_MID_MIN:.0f}p)"
        print(f"[EntryGuard] 🔀 {sym}: Dùng chiến lược {strategy}")
        return confirm_entry_vwap(addr, sym, chain, snapshot_price, snapshot_vol)


def _execute_buy(token: dict):
    """Chạy trong thread riêng — không block buyer_thread."""
    addr   = token["address"]
    sym    = token["symbol"]
    score  = token.get("_opp_score", 0)

    # Double-check trước khi mua (perm_ban + blacklist cứng)
    if db_is_perm_banned(addr) or db_is_blacklisted(addr) or db_has_position(addr):
        print(f"[Buyer] ⏭  {sym}: banned/blacklist/đã có position")
        return

    chain     = token.get("chain", "solana")
    age_min   = token.get("token_age_minutes")

    # Determine buy-from token and raw amount (SOL for Solana, USDC for Base)
    from_token, buy_raw = _buy_token_and_raw(chain)
    buy_display = f"{BUY_AMOUNT_SOL} SOL" if chain == "solana" else f"{BUY_AMOUNT_USDC} USDC"

    # ── Risk Manager check ──────────────────────────────────────────
    open_pos = len(db_get_positions())
    ok, reason = _risk_manager.can_buy(open_pos)
    if not ok:
        print(f"[Buyer] 🛑 {sym}: Risk block — {reason}")
        return

    age_log = f"{age_min:.1f}p" if age_min is not None else "N/A"
    print(f"[Buyer] 🟢 {sym} [{chain.upper()}] | Score: {score}/100 | Tuổi: {age_log}")
    print(f"[Buyer] 💰 Mua {buy_display} | {_risk_manager.status()}")

    # ── XÁC NHẬN GIÁ VÀO LỆNH ────────────────────────────────────────
    # Token < 5p → chờ Dip 3s | Token >= 5p → chờ chạm BBL 30s
    should_buy, confirmed_price = confirm_entry_price(token)
    if not should_buy:
        print(f"[Buyer] ⛔ {sym}: EntryGuard từ chối → BỎ QUA (tránh đu đỉnh)")
        return

    # Cập nhật giá snapshot với giá đã xác nhận (chính xác hơn)
    if confirmed_price > 0 and confirmed_price != token.get("price_usd", 0):
        old_price = token.get("price_usd", 0)
        token["price_usd"] = confirmed_price
        change_pct = (confirmed_price - old_price) / old_price * 100 if old_price > 0 else 0
        print(f"[Buyer] 🎯 {sym}: Vào lệnh @ ${confirmed_price:.10f} "
              f"({change_pct:+.2f}% vs lúc detect)")

    sig = execute_swap(from_token, addr, buy_raw,
                       label=f"BUY {sym}", chain=chain,
                       token_age_min=age_min)

    if not sig:
        print(f"[Buyer] ❌ {sym}: swap thất bại")
        send_error_alert(f"❌ Mua {sym} [{chain}] thất bại\n<code>{addr}</code>")
        return

    # Amount spent: SOL for Solana, USDC for Base
    spent_amount = BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC

    # Ghi nhận lệnh mua vào RiskManager
    _risk_manager.record_trade()

    # Lưu DB + blacklist cứng NGAY với giá DexScreener (tạm thời)
    db_save_position(token, sig, spent_amount)
    db_add_blacklist(addr)
    print(f"[Buyer] ✅ {sym} | sig: {sig[:20]}... → xác nhận balance...")

    # ── Lấy balance trước swap để tính delta chính xác ──────────────
    # Phải lấy TRƯỚC khi gửi TX để tính delta = token nhận được thực tế
    decimals      = get_token_decimals_rpc(addr) if chain == "solana" else 18
    bal_before_raw = get_token_raw_balance(addr, chain=chain)
    bal_before     = int(bal_before_raw) if bal_before_raw.isdigit() else 0

    # Background: poll TX confirm → đọc balance sau → tính actual_price
    def _confirm_entry():
        """
        Sau khi TX confirmed:
          delta_tokens = balance_sau - balance_truoc
          actual_price = usdc_thuc_chi / delta_tokens

        Để biết USDC thực chi, đọc tx log trên Solana RPC (getTransaction).
        Nếu không parse được log → fallback: giả sử đúng BUY_AMOUNT_USDC.
        """
        import traceback as _tb
        try:
            # ── Bước 1: chờ TX confirmed ─────────────────────────────
            confirmed = _wait_tx_confirmed(sig, chain, sym)
            if not confirmed:
                print(f"[Confirm] ⏰ {sym}: TX timeout — dùng giá DexScreener")
                _send_tg_buy_signal()
                return

            # ── Bước 2: đọc spend thực tế từ TX ─────────────────────
            # Solana → SOL spent (lamport delta); Base → USDC spent
            fallback_spend = BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC
            spend_label    = "SOL" if chain == "solana" else "USDC"
            usdc_spent_actual = _get_spend_from_tx(sig, chain, sym)
            if usdc_spent_actual <= 0:
                usdc_spent_actual = fallback_spend
                print(f"[Confirm] ⚠️  {sym}: cannot parse tx log → using {fallback_spend} {spend_label}")

            # ── Bước 3: đọc balance SAU → tính delta ─────────────────
            raw_after = "0"
            for attempt in range(12):
                time.sleep(3)
                raw_after = get_token_raw_balance(addr, chain=chain)
                if raw_after != "0" and int(raw_after) > bal_before:
                    break
                print(f"[Confirm] ⏳ {sym}: chờ balance RPC... ({attempt+1}/12)")

            if raw_after == "0" or int(raw_after) <= bal_before:
                print(f"[Confirm] ⚠️  {sym}: balance không tăng — dùng giá DexScreener")
                _send_tg_buy_signal()
                return

            # delta = số token thực nhận được (loại bỏ số dư cũ)
            bal_after    = int(raw_after)
            delta_raw    = bal_after - bal_before
            delta_tokens = delta_raw / (10 ** decimals)

            # ── Bước 4: tính actual_price (USD/token) ─────────────────
            #
            # Solana: spend_amount = SOL thực chi (float)
            #   → cần × giá SOL/USD để ra USD
            #   → actual_price_usd = (sol_spent × sol_usd) / delta_tokens
            #
            # Base: spend_amount = USDC thực chi → dùng trực tiếp
            #   → actual_price_usd = usdc_spent / delta_tokens
            #
            # Đảm bảo actual_entry_price cùng đơn vị với get_price_usd()
            # (USD/token) để monitor so sánh không bị lệch.
            est_price = token.get("price_usd", 0)

            if chain == "solana":
                sol_usd = get_sol_price_usd()
                if sol_usd > 0:
                    usd_spent = usdc_spent_actual * sol_usd
                    print(f"[Confirm] 💲 {sym}: {usdc_spent_actual:.6f} SOL × ${sol_usd:.2f} = ${usd_spent:.4f}")
                else:
                    # Fallback: ngược lại từ giá DexScreener nếu không lấy được giá SOL
                    if est_price > 0 and delta_tokens > 0:
                        usd_spent = est_price * delta_tokens
                        print(f"[Confirm] ⚠️  {sym}: không lấy được giá SOL → ước USD từ DexScreener")
                    else:
                        usd_spent = usdc_spent_actual   # worst-case giữ nguyên
                        print(f"[Confirm] ⚠️  {sym}: fallback usd_spent = sol_spent (không chính xác)")
            else:
                usd_spent = usdc_spent_actual   # Base: đã là USDC

            actual_price    = usd_spent / delta_tokens   # USD/token — cùng đơn vị get_price_usd()
            slippage_actual = ((actual_price / est_price - 1) * 100) if est_price > 0 else 0

            print(
                f"[Confirm] ✅ {sym}\n"
                f"           Giá ước tính : ${est_price:.10f}\n"
                f"           Giá thực tế  : ${actual_price:.10f}  (USD/token)\n"
                f"           Slippage thực: {slippage_actual:+.2f}%\n"
                f"           {'SOL' if chain == 'solana' else 'USDC'} tiêu : {usdc_spent_actual:.6f}\n"
                f"           USD quy đổi  : ${usd_spent:.4f}\n"
                f"           Token nhận   : {delta_tokens:.6f}\n"
                f"           TP target    : ${actual_price * (1 + TAKE_PROFIT_PCT/100):.10f}"
            )

            # ── Bước 5: lưu DB với giá thực ──────────────────────────
            db_update_actual_entry(
                addr, actual_price, delta_tokens, str(delta_raw), decimals,
                native_spent=usdc_spent_actual   # SOL (Solana) hoặc USDC (Base)
            )

            token["actual_entry_price"] = actual_price
            token["token_amount"]       = delta_tokens
            token["slippage_actual"]    = slippage_actual
            token["usdc_spent_actual"]  = usdc_spent_actual   # SOL hoặc USDC thô
            token["usd_spent_actual"]   = usd_spent           # USD quy đổi (dùng cho TP P&L)

            # ── Ghi lịch sử mua vào trade_history ────────────────
            db_log_buy(
                token, sig,
                native_spent   = usdc_spent_actual,
                usd_spent      = usd_spent,
                token_amount   = delta_tokens,
                buy_price_actual = actual_price,
                slippage_pct   = slippage_actual,
            )
            _send_tg_buy_signal()

        except Exception as e:
            print(f"[Confirm] ❌ {sym}: {e}\n{_tb.format_exc()}")
            _send_tg_buy_signal()

    def _send_tg_buy_signal():
        try:
            send_buy_signal(token, score, token.get("_score_detail", []), sig)
        except Exception as e:
            print(f"[Buyer] ⚠️  Telegram lỗi: {e}")

    threading.Thread(
        target=_confirm_entry, daemon=True, name=f"confirm-{addr[:8]}"
    ).start()

def buyer_thread(stop_event: threading.Event):
    print("[Buyer] 🟢 Bắt đầu mua...")
    while not stop_event.is_set():
        try:
            token = BUY_QUEUE.get(timeout=1)
            # Mỗi lệnh mua chạy trong thread riêng — buyer_thread không block
            threading.Thread(
                target=_execute_buy, args=(token,),
                daemon=True, name=f"buy-{token['address'][:8]}"
            ).start()
        except Empty:
            continue
        except Exception as e:
            print(f"[Buyer] ❌ {e}")

# ================================================================
# THREAD 4 — MONITOR (TP)
# ================================================================

def _is_hold_position(pos: dict) -> bool:
    """
    Returns True if this position should NOT be sold (held forever).

    Rules:
      SELL_MODE="sell"  → always False (never hold)
      SELL_MODE="hold"  → always True  (never sell)
      SELL_MODE="smart" → True only if opp_score >= HOLD_MIN_SCORE
    """
    if SELL_MODE == "hold":
        return True
    if SELL_MODE == "smart":
        score = int(pos.get("opp_score", 0))
        return score >= HOLD_MIN_SCORE
    return False   # default "sell" mode


# Track mints đã gửi HOLD alert để không lặp lại mỗi vòng monitor
# Key: "{mint}:{reason}"  (rug / fast_dump / tp)
# Value: timestamp lần alert gần nhất
_hold_alerted: Dict[str, float] = {}
_HOLD_ALERT_COOLDOWN_S = 3600   # 1 tiếng mới alert lại (nếu giá vẫn còn xấu)


def _hold_alert_once(mint: str, reason: str, msg: str) -> bool:
    """
    Gửi Telegram alert cho HOLD position — chỉ 1 lần mỗi reason, không lặp.
    Trả về True nếu đã gửi, False nếu bỏ qua (cooldown chưa hết).
    """
    key  = f"{mint}:{reason}"
    now  = time.time()
    last = _hold_alerted.get(key, 0)
    if now - last < _HOLD_ALERT_COOLDOWN_S:
        return False   # đã báo rồi, im lặng
    _hold_alerted[key] = now
    _alert(msg)
    return True


def _sell_position(pos: dict, reason: str = "TP") -> Optional[str]:
    """
    Bán toàn bộ token cho 1 position. Thread-safe, non-blocking.
    reason: "TP" | "RUG" | "FAST_DUMP"
    Trả về TX signature nếu gửi được lệnh bán, None nếu thất bại.
    """
    mint  = pos["mint"]
    sym   = pos["symbol"]
    chain = pos.get("chain", "solana")
    # Sell target: SOL for Solana, USDC for Base
    to_token = BASE_USDC if chain == "base" else SOL_NATIVE

    raw = get_token_raw_balance(mint, chain=chain)
    if raw == "0":
        print(f"  [Monitor] ⚠️  {sym}: balance=0 → đóng position")
        db_close_position(mint)
        return "balance_zero"   # treat as success

    sig = execute_swap(mint, to_token, raw, label=f"{reason} {sym}", chain=chain)
    return sig   # str or None


def monitor_thread(stop_event: threading.Event):
    print("[Monitor] 🟢 Bắt đầu theo dõi giá (TP + Rug + Fast Dump + EMA)...")
    while not stop_event.is_set():
        try:
            positions = db_get_positions()
            if not positions:
                stop_event.wait(TP_CHECK_INTERVAL)
                continue

            print(f"\n[Monitor] Kiểm tra {len(positions)} vị thế... | {_risk_manager.status()}")

            # ── Adaptive TP Check: tính interval ngắn nhất cần thiết ──
            # Nếu có bất kỳ token "mới" nào thì dùng interval ngắn hơn
            min_age_held = None
            for pos in positions:
                age_s = int(time.time()) - pos.get("entry_time", int(time.time()))
                entry_age = pos.get("token_age_minutes")  # tuổi token lúc mua
                # Dùng entry_age nếu có, nếu không thì ước từ thời gian hold
                token_age_est = (entry_age or 0) + age_s / 60
                if min_age_held is None or token_age_est < min_age_held:
                    min_age_held = token_age_est

            if min_age_held is not None and min_age_held < 5:
                adaptive_interval = TP_CHECK_INTERVAL_NEW
            elif min_age_held is not None and min_age_held < 30:
                adaptive_interval = TP_CHECK_INTERVAL_MID
            else:
                adaptive_interval = TP_CHECK_INTERVAL_OLD

            for pos in positions:
                mint      = pos["mint"]
                sym       = pos["symbol"]
                chain     = pos.get("chain", "solana")
                confirmed = bool(pos.get("price_confirmed", 0))

                cur = get_price_usd(mint, chain=chain)
                if cur <= 0:
                    continue

                # ── Cập nhật PriceTracker ──────────────────────────────
                _price_tracker.update(mint, cur)

                # ── Fast Dump Detection (chạy TRƯỚC mọi check khác) ───
                _fd_window = float(cfg("FAST_DUMP_WINDOW_S") or FAST_DUMP_WINDOW_S)
                _fd_pct    = float(cfg("FAST_DUMP_PCT")      or FAST_DUMP_PCT)
                fast_dump = _price_tracker.fast_dump_pct(mint, window_s=_fd_window)
                if fast_dump <= _fd_pct:
                    if _is_hold_position(pos):
                        chain_name = "SOLANA" if chain == "solana" else "BASE"
                        hold_tag   = _t("HOLD mode — manual action required!",
                                        "Chế độ HOLD — cần xử lý thủ công!")
                        sent = _hold_alert_once(mint, "fast_dump",
                            f"⚡🟡 <b>FAST DUMP [{chain_name}] — {hold_tag}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🪙 <b>${sym}</b>  <code>{mint}</code>\n\n"
                            f"📉 {_t('Fast drop','Giảm nhanh')}: <b>{fast_dump:.1f}%</b> {_t('in','trong')} {FAST_DUMP_WINDOW_S:.0f}s\n"
                            f"🔒 {_t('Bot is holding (score','Bot giữ nguyên (score')} "
                            f"{pos.get('opp_score',0)}{_t(')',')')}"
                        )
                        if sent:
                            print(f"  [Monitor] ⚡ FAST DUMP {sym} {_fmt_pct(fast_dump)} — "
                                  f"HOLD mode (score={pos.get('opp_score',0)}) → alert sent (1 lần)")
                        else:
                            print(f"  [Monitor] ⚡ FAST DUMP {sym} — HOLD, alert đã gửi rồi, bỏ qua")
                        continue

                    print(f"  [Monitor] ⚡ FAST DUMP: {sym} {_fmt_pct(fast_dump)} trong "
                          f"{FAST_DUMP_WINDOW_S:.0f}s → BÁN NGAY")
                    entry_px = float(pos.get("actual_entry_price") or pos.get("entry_price", 0))
                    total_loss = (entry_px * float(pos.get("token_amount", 0)) *
                                  (fast_dump / 100)) if entry_px > 0 else 0
                    chain_name = "SOLANA" if chain == "solana" else "BASE"
                    _alert(
                        f"⚡🔴 <b>FAST DUMP [{chain_name}]</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🪙 <b>${sym}</b>  <code>{mint}</code>\n\n"
                        f"📉 Giảm nhanh: <b>{fast_dump:.1f}%</b> trong {FAST_DUMP_WINDOW_S:.0f}s\n"
                        f"🔴 <b>Bot bán khẩn cấp!</b>"
                    )
                    sell_sig_fd = _sell_position(pos, reason="FAST_DUMP")
                    ok = bool(sell_sig_fd)
                    native_spent = float(pos.get("usdc_spent", BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC))
                    _risk_manager.record_close(-native_spent * abs(fast_dump) / 100)
                    _price_tracker.remove(mint)
                    if ok:
                        db_log_sell(mint, sell_sig_fd or "", "FAST_DUMP", cur, 0.0)
                    db_add_perm_ban(mint, f"fast_dump={fast_dump:.1f}%")
                    db_close_position(mint)
                    if not ok:
                        send_error_alert(f"❌ Bán Fast Dump thất bại: {sym}\n"
                                         f"<code>{mint}</code> — bán thủ công ngay!")
                    continue

                # ── EMA Downtrend Alert (log, chưa bán tự động) ────────
                if _price_tracker.is_downtrend(mint):
                    ema_now = _price_tracker.get_ema(mint)
                    print(f"  [Monitor] ⚠️  {sym} EMA downtrend | EMA=${ema_now:.10f}")

                # ── Chưa confirm giá thực ──────────────────────────────
                # Chỉ check rug bằng giá DexScreener, KHÔNG trigger TP
                if not confirmed:
                    est_entry = float(pos.get("entry_price", 0))
                    if est_entry > 0:
                        drop_pct = (cur / est_entry - 1) * 100
                        if drop_pct <= RUG_PRICE_DROP_1H:
                            if _is_hold_position(pos):
                                chain_name = "SOLANA" if chain == "solana" else "BASE"
                                sent = _hold_alert_once(mint, "rug_preconfirm",
                                    f"🚨🟡 <b>RUG WARNING [{chain_name}]</b>\n"
                                    f"━━━━━━━━━━━━━━━━━━━━\n"
                                    f"🪙 <b>${sym}</b>\n"
                                    f"<code>{mint}</code>\n\n"
                                    f"📉 {_t('Drop','Giảm')}: <b>{drop_pct:.1f}%</b>\n"
                                    f"🔒 {_t('HOLD mode — NOT selling (score','Chế độ HOLD — KHÔNG bán (score')} "
                                    f"{pos.get('opp_score',0)})\n"
                                    f"⚠️ {_t('Manual action may be needed!','Có thể cần xử lý thủ công!')}"
                                )
                                if sent:
                                    print(f"  [Monitor] 🚨 RUG (pre-confirm) {sym} {_fmt_pct(drop_pct)} — "
                                          f"HOLD → alert sent (1 lần)")
                                else:
                                    print(f"  [Monitor] 🚨 RUG (pre-confirm) {sym} — HOLD, đã báo rồi")
                                continue

                            print(f"  [Monitor] 🚨 RUG (pre-confirm): {sym} {_fmt_pct(drop_pct)}")
                            _alert(
                                f"🚨 <b>RUG DETECTED! [{chain.upper()}]</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🪙 <b>${sym}</b>\n"
                                f"<code>{mint}</code>\n\n"
                                f"📉 Giảm: <b>{drop_pct:.1f}%</b>\n"
                                f"⚡ Trạng thái: Chưa confirm giá\n"
                                f"🔴 <b>Bot đang bán khẩn cấp!</b>"
                            )
                            sell_sig_rug = _sell_position(pos, reason="RUG")
                            ok = bool(sell_sig_rug)
                            native_spent = float(pos.get("usdc_spent", BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC))
                            _risk_manager.record_close(-native_spent * abs(drop_pct) / 100)
                            _price_tracker.remove(mint)
                            if ok:
                                db_log_sell(mint, sell_sig_rug or "", "RUG", cur, 0.0)
                            db_add_perm_ban(mint, f"rug_detected drop={drop_pct:.1f}%")
                            db_close_position(mint)
                            if not ok:
                                send_error_alert(
                                    f"❌ Bán RUG thất bại: {sym}\n"
                                    f"<code>{mint}</code> — bán thủ công ngay!")
                        else:
                            print(f"  [Monitor] ⏳ {sym:<10} [{chain}] chờ confirm giá | "
                                  f"DexPrice: ${cur:.10f} | {_fmt_pct(drop_pct)} vs est")
                    continue  # Không trigger TP cho đến khi actual_price sẵn sàng

                # ── Đã confirm: dùng actual_entry_price ───────────────
                entry = float(pos["actual_entry_price"])
                if entry <= 0:
                    print(f"  [Monitor] ⚠️  {sym}: actual_entry_price=0, skip")
                    continue

                pct       = (cur / entry - 1) * 100
                drop_pct  = pct
                _tp_pct   = float(cfg("TAKE_PROFIT_PCT") or TAKE_PROFIT_PCT)
                tp_target = entry * (1 + _tp_pct / 100)
                ema_val   = _price_tracker.get_ema(mint)

                print(f"  [Monitor] {sym:<10} [{chain}] | "
                      f"Entry: ${entry:.10f} | Cur: ${cur:.10f} | EMA: ${ema_val:.10f} | "
                      f"{_fmt_pct(pct)} | TP@${tp_target:.10f}")

                # ── RUG DETECTION: drop >= ngưỡng ─────────────────
                if drop_pct <= RUG_PRICE_DROP_1H:
                    chain_name = "SOLANA" if chain == "solana" else "BASE"
                    scan_url   = (f"https://basescan.org/token/{mint}" if chain == "base"
                                  else f"https://solscan.io/token/{mint}")

                    if _is_hold_position(pos):
                        sent = _hold_alert_once(mint, "rug_confirmed",
                            f"🚨🟡 <b>RUG WARNING [{chain_name}]</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🪙 <b>${sym}</b>\n"
                            f"<code>{mint}</code>\n\n"
                            f"📉 {_t('Drop','Giảm')}: <b>{drop_pct:.1f}%</b>  "
                            f"({_t('threshold','ngưỡng')}: {RUG_PRICE_DROP_1H:.0f}%)\n"
                            f"🔒 {_t('HOLD mode — NOT selling (score','Chế độ HOLD — KHÔNG bán (score')} "
                            f"{pos.get('opp_score',0)})\n"
                            f"⚠️ {_t('Manual action may be needed!','Có thể cần xử lý thủ công!')}\n\n"
                            f"<a href='{scan_url}'>🔍 Explorer</a>"
                        )
                        if sent:
                            print(f"  [Monitor] 🚨 RUG {sym} {_fmt_pct(drop_pct)} — "
                                  f"HOLD (score={pos.get('opp_score',0)}) → alert sent (1 lần)")
                        else:
                            print(f"  [Monitor] 🚨 RUG {sym} — HOLD, đã báo rồi")
                        continue

                    print(f"  [Monitor] 🚨 RUG DETECTED: {sym} {_fmt_pct(drop_pct)} → BÁN NGAY")
                    _alert(
                        f"🚨🔴 <b>RUG DETECTED! [{chain_name}]</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🪙 <b>${sym}</b>\n"
                        f"<code>{mint}</code>\n\n"
                        f"📉 Giảm: <b>{drop_pct:.1f}%</b>  (ngưỡng: {RUG_PRICE_DROP_1H:.0f}%)\n"
                        f"💀 <b>Bot đang bán cắt lỗ ngay!</b>\n\n"
                        f"<a href='{scan_url}'>🔍 Explorer</a>"
                    )
                    sell_sig_rug = _sell_position(pos, reason="RUG")
                    ok = bool(sell_sig_rug)
                    native_spent = float(pos.get("usdc_spent", BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC))
                    _risk_manager.record_close(-native_spent * abs(drop_pct) / 100)
                    _price_tracker.remove(mint)
                    if ok:
                        db_log_sell(mint, sell_sig_rug or "", "RUG", cur, 0.0)
                    # Dù bán được hay không, cấm vĩnh viễn
                    db_add_perm_ban(mint, f"rug_detected drop={drop_pct:.1f}%")
                    db_close_position(mint)
                    if not ok:
                        send_error_alert(
                            f"❌ Bán RUG thất bại: {sym}\n"
                            f"<code>{mint}</code> — bán thủ công ngay!")
                    continue

                # ── TAKE PROFIT + TRAILING STOP ────────────────────
                #
                # Logic:
                #   - pct >= TRAIL_TARGET_PCT (30%) → chốt ngay lập tức
                #   - pct >= TRAIL_TRIGGER_PCT (20%) → bắt đầu trailing:
                #       * Ghi nhớ peak_price
                #       * Mỗi tick: nếu cur > peak → cập nhật peak
                #       * Nếu cur < peak (giá tick này < tick trước) → chốt ngay
                #   - pct < TRAIL_TRIGGER_PCT → chưa trailing, chờ tiếp
                #
                should_sell_tp = False
                tp_reason      = ""

                if pct >= TP2_PCT:
                    # Đạt TP2 → chốt luôn
                    should_sell_tp = True
                    tp_reason      = f"🎯 TP2 +{pct:.1f}% (≥{TP2_PCT:.0f}%)"
                    _trailing_peaks.pop(mint, None)
                    _tp1_alerted.discard(mint)

                elif pct >= TRAIL_TRIGGER_PCT:
                    # Vào vùng trailing (TRAILING_START)
                    if mint not in _trailing_peaks:
                        _trailing_peaks[mint] = cur
                        if mint not in _tp1_alerted and pct >= TP1_PCT:
                            _tp1_alerted.add(mint)
                            print(f"  [Monitor] 🥇 {sym}: chạm TP1 +{pct:.1f}% (≥{TP1_PCT:.0f}%)")
                        print(f"  [Monitor] 🔔 {sym}: +{pct:.1f}% → BẮT ĐẦU TRAILING "
                              f"(peak=${cur:.10f}, stop={TRAILING_STOP_PCT:.0f}%, TP2={TP2_PCT:.0f}%)")
                    else:
                        prev_peak = _trailing_peaks[mint]
                        if cur > prev_peak:
                            _trailing_peaks[mint] = cur
                            print(f"  [Monitor] 📈 {sym}: trailing peak cập nhật "
                                  f"${prev_peak:.10f} → ${cur:.10f} (+{pct:.1f}%)")
                        else:
                            drop_from_peak_pct = ((prev_peak - cur) / prev_peak * 100) if prev_peak > 0 else 0
                            if drop_from_peak_pct >= TRAILING_STOP_PCT:
                                should_sell_tp = True
                                tp_reason      = (f"🔔 Trailing stop {drop_from_peak_pct:.1f}% "
                                                  f"(≥{TRAILING_STOP_PCT:.0f}%) từ peak ${prev_peak:.10f}")
                                _trailing_peaks.pop(mint, None)
                                _tp1_alerted.discard(mint)
                else:
                    _trailing_peaks.pop(mint, None)
                    _tp1_alerted.discard(mint)

                if should_sell_tp:
                    # ── HOLD mode: alert but do not sell ──────────────
                    if _is_hold_position(pos):
                        chain_name = "SOLANA" if chain == "solana" else "BASE"
                        _hold_alert_once(mint, f"tp_{pct:.0f}",
                            f"{'🟣' if chain == 'solana' else '🔵'}🔒 "
                            f"<b>{_t('TP TARGET HIT','TP ĐẠT MỤC TIÊU')} [{chain_name}] — "
                            f"{_t('HOLDING','GIỮ NGUYÊN')}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🪙 <b>${sym}</b>  <code>{mint}</code>\n\n"
                            f"📈 {_t('Gain','Lãi')}: <b>+{pct:.1f}%</b>  |  "
                            f"{_t('Price','Giá')}: <code>${cur:.10f}</code>\n"
                            f"🔒 {_t('HOLD mode (score','Chế độ HOLD (score')} "
                            f"{pos.get('opp_score',0)}{_t(' ≥ ','≥')}{HOLD_MIN_SCORE if SELL_MODE == 'smart' else '—'})\n"
                            f"💎 {_t('Holding — manual sell when ready','Giữ — tự bán thủ công khi muốn')}"
                        )
                        print(f"  [Monitor] 🔒 {sym} TP +{pct:.1f}% — "
                              f"HOLD (score={pos.get('opp_score',0)}) → không bán")
                        _trailing_peaks.pop(mint, None)   # reset trailing
                        continue

                    print(f"  [Monitor] {tp_reason} → BÁN")
                    to_token = BASE_USDC if chain == "base" else SOL_NATIVE
                    raw  = get_token_raw_balance(mint, chain=chain)
                    if raw == "0":
                        db_close_position(mint)
                        _price_tracker.remove(mint)
                        continue
                    sell_sig = execute_swap(mint, to_token, raw,
                                            label=f"SELL TP {sym}", chain=chain)
                    if sell_sig:
                        native_spent = float(pos.get("usdc_spent", BUY_AMOUNT_SOL if chain == "solana" else BUY_AMOUNT_USDC))
                        _risk_manager.record_close(native_spent * pct / 100)
                        _price_tracker.remove(mint)
                        # Log sell — native_received=0 tạm thời; send_tp_signal sẽ đọc TX thực
                        db_log_sell(mint, sell_sig, "TP", cur, 0.0)
                        db_mark_tp_sent(mint)
                        db_close_position(mint)
                        print(f"  [Monitor] ✅ {sym} TP xong | +{pct:.1f}% → tính lãi thực...")
                        def _send_tp_async(p=pos, cp=cur, pp=pct, ss=sell_sig):
                            try:
                                send_tp_signal(p, cp, pp, ss)
                            except Exception as _e:
                                print(f"[TP Signal] ❌ {_e}")
                        threading.Thread(
                            target=_send_tp_async, daemon=True,
                            name=f"tp-sig-{mint[:8]}"
                        ).start()
                    else:
                        send_error_alert(
                            f"❌ <b>Bán TP thất bại!</b>\n"
                            f"Token: {sym}\n<code>{mint}</code>\n"
                            f"Giá: +{pct:.1f}% — bán thủ công!"
                        )
        except Exception as e:
            import traceback
            print(f"[Monitor] ❌ {e}\n{traceback.format_exc()}")

        # ── Adaptive TP Check interval ──────────────────────────────
        _tp_int = float(cfg("TP_CHECK_INTERVAL_NEW") or TP_CHECK_INTERVAL_NEW) if adaptive_interval == TP_CHECK_INTERVAL_NEW \
             else float(cfg("TP_CHECK_INTERVAL_MID") or TP_CHECK_INTERVAL_MID) if adaptive_interval == TP_CHECK_INTERVAL_MID \
             else float(cfg("TP_CHECK_INTERVAL_OLD") or TP_CHECK_INTERVAL_OLD)
        stop_event.wait(_tp_int if positions else float(cfg("TP_CHECK_INTERVAL_OLD") or TP_CHECK_INTERVAL))

# ================================================================
# MAIN
# ================================================================

def main():
    # Init dynamic config từ .env
    _cfg_init()

    # ── Validate config bắt buộc (Solana + Telegram) ──────────────
    missing = [k for k, v in {
        "SOLANA_WALLET_ADDRESS": WALLET_ADDRESS,
        "SOLANA_PRIVATE_KEY":    WALLET_PRIVKEY,
        "TELEGRAM_BOT_TOKEN":    TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":      TELEGRAM_CHAT_ID,
    }.items() if not v]
    if missing:
        print(f"⚠️  Thiếu .env: {', '.join(missing)}")
        return

    # ── Cảnh báo nếu dùng Base nhưng thiếu config ─────────────────
    base_missing = [k for k, v in {
        "BASE_PRIVATE_KEY": BASE_WALLET_PRIVKEY,
        "OKX_API_KEY":      OKX_API_KEY,
        "OKX_SECRET_KEY":   OKX_SECRET_KEY,
        "OKX_API_PASSPHRASE": OKX_API_PASSPHRASE,
        "OKX_PROJECT_ID":   OKX_PROJECT_ID,
    }.items() if not v]
    if base_missing:
        print(f"⚠️  Base chain DISABLED (thiếu: {', '.join(base_missing)})")
        print("   Bot chỉ trade Solana. Thêm các key trên vào .env để bật Base.")

    # Init DB
    init_db()

    solana_ok = bool(WALLET_ADDRESS and WALLET_PRIVKEY)
    base_ok   = bool(BASE_WALLET_PRIVKEY and OKX_API_KEY and OKX_SECRET_KEY)
    chains_on = ("🟣 Solana" if solana_ok else "") + (" + 🔵 Base" if base_ok else "")

    print("=" * 65)
    print(f"🤖  MULTI-CHAIN TRADER v4.1  {chains_on}")
    print(f"    Solana swap: Jupiter API v6  |  Base swap: OKX DEX")
    print("=" * 65)
    print(f"  Wallet     : {WALLET_ADDRESS[:12]}...{WALLET_ADDRESS[-6:]}")
    print(f"  Jupiter    : {JUP_QUOTE_URL}")
    print(f"  Buy (SOL)  : {BUY_AMOUNT_SOL} SOL / trade  (Solana)")
    print(f"  Buy (Base) : {BUY_AMOUNT_USDC} USDC / trade (Base)")
    print(f"  Sell to    : SOL (Solana) / USDC (Base)")
    # Sell mode display
    if SELL_MODE == "hold":
        sell_mode_str = "🔒 HOLD — never sell (manual only)"
    elif SELL_MODE == "smart":
        sell_mode_str = f"💎 SMART — hold if score ≥ {HOLD_MIN_SCORE}, else sell normally"
    else:
        sell_mode_str = "💰 SELL — auto TP + rug + fast dump"
    print(f"  Sell mode  : {sell_mode_str}")
    print(f"  Language   : {'Vietnamese 🇻🇳' if _LANG == 'vi' else 'English 🇬🇧'} (LANG={_LANG})")
    print(f"  Min Score  : {MIN_SCORE}/100")
    print(f"  Signal Alert: score ≥ {SIGNAL_ALERT_MIN_SCORE} → thông báo bạn bè")
    print(f"  Take Profit: +{TAKE_PROFIT_PCT:.0f}%")
    print(f"  Slippage   : {SLIPPAGE_PCT}% (dyn: <5p={SLIPPAGE_TOKEN_NEW}%, <30p={SLIPPAGE_TOKEN_MID}%)")
    print(f"  Scan every : {SCAN_INTERVAL}s")
    print(f"  TP check   : adaptive {TP_CHECK_INTERVAL_NEW}s/{TP_CHECK_INTERVAL_MID}s/{TP_CHECK_INTERVAL_OLD}s")
    print(f"  Fast Dump  : {FAST_DUMP_PCT}% trong {FAST_DUMP_WINDOW_S:.0f}s → bán ngay")
    print(f"  EntryGuard : <{ENTRY_AGE_ULTRA_MIN:.0f}p → Dip{ENTRY_ULTRA_DIP_S:.0f}s+Vol | "
          f"{ENTRY_AGE_ULTRA_MIN:.0f}-{ENTRY_AGE_NEW_MIN:.0f}p → Dip{ENTRY_DIP_WINDOW_S:.0f}s+RSI | "
          f"{ENTRY_AGE_NEW_MIN:.0f}-{ENTRY_AGE_MID_MIN:.0f}p → BBL{ENTRY_BBL_WINDOW_S:.0f}s+MACD | "
          f">{ENTRY_AGE_MID_MIN:.0f}p → VWAP{ENTRY_VWAP_WINDOW_S:.0f}s")
    print(f"  Priority   : {PRIORITY_FEE_DEFAULT//1000}k/{PRIORITY_FEE_HIGH//1000}k/{PRIORITY_FEE_VERY_HIGH//1000}k µlamports (auto)")
    print(f"  Risk Mgr   : max {RISK_MAX_POSITIONS} vị thế | {RISK_MAX_DAILY_TRADES} lệnh/ngày | stop lỗ ${RISK_MAX_DAILY_LOSS_USD}")
    print(f"  Validators : {VALIDATOR_WORKERS} threads song song")
    print(f"  DB         : {DB_PATH} (SQLite)")
    print(f"  Signal →   : {', '.join(TELEGRAM_SIGNAL_CHANNELS) or '(chưa set)'}")
    print(f"  Positions  : {len(db_get_positions())} đang mở")
    print("=" * 65)
    print()
    print("  THREAD 1: Scanner   → TOKEN_QUEUE")
    print("  THREAD 2: Validator → BUY_QUEUE  (20 parallel)")
    print("  THREAD 3: Buyer     → Jupiter (SOL) / OKX (Base)")
    print("  THREAD 4: Monitor   → TP + Rug + Fast Dump + EMA")
    print("  THREAD 5: CmdHandler→ Telegram /block /sell /status /positions")
    print("=" * 65)

    stop = threading.Event()

    def _csv_export_thread(stop_event: threading.Event):
        """Export trade_history.csv tự động mỗi 24h."""
        import os as _os
        while not stop_event.is_set():
            stop_event.wait(86400)   # chờ 24 tiếng
            if stop_event.is_set():
                break
            try:
                path = db_export_csv("trade_history.csv")
                size = _os.path.getsize(path) if _os.path.exists(path) else 0
                print(f"[TradeLog] 🗓️  Auto-export: {path} ({size} bytes)")
            except Exception as _e:
                print(f"[TradeLog] ❌ Auto-export lỗi: {_e}")

    threads = [
        threading.Thread(target=scanner_thread,     args=(stop,), name="Scanner",   daemon=True),
        threading.Thread(target=validator_thread,   args=(stop,), name="Validator", daemon=True),
        threading.Thread(target=buyer_thread,       args=(stop,), name="Buyer",     daemon=True),
        threading.Thread(target=monitor_thread,     args=(stop,), name="Monitor",   daemon=True),
        threading.Thread(target=_csv_export_thread, args=(stop,), name="CSVExport", daemon=True),
        threading.Thread(target=telegram_cmd_thread,args=(stop,), name="CmdHandler",daemon=True),
    ]

    for t in threads:
        t.start()

    try:
        while all(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 Đang dừng...")
        stop.set()
        for t in threads:
            t.join(timeout=5)
        # Export CSV khi thoát
        try:
            path = db_export_csv("trade_history.csv")
            print(f"✅ Đã export lịch sử giao dịch → {path}")
        except Exception as _e:
            print(f"⚠️  Export CSV khi thoát thất bại: {_e}")
        print("✅ Đã dừng.")

# ================================================================
# TELEGRAM COMMAND HANDLER  (Inline Keyboard)
# ================================================================
# Giao diện nút bấm — user chỉ cần bấm, không cần gõ lệnh tay.
#
# Menu chính (/start hoặc /menu):
#   [📊 Trạng Thái]  [🚫 Block]    [📈 Báo Cáo]
#   [📋 Vị Thế]      [💰 Bán]      [⚙️ Cài Đặt]
#   [🔓 Unblock]                   [❓ Hướng Dẫn]
#
# Bảo mật: chỉ TELEGRAM_CHAT_ID (owner) được tương tác.
# callback_data dùng prefix "cb_" để phân biệt với text thường.
# ================================================================

# offset toàn cục để Telegram getUpdates không nhận lại tin cũ
_tg_cmd_offset: int = 0
_tg_cmd_lock = threading.Lock()

# Trạng thái đang chờ nhập liệu từ user (mint / value)
# {chat_id: {"action": "block"|"unblock"|"sell"|"set_key"|"set_val", "data": ...}}
_tg_state: Dict[str, dict] = {}
_tg_state_lock = threading.Lock()

# Set chứa mint đang pending sell thủ công (tránh double sell)
_manual_sell_pending: set = set()
_manual_sell_lock = threading.Lock()


# ── Telegram API helpers ──────────────────────────────────────────

def _tg_api(method: str, payload: dict) -> Optional[dict]:
    """Gọi Telegram Bot API. Trả về response JSON hoặc None nếu lỗi."""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            json=payload, timeout=10,
        )
        return r.json()
    except Exception as e:
        print(f"[CMD] ❌ _tg_api({method}): {e}")
        return None


def _reply(chat_id: str, text: str, keyboard: list = None):
    """
    Gửi tin nhắn, tuỳ chọn kèm inline keyboard.
    keyboard = list of rows, mỗi row = list of button dicts:
        [{"text": "Label", "callback_data": "cb_key"}]
    """
    payload = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    _tg_api("sendMessage", payload)


def _edit_reply(chat_id: str, message_id: int, text: str, keyboard: list = None):
    """Chỉnh sửa tin nhắn đã gửi (dùng khi user bấm nút callback)."""
    payload = {
        "chat_id":    chat_id,
        "message_id": message_id,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    _tg_api("editMessageText", payload)


def _answer_cb(callback_query_id: str, text: str = "", alert: bool = False):
    """Xác nhận callback query (bắt buộc để Telegram tắt loading)."""
    _tg_api("answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text":              text,
        "show_alert":        alert,
    })


# ── Menu chính ───────────────────────────────────────────────────

_MAIN_MENU_KB = [
    [
        {"text": "📊 Trạng Thái",  "callback_data": "cb_status"},
        {"text": "🚫 Block",        "callback_data": "cb_block"},
        {"text": "📈 Báo Cáo",     "callback_data": "cb_report"},
    ],
    [
        {"text": "📋 Vị Thế",      "callback_data": "cb_positions"},
        {"text": "💰 Bán Token",   "callback_data": "cb_sell"},
        {"text": "⚙️ Cài Đặt",    "callback_data": "cb_config"},
    ],
    [
        {"text": "🧪 Profile A/B", "callback_data": "cb_profiles"},
    ],
    [
        {"text": "🔓 Unblock",     "callback_data": "cb_unblock"},
        {"text": "💾 Lưu .env",    "callback_data": "cb_save"},
        {"text": "❓ Hướng Dẫn",   "callback_data": "cb_help"},
    ],
]

_BACK_KB = [[{"text": "🔙 Menu Chính", "callback_data": "cb_menu"}]]

_CONFIG_GROUP_KB = [
    [
        {"text": "💰 Trading",    "callback_data": "cb_cfg_trading"},
        {"text": "🛡 Risk",       "callback_data": "cb_cfg_risk"},
    ],
    [
        {"text": "⏱ Timing",     "callback_data": "cb_cfg_timing"},
        {"text": "📐 Slippage",   "callback_data": "cb_cfg_slippage"},
    ],
    [{"text": "🧪 Profile A/B", "callback_data": "cb_profiles"}],
    [{"text": "🔙 Menu Chính",   "callback_data": "cb_menu"}],
]


def _content_profiles() -> str:
    return (
        "🧪 <b>Profile A/B</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Chọn cấu hình chạy nhanh:\n"
        "• <b>⚡ Early Sniper</b> — phản ứng nhanh, fill cao\n"
        "• <b>🛡 Safe Trend</b> — an toàn hơn, giảm churn\n\n"
        "Lệnh text tương đương:\n"
        "<code>/profile early_sniper</code>\n"
        "<code>/profile safe_trend</code>\n"
        "Sau khi áp dụng, dùng <code>/save</code> để lưu .env."
    )


def _apply_profile_runtime(profile_name: str) -> Tuple[bool, str]:
    profile_key = (profile_name or "").strip().lower()
    preset = PROFILE_PRESETS.get(profile_key)
    if not preset:
        return False, "Profile không hợp lệ. Dùng: early_sniper | safe_trend"

    changed = []
    for key, val in preset.items():
        ok, msg = cfg_set(key, val)
        if ok:
            changed.append(key)
        else:
            return False, f"Không áp dụng được {key}: {msg}"

    return True, (
        f"✅ Đã áp dụng profile <b>{profile_key}</b> ({len(changed)} thông số).\n"
        "💾 Dùng /save để lưu vào .env."
    )


def _show_menu(chat_id: str, edit_msg_id: int = None):
    text = (
        "🤖 <b>MULTI-CHAIN TRADER</b> — Menu Điều Khiển\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Chọn chức năng bên dưới 👇"
    )
    if edit_msg_id:
        _edit_reply(chat_id, edit_msg_id, text, _MAIN_MENU_KB)
    else:
        _reply(chat_id, text, _MAIN_MENU_KB)


# ── Nội dung từng chức năng ──────────────────────────────────────

def _content_status() -> str:
    positions = db_get_positions()
    risk_txt  = _risk_manager.status()
    n_pos     = len(positions)
    sol_amt   = cfg("BUY_AMOUNT_SOL")   or BUY_AMOUNT_SOL
    usdc_amt  = cfg("BUY_AMOUNT_USDC")  or BUY_AMOUNT_USDC
    min_sc    = cfg("MIN_SCORE")         or MIN_SCORE
    tp_pct    = cfg("TAKE_PROFIT_PCT")   or TAKE_PROFIT_PCT
    fd_pct    = cfg("FAST_DUMP_PCT")     or FAST_DUMP_PCT
    fd_win    = cfg("FAST_DUMP_WINDOW_S")or FAST_DUMP_WINDOW_S
    scan_iv   = cfg("SCAN_INTERVAL")    or SCAN_INTERVAL
    return (
        "📊 <b>Trạng Thái Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Vị thế đang mở: <b>{n_pos}/{int(cfg('RISK_MAX_POSITIONS') or RISK_MAX_POSITIONS)}</b>\n"
        f"⚙️  Risk: {risk_txt}\n"
        f"📡 Scanner: mỗi <b>{float(scan_iv):.0f}s</b>\n"
        f"🎯 Min score: <b>{int(float(min_sc))}/100</b>\n"
        f"💰 Take Profit: <b>+{float(tp_pct):.0f}%</b>\n"
        f"🛡  Fast Dump: <b>{float(fd_pct):.0f}%</b> / {float(fd_win):.0f}s\n"
        f"🏦 SOL/trade: <b>{float(sol_amt)} SOL</b>\n"
        f"🏦 USDC/trade: <b>{float(usdc_amt)} USDC</b>\n"
        f"🔒 Sell mode: <b>{SELL_MODE.upper()}</b>"
    )


def _content_positions() -> tuple:
    """Trả về (text, keyboard) cho màn hình Vị Thế."""
    positions = db_get_positions()
    if not positions:
        return "📭 <b>Không có vị thế nào đang mở.</b>", _BACK_KB

    lines = [f"📋 <b>Vị Thế Đang Hold ({len(positions)})</b>", "━━━━━━━━━━━━━━━━━━━━"]
    sell_buttons = []
    for pos in positions:
        mint     = pos["mint"]
        sym      = pos.get("symbol", "?")
        chain    = pos.get("chain", "solana")
        entry_p  = float(pos.get("entry_price") or 0)
        spent    = float(pos.get("usdc_spent") or 0)
        score    = pos.get("opp_score", 0)
        entry_ts = pos.get("entry_time", 0)
        hold_dur = _duration_str(entry_ts) if entry_ts else "N/A"

        cur_price = get_price_usd(mint, chain=chain)
        if entry_p > 0 and cur_price > 0:
            pct     = (cur_price / entry_p - 1) * 100
            pct_str = f"{'🟢' if pct >= 0 else '🔴'}{pct:+.1f}%"
        else:
            pct_str = "⏳ chờ"

        chain_e = "🟣" if chain == "solana" else "🔵"
        lines.append(
            f"\n{chain_e} <b>${sym}</b>  Score:{score}  |  {pct_str}  |  {hold_dur}\n"
            f"   Entry: <code>${entry_p:.8f}</code>  Nay: <code>${cur_price:.8f}</code>\n"
            f"   Spent: {spent} {'SOL' if chain=='solana' else 'USDC'}\n"
            f"   <code>{mint}</code>"
        )
        # Nút bán nhanh cho từng token
        sell_buttons.append({"text": f"💰 Bán ${sym}", "callback_data": f"cb_sell_{mint}"})

    # Chia sell buttons thành rows 2 cột
    kb = []
    for i in range(0, len(sell_buttons), 2):
        kb.append(sell_buttons[i:i+2])
    kb.append([{"text": "🔙 Menu Chính", "callback_data": "cb_menu"}])

    return "\n".join(lines), kb


def _content_config(group: str) -> str:
    """Nội dung cài đặt cho 1 nhóm."""
    groups = {
        "trading":  ["BUY_AMOUNT_SOL","BUY_AMOUNT_USDC","MIN_SCORE","TAKE_PROFIT_PCT","MIN_LIQUIDITY_USD"],
        "risk":     ["RISK_MAX_POSITIONS","RISK_MAX_DAILY_TRADES","RISK_MAX_DAILY_LOSS"],
        "timing":   ["SCAN_INTERVAL","TP_CHECK_INTERVAL_NEW","TP_CHECK_INTERVAL_MID",
                     "TP_CHECK_INTERVAL_OLD","FAST_DUMP_PCT","FAST_DUMP_WINDOW_S"],
        "slippage": ["SLIPPAGE_PCT","SLIPPAGE_TOKEN_NEW","SLIPPAGE_TOKEN_MID",
                     "PRIORITY_FEE_DEFAULT","PRIORITY_FEE_HIGH","PRIORITY_FEE_VERY_HIGH"],
    }
    emoji = {"trading":"💰","risk":"🛡","timing":"⏱","slippage":"📐"}
    label = {"trading":"Trading","risk":"Risk","timing":"Timing","slippage":"Slippage & Fee"}

    keys  = groups.get(group, [])
    lines = [f"{emoji[group]} <b>Cài Đặt {label[group]}</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for key in keys:
        val    = cfg(key)
        schema = _CFG_SCHEMA.get(key, {})
        desc   = schema.get("desc","")
        mn, mx = schema.get("min",""), schema.get("max","")
        val_str = str(int(float(val))) if schema.get("type") == int else str(val)
        lines.append(f"  <code>{key}</code> = <b>{val_str}</b>\n    <i>{desc} [{mn}~{mx}]</i>")

    lines.append("\n━━━━━━━━━━━━━━━━━━━━")
    lines.append("✏️ Để thay đổi, gõ:\n<code>/set KEY GIÁ_TRỊ</code>")
    lines.append("\nVí dụ:\n<code>/set BUY_AMOUNT_SOL 0.05</code>")
    return "\n".join(lines)


def _content_report() -> str:
    """Báo cáo giao dịch tổng hợp từ trade_history."""
    try:
        with _db_lock:
            conn = _get_conn()
            today_ts = int(time.time()) - 86400
            rows = conn.execute("""
                SELECT sell_reason,
                       COUNT(*) as cnt,
                       SUM(net_profit_native) as total_pnl,
                       AVG(net_profit_pct) as avg_pct,
                       SUM(CASE WHEN net_profit_native > 0 THEN 1 ELSE 0 END) as wins
                FROM trade_history
                WHERE sell_time IS NOT NULL AND buy_time >= ?
                GROUP BY sell_reason
            """, (today_ts,)).fetchall()

            total_row = conn.execute("""
                SELECT COUNT(*) as total_trades,
                       SUM(net_profit_native) as total_pnl,
                       AVG(net_profit_pct) as avg_pct,
                       SUM(CASE WHEN net_profit_native > 0 THEN 1 ELSE 0 END) as wins,
                       MIN(net_profit_pct) as worst,
                       MAX(net_profit_pct) as best
                FROM trade_history
                WHERE sell_time IS NOT NULL AND buy_time >= ?
            """, (today_ts,)).fetchone()
            conn.close()

        if not total_row or total_row["total_trades"] == 0:
            return "📈 <b>Báo Cáo 24h</b>\n━━━━━━━━━━━━━━━━━━━━\n📭 Chưa có giao dịch nào trong 24h."

        total  = total_row["total_trades"] or 0
        pnl    = float(total_row["total_pnl"] or 0)
        avg    = float(total_row["avg_pct"] or 0)
        wins   = int(total_row["wins"] or 0)
        worst  = float(total_row["worst"] or 0)
        best   = float(total_row["best"] or 0)
        wr     = wins / total * 100 if total > 0 else 0
        pnl_e  = "🟢" if pnl >= 0 else "🔴"

        lines = [
            "📈 <b>Báo Cáo 24h</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"📊 Tổng giao dịch: <b>{total}</b>",
            f"🏆 Win rate: <b>{wr:.1f}%</b>  ({wins}W/{total-wins}L)",
            f"{pnl_e} PnL: <b>{pnl:+.4f}</b> native",
            f"📉 Tệ nhất: <b>{worst:+.1f}%</b>  |  📈 Tốt nhất: <b>{best:+.1f}%</b>",
            f"〽️ Trung bình: <b>{avg:+.1f}%</b>",
            "\n<b>Theo loại:</b>",
        ]
        reason_e = {"TP":"💰","RUG":"🚨","FAST_DUMP":"⚡","MANUAL":"👤"}
        for row in rows:
            reason = row["sell_reason"] or "?"
            e      = reason_e.get(reason, "❓")
            r_pnl  = float(row["total_pnl"] or 0)
            lines.append(f"  {e} {reason}: {row['cnt']} lệnh | {r_pnl:+.4f} native")

        return "\n".join(lines)
    except Exception as ex:
        return f"❌ Lỗi báo cáo: {ex}"


def _content_guide() -> str:
    return (
        "❓ <b>Hướng Dẫn Sử Dụng</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📌 <b>Các nút chính:</b>\n"
        "• <b>📊 Trạng Thái</b> — Xem PnL, risk, thông số đang chạy\n"
        "• <b>📋 Vị Thế</b> — Danh sách token đang hold + nút Bán nhanh\n"
        "• <b>📈 Báo Cáo</b> — Thống kê giao dịch 24h (win rate, PnL)\n"
        "• <b>⚙️ Cài Đặt</b> — Xem và điều chỉnh thông số bot\n"
        "• <b>💰 Bán Token</b> — Nhập địa chỉ token để bán thủ công\n"
        "• <b>🚫 Block</b> — Blacklist vĩnh viễn 1 token\n"
        "• <b>🔓 Unblock</b> — Gỡ token khỏi blacklist\n"
        "• <b>💾 Lưu .env</b> — Lưu cài đặt hiện tại để giữ khi restart\n\n"
        "✏️ <b>Lệnh text vẫn hoạt động:</b>\n"
        "<code>/set KEY VALUE</code> — Thay đổi thông số ngay\n"
        "<code>/save</code> — Lưu vào .env\n"
        "<code>/block MINT</code> — Block nhanh\n"
        "<code>/sell MINT</code> — Bán nhanh\n"
        "<code>/profile early_sniper|safe_trend</code> — Đổi profile A/B\n<code>/scores [N]</code> — Xem thống kê điểm token vừa quét\n\n"
        "⚙️ <b>Ví dụ /set:</b>\n"
        "<code>/set BUY_AMOUNT_SOL 0.05</code>\n"
        "<code>/set MIN_SCORE 80</code>\n"
        "<code>/set TAKE_PROFIT_PCT 50</code>\n"
        "<code>/set FAST_DUMP_PCT -20</code>\n"
        "<code>/set RISK_MAX_POSITIONS 3</code>\n\n"
        "⚠️ <i>Chỉ owner (TELEGRAM_CHAT_ID) mới điều khiển được bot.</i>"
    )


# ── Xử lý callback (nút bấm) ─────────────────────────────────────

def _handle_callback(cb: dict):
    """Xử lý 1 callback_query từ Telegram."""
    cb_id   = cb["id"]
    chat_id = str(cb["from"]["id"])
    msg_id  = cb["message"]["message_id"]
    data    = cb.get("data", "")

    # Bảo mật
    if chat_id != str(TELEGRAM_CHAT_ID):
        _answer_cb(cb_id, "⛔ Không có quyền!", alert=True)
        return

    _answer_cb(cb_id)   # tắt loading ngay

    # ── Menu chính ────────────────────────────────────────────────
    if data == "cb_menu":
        _show_menu(chat_id, edit_msg_id=msg_id)

    # ── Trạng thái ────────────────────────────────────────────────
    elif data == "cb_status":
        _edit_reply(chat_id, msg_id, _content_status(), _BACK_KB)

    # ── Báo cáo ───────────────────────────────────────────────────
    elif data == "cb_report":
        _edit_reply(chat_id, msg_id, _content_report(), _BACK_KB)

    # ── Vị thế ────────────────────────────────────────────────────
    elif data == "cb_positions":
        text, kb = _content_positions()
        _edit_reply(chat_id, msg_id, text, kb)

    # ── Hướng dẫn ─────────────────────────────────────────────────
    elif data == "cb_help":
        _edit_reply(chat_id, msg_id, _content_guide(), _BACK_KB)

    # ── Lưu .env ──────────────────────────────────────────────────
    elif data == "cb_save":
        ok, msg_txt = cfg_save_env()
        icon  = "💾" if ok else "❌"
        _edit_reply(chat_id, msg_id,
                    f"{icon} <b>{'Đã lưu!' if ok else 'Lỗi:'}</b>\n{msg_txt}",
                    _BACK_KB)

    # ── Cài đặt — chọn nhóm ───────────────────────────────────────
    elif data == "cb_config":
        _edit_reply(chat_id, msg_id,
                    "⚙️ <b>Cài Đặt</b> — Chọn nhóm thông số:",
                    _CONFIG_GROUP_KB)

    elif data in ("cb_cfg_trading","cb_cfg_risk","cb_cfg_timing","cb_cfg_slippage"):
        group = data.replace("cb_cfg_","")
        kb    = [[{"text": "🔙 Cài Đặt", "callback_data": "cb_config"},
                  {"text": "🏠 Menu",    "callback_data": "cb_menu"}]]
        _edit_reply(chat_id, msg_id, _content_config(group), kb)

    elif data == "cb_profiles":
        kb = [
            [{"text": "⚡ Early Sniper", "callback_data": "cb_profile_early_sniper"}],
            [{"text": "🛡 Safe Trend",  "callback_data": "cb_profile_safe_trend"}],
            [{"text": "🔙 Cài Đặt", "callback_data": "cb_config"},
             {"text": "🏠 Menu",   "callback_data": "cb_menu"}],
        ]
        _edit_reply(chat_id, msg_id, _content_profiles(), kb)

    elif data.startswith("cb_profile_"):
        prof = data.replace("cb_profile_", "", 1)
        ok, msg_txt = _apply_profile_runtime(prof)
        kb = [
            [{"text": "🧪 Profile A/B", "callback_data": "cb_profiles"}],
            [{"text": "💾 Lưu .env", "callback_data": "cb_save"},
             {"text": "🏠 Menu", "callback_data": "cb_menu"}],
        ]
        _edit_reply(chat_id, msg_id, msg_txt if ok else f"❌ {msg_txt}", kb)

    # ── Bán — yêu cầu nhập mint ──────────────────────────────────
    elif data == "cb_sell":
        positions = db_get_positions()
        if not positions:
            _edit_reply(chat_id, msg_id,
                        "📭 Không có vị thế nào.\nKhông có gì để bán.", _BACK_KB)
            return
        # Hiển thị nút bán nhanh cho từng vị thế
        sell_btns = [
            {"text": f"💰 Bán ${p.get('symbol','?')}",
             "callback_data": f"cb_sell_{p['mint']}"}
            for p in positions
        ]
        kb = []
        for i in range(0, len(sell_btns), 2):
            kb.append(sell_btns[i:i+2])
        kb.append([{"text": "✍️ Nhập địa chỉ thủ công", "callback_data": "cb_sell_manual"}])
        kb.append([{"text": "🔙 Menu Chính", "callback_data": "cb_menu"}])
        _edit_reply(chat_id, msg_id, "💰 <b>Chọn token muốn bán:</b>", kb)

    elif data == "cb_sell_manual":
        with _tg_state_lock:
            _tg_state[chat_id] = {"action": "sell"}
        _edit_reply(chat_id, msg_id,
                    "✍️ <b>Nhập địa chỉ token muốn bán:</b>\n"
                    "<i>Paste mint address vào đây</i>",
                    [[{"text": "❌ Huỷ", "callback_data": "cb_menu"}]])

    elif data.startswith("cb_sell_"):
        mint = data[len("cb_sell_"):]
        threading.Thread(
            target=_do_sell_mint, args=(chat_id, mint, msg_id),
            daemon=True, name=f"sell-{mint[:8]}"
        ).start()

    # ── Block ─────────────────────────────────────────────────────
    elif data == "cb_block":
        with _tg_state_lock:
            _tg_state[chat_id] = {"action": "block"}
        _edit_reply(chat_id, msg_id,
                    "🚫 <b>Block Token</b>\n"
                    "Nhập địa chỉ mint token muốn blacklist vĩnh viễn:",
                    [[{"text": "❌ Huỷ", "callback_data": "cb_menu"}]])

    # ── Unblock ───────────────────────────────────────────────────
    elif data == "cb_unblock":
        with _tg_state_lock:
            _tg_state[chat_id] = {"action": "unblock"}
        _edit_reply(chat_id, msg_id,
                    "🔓 <b>Unblock Token</b>\n"
                    "Nhập địa chỉ mint token muốn gỡ khỏi blacklist:",
                    [[{"text": "❌ Huỷ", "callback_data": "cb_menu"}]])

    else:
        _answer_cb(cb_id, "❓ Nút không xác định", alert=True)


# ── Thực thi bán async ───────────────────────────────────────────

def _do_sell_mint(chat_id: str, mint: str, origin_msg_id: int = None):
    """Bán thủ công 1 token — chạy trong thread riêng."""
    with _manual_sell_lock:
        if mint in _manual_sell_pending:
            _reply(chat_id, f"⏳ Đang bán <code>{mint[:20]}...</code> — chờ xíu...")
            return
        _manual_sell_pending.add(mint)

    positions = db_get_positions()
    pos = next((p for p in positions if p["mint"] == mint), None)
    if not pos:
        _reply(chat_id,
               f"❌ Không tìm thấy vị thế:\n<code>{mint}</code>",
               _BACK_KB)
        with _manual_sell_lock:
            _manual_sell_pending.discard(mint)
        return

    sym   = pos.get("symbol", "?")
    chain = pos.get("chain", "solana")
    _reply(chat_id,
           f"⏳ <b>Đang bán ${sym}...</b>\n<code>{mint}</code>\nVui lòng chờ TX confirm...",
           None)

    try:
        to_token = BASE_USDC if chain == "base" else SOL_NATIVE
        raw      = get_token_raw_balance(mint, chain=chain)

        if raw == "0":
            _reply(chat_id,
                   f"⚠️ <b>${sym}</b>: số dư = 0.\nToken đã bán hoặc chưa nhận về?",
                   _BACK_KB)
            return

        sell_sig = execute_swap(mint, to_token, raw,
                                label=f"MANUAL SELL {sym}", chain=chain)
        if sell_sig:
            cur_price = get_price_usd(mint, chain=chain)
            entry_p   = float(pos.get("entry_price") or 0)
            pct = ((cur_price / entry_p - 1) * 100) if entry_p > 0 and cur_price > 0 else 0
            db_log_sell(mint, sell_sig, "MANUAL", cur_price, 0.0)
            db_close_position(mint)
            _price_tracker.remove(mint)
            pct_str  = f"{'🟢' if pct >= 0 else '🔴'}{pct:+.1f}%"
            explorer = (f"https://basescan.org/tx/{sell_sig}" if chain == "base"
                        else f"https://solscan.io/tx/{sell_sig}")
            _reply(chat_id,
                   f"✅ <b>Bán thành công!</b>\n"
                   f"🪙 Token: <b>${sym}</b>\n"
                   f"📈 Lãi/Lỗ ước tính: <b>{pct_str}</b>\n"
                   f"🔗 <a href='{explorer}'>Xem TX</a>",
                   _BACK_KB)
            print(f"[CMD] ✅ Manual sell OK: {sym} sig={sell_sig[:20]}...")
        else:
            _reply(chat_id,
                   f"❌ <b>Bán thất bại!</b>\n${sym}\nKiểm tra RPC / slippage.",
                   _BACK_KB)
    except Exception as e:
        _reply(chat_id, f"❌ Lỗi bán <b>${sym}</b>: {e}", _BACK_KB)
    finally:
        with _manual_sell_lock:
            _manual_sell_pending.discard(mint)


# ── Xử lý text nhập liệu (trạng thái chờ) ───────────────────────

def _handle_state_input(chat_id: str, text: str):
    """
    Xử lý tin nhắn text khi bot đang chờ nhập liệu từ user.
    Trả về True nếu đã xử lý (là state input), False nếu không.
    """
    with _tg_state_lock:
        state = _tg_state.get(chat_id)
        if not state:
            return False
        action = state.get("action")

    if action == "block":
        mint = text.strip()
        with _tg_state_lock:
            _tg_state.pop(chat_id, None)
        if db_has_position(mint):
            _reply(chat_id,
                   f"⚠️ Token đang có vị thế mở.\nHãy Bán trước rồi mới Block.",
                   _BACK_KB)
        else:
            db_add_perm_ban(mint, "manual block via Telegram")
            _reply(chat_id,
                   f"🚫 <b>Đã blacklist vĩnh viễn:</b>\n<code>{mint}</code>",
                   _BACK_KB)
        return True

    elif action == "unblock":
        mint = text.strip()
        with _tg_state_lock:
            _tg_state.pop(chat_id, None)
        with _db_lock:
            conn = _get_conn()
            conn.execute("DELETE FROM permanent_ban WHERE mint=?", (mint,))
            conn.execute("DELETE FROM blacklist WHERE mint=?", (mint,))
            conn.commit()
            conn.close()
        _reply(chat_id,
               f"✅ <b>Đã gỡ khỏi blacklist:</b>\n<code>{mint}</code>",
               _BACK_KB)
        return True

    elif action == "sell":
        mint = text.strip()
        with _tg_state_lock:
            _tg_state.pop(chat_id, None)
        threading.Thread(
            target=_do_sell_mint, args=(chat_id, mint),
            daemon=True, name=f"sell-{mint[:8]}"
        ).start()
        return True

    return False


# ── Xử lý lệnh text (/start, /set, v.v.) ─────────────────────────

def _handle_text_command(chat_id: str, text: str):
    """Xử lý lệnh text (vẫn hỗ trợ song song với nút bấm)."""
    parts   = text.split(maxsplit=1)
    command = parts[0].lower().split("@")[0]
    arg     = parts[1].strip() if len(parts) > 1 else ""

    print(f"[CMD] 📥 Text cmd: {command!r} arg={arg[:30]!r}")

    if command in ("/start", "/menu", "/help"):
        _show_menu(chat_id)

    elif command == "/status":
        _reply(chat_id, _content_status(), _BACK_KB)

    elif command == "/positions":
        text_p, kb = _content_positions()
        _reply(chat_id, text_p, kb)

    elif command == "/report":
        _reply(chat_id, _content_report(), _BACK_KB)

    elif command == "/scores":
        try:
            limit = int(arg) if arg else 200
            limit = max(20, min(limit, 5000))
        except Exception:
            limit = 200
        _reply(chat_id, _content_score_history(limit), _BACK_KB)

    elif command == "/config":
        group = arg.lower().strip()
        valid = ("trading","risk","timing","slippage")
        if group in valid:
            kb = [[{"text": "🔙 Cài Đặt", "callback_data": "cb_config"},
                   {"text": "🏠 Menu",    "callback_data": "cb_menu"}]]
            _reply(chat_id, _content_config(group), kb)
        else:
            _reply(chat_id,
                   "⚙️ <b>Cài Đặt</b> — Chọn nhóm thông số:",
                   _CONFIG_GROUP_KB)

    elif command == "/set":
        parts2 = arg.strip().split(maxsplit=1)
        if len(parts2) < 2:
            _reply(chat_id,
                   "❓ <b>Cú pháp:</b> <code>/set KEY GIÁ_TRỊ</code>\n\n"
                   "Ví dụ:\n<code>/set BUY_AMOUNT_SOL 0.05</code>\n"
                   "<code>/set MIN_SCORE 80</code>\n\n"
                   "Xem danh sách key: /config")
            return
        key, raw_val = parts2[0].upper(), parts2[1].strip().lstrip("= ").strip()
        ok, msg_txt = cfg_set(key, raw_val)
        schema = _CFG_SCHEMA.get(key, {})
        if ok:
            _reply(chat_id,
                   f"✅ <b>Đã cập nhật!</b>\n{msg_txt}\n"
                   f"<i>{schema.get('desc','')}</i>\n\n"
                   f"💾 Dùng /save hoặc nút <b>💾 Lưu .env</b> để persist.",
                   _BACK_KB)
        else:
            _reply(chat_id,
                   f"❌ <b>Lỗi:</b> {msg_txt}\n"
                   f"Phạm vi: [{schema.get('min','')} ~ {schema.get('max','')}]")

    elif command == "/save":
        ok, msg_txt = cfg_save_env()
        icon = "💾" if ok else "❌"
        _reply(chat_id, f"{icon} {msg_txt}", _BACK_KB)

    elif command == "/profile":
        if not arg:
            kb = [
                [{"text": "⚡ Early Sniper", "callback_data": "cb_profile_early_sniper"}],
                [{"text": "🛡 Safe Trend",  "callback_data": "cb_profile_safe_trend"}],
                [{"text": "🔙 Menu Chính", "callback_data": "cb_menu"}],
            ]
            _reply(chat_id, _content_profiles(), kb)
            return
        ok, msg_txt = _apply_profile_runtime(arg)
        _reply(chat_id, msg_txt if ok else f"❌ {msg_txt}", _BACK_KB)

    elif command == "/block":
        if not arg:
            with _tg_state_lock:
                _tg_state[chat_id] = {"action": "block"}
            _reply(chat_id,
                   "🚫 Nhập địa chỉ mint token muốn blacklist:",
                   [[{"text": "❌ Huỷ", "callback_data": "cb_menu"}]])
        else:
            if db_has_position(arg):
                _reply(chat_id, "⚠️ Token đang có vị thế — bán trước rồi mới block.", _BACK_KB)
            else:
                db_add_perm_ban(arg, "manual block via Telegram")
                _reply(chat_id, f"🚫 Đã blacklist:\n<code>{arg}</code>", _BACK_KB)

    elif command == "/unblock":
        if not arg:
            with _tg_state_lock:
                _tg_state[chat_id] = {"action": "unblock"}
            _reply(chat_id, "🔓 Nhập địa chỉ mint token muốn gỡ:", [[{"text":"❌ Huỷ","callback_data":"cb_menu"}]])
        else:
            with _db_lock:
                conn = _get_conn()
                conn.execute("DELETE FROM permanent_ban WHERE mint=?", (arg,))
                conn.execute("DELETE FROM blacklist WHERE mint=?", (arg,))
                conn.commit(); conn.close()
            _reply(chat_id, f"✅ Đã gỡ:\n<code>{arg}</code>", _BACK_KB)

    elif command == "/sell":
        if not arg:
            with _tg_state_lock:
                _tg_state[chat_id] = {"action": "sell"}
            _reply(chat_id, "💰 Nhập địa chỉ mint token muốn bán:", [[{"text":"❌ Huỷ","callback_data":"cb_menu"}]])
        else:
            threading.Thread(
                target=_do_sell_mint, args=(chat_id, arg),
                daemon=True, name=f"sell-{arg[:8]}"
            ).start()

    else:
        _show_menu(chat_id)


# ── Xử lý 1 update tổng quát ─────────────────────────────────────

def _process_update(update: dict):
    """Xử lý 1 update từ Telegram getUpdates (message hoặc callback_query)."""

    # ── Callback query (nút bấm) ──────────────────────────────────
    if "callback_query" in update:
        cb      = update["callback_query"]
        chat_id = str(cb["from"]["id"])
        if chat_id != str(TELEGRAM_CHAT_ID):
            _answer_cb(cb["id"], "⛔ Không có quyền!", alert=True)
            return
        try:
            _handle_callback(cb)
        except Exception as e:
            print(f"[CMD] ❌ _handle_callback: {e}")
        return

    # ── Tin nhắn text ─────────────────────────────────────────────
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return

    chat_id = str(msg.get("chat", {}).get("id", ""))
    text    = (msg.get("text") or "").strip()

    if chat_id != str(TELEGRAM_CHAT_ID):
        print(f"[CMD] ⚠️  chat_id lạ: {chat_id} — bỏ qua")
        return
    if not text:
        return

    # Ưu tiên: state input (đang chờ nhập mint / value)
    if not text.startswith("/"):
        if not _handle_state_input(chat_id, text):
            _show_menu(chat_id)   # text bình thường → hiện menu
        return

    _handle_text_command(chat_id, text)


# ── Polling thread ────────────────────────────────────────────────

def telegram_cmd_thread(stop_event: threading.Event):
    """
    Thread polling Telegram getUpdates (message + callback_query).
    Long-poll timeout=30s — hiệu quả, không tốn tài nguyên.
    """
    global _tg_cmd_offset

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[CMD] ⚠️  TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa set — tắt cmd handler")
        return

    print("[CMD] 🟢 Telegram command handler bắt đầu (inline keyboard, long-poll 30s)...")

    # Skip tất cả tin cũ khi mới khởi động
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"offset": -1, "limit": 1}, timeout=15,
        )
        updates = r.json().get("result", [])
        if updates:
            with _tg_cmd_lock:
                _tg_cmd_offset = updates[-1]["update_id"] + 1
            print(f"[CMD] ⏩ Skip tin cũ — offset={_tg_cmd_offset}")
    except Exception as e:
        print(f"[CMD] ⚠️  Skip cũ thất bại: {e}")

    # Gửi menu khởi động
    try:
        _show_menu(str(TELEGRAM_CHAT_ID))
    except Exception:
        pass

    while not stop_event.is_set():
        try:
            with _tg_cmd_lock:
                offset = _tg_cmd_offset

            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={
                    "offset":          offset,
                    "timeout":         30,
                    "allowed_updates": '["message","edited_message","callback_query"]',
                },
                timeout=40,
            )

            if r.status_code != 200:
                print(f"[CMD] ⚠️  getUpdates HTTP {r.status_code}")
                stop_event.wait(5)
                continue

            for upd in r.json().get("result", []):
                uid = upd.get("update_id", 0)
                try:
                    _process_update(upd)
                except Exception as e:
                    print(f"[CMD] ❌ _process_update: {e}")
                finally:
                    with _tg_cmd_lock:
                        _tg_cmd_offset = max(_tg_cmd_offset, uid + 1)

        except requests.exceptions.Timeout:
            continue
        except Exception as e:
            print(f"[CMD] ❌ polling loop: {e}")
            stop_event.wait(10)

    print("[CMD] 🔴 Command handler dừng.")


if __name__ == "__main__":
    main()
