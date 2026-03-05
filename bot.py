"""
╔══════════════════════════════════════════════════════════════════╗
║    MULTI-CHAIN AUTO TRADER v3.1  🟣 Solana  🔵 Base  — OKX DEX ║
║                                                                  ║
║  KIẾN TRÚC:                                                      ║
║  Thread 1 → Scanner    (3s, DexScreener: Solana + Base)          ║
║  Thread 2 → Validator  (GoPlus parallel x20, chain-aware)        ║
║  Thread 3 → Buyer      (OKX swap, non-blocking, chain-dispatch)  ║
║  Thread 4 → Monitor    (TP + Rug detection, chain-aware sell)    ║
║                                                                  ║
║  TÍNH NĂNG MỚI:                                                  ║
║  ✅ Base (EVM) chain via web3.py                                  ║
║  ✅ Social score (+5 website, +5 twitter)                         ║
║  ✅ Rug detection (drop >= -60% → bán + perm-ban)                ║
║  ✅ Blacklist cứng (permanent_ban SQLite)                         ║
║                                                                  ║
║  BUG FIXES v3.1:                                                 ║
║  🔧 get_price_usd() — thêm tham số chain, fix Monitor Base       ║
║  🔧 gasPrice Base — hỗ trợ EIP-1559 + legacy hex parsing         ║
║  🔧 TX receipt status — int(rcpt["status"]) thay .get()          ║
║  🔧 GoPlus Base — lowercase addr để tránh case-mismatch          ║
║  🔧 Scanner Nguồn A — quota riêng cho Solana và Base             ║
║  🔧 main() — validate BASE_PRIVATE_KEY khi khởi động             ║
║  FLOW:  DexScreener → TOKEN_QUEUE → VALIDATOR → BUY_QUEUE → BUY  ║
║  DB:    SQLite (positions + blacklist + permanent_ban)            ║
╚══════════════════════════════════════════════════════════════════╝

Cài đặt:
    pip install requests python-dotenv solders base58 web3

.env cần có:
    OKX_API_KEY / OKX_SECRET_KEY / OKX_API_PASSPHRASE / OKX_PROJECT_ID
    SOLANA_WALLET_ADDRESS / SOLANA_PRIVATE_KEY  (base58)
    SOLANA_RPC_URL
    BASE_PRIVATE_KEY  (hex, 0x-prefixed optional)
    BASE_RPC_URL  (optional, default: https://mainnet.base.org)
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / TELEGRAM_SIGNAL_CHANNEL
"""

import os, json, time, hmac, hashlib, base64, threading, sqlite3, re
import requests, urllib.parse
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

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

WALLET_ADDRESS = os.getenv("SOLANA_WALLET_ADDRESS", "")
WALLET_PRIVKEY = os.getenv("SOLANA_PRIVATE_KEY", "")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

BUY_AMOUNT_USDC       = float(os.getenv("BUY_AMOUNT_USDC",       "10"))
MIN_SCORE             = int(  os.getenv("MIN_OPPORTUNITY_SCORE",  "70"))
TAKE_PROFIT_PCT       = float(os.getenv("TAKE_PROFIT_PCT",        "30"))
SLIPPAGE_PCT          = float(os.getenv("SLIPPAGE_PCT",           "10"))
SCAN_INTERVAL         = float(os.getenv("SCAN_INTERVAL_SECONDS",  "3"))
TP_CHECK_INTERVAL     = float(os.getenv("TP_CHECK_INTERVAL_SECONDS", "5"))
MIN_LIQUIDITY_USD     = float(os.getenv("MIN_LIQUIDITY_USD",      "5000"))
MAX_AGE_MIN           = float(os.getenv("MAX_TOKEN_AGE_MINUTES",  "120"))
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

SOL_CHAIN_INDEX     = "501"
SOL_USDC            = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
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

GOPLUS_CRITICAL = {"is_honeypot": "Honeypot", "is_blacklisted": "Blacklisted"}
GOPLUS_NONCRIT  = {
    "mintable":            "Mintable",
    "transfer_fee_enable": "Phí transfer",
    "can_freeze_account":  "Đóng băng account",
}

DB_PATH = "positions.db"

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

    def get(self, key: str):
        with self._lock:
            v = self._cache.get(key)
            if not v:
                return None
            if time.time() - v["time"] > self.ttl:
                return None
            return v["data"]

    def set(self, key: str, data):
        with self._lock:
            self._cache[key] = {"data": data, "time": time.time()}

_dex_cache = DexCache(ttl=5)

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
    print("[DB] ✅ SQLite ready (positions + blacklist + permanent_ban)")

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
                           token_decimals: int):
    """
    Cập nhật giá vào thực tế sau khi xác nhận balance trên RPC.
    actual_entry_price = usdc_spent / token_amount  ← giá thực sau slippage
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
            WHERE mint = ?
        """, (actual_price, actual_price, token_amount, raw_balance, token_decimals, mint))
        conn.commit()
        conn.close()

def db_get_positions() -> List[dict]:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM positions WHERE tp_sent=0").fetchall()
        conn.close()
        return [dict(r) for r in rows]

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

# ================================================================
# TELEGRAM
# ================================================================

def _send_tg(text: str, chat_id: str = None):
    targets = [chat_id] if chat_id else (TELEGRAM_SIGNAL_CHANNELS or [TELEGRAM_CHAT_ID])
    for cid in targets:
        if not cid: continue
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=10
            )
        except Exception as e:
            print(f"[TG] ❌ {e}")

def _alert(text: str):
    if TELEGRAM_CHAT_ID:
        _send_tg(text, chat_id=TELEGRAM_CHAT_ID)

def send_error_alert(msg: str):
    _alert(f"⚠️ <b>SOLANA TRADER ERROR</b>\n\n{msg}")

# ================================================================
# OKX AUTH
# ================================================================

def _okx_headers(method: str, path: str, query: str = "", body: str = "") -> dict:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
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
    try:
        from solders.transaction import VersionedTransaction
        from solders.keypair import Keypair
        import base58 as _b58

        secret  = _b58.b58decode(WALLET_PRIVKEY)
        keypair = Keypair.from_bytes(secret)

        try:
            tx_bytes = _b58.b58decode(tx_b64)
        except Exception:
            tx_bytes = base64.b64decode(tx_b64)

        tx        = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [keypair])
        encoded   = base64.b64encode(bytes(signed_tx)).decode()

        resp   = requests.post(SOLANA_RPC_URL, json={
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


def _get_usdc_spent_from_tx(sig: str, chain: str, label: str = "") -> float:
    """
    Đọc TX đã confirmed → tính USDC thực tế đã tiêu.

    Solana: getTransaction → pre/post tokenBalances → delta USDC account của ví.
    Base:   log decode ERC-20 Transfer từ USDC → ví đã gửi bao nhiêu.
    Trả về float USDC (không nhân decimals). 0.0 nếu không parse được.
    """
    try:
        if chain == "base":
            return _get_usdc_spent_base(sig)
        else:
            return _get_usdc_spent_solana(sig)
    except Exception as e:
        print(f"[Confirm] ⚠️  {label} parse usdc_spent: {e}")
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

def execute_swap_base(from_token: str, to_token: str, amount_raw: str,
                      label: str = "SWAP") -> Optional[str]:
    """
    Swap EVM trên Base qua OKX DEX API.
    OKX trả về calldata → web3.py ký và gửi qua Base RPC.
    Không block — return tx hash ngay sau khi gửi.
    """
    if not BASE_WALLET_PRIVKEY:
        print("[Base] ❌ BASE_PRIVATE_KEY chưa set trong .env")
        return None

    data = okx_get("/api/v5/dex/aggregator/swap", {
        "chainIndex":        BASE_CHAIN_INDEX,
        "fromTokenAddress":  from_token,
        "toTokenAddress":    to_token,
        "amount":            amount_raw,
        "slippagePercent":   str(SLIPPAGE_PCT),
        "userWalletAddress": _get_base_wallet_address(),
    })
    if not data:
        return None

    try:
        from web3 import Web3
        w3 = _get_base_w3()

        tx_obj = data["data"][0].get("tx", {})
        privkey = BASE_WALLET_PRIVKEY if BASE_WALLET_PRIVKEY.startswith("0x") \
                  else "0x" + BASE_WALLET_PRIVKEY
        acct    = w3.eth.account.from_key(privkey)

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
        raw_tx = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
        if raw_tx is None:
            raise RuntimeError("Không lấy được signed raw transaction từ web3 account")
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

def get_token_decimals_base(token_addr: str) -> int:
    """Đọc decimals ERC-20 trên Base. Fallback 18 nếu lỗi."""
    try:
        from web3 import Web3
        w3 = _get_base_w3()
        abi = [{"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],
                "stateMutability":"view","type":"function"}]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token_addr), abi=abi)
        return int(contract.functions.decimals().call())
    except Exception as e:
        print(f"[Base] ⚠️  get_decimals: {e} — fallback 18")
        return 18

# ================================================================
# SWAP (chain-agnostic dispatcher)
# ================================================================

def _usdc_raw(usdc: float, chain: str = "solana") -> str:
    """Chuyển USDC sang raw amount. Base USDC cũng 6 decimals."""
    return str(int(usdc * 10 ** USDC_DECIMALS))

def execute_swap(from_token: str, to_token: str, amount_raw: str,
                 label: str = "SWAP", chain: str = "solana") -> Optional[str]:
    """
    Chain-agnostic swap dispatcher.
    - chain="solana" → OKX + Solana RPC (VersionedTransaction)
    - chain="base"   → OKX + web3.py EVM signing
    """
    if chain == "base":
        return execute_swap_base(from_token, to_token, amount_raw, label)

    # Solana path (giữ nguyên logic cũ)
    data = okx_get("/api/v5/dex/aggregator/swap", {
        "chainIndex":        SOL_CHAIN_INDEX,
        "fromTokenAddress":  from_token,
        "toTokenAddress":    to_token,
        "amount":            amount_raw,
        "slippagePercent":   str(SLIPPAGE_PCT),
        "userWalletAddress": WALLET_ADDRESS,
    })
    if not data:
        return None
    try:
        tx_b64 = data["data"][0].get("tx", {}).get("data", "")
        if not tx_b64:
            print(f"[OKX] ❌ Không có tx.data")
            return None
        sig = _sign_and_send_tx(tx_b64)
        if sig:
            print(f"[OKX] ✅ {label}: {sig[:20]}...")
        return sig
    except Exception as e:
        print(f"[OKX] ❌ execute_swap: {e}")
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

def get_token_decimals(mint: str, chain: str = "solana") -> int:
    if chain == "base":
        return get_token_decimals_base(mint)
    return get_token_decimals_rpc(mint)

# ================================================================
# GOPLUS SECURITY
# ================================================================

def check_goplus(addr: str, age_min: Optional[float] = None,
                  chain: str = "solana") -> Tuple[bool, str]:
    """GoPlus security check. chain=base dùng EVM endpoint."""
    try:
        if chain == "base":
            # FIX: EVM địa chỉ phải lowercase khi query GoPlus
            addr_q = addr.lower()
            url = f"https://api.gopluslabs.io/api/v1/token_security/8453?contract_addresses={addr_q}"
        else:
            addr_q = addr
            url = f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={addr_q}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        result = r.json().get("result", {})
        # FIX: lookup cả lowercase lẫn original để không miss kết quả
        res = result.get(addr_q) or result.get(addr) or {}
        if not res:
            return True, ""
        bad = [lbl for k, lbl in GOPLUS_CRITICAL.items() if str(res.get(k,"0")) == "1"]
        if bad:
            return False, ", ".join(bad)
        relax = (age_min is not None and age_min < GOPLUS_RELAX_UNDER)
        if not relax:
            noncrit = [lbl for k, lbl in GOPLUS_NONCRIT.items() if str(res.get(k,"0")) == "1"]
            if noncrit:
                return False, ", ".join(noncrit)
        return True, ""
    except Exception as e:
        print(f"[GoPlus] ⚠️ {addr[:12]}: {e} — cho qua")
        return True, ""

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
    score, detail = 0, []

    liq = t.get("liquidity_usd", 0)
    if 5_000 <= liq <= 40_000:    score += 20; detail.append("💧 Liq $5K–$40K: +20")
    elif 40_000 < liq <= 100_000: score += 15; detail.append("💧 Liq $40K–$100K: +15")
    elif liq > 100_000:           score += 10; detail.append("💧 Liq >$100K: +10")

    vol1h = t.get("volume_1h", 0)
    if liq > 0:
        if vol1h >= liq:          score += 25; detail.append("📊 Vol1h ≥ Liq: +25")
        elif vol1h >= liq * 0.5:  score += 18; detail.append("📊 Vol1h ≥50% Liq: +18")
        elif vol1h >= 20_000:     score += 12; detail.append("📊 Vol1h ≥$20K: +12")
        elif vol1h >= 10_000:     score += 6;  detail.append("📊 Vol1h ≥$10K: +6")

    buys  = t.get("buys_24h", 0)
    sells = t.get("sells_24h", 1)
    ratio = buys / sells if sells > 0 else 0
    total = buys + sells
    bp    = (buys / total * 100) if total > 0 else 0
    if ratio >= 3.0:   score += 20; detail.append(f"🔥 B/S {ratio:.1f}:1 ≥3:1: +20")
    elif ratio >= 2.0: score += 15; detail.append(f"🔥 B/S {ratio:.1f}:1 ≥2:1: +15")
    elif ratio >= 1.5: score += 8;  detail.append(f"🔄 B/S {ratio:.1f}:1: +8")
    if bp >= 70:       score += 10; detail.append("💥 >70% giao dịch là MUA: +10")

    age = t.get("token_age_minutes")
    if age is not None:
        if 3 <= age <= 15:    score += 30; detail.append(f"🆕 {age:.0f}p (EARLY GEM): +30")
        elif 15 < age <= 30:  score += 18; detail.append(f"⏰ {age:.0f}p: +18")
        elif 30 < age <= 60:  score += 8;  detail.append(f"⏰ {age:.0f}p: +8")
        else:                 score -= 5;  detail.append(f"⏳ {age:.0f}p (>1h): -5")

    lp = t.get("lp_status", "")
    if lp == "burned":   score += 12; detail.append("🔒 LP đốt 100%: +12")
    elif lp == "locked": score += 6;  detail.append("🔒 LP khóa: +6")
    else:                score -= 5;  detail.append("⚠️ LP không rõ: -5")

    # Social score (+5 website, +5 twitter — mỗi loại tính 1 lần)
    social_added = set()
    for kind, url in [
        ("website", t.get("website", "")),
        ("twitter", t.get("twitter", "")),
    ]:
        if not url or kind in social_added:
            continue
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower().lstrip("www.")
            if domain and domain not in _SPAM_DOMAINS and "." in domain:
                score += 5
                detail.append(f"🌐 {kind.capitalize()} hợp lệ ({domain}): +5")
                social_added.add(kind)
        except Exception:
            pass

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
        if 3 <= age <= 15:    score += 30
        elif 15 < age <= 30:  score += 18
        elif 30 < age <= 60:  score += 8
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
    if buys < _get_min_buys(age_min):
        return None
    return {
        "address":          addr,
        "symbol":           (pair.get("baseToken") or {}).get("symbol", ""),
        "name":             (pair.get("baseToken") or {}).get("name", ""),
        "pair_address":     pair.get("pairAddress", ""),
        "price_usd":        float(pair.get("priceUsd", 0) or 0),
        "liquidity_usd":    liq,
        "volume_1h":        (pair.get("volume") or {}).get("h1", 0) or 0,
        "volume_24h":       (pair.get("volume") or {}).get("h24", 0) or 0,
        "buys_24h":         buys,
        "sells_24h":        sells,
        "price_change_1h":  (pair.get("priceChange") or {}).get("h1", 0) or 0,
        "price_change_6h":  (pair.get("priceChange") or {}).get("h6", 0) or 0,
        "price_change_24h": (pair.get("priceChange") or {}).get("h24", 0) or 0,
        "fdv":              pair.get("fdv", 0) or 0,
        "market_cap":       pair.get("marketCap", 0) or 0,
        "quote_symbol":     quote,
        "dex_key":          _norm_dex(pair.get("dexId", "")),
        "token_age_minutes": age_min,
        "lp_status":         "unknown",
        "chain":             chain_id,
        # Social links từ DexScreener info block
        "website":           _extract_social(pair, "website"),
        "twitter":           _extract_social(pair, "twitter"),
    }

def fetch_profile_addresses() -> List[str]:
    """Nguồn A: lấy danh sách địa chỉ token từ 3 endpoint Profile/Boost.
    FIX: tách quota riêng cho solana và base, tránh base bị Solana chiếm hết slot."""
    sol_addrs, base_addrs = [], []
    sol_seen, base_seen   = set(), set()
    for url in PROFILE_SOURCES:
        cached = _dex_cache.get(url)
        if cached is not None:
            for item in cached:
                if item.get("chain") == "base" and item["addr"] not in base_seen:
                    base_seen.add(item["addr"]); base_addrs.append(item["addr"])
                elif item.get("chain") == "solana" and item["addr"] not in sol_seen:
                    sol_seen.add(item["addr"]); sol_addrs.append(item["addr"])
            continue
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
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
    # Giới hạn quota riêng cho mỗi chain
    return sol_addrs[:SOL_MAX_PROFILE_ADDRS] + base_addrs[:BASE_MAX_PROFILE_ADDRS]

def fetch_token_pairs(addr: str) -> List[dict]:
    """Lấy pairs của 1 token address. Có cache 5s."""
    key = f"pairs:{addr}"
    cached = _dex_cache.get(key)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{addr}", timeout=10)
        r.raise_for_status()
        data = r.json()
        ps   = data.get("pairs") if isinstance(data, dict) else None
        result = ps if isinstance(ps, list) else []
        _dex_cache.set(key, result)
        return result
    except:
        return []

def fetch_new_pairs() -> List[dict]:
    """Nguồn B: /pairs/{chain}/new cho Solana + Base. Cache 5s."""
    cached = _dex_cache.get("new_pairs")
    if cached is not None:
        return cached
    result = []
    for url in NEW_PAIRS_URLS:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                result.extend(data)
            elif isinstance(data, dict):
                result.extend(data.get("pairs") or data.get("data") or [])
        except Exception as e:
            print(f"[Scan-B] ⚠️  {url.split('/')[-2]}: {e}")
    _dex_cache.set("new_pairs", result)
    return result

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
# ================================================================
# TELEGRAM SIGNALS
# ================================================================

def send_buy_signal(token: dict, score: int, detail: list, buy_sig: str):
    bar, label, border = _score_bar(score)
    bp    = token["buys_24h"] / max(token["buys_24h"] + token["sells_24h"], 1) * 100
    press = "🔥 MUA MẠNH" if bp >= 65 else ("📈 Bullish" if bp >= 50 else "⚠️ Bearish")
    age   = token.get("token_age_minutes")
    age_badge = (f"🆕 {_age_str(age)} — MỚI LAUNCH" if age and age <= 10
                 else f"⏰ {_age_str(age)}" if age else "N/A")
    lp_map  = {"burned":"🔥 LP đốt 100%","locked":"🔒 LP khóa","unknown":"❓ Không rõ"}
    chain   = token.get("chain", "solana")
    chain_emoji = "🟣" if chain == "solana" else "🔵"

    # Giá vào: dùng actual nếu đã xác nhận, fallback DexScreener
    est_price    = token.get("price_usd", 0)
    actual_price = token.get("actual_entry_price", 0)
    slip_actual  = token.get("slippage_actual", 0)
    token_amount = token.get("token_amount", 0)

    if actual_price > 0:
        price_line = (
            f"💰 Giá ước tính: ${est_price:.10f}\n"
            f"✅ Giá thực tế:  ${actual_price:.10f}\n"
            f"📉 Slippage thực: {slip_actual:+.2f}%\n"
            f"🪙 Số token nhận: {token_amount:.6f}\n"
        )
        tp_price = actual_price * (1 + TAKE_PROFIT_PCT / 100)
        tp_line  = (f"🎯 Chốt lời khi giá ≥ ${tp_price:.10f} "
                    f"(+{TAKE_PROFIT_PCT:.0f}% từ giá thực)")
    else:
        price_line = (
            f"💰 Giá vào (ước tính): ${est_price:.10f}\n"
            f"⏳ Đang xác nhận giá thực...\n"
        )
        tp_line = f"🎯 Chốt lời: +{TAKE_PROFIT_PCT:.0f}% (sẽ tính từ giá thực sau confirm)"

    explorer = ("https://basescan.org/tx/" if chain == "base"
                else "https://solscan.io/tx/")
    chart_url = f"https://dexscreener.com/{chain}/{token['pair_address']}"
    scan_url  = (f"https://basescan.org/token/{token['address']}" if chain == "base"
                 else f"https://solscan.io/token/{token['address']}")

    msg = (
        f"{chain_emoji} <b>AUTO BUY 🤖 — {chain.upper()}</b>\n{border}\n"
        f"🎯 <b>ĐIỂM: {score}/100</b>  {label}\n{bar}\n{border}\n\n"
        f"🪙 <b>{token['name']}</b>  (<code>${token['symbol']}</code>)\n\n"
        f"📋 <b>Contract:</b>\n<code>{token['address']}</code>\n<i>👆 Nhấn để copy</i>\n\n"
        f"{price_line}"
        f"💵 Đã chi: {BUY_AMOUNT_USDC} USDC\n"
        f"{tp_line}\n\n"
        f"💧 Liq: {_fmt_usd(token['liquidity_usd'])} | 📊 Vol1h: {_fmt_usd(token['volume_1h'])}\n"
        f"🔄 Mua: {token['buys_24h']} | Bán: {token['sells_24h']}  {press} ({bp:.0f}% mua)\n"
        f"🏊 {token['quote_symbol']} — {token['dex_key'].upper()} | {age_badge} | "
        f"{lp_map.get(token.get('lp_status','unknown'),'❓')}\n\n"
        f"🔗 <a href='{explorer}{buy_sig}'>✅ TX</a> | "
        f"<a href='{chart_url}'>📉 Chart</a> | "
        f"<a href='{scan_url}'>🔍 Explorer</a>\n"
        f"#AutoBuy #{chain.capitalize()} #{token['symbol']}"
    )
    _send_tg(msg)

def send_tp_signal(pos: dict, cur_price: float, pct: float, sell_sig: str):
    profit = pos["usdc_spent"] * pct / 100
    emoji  = "🤑🚀" if pct >= 100 else ("💰🔥" if pct >= 50 else "✅📈")
    chain = pos.get("chain", "solana")
    chain_name = "Base" if chain == "base" else "Solana"
    tx_url = "https://basescan.org/tx/" if chain == "base" else "https://solscan.io/tx/"
    chart_url = f"https://dexscreener.com/{chain}/{pos.get('pair_address','')}"
    msg = (
        f"{emoji} <b>TAKE PROFIT! {chain_name}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>${pos['symbol']}</b>  (<code>{pos['mint']}</code>)\n\n"
        f"  Giá vào:   ${pos['entry_price']:.10f}\n"
        f"  Giá bán:   ${cur_price:.10f}\n"
        f"  Lợi nhuận: <b>+{pct:.2f}%</b>  ({_fmt_usd(profit)})\n"
        f"  Đầu tư:    {pos['usdc_spent']:.2f} USDC\n"
        f"  Giữ:       {_duration_str(pos['entry_time'])}\n\n"
        f"<a href='{tx_url}{sell_sig}'>✅ TX bán</a> | "
        f"<a href='{chart_url}'>📉 Chart</a>\n"
        f"#TakeProfit #{chain_name} #{pos['symbol']}"
    )
    _send_tg(msg)

# ================================================================
# THREAD 1 — SCANNER (4 nguồn, song song)
# ================================================================

def scan_once() -> List[dict]:
    """
    Quét token từ 4 nguồn:
      Nguồn A (3 endpoint): Profile/Boost → địa chỉ → fetch_token_pairs → _pair_to_token
      Nguồn B (1 endpoint): /pairs/solana/new → pair object → _pair_to_token trực tiếp
    Nguồn B bắt được token 0–10 phút đầu không cần Profile/Boost.
    Bộ lọc MIN_BUY_TXN được điều chỉnh động theo tuổi token.
    Trả về list token info dict đã qua pre-filter (chưa GoPlus).
    """
    now_ts    = time.time()
    seen_pair: set = set()
    cands: List[dict] = []

    # ── Nguồn A: Profile/Boost ────────────────────────────────────
    all_addrs = fetch_profile_addresses()
    addrs     = all_addrs  # đã được giới hạn quota riêng trong fetch_profile_addresses()
    sol_cnt   = sum(1 for a in addrs if len(a) > 42)   # Solana addr dài hơn EVM
    base_cnt  = len(addrs) - sol_cnt
    print(f"  [SCAN-A] {len(addrs)} địa chỉ từ Profile/Boost (🟣 Sol: {sol_cnt} | 🔵 Base: {base_cnt})")

    for addr in addrs:
        if db_is_perm_banned(addr) or db_is_blacklisted(addr) or db_has_position(addr):
            continue
        pairs = fetch_token_pairs(addr)
        time.sleep(0.15)   # tránh rate limit DexScreener
        for pair in pairs:
            pa = pair.get("pairAddress", "")
            if not pa or pa in seen_pair:
                continue
            # Pre-filter tuổi
            pcAt    = pair.get("pairCreatedAt")
            age_min = ((now_ts - pcAt / 1000) / 60) if pcAt else None
            if age_min is not None and age_min > MAX_AGE_MIN:
                continue
            # Quick score pre-filter
            if _quick_score(pair) < SOL_PREFILTER_MIN_SCORE:
                continue
            token = _pair_to_token(pair)
            if token:
                seen_pair.add(pa)
                cands.append(token)

    # ── Nguồn B: New pairs (bắt token mới nhất 0-10p) ────────────
    new_pairs = fetch_new_pairs()
    print(f"  [SCAN-B] {len(new_pairs)} pair từ /new")
    for pair in new_pairs:
        pa        = pair.get("pairAddress", "")
        base_addr = (pair.get("baseToken") or {}).get("address", "")
        if not pa or pa in seen_pair:
            continue
        if base_addr and (db_is_perm_banned(base_addr) or db_is_blacklisted(base_addr) or db_has_position(base_addr)):
            continue
        if _quick_score(pair) < SOL_PREFILTER_MIN_SCORE:
            continue
        token = _pair_to_token(pair)
        if token:
            seen_pair.add(pa)
            cands.append(token)

    print(f"  [SCAN] {len(cands)} candidate đưa vào validate queue")
    return cands


def scanner_thread(stop_event: threading.Event):
    print("[Scanner] 🟢 Bắt đầu scan (4 nguồn)...")
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

        stop_event.wait(SCAN_INTERVAL)
# ================================================================
# THREAD 2 — VALIDATOR
# ================================================================

def _validate_one(token: dict) -> Optional[dict]:
    """Validate 1 token: perm-ban check + GoPlus + LP + score."""
    addr    = token["address"]
    chain   = token.get("chain", "solana")
    age_min = token.get("token_age_minutes")

    # Hard blacklist check (perm_ban + blacklist)
    if db_is_perm_banned(addr) or db_is_blacklisted(addr) or db_has_position(addr):
        return None

    # Kiểm tra min buys
    buys = token.get("buys_24h", 0)
    if buys < _get_min_buys(age_min):
        return None

    # GoPlus security — chỉ có API cho solana; Base dùng EVM endpoint
    ok, reason = check_goplus(addr, age_min, chain=chain)
    if not ok:
        print(f"[Validator] 🚫 {token['symbol']}: {reason}")
        return None

    # LP status (solana only — Base skip)
    token["lp_status"] = check_lp_status(addr) if chain == "solana" else "unknown"

    # Final score
    score, detail = calculate_score(token)
    token["_opp_score"] = score
    token["_score_detail"] = detail

    age_log = f"{age_min:.0f}p" if age_min is not None else "N/A"
    print(f"[Validator] {'✅' if score >= MIN_SCORE else '⏭ '} {token['symbol']:<10} "
          f"| Score: {score}/100 | Tuổi: {age_log} | Liq: {_fmt_usd(token['liquidity_usd'])}")

    if score >= MIN_SCORE:
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
    usdc_addr = BASE_USDC if chain == "base" else SOL_USDC

    print(f"[Buyer] 🟢 {sym} [{chain.upper()}] | Score: {score}/100 → MUA {BUY_AMOUNT_USDC} USDC")
    sig = execute_swap(usdc_addr, addr, _usdc_raw(BUY_AMOUNT_USDC),
                       label=f"BUY {sym}", chain=chain)

    if not sig:
        print(f"[Buyer] ❌ {sym}: swap thất bại")
        send_error_alert(f"❌ Mua {sym} [{chain}] thất bại\n<code>{addr}</code>")
        return

    # Lưu DB + blacklist cứng NGAY với giá DexScreener (tạm thời)
    db_save_position(token, sig, BUY_AMOUNT_USDC)
    db_add_blacklist(addr)
    print(f"[Buyer] ✅ {sym} | sig: {sig[:20]}... → xác nhận balance...")

    # ── Lấy balance trước swap để tính delta chính xác ──────────────
    # Phải lấy TRƯỚC khi gửi TX để tính delta = token nhận được thực tế
    decimals      = get_token_decimals(addr, chain=chain)
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

            # ── Bước 2: đọc USDC thực tiêu từ TX ────────────────────
            usdc_spent_actual = _get_usdc_spent_from_tx(sig, chain, sym)
            if usdc_spent_actual <= 0:
                # Không parse được → dùng BUY_AMOUNT_USDC
                usdc_spent_actual = BUY_AMOUNT_USDC
                print(f"[Confirm] ⚠️  {sym}: không parse tx log → dùng {BUY_AMOUNT_USDC} USDC")

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

            # ── Bước 4: tính actual_price ─────────────────────────────
            # actual_price = USDC đã chi / số token nhận = giá vào thực tế
            actual_price    = usdc_spent_actual / delta_tokens
            est_price       = token.get("price_usd", 0)
            slippage_actual = ((actual_price / est_price - 1) * 100) if est_price > 0 else 0

            print(
                f"[Confirm] ✅ {sym}\n"
                f"           Giá ước tính : ${est_price:.10f}\n"
                f"           Giá thực tế  : ${actual_price:.10f}\n"
                f"           Slippage thực: {slippage_actual:+.2f}%\n"
                f"           USDC tiêu    : {usdc_spent_actual:.4f}\n"
                f"           Token nhận   : {delta_tokens:.6f}\n"
                f"           TP target    : ${actual_price * (1 + TAKE_PROFIT_PCT/100):.10f}"
            )

            # ── Bước 5: lưu DB với giá thực ──────────────────────────
            db_update_actual_entry(
                addr, actual_price, delta_tokens, str(delta_raw), decimals
            )

            token["actual_entry_price"] = actual_price
            token["token_amount"]       = delta_tokens
            token["slippage_actual"]    = slippage_actual
            token["usdc_spent_actual"]  = usdc_spent_actual
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

def _sell_position(pos: dict, reason: str = "TP") -> bool:
    """
    Bán toàn bộ token cho 1 position. Thread-safe, non-blocking.
    reason: "TP" | "RUG"
    Trả về True nếu gửi được lệnh bán.
    """
    mint  = pos["mint"]
    sym   = pos["symbol"]
    chain = pos.get("chain", "solana")
    usdc  = BASE_USDC if chain == "base" else SOL_USDC

    raw = get_token_raw_balance(mint, chain=chain)
    if raw == "0":
        print(f"  [Monitor] ⚠️  {sym}: balance=0 → đóng position")
        db_close_position(mint)
        return True

    sig = execute_swap(mint, usdc, raw, label=f"{reason} {sym}", chain=chain)
    return sig is not None


def monitor_thread(stop_event: threading.Event):
    print("[Monitor] 🟢 Bắt đầu theo dõi giá (TP + Rug detection)...")
    while not stop_event.is_set():
        try:
            positions = db_get_positions()
            if not positions:
                stop_event.wait(TP_CHECK_INTERVAL)
                continue

            print(f"\n[Monitor] Kiểm tra {len(positions)} vị thế...")
            for pos in positions:
                mint      = pos["mint"]
                sym       = pos["symbol"]
                chain     = pos.get("chain", "solana")
                confirmed = bool(pos.get("price_confirmed", 0))

                cur = get_price_usd(mint, chain=chain)
                if cur <= 0:
                    continue

                # ── Chưa confirm giá thực ──────────────────────────────
                # Chỉ check rug bằng giá DexScreener, KHÔNG trigger TP
                if not confirmed:
                    est_entry = float(pos.get("entry_price", 0))
                    if est_entry > 0:
                        drop_pct = (cur / est_entry - 1) * 100
                        if drop_pct <= RUG_PRICE_DROP_1H:
                            print(f"  [Monitor] 🚨 RUG (pre-confirm): {sym} {_fmt_pct(drop_pct)}")
                            _alert(
                                f"🚨 <b>RUG DETECTED!</b>\n"
                                f"Token: <b>{sym}</b> [{chain}]\n"
                                f"<code>{mint}</code>\n"
                                f"Drop: <b>{drop_pct:.1f}%</b> — bán khẩn cấp!"
                            )
                            ok = _sell_position(pos, reason="RUG")
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
                tp_target = entry * (1 + TAKE_PROFIT_PCT / 100)

                print(f"  [Monitor] {sym:<10} [{chain}] | "
                      f"Entry: ${entry:.10f} | Cur: ${cur:.10f} | "
                      f"{_fmt_pct(pct)} | TP@${tp_target:.10f}")

                # ── RUG DETECTION: drop >= ngưỡng ─────────────────
                if drop_pct <= RUG_PRICE_DROP_1H:
                    print(f"  [Monitor] 🚨 RUG DETECTED: {sym} {_fmt_pct(drop_pct)} → BÁN NGAY")
                    _alert(
                        f"🚨 <b>RUG DETECTED!</b>\n"
                        f"Token: <b>{sym}</b> [{chain}]\n"
                        f"<code>{mint}</code>\n"
                        f"Drop: <b>{drop_pct:.1f}%</b> — bán khẩn cấp!"
                    )
                    ok = _sell_position(pos, reason="RUG")
                    # Dù bán được hay không, cấm vĩnh viễn
                    db_add_perm_ban(mint, f"rug_detected drop={drop_pct:.1f}%")
                    db_close_position(mint)
                    if not ok:
                        send_error_alert(
                            f"❌ Bán RUG thất bại: {sym}\n"
                            f"<code>{mint}</code> — bán thủ công ngay!")
                    continue

                # ── TAKE PROFIT ────────────────────────────────────
                if pct >= TAKE_PROFIT_PCT:
                    print(f"  [Monitor] 🎯 {sym}: +{pct:.1f}% → BÁN TP")
                    usdc = BASE_USDC if chain == "base" else SOL_USDC
                    raw  = get_token_raw_balance(mint, chain=chain)
                    if raw == "0":
                        db_close_position(mint)
                        continue
                    sell_sig = execute_swap(mint, usdc, raw,
                                            label=f"SELL TP {sym}", chain=chain)
                    if sell_sig:
                        send_tp_signal(pos, cur, pct, sell_sig)
                        db_mark_tp_sent(mint)
                        db_close_position(mint)
                        print(f"  [Monitor] ✅ {sym} TP xong | +{pct:.1f}%")
                    else:
                        send_error_alert(
                            f"❌ <b>Bán TP thất bại!</b>\n"
                            f"Token: {sym}\n<code>{mint}</code>\n"
                            f"Giá: +{pct:.1f}% — bán thủ công!"
                        )
        except Exception as e:
            import traceback
            print(f"[Monitor] ❌ {e}\n{traceback.format_exc()}")

        stop_event.wait(TP_CHECK_INTERVAL)

# ================================================================
# MAIN
# ================================================================

def main():
    # Validate config
    missing = [k for k, v in {
        "OKX_API_KEY": OKX_API_KEY,
        "OKX_SECRET_KEY": OKX_SECRET_KEY,
        "OKX_API_PASSPHRASE": OKX_API_PASSPHRASE,
        "OKX_PROJECT_ID": OKX_PROJECT_ID,
        "SOLANA_WALLET_ADDRESS": WALLET_ADDRESS,
        "SOLANA_PRIVATE_KEY": WALLET_PRIVKEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
        # FIX: kiểm tra BASE_PRIVATE_KEY để fail-fast nếu chưa set
        "BASE_PRIVATE_KEY": BASE_WALLET_PRIVKEY,
    }.items() if not v]
    if missing:
        print(f"⚠️  Thiếu .env: {', '.join(missing)}")
        return

    # Init DB
    init_db()

    print("=" * 65)
    print("🤖  MULTI-CHAIN TRADER v3  🟣 Solana + 🔵 Base  — OKX DEX API")
    print("=" * 65)
    print(f"  Wallet     : {WALLET_ADDRESS[:12]}...{WALLET_ADDRESS[-6:]}")
    print(f"  Buy amount : {BUY_AMOUNT_USDC} USDC / lần")
    print(f"  Min Score  : {MIN_SCORE}/100")
    print(f"  Take Profit: +{TAKE_PROFIT_PCT:.0f}%")
    print(f"  Slippage   : {SLIPPAGE_PCT}%")
    print(f"  Scan every : {SCAN_INTERVAL}s")
    print(f"  TP check   : mỗi {TP_CHECK_INTERVAL}s")
    print(f"  Validators : {VALIDATOR_WORKERS} threads song song")
    print(f"  DB         : {DB_PATH} (SQLite)")
    print(f"  Signal →   : {', '.join(TELEGRAM_SIGNAL_CHANNELS)}")
    print(f"  Positions  : {len(db_get_positions())} đang mở")
    print("=" * 65)
    print()
    print("  THREAD 1: Scanner  → TOKEN_QUEUE")
    print("  THREAD 2: Validator → BUY_QUEUE  (20 parallel)")
    print("  THREAD 3: Buyer    → OKX Swap")
    print("  THREAD 4: Monitor  → TP Check")
    print("=" * 65)

    stop = threading.Event()

    threads = [
        threading.Thread(target=scanner_thread,   args=(stop,), name="Scanner",   daemon=True),
        threading.Thread(target=validator_thread,  args=(stop,), name="Validator", daemon=True),
        threading.Thread(target=buyer_thread,      args=(stop,), name="Buyer",     daemon=True),
        threading.Thread(target=monitor_thread,    args=(stop,), name="Monitor",   daemon=True),
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
        print("✅ Đã dừng.")

if __name__ == "__main__":
    main()
