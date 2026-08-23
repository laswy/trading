# Hướng dẫn sử dụng Solana Meme Bot

Tài liệu này hướng dẫn chạy bot từ đầu, áp dụng profile, tối ưu tham số tự động và checklist vận hành an toàn.

## 0) Chạy nhanh (Linux, 1 lệnh)

Từ thư mục `solana-meme-bot/`:

```bash
./run.sh
```

Script tự làm: tạo `.venv` nếu chưa có → cài `requirements.txt` → nếu chưa có
`.env` thì tạo từ `.env.example` rồi dừng lại để bạn điền secrets → lần chạy
tiếp theo sẽ tự động khởi động bot. Không cần làm thủ công các bước ở mục 1-2
bên dưới nữa — chỉ cần chạy lại `./run.sh` sau khi điền xong `.env`.

Nếu `./run.sh: Permission denied`, chạy `chmod +x run.sh` một lần rồi thử lại
(hoặc dùng `bash run.sh`).

## 1) Chuẩn bị môi trường thủ công (nếu không dùng run.sh)

Từ thư mục `solana-meme-bot/`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Tạo file `.env`

Copy file mẫu và điền secrets:

```bash
cp .env.example .env
```

Các biến bắt buộc tối thiểu:

- `SOLANA_WALLET_ADDRESS`
- `SOLANA_PRIVATE_KEY`
- `HELIUS_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Bộ tham số memecoin baseline đã có sẵn trong `.env.example` (score, entry windows, TP/trailing, volume spike, LP, token age).

### 2a) Chế độ NOTIFY-ONLY vs AUTO-BUY

Mặc định (`AUTO_BUY_ENABLED=0`): bot **không tự mua/bán**. Khi 1 token đạt
`MIN_OPPORTUNITY_SCORE` và qua EntryGuard, bot gửi thông báo Telegram
"🔔 CƠ HỘI MUA" kèm điểm số, giá, mức mua gợi ý, và buy-links (OKX Web3,
Jupiter, Bitget, Binance Web3, DexScreener...) để bạn tự vào lệnh bằng ví
của chính bạn. Bot không ký/gửi bất kỳ transaction nào, không lưu position,
không tự chốt lời/cắt lỗ.

Muốn quay lại chế độ tự động mua + quản lý TP/rug/trailing-stop như trước:
đặt `AUTO_BUY_ENABLED=1` trong `.env`.

Log khởi động (`python bot.py`) in rõ đang chạy chế độ nào (`🔔 CHẾ ĐỘ
NOTIFY-ONLY` hoặc banner auto-trade đầy đủ), và lệnh Telegram `/status` luôn
hiển thị mode hiện tại ở đầu.

### 2b) Bật thêm chain: BNB / Ethereum / Robinhood Chain (tùy chọn)

Bot mặc định trade Solana (Jupiter) + Base (OKX aggregator). Có thể bật thêm
3 chain nữa, swap **thẳng qua router on-chain** (không qua OKX):

| Chain | DEX | Cần điền tối thiểu |
|---|---|---|
| BNB Chain | PancakeSwap V2 | `EVM_PRIVATE_KEY` (có default RPC + router) |
| Ethereum | Uniswap V2 | `EVM_PRIVATE_KEY`, `ETH_RPC_URL` (Infura/Alchemy — không có public RPC ổn định) |
| Robinhood Chain | Uniswap V3 | `EVM_PRIVATE_KEY`, `ROBINHOOD_ROUTER_ADDRESS`, `ROBINHOOD_QUOTER_ADDRESS`, `ROBINHOOD_WETH_ADDRESS` |

`EVM_PRIVATE_KEY` dùng chung 1 ví cho cả Base/BNB/ETH/Robinhood (địa chỉ EVM
giống nhau trên mọi chain EVM). Nếu đã có `BASE_PRIVATE_KEY`, bot tự dùng lại
key đó — không bắt buộc set thêm.

**⚠️ Robinhood Chain — bắt buộc tự xác minh trước khi bật:**
Chain này mainnet từ 7/2026 (rất mới). Router/Quoter/WETH address **không có
default hardcode trong code** — bot sẽ báo `DISABLED` khi khởi động nếu thiếu.
Trước khi điền vào `.env`:

1. Vào https://robinhoodchain.blockscout.com, tự tra và xác nhận địa chỉ
   Uniswap V3 SwapRouter02 / QuoterV2 / WETH9 đang hoạt động thật trên chain
   (chain_id `4663`) — đừng tin bất kỳ địa chỉ nào chép từ nơi khác mà chưa
   tự kiểm tra trên explorer.
2. Chỉ sau khi xác nhận, điền `ROBINHOOD_ROUTER_ADDRESS` /
   `ROBINHOOD_QUOTER_ADDRESS` / `ROBINHOOD_WETH_ADDRESS` vào `.env`.
3. Test với `BUY_AMOUNT_ETH_ROBINHOOD` cực nhỏ (vd `0.001`) trước khi tăng lên
   mức thật.

Log khởi động (`python bot.py`) sẽ in rõ chain nào ENABLED/DISABLED và lý do
thiếu config, ví dụ `⚠️ ROBINHOOD chain DISABLED (thiếu: thiếu router address)`.

## 3) Chạy bot

```bash
python bot.py
```

(hoặc đơn giản hơn: `./run.sh` — xem mục 0)

Bot sẽ:

- scan token mới,
- validate theo security + quality filters,
- chấm điểm cơ hội,
- **mặc định (`AUTO_BUY_ENABLED=0`)**: gửi thông báo Telegram kèm buy-links để bạn tự vào lệnh — xem mục 2a,
- **nếu `AUTO_BUY_ENABLED=1`**: tự vào lệnh khi đủ điều kiện và quản lý thoát lệnh theo TP/trailing/risk.

## 4) Đổi nhanh cấu hình bằng profile

Có 2 profile có sẵn:

- `early_sniper` (aggressive)
- `safe_trend` (conservative)

Áp dụng profile (merge vào `.env`, giữ secrets cũ):

```bash
python scripts/apply_profile.py early_sniper
# hoặc
python scripts/apply_profile.py safe_trend
```

Đổi profile trực tiếp trên Telegram (runtime):

```text
/profile early_sniper
/profile safe_trend
```

Sau khi chọn profile trên Telegram, dùng `/save` (hoặc nút `💾 Lưu .env`) để persist.

## 5) Dùng AUTO PARAMETER OPTIMIZER

Xem đề xuất tham số từ lịch sử trade log:

```bash
python scripts/auto_parameter_optimizer.py
```

Ghi đè trực tiếp vào `.env`:

```bash
python scripts/auto_parameter_optimizer.py --apply
```

Gợi ý vận hành:

- Chạy optimizer sau mỗi 1-2 ngày có đủ số lệnh.
- A/B theo block thời gian cố định để so sánh công bằng.
- Luôn backup `.env` trước khi `--apply`.

## 5b) Bảo vệ vốn nâng cao (chỉ áp dụng khi `AUTO_BUY_ENABLED=1`)

Các tính năng dưới đây học từ 1 bản bot cũ, chỉ có tác dụng khi bot đang tự
động mua/bán (`AUTO_BUY_ENABLED=1`) — ở chế độ notify-only chúng không chạy
vì không có vị thế tự động nào để theo dõi.

- **Tự pause khi lỗ liên tiếp** (`RISK_CONSEC_LOSS_STOP`, mặc định 3): sau N
  lệnh lỗ liên tiếp, bot tự dừng mua mới (vị thế đang mở vẫn được theo dõi/bán
  bình thường). Dùng `/resume` trên Telegram để tiếp tục.
- **Pause/Resume thủ công**: `/pause` dừng mua ngay lập tức, `/resume` mở lại
  — hữu ích khi muốn tạm ngừng mà không cần tắt bot.
- **Time-Based Stop** (`TIME_STOP_ENABLED`, `TIME_STOP_MIN`, `TIME_STOP_MIN_PNL`):
  bán vị thế nếu giữ quá `TIME_STOP_MIN` phút mà PnL vẫn ≤ `TIME_STOP_MIN_PNL`%
  (đang "stuck") — giải phóng vốn thay vì ôm bag vô thời hạn.
- **Volume-Based Stop** (`VOL_STOP_ENABLED`, `VOL_STOP_DROP`, `VOL_STOP_MIN_PROFIT`):
  đang lãi ≥ `VOL_STOP_MIN_PROFIT`% nhưng volume 5m sập ≥ `VOL_STOP_DROP`% từ
  đỉnh → chốt lời bảo toàn trước khi thanh khoản cạn, không chờ TP2/trailing.
- **Watchdog**: tự restart thread nào chết bất ngờ (không cần bạn tự khởi động
  lại bot), kèm cảnh báo Telegram mỗi lần restart.
- **Daily PnL Report** (`DAILY_REPORT_ENABLED`): gửi báo cáo PnL quy đổi USD
  lúc 00:00 UTC mỗi ngày. Xem thủ công bất kỳ lúc nào bằng `/pnl [N]` (N ngày,
  mặc định 1, 0 = toàn bộ lịch sử) — khác `/report` (chỉ 24h, cộng theo native
  unit nên không gộp được PnL giữa SOL/USDC/BNB/ETH), `/pnl` quy đổi USD nên
  cộng dồn đúng across mọi chain.

## 6) Quy trình vận hành khuyến nghị

1. Apply `safe_trend` để khởi động an toàn.
2. Chạy bot, theo dõi telegram alert + log lệnh.
3. Sau khi có dữ liệu, chạy optimizer (không apply trước).
4. Review tham số đề xuất.
5. Backup `.env` rồi mới `--apply`.
6. Chạy tiếp và so sánh KPI: winrate, avg profit, drawdown, slippage.

## 7) Troubleshooting nhanh

- **Bot không gửi thông báo / không vào lệnh**: kiểm tra `MIN_OPPORTUNITY_SCORE`, volume spike filter, token age filter quá chặt.
- **Thông báo/vào lệnh ít**: giảm nhẹ `MIN_OPPORTUNITY_SCORE` hoặc `VOLUME_SPIKE_MULTIPLIER`.
- **Muốn bot tự mua thay vì chỉ thông báo**: set `AUTO_BUY_ENABLED=1` trong `.env` — xem mục 2a.
- **Chốt lời quá sớm**: tăng `TP2_PCT` hoặc nới `TRAILING_STOP_PCT`.
- **Rủi ro cao**: tăng `MIN_HOLDER_COUNT`, giảm `MAX_TOP10_HOLDER_PCT`, tăng `MIN_TOKEN_AGE_S`.
- **Chain BNB/ETH/Robinhood không hoạt động**: xem log khởi động — bot in rõ `DISABLED (thiếu: ...)` cho từng chain. Điền đủ biến còn thiếu trong `.env` (mục 2b) rồi khởi động lại.

## 8) Lệnh kiểm tra nhanh

```bash
python -m py_compile core/engine_legacy.py scripts/auto_parameter_optimizer.py scripts/apply_profile.py
```

Nếu command trên pass, code Python chính không có lỗi cú pháp.
