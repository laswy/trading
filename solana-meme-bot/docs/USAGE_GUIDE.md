# Hướng dẫn sử dụng Solana Meme Bot

Tài liệu này hướng dẫn chạy bot từ đầu, áp dụng profile, tối ưu tham số tự động và checklist vận hành an toàn.

## 1) Chuẩn bị môi trường

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

Log khởi động (`python main.py`) sẽ in rõ chain nào ENABLED/DISABLED và lý do
thiếu config, ví dụ `⚠️ ROBINHOOD chain DISABLED (thiếu: thiếu router address)`.

## 3) Chạy bot

```bash
python main.py
```

Bot sẽ:

- scan token mới,
- validate theo security + quality filters,
- chấm điểm cơ hội,
- tự vào lệnh khi đủ điều kiện,
- quản lý thoát lệnh theo TP/trailing/risk.

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

## 6) Quy trình vận hành khuyến nghị

1. Apply `safe_trend` để khởi động an toàn.
2. Chạy bot, theo dõi telegram alert + log lệnh.
3. Sau khi có dữ liệu, chạy optimizer (không apply trước).
4. Review tham số đề xuất.
5. Backup `.env` rồi mới `--apply`.
6. Chạy tiếp và so sánh KPI: winrate, avg profit, drawdown, slippage.

## 7) Troubleshooting nhanh

- **Bot không vào lệnh**: kiểm tra `MIN_OPPORTUNITY_SCORE`, volume spike filter, token age filter quá chặt.
- **Vào lệnh ít**: giảm nhẹ `MIN_OPPORTUNITY_SCORE` hoặc `VOLUME_SPIKE_MULTIPLIER`.
- **Chốt lời quá sớm**: tăng `TP2_PCT` hoặc nới `TRAILING_STOP_PCT`.
- **Rủi ro cao**: tăng `MIN_HOLDER_COUNT`, giảm `MAX_TOP10_HOLDER_PCT`, tăng `MIN_TOKEN_AGE_S`.
- **Chain BNB/ETH/Robinhood không hoạt động**: xem log khởi động — bot in rõ `DISABLED (thiếu: ...)` cho từng chain. Điền đủ biến còn thiếu trong `.env` (mục 2b) rồi khởi động lại.

## 8) Lệnh kiểm tra nhanh

```bash
python -m py_compile core/engine_legacy.py scripts/auto_parameter_optimizer.py scripts/apply_profile.py
```

Nếu command trên pass, code Python chính không có lỗi cú pháp.
