# A/B Profile Testing Guide

> Hướng dẫn vận hành đầy đủ: `docs/USAGE_GUIDE.md`.


## Mục tiêu
A/B test 2 cấu hình mà **không thay đổi logic thuật toán** trong `core/engine_legacy.py`.

## Profiles
- `profiles/early_sniper.env`: ưu tiên tốc độ + fill cho token mới.
- `profiles/safe_trend.env`: ưu tiên an toàn + giảm churn.

## Cách áp dụng
Chạy từ thư mục `solana-meme-bot/`:

```bash
python scripts/apply_profile.py early_sniper
# hoặc
python scripts/apply_profile.py safe_trend
```

Script sẽ merge profile vào `.env`, giữ lại các biến nhạy cảm đã có (wallet/API keys).

## Kịch bản test đề xuất
- Day 1-2: `early_sniper`
- Day 3-4: `safe_trend`
- Cùng khung giờ giao dịch, cùng vốn/lệnh để so sánh công bằng.

## KPI nên theo dõi
- Fill rate (tỷ lệ mua thành công)
- Slippage realized
- Win rate
- PnL net/ngày
- Max drawdown nội ngày
- Số lần FAST_DUMP exit

## Auto Parameter Optimizer

Tự động đề xuất/ghi đè bộ tham số theo trade log:

```bash
python scripts/auto_parameter_optimizer.py
python scripts/auto_parameter_optimizer.py --apply
```

Các tham số tối ưu gồm: score ngưỡng, bộ TP/trailing, volume spike, LP SOL tối thiểu, token age filter.


## Cập nhật .env (quan trọng)

Có, cần cập nhật `.env` chính (file chạy bot) với bộ biến mới.

Bạn có thể copy nhanh từ `.env.example` hoặc thêm trực tiếp các key sau:

```env
MIN_OPPORTUNITY_SCORE=83
MIN_HOLDER_COUNT=5
MAX_TOP10_HOLDER_PCT=20
ENTRY_DIP_WINDOW_S=5
ENTRY_BBL_WINDOW_S=15
ENTRY_BBL_PERIOD=14
ENTRY_BBL_STD=1.8
TP1_PCT=25
TP2_PCT=60
TRAIL_TRIGGER_PCT=20
TRAILING_START_PCT=20
TRAILING_STOP_PCT=10
VOLUME_SPIKE_MULTIPLIER=2
MIN_LP_SOL=5
MIN_TOKEN_AGE_S=90
```

Nếu dùng optimizer (`--apply`), script sẽ ghi các biến này vào `.env` tự động.


## Đổi profile qua Telegram

Bạn có thể đổi nhanh khi bot đang chạy:

```text
/profile early_sniper
/profile safe_trend
```

Sau đó dùng `/save` để lưu vào `.env`.
