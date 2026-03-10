# A/B Profile Testing Guide

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
