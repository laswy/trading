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
