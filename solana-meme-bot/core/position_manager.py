from pathlib import Path
from core.engine_legacy import (
    db_save_position,
    db_close_position,
    db_get_positions,
    db_has_position,
    monitor_thread,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
POSITIONS_FILE = DATA_DIR / "positions.json"
TRADE_LOG_FILE = DATA_DIR / "trade_log.json"
