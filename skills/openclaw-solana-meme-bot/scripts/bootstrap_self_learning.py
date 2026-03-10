#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BOT = ROOT / "solana-meme-bot"
ML = BOT / "ml"

FILES = {
    "README.md": """# ML Pipeline Scaffold

This folder contains self-learning scaffolding for the trading bot.

- data_pipeline.py: export/prepare datasets
- train.py: train candidate model
- evaluate.py: compare candidate vs champion
- promote.py: register/promote candidate model
- infer.py: runtime inference entrypoint
""",
    "data_pipeline.py": """def export_dataset():
    print('TODO: export dataset from trades/events/logs')

if __name__ == '__main__':
    export_dataset()
""",
    "train.py": """def train():
    print('TODO: train candidate model')

if __name__ == '__main__':
    train()
""",
    "evaluate.py": """def evaluate():
    print('TODO: evaluate candidate vs champion')

if __name__ == '__main__':
    evaluate()
""",
    "promote.py": """def promote():
    print('TODO: promote model if guardrails pass')

if __name__ == '__main__':
    promote()
""",
    "infer.py": """def predict(features: dict) -> dict:
    return {'action': 'hold', 'confidence': 0.0}
""",
}

ENV_APPEND = """
# --- Self-learning knobs (optional) ---
ML_MODE=off
ML_SHADOW=true
ML_CANARY_RATIO=0.1
ML_RETRAIN_SCHEDULE=daily
ML_MAX_DD_PCT=10
""".strip() + "\n"


def ensure_file(path: Path, content: str) -> None:
    if path.exists():
        return
    path.write_text(content, encoding="utf-8")


def main() -> None:
    if not BOT.exists():
        raise SystemExit(f"Missing bot directory: {BOT}")

    ML.mkdir(parents=True, exist_ok=True)
    for name, content in FILES.items():
        ensure_file(ML / name, content)

    env_path = BOT / ".env"
    if env_path.exists():
        txt = env_path.read_text(encoding="utf-8")
        if "ML_MODE=" not in txt:
            with env_path.open("a", encoding="utf-8") as f:
                f.write("\n" + ENV_APPEND)

    print(f"[OK] scaffold ready: {ML}")


if __name__ == "__main__":
    main()
