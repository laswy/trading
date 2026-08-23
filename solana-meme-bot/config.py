from pathlib import Path
import os
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


def get_env(key: str, default=None):
    return os.getenv(key, default)
