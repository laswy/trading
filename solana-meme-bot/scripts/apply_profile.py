#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
PROFILES_DIR = ROOT / "profiles"


def parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        out[key.strip()] = val.strip()
    return out


def write_env_file(path: Path, values: dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in sorted(values.items())]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply trading profile overrides into .env")
    parser.add_argument("profile", choices=["early_sniper", "safe_trend"], help="Profile name")
    args = parser.parse_args()

    profile_path = PROFILES_DIR / f"{args.profile}.env"
    if not profile_path.exists():
        raise SystemExit(f"Profile file not found: {profile_path}")

    base = parse_env_file(ENV_PATH)
    overrides = parse_env_file(profile_path)
    base.update(overrides)
    write_env_file(ENV_PATH, base)

    print(f"Applied profile: {args.profile}")
    print(f"Updated: {ENV_PATH}")


if __name__ == "__main__":
    main()
