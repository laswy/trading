"""Telegram notifier for signals, risk events, and portfolio updates."""

from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from config import RUNTIME_CONFIG, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


class TelegramNotifier:
    def __init__(self, bot_token: str = TELEGRAM_BOT_TOKEN, chat_id: str = TELEGRAM_CHAT_ID) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    async def send(self, message: str) -> bool:
        if not self.bot_token or not self.chat_id:
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message}

        async with httpx.AsyncClient(timeout=RUNTIME_CONFIG.http_timeout_s) as client:
            for attempt in range(1, RUNTIME_CONFIG.retry_attempts + 1):
                try:
                    response = await client.post(url, json=payload)
                    response.raise_for_status()
                    return True
                except Exception:
                    if attempt == RUNTIME_CONFIG.retry_attempts:
                        return False
                    await asyncio.sleep(RUNTIME_CONFIG.retry_backoff_s * attempt)
        return False
