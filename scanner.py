"""Token discovery engine using multi-source on-chain and market APIs."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List

import httpx

from config import DEXSCREENER_URL, RUNTIME_CONFIG, SUPPORTED_CHAINS, TRADING_CONFIG


class TTLCache:
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = ttl_seconds
        self._store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any:
        value = self._store.get(key)
        if not value:
            return None
        ts, payload = value
        if time.time() - ts > self.ttl_seconds:
            self._store.pop(key, None)
            return None
        return payload

    def set(self, key: str, payload: Any) -> None:
        self._store[key] = (time.time(), payload)


class TokenScanner:
    def __init__(self) -> None:
        self.cache = TTLCache(RUNTIME_CONFIG.cache_ttl_s)

    async def _request_json(self, client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, RUNTIME_CONFIG.retry_attempts + 1):
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except Exception:
                if attempt == RUNTIME_CONFIG.retry_attempts:
                    return {}
                await asyncio.sleep(RUNTIME_CONFIG.retry_backoff_s * attempt)
        return {}

    async def _fetch_chain(self, client: httpx.AsyncClient, chain: str) -> List[Dict[str, Any]]:
        cache_key = f"{chain}:dexscreener"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        payload = await self._request_json(client, DEXSCREENER_URL, {"q": chain})
        pairs = payload.get("pairs", []) if isinstance(payload, dict) else []
        out: List[Dict[str, Any]] = []
        for pair in pairs:
            age_days = pair.get("pairCreatedAt", 0)
            if age_days:
                age_days = max((time.time() * 1000 - age_days) / (24 * 3600 * 1000), 0)
            metrics = {
                "chain": chain,
                "token_address": pair.get("baseToken", {}).get("address"),
                "symbol": pair.get("baseToken", {}).get("symbol", "UNKNOWN"),
                "price": float(pair.get("priceUsd") or 0),
                "liquidity": float((pair.get("liquidity") or {}).get("usd") or 0),
                "market_cap": float(pair.get("marketCap") or 0),
                "token_age_days": float(age_days or 9999),
                "volume_24h": float((pair.get("volume") or {}).get("h24") or 0),
                "volume_1h": float((pair.get("volume") or {}).get("h1") or 0),
                "holder_count": int(pair.get("fdv") or 0),
                "dex_pool": pair.get("pairAddress", ""),
                "source": "dexscreener",
            }
            if metrics["token_address"]:
                out.append(metrics)

        filtered = [
            t
            for t in out
            if TRADING_CONFIG.min_token_age_days <= t["token_age_days"] <= TRADING_CONFIG.max_token_age_days
        ]
        self.cache.set(cache_key, filtered)
        return filtered

    async def scan_tokens(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=RUNTIME_CONFIG.http_timeout_s) as client:
            jobs = [self._fetch_chain(client, chain) for chain in SUPPORTED_CHAINS]
            chunks = await asyncio.gather(*jobs)
        merged = [item for chunk in chunks for item in chunk]
        merged.sort(key=lambda x: x["volume_24h"], reverse=True)
        return merged
