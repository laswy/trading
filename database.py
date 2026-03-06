"""Async database layer for storing positions, trades, and analytics artifacts."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from sqlalchemy import JSON, Column, DateTime, Float, Integer, MetaData, String, Table, Text, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from config import DATABASE_URL

metadata = MetaData()

positions = Table(
    "positions",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("token_address", String(128), unique=True, index=True, nullable=False),
    Column("chain", String(24), nullable=False),
    Column("entry_price", Float, nullable=False),
    Column("current_price", Float, nullable=False),
    Column("size_usd", Float, nullable=False),
    Column("opened_at", DateTime, default=datetime.utcnow, nullable=False),
    Column("status", String(24), default="open", nullable=False),
)

trades = Table(
    "trades",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("token_address", String(128), nullable=False),
    Column("chain", String(24), nullable=False),
    Column("side", String(8), nullable=False),
    Column("qty", Float, nullable=False),
    Column("price", Float, nullable=False),
    Column("score", Float, nullable=False),
    Column("metadata", JSON, nullable=True),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)


def _generic_table(name: str) -> Table:
    return Table(
        name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("token_address", String(128), index=True, nullable=False),
        Column("chain", String(24), nullable=False),
        Column("payload", JSON, nullable=False),
        Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
        extend_existing=True,
    )


tokens = _generic_table("tokens")
smart_wallets = _generic_table("smart_wallets")
wallet_clusters = _generic_table("wallet_clusters")
price_history = _generic_table("price_history")
liquidity_events = _generic_table("liquidity_events")
model_predictions = _generic_table("model_predictions")


class Database:
    def __init__(self, url: str = DATABASE_URL) -> None:
        self.engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)

    async def init(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.begin() as conn:
            yield conn

    async def insert(self, table: Table, values: Dict[str, Any]) -> None:
        async with self.connection() as conn:
            await conn.execute(table.insert().values(**values))

    async def insert_many(self, table: Table, rows: Iterable[Dict[str, Any]]) -> None:
        data = list(rows)
        if not data:
            return
        async with self.connection() as conn:
            await conn.execute(table.insert(), data)

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        async with self.connection() as conn:
            query = select(positions).where(positions.c.status == "open")
            result = await conn.execute(query)
            return [dict(r._mapping) for r in result.fetchall()]

    async def update_position_price(self, token_address: str, new_price: float) -> None:
        async with self.connection() as conn:
            await conn.execute(
                positions.update()
                .where(positions.c.token_address == token_address)
                .values(current_price=new_price)
            )

    async def close(self) -> None:
        await self.engine.dispose()
