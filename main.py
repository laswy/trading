"""Main runtime: distributed async workers for scan -> validate -> score -> execute -> monitor."""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List

from alpha_model import AlphaModel
from config import RUNTIME_CONFIG, TRADING_CONFIG
from database import Database, liquidity_events, model_predictions, smart_wallets, tokens, trades
from liquidity_flow import LiquidityFlowEngine
from market_structure import MarketStructureEngine
from monitor_engine import MonitorEngine
from portfolio_optimizer import PortfolioOptimizer
from position_manager import PositionManager, PositionState
from risk_engine import RiskEngine, RiskState
from scanner import TokenScanner
from scoring_engine import OpportunityScoringEngine
from smart_money import SmartMoneyEngine
from telegram_notifier import TelegramNotifier
from trade_executor import TradeExecutionEngine
from validator import TokenValidator
from wallet_clusters import WalletClusterEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("runtime")


class TradingRuntime:
    def __init__(self) -> None:
        self.db = Database()
        self.scanner = TokenScanner()
        self.validator = TokenValidator()
        self.market = MarketStructureEngine()
        self.smart_money = SmartMoneyEngine()
        self.clusters = WalletClusterEngine()
        self.liquidity = LiquidityFlowEngine()
        self.alpha = AlphaModel()
        self.scoring = OpportunityScoringEngine()
        self.executor = TradeExecutionEngine()
        self.position_manager = PositionManager()
        self.portfolio_optimizer = PortfolioOptimizer()
        self.risk_engine = RiskEngine()
        self.monitor = MonitorEngine()
        self.notifier = TelegramNotifier()
        self.scan_queue: asyncio.Queue = asyncio.Queue(maxsize=400)
        self.trade_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.positions: Dict[str, PositionState] = {}
        self.risk_state = RiskState(start_equity=100_000, current_equity=100_000)

    async def _scanner_worker(self) -> None:
        while True:
            tokens_found = await self.scanner.scan_tokens()
            for token in tokens_found:
                await self.scan_queue.put(token)
            logger.info("scanner: discovered=%s", len(tokens_found))
            await asyncio.sleep(RUNTIME_CONFIG.scan_interval_s)

    async def _validator_worker(self, worker_id: int) -> None:
        while True:
            token = await self.scan_queue.get()
            token.setdefault("contract_verified", True)
            token.setdefault("honeypot_risk", False)
            token.setdefault("buy_tax", 2.0)
            token.setdefault("sell_tax", 2.0)
            token.setdefault("top10_holder_pct", 20.0)
            token.setdefault("lp_locked", True)
            token.setdefault("owner_suspicious", False)

            ok, issues = self.validator.validate(token)
            if not ok:
                logger.debug("validator[%s]: %s rejected: %s", worker_id, token.get("symbol"), issues)
                self.scan_queue.task_done()
                continue

            await self.db.insert(tokens, {"token_address": token["token_address"], "chain": token["chain"], "payload": token})
            await self.trade_queue.put(token)
            self.scan_queue.task_done()

    async def _decision_worker(self) -> None:
        while True:
            token = await self.trade_queue.get()

            dummy_candles = [{"close": token["price"] * (1 + i * 0.001), "volume": max(token["volume_1h"], 1)} for i in range(1, 220)]
            market_signal = self.market.analyze(dummy_candles)
            momentum = self.market.momentum_score(token["volume_1h"], token["volume_24h"], [0.01, 0.03, -0.005, 0.02])

            flow_signal = self.smart_money.analyze_flows([])
            cluster_data = self.clusters.cluster([])
            liquidity_signal = self.liquidity.analyze([])
            cluster_score = self.clusters.score(cluster_data)

            features = {
                "trend_score": market_signal["trend_score"],
                "momentum_score": momentum,
                "liquidity_score": liquidity_signal["liquidity_score"],
                "smart_money_score": flow_signal["smart_money_score"],
                "wallet_cluster_score": cluster_score,
                "volatility": 0.3,
                "rsi": market_signal["rsi"],
                "macd": market_signal["macd"],
                "liquidity_inflow": 1.0 if liquidity_signal["liquidity_inflow"] else 0.0,
            }

            ai_prob = self.alpha.predict_probability(features)
            scored = self.scoring.score(features, ai_prob)

            await self.db.insert(model_predictions, {
                "token_address": token["token_address"],
                "chain": token["chain"],
                "payload": {"probability": ai_prob, "features": features, "score": scored},
            })
            await self.db.insert(liquidity_events, {
                "token_address": token["token_address"],
                "chain": token["chain"],
                "payload": liquidity_signal,
            })
            await self.db.insert(smart_wallets, {
                "token_address": token["token_address"],
                "chain": token["chain"],
                "payload": flow_signal,
            })

            if scored["decision"] in {"buy", "strong_buy"} and not self.risk_state.halted:
                position_size = self.position_manager.size_position(
                    self.risk_state.current_equity,
                    ai_prob,
                    volatility=0.3,
                    liquidity=token["liquidity"],
                )
                result = await self.executor.execute(token, "buy", position_size, pending_swaps=[])
                if result["status"] == "submitted":
                    self.positions[token["token_address"]] = PositionState(
                        token_address=token["token_address"],
                        entry_price=token["price"],
                        size_usd=position_size,
                        peak_price=token["price"],
                    )
                    await self.db.insert(trades, {
                        "token_address": token["token_address"],
                        "chain": token["chain"],
                        "side": "buy",
                        "qty": position_size / max(token["price"], 1e-9),
                        "price": token["price"],
                        "score": scored["score"],
                        "metadata": result,
                    })
                    await self.notifier.send(f"BUY {token['symbol']} score={scored['score']} prob={ai_prob:.2%}")
            self.trade_queue.task_done()

    async def _monitor_worker(self) -> None:
        while True:
            if self.risk_engine.evaluate_system_state(self.risk_state) == "halt":
                await self.notifier.send("⚠️ Risk shutdown: max daily loss exceeded")
            for token_address, position in list(self.positions.items()):
                simulated_price = position.peak_price * 0.99
                ladder = self.position_manager.apply_profit_ladder(position, simulated_price)
                if ladder["actions"]:
                    await self.notifier.send(f"Profit ladder triggered {token_address}: {ladder['actions']}")
                if self.position_manager.trailing_stop_triggered(position, simulated_price):
                    await self.notifier.send(f"Trailing stop hit {token_address}")
                    self.positions.pop(token_address, None)
                if self.risk_engine.position_loss_exceeded(position.entry_price, simulated_price):
                    await self.notifier.send(f"Hard stop loss {token_address}")
                    self.positions.pop(token_address, None)

                reason = self.monitor.exit_signal({
                    "trend_breakdown": False,
                    "volume_collapse": False,
                    "whale_selling": False,
                    "liquidity_removal": False,
                })
                if reason:
                    await self.notifier.send(f"Exit signal {token_address}: {reason}")
                    self.positions.pop(token_address, None)
            await asyncio.sleep(RUNTIME_CONFIG.monitor_interval_s)

    async def run(self) -> None:
        await self.db.init()
        workers: List[asyncio.Task] = [asyncio.create_task(self._scanner_worker())]
        workers.extend(asyncio.create_task(self._validator_worker(i)) for i in range(RUNTIME_CONFIG.max_parallel_workers))
        workers.append(asyncio.create_task(self._decision_worker()))
        workers.append(asyncio.create_task(self._monitor_worker()))

        try:
            await asyncio.gather(*workers)
        finally:
            for worker in workers:
                worker.cancel()
            await self.db.close()


async def main() -> None:
    runtime = TradingRuntime()
    await runtime.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
