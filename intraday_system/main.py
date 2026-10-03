"""
Main orchestrator and event loop for the Rapid Fire Intraday Trading System.

This module coordinates all components of the trading system:
- Data feed management
- Strategy execution
- Signal aggregation
- Risk management
- Order execution
- Portfolio tracking
- Notifications

The system runs in an async event loop with sub-second latency targets.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import aiohttp

# Import all models
from .models import (
    AggregatedSignal,
    BrokerType,
    OHLCV,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    PerformanceMetrics,
    PortfolioState,
    Position,
    PositionSide,
    RiskParameters,
    RiskState,
    SignalType,
    StrategyName,
    SystemState,
    Tick,
    TimeFrame,
    Trade,
    TradeStatus,
)

# Import system components
from .config_loader import ConfigLoader
from .data_feed import DataFeed
from .execution_engine import ExecutionEngine
from .logger import get_logger
from .order_manager import OrderManager
from .portfolio_tracker import PortfolioTracker
from .risk_manager import RiskManager
from .signal_aggregator import SignalAggregator
from .strategy_engine import StrategyEngine


class TradingSystem:
    """
    Main trading system orchestrator.
    
    Coordinates all components and manages the event loop for:
    - Real-time market data processing
    - Strategy signal generation
    - Signal aggregation and voting
    - Risk checks and position sizing
    - Order placement and management
    - Portfolio tracking and PnL calculation
    - Notifications and alerts
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the trading system.
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = get_logger("main")
        self.config_path = config_path
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load_config()
        
        # Initialize system state
        self.system_state = SystemState(
            is_running=False,
            is_paper_trading=self.config.trading.paper_trading,
            current_mode="PAPER" if self.config.trading.paper_trading else "LIVE"
        )
        
        # Component instances (initialized in setup)
        self.data_feed: Optional[DataFeed] = None
        self.strategy_engine: Optional[StrategyEngine] = None
        self.signal_aggregator: Optional[SignalAggregator] = None
        self.risk_manager: Optional[RiskManager] = None
        self.order_manager: Optional[OrderManager] = None
        self.execution_engine: Optional[ExecutionEngine] = None
        self.portfolio_tracker: Optional[PortfolioTracker] = None
        
        # Event queues for inter-component communication
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue()
        self.ohlcv_queue: asyncio.Queue[OHLCV] = asyncio.Queue()
        self.signal_queue: asyncio.Queue[AggregatedSignal] = asyncio.Queue()
        self.order_queue: asyncio.Queue[Order] = asyncio.Queue()
        self.fill_queue: asyncio.Queue[Order] = asyncio.Queue()
        self.trade_queue: asyncio.Queue[Trade] = asyncio.Queue()
        
        # Shared state
        self.current_ticks: Dict[str, Tick] = {}
        self.current_prices: Dict[str, float] = {}
        self.active_symbols: Set[str] = set(self.config.trading.symbols)
        
        # Shutdown flag
        self._shutdown = False
        self._shutdown_event = asyncio.Event()
        
        # Performance tracking
        self.performance_metrics = PerformanceMetrics()
        
        self.logger.info("TradingSystem initialized")
    
    async def setup(self) -> None:
        """
        Initialize all system components.
        """
        self.logger.info("Setting up trading system components...")
        
        # Initialize components
        self.data_feed = DataFeed(
            config=self.config,
            tick_queue=self.tick_queue,
            ohlcv_queue=self.ohlcv_queue,
            system_state=self.system_state
        )
        
        self.portfolio_tracker = PortfolioTracker(
            config=self.config,
            trade_queue=self.trade_queue
        )
        
        self.risk_manager = RiskManager(
            config=self.config,
            portfolio_tracker=self.portfolio_tracker,
            system_state=self.system_state
        )
        
        self.order_manager = OrderManager(
            config=self.config,
            order_queue=self.order_queue,
            fill_queue=self.fill_queue,
            system_state=self.system_state
        )
        
        self.strategy_engine = StrategyEngine(
            config=self.config,
            ohlcv_queue=self.ohlcv_queue,
            system_state=self.system_state
        )
        
        self.signal_aggregator = SignalAggregator(
            config=self.config,
            signal_queue=self.signal_queue,
            system_state=self.system_state
        )
        
        self.execution_engine = ExecutionEngine(
            config=self.config,
            signal_queue=self.signal_queue,
            order_queue=self.order_queue,
            risk_manager=self.risk_manager,
            portfolio_tracker=self.portfolio_tracker,
            system_state=self.system_state
        )
        
        # Set up cross-component references
        self.data_feed.portfolio_tracker = self.portfolio_tracker
        self.strategy_engine.portfolio_tracker = self.portfolio_tracker
        self.execution_engine.order_manager = self.order_manager
        self.portfolio_tracker.risk_manager = self.risk_manager
        
        self.logger.info("All components initialized")
    
    async def start_data_feed(self) -> None:
        """
        Start the market data feed.
        """
        if self.data_feed:
            await self.data_feed.start()
            self.logger.info("Data feed started")
    
    async def start_strategy_engine(self) -> None:
        """
        Start the strategy engine.
        """
        if self.strategy_engine:
            await self.strategy_engine.start()
            self.logger.info("Strategy engine started")
    
    async def start_signal_aggregator(self) -> None:
        """
        Start the signal aggregator.
        """
        if self.signal_aggregator:
            await self.signal_aggregator.start()
            self.logger.info("Signal aggregator started")
    
    async def start_execution_engine(self) -> None:
        """
        Start the execution engine.
        """
        if self.execution_engine:
            await self.execution_engine.start()
            self.logger.info("Execution engine started")
    
    async def start_order_manager(self) -> None:
        """
        Start the order manager.
        """
        if self.order_manager:
            await self.order_manager.start()
            self.logger.info("Order manager started")
    
    async def start_portfolio_tracker(self) -> None:
        """
        Start the portfolio tracker.
        """
        if self.portfolio_tracker:
            await self.portfolio_tracker.start()
            self.logger.info("Portfolio tracker started")
    
    async def process_ticks(self) -> None:
        """
        Process incoming ticks and update shared state.
        """
        self.logger.info("Tick processor started")
        
        while not self._shutdown:
            try:
                # Process ticks with timeout to allow graceful shutdown
                try:
                    tick = await asyncio.wait_for(
                        self.tick_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Update current tick and price
                self.current_ticks[tick.symbol] = tick
                self.current_prices[tick.symbol] = tick.last_price
                
                # Update system state
                self.system_state.last_tick_time[tick.symbol] = tick.timestamp
                
                # Pass tick to portfolio tracker for PnL updates
                if self.portfolio_tracker:
                    await self.portfolio_tracker.on_tick(tick)
                
                # Check for kill switch conditions
                if self.risk_manager:
                    await self.risk_manager.check_kill_switch()
                
            except Exception as e:
                self.logger.error(f"Error processing tick: {e}", exc_info=True)
            finally:
                # Mark task as done
                self.tick_queue.task_done()
        
        self.logger.info("Tick processor stopped")
    
    async def process_fills(self) -> None:
        """
        Process order fills and update portfolio.
        """
        self.logger.info("Fill processor started")
        
        while not self._shutdown:
            try:
                try:
                    fill = await asyncio.wait_for(
                        self.fill_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Update portfolio with fill
                if self.portfolio_tracker and fill.status == OrderStatus.EXECUTED:
                    await self.portfolio_tracker.on_fill(fill)
                
                # Update order manager
                if self.order_manager:
                    await self.order_manager.on_fill(fill)
                
                # Update execution engine
                if self.execution_engine:
                    await self.execution_engine.on_fill(fill)
                
                self.logger.debug(f"Processed fill: {fill.order_id} - {fill.filled_quantity} @ {fill.average_fill_price}")
                
            except Exception as e:
                self.logger.error(f"Error processing fill: {e}", exc_info=True)
            finally:
                self.fill_queue.task_done()
        
        self.logger.info("Fill processor stopped")
    
    async def process_trades(self) -> None:
        """
        Process completed trades for performance tracking.
        """
        self.logger.info("Trade processor started")
        
        while not self._shutdown:
            try:
                try:
                    trade = await asyncio.wait_for(
                        self.trade_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Update portfolio tracker
                if self.portfolio_tracker:
                    await self.portfolio_tracker.on_trade_complete(trade)
                
                # Update performance metrics
                self._update_performance_metrics(trade)
                
                self.logger.info(f"Trade completed: {trade.trade_id} - {trade.symbol} - PnL: {trade.net_pnl:.2f}")
                
            except Exception as e:
                self.logger.error(f"Error processing trade: {e}", exc_info=True)
            finally:
                self.trade_queue.task_done()
        
        self.logger.info("Trade processor stopped")
    
    def _update_performance_metrics(self, trade: Trade) -> None:
        """
        Update performance metrics based on completed trade.
        """
        self.performance_metrics.total_trades += 1
        self.performance_metrics.total_pnl += trade.pnl or 0
        self.performance_metrics.total_net_pnl += trade.net_pnl
        
        if trade.net_pnl > 0:
            self.performance_metrics.winning_trades += 1
        else:
            self.performance_metrics.losing_trades += 1
        
        # Update win rate
        if self.performance_metrics.total_trades > 0:
            self.performance_metrics.win_rate = (
                self.performance_metrics.winning_trades / 
                self.performance_metrics.total_trades
            )
        
        # Update averages
        if self.performance_metrics.winning_trades > 0:
            winning_pnl = [
                t.net_pnl for t in self.portfolio_tracker.closed_trades 
                if t.net_pnl > 0
            ]
            self.performance_metrics.avg_win = sum(winning_pnl) / len(winning_pnl)
        
        if self.performance_metrics.losing_trades > 0:
            losing_pnl = [
                t.net_pnl for t in self.portfolio_tracker.closed_trades 
                if t.net_pnl < 0
            ]
            self.performance_metrics.avg_loss = sum(losing_pnl) / len(losing_pnl)
        
        # Update profit factor
        if self.performance_metrics.avg_loss != 0:
            self.performance_metrics.profit_factor = (
                self.performance_metrics.avg_win / abs(self.performance_metrics.avg_loss)
            )
    
    async def check_time_stops(self) -> None:
        """
        Check for time-based exits (e.g., end of trading day).
        """
        while not self._shutdown:
            try:
                now = datetime.now()
                current_time = now.time()
                
                # Check if it's time to close all positions
                time_stop_time = self.config.risk.time_stop_time
                if current_time >= time_stop_time:
                    self.logger.info(f"Time stop triggered at {current_time}")
                    
                    if self.execution_engine:
                        await self.execution_engine.time_based_exit_all()
                    
                    # Wait for positions to be closed
                    await asyncio.sleep(30)
                    
                # Check for kill switch file
                kill_file = Path("/tmp/KILL")
                if kill_file.exists():
                    self.logger.critical("Kill switch file detected! Shutting down...")
                    await self._emergency_shutdown()
                    break
                
                await asyncio.sleep(5.0)
                
            except Exception as e:
                self.logger.error(f"Error in time stop checker: {e}", exc_info=True)
    
    async def _emergency_shutdown(self) -> None:
        """
        Emergency shutdown procedure.
        """
        self.logger.critical("EMERGENCY SHUTDOWN INITIATED")
        self._shutdown = True
        self._shutdown_event.set()
        
        # Cancel all open orders
        if self.order_manager:
            await self.order_manager.cancel_all_orders()
        
        # Close all positions
        if self.execution_engine:
            await self.execution_engine.time_based_exit_all()
        
        # Stop all components
        tasks = [
            self.data_feed.stop() if self.data_feed else None,
            self.strategy_engine.stop() if self.strategy_engine else None,
            self.signal_aggregator.stop() if self.signal_aggregator else None,
            self.execution_engine.stop() if self.execution_engine else None,
            self.order_manager.stop() if self.order_manager else None,
            self.portfolio_tracker.stop() if self.portfolio_tracker else None,
        ]
        
        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
    
    async def monitor_system_health(self) -> None:
        """
        Monitor system health and log status periodically.
        """
        self.logger.info("System health monitor started")
        
        while not self._shutdown:
            try:
                # Log system status
                if self.portfolio_tracker:
                    portfolio_state = self.portfolio_tracker.get_state()
                    self.logger.info(
                        f"Portfolio: PnL={portfolio_state.realized_pnl:.2f}, "
                        f"Positions={portfolio_state.num_open_positions}, "
                        f"Equity={portfolio_state.equity:.2f}"
                    )
                
                if self.risk_manager:
                    risk_state = self.risk_manager.get_state()
                    self.logger.info(
                        f"Risk: Daily PnL%={risk_state.daily_pnl_pct:.2f}%, "
                        f"Exposure%={risk_state.current_exposure_pct:.2f}%"
                    )
                
                # Log performance metrics
                self.logger.info(
                    f"Performance: Trades={self.performance_metrics.total_trades}, "
                    f"Win Rate={self.performance_metrics.win_rate:.2%}, "
                    f"Net PnL={self.performance_metrics.total_net_pnl:.2f}"
                )
                
                await asyncio.sleep(60.0)  # Log every minute
                
            except Exception as e:
                self.logger.error(f"Error in system health monitor: {e}", exc_info=True)
        
        self.logger.info("System health monitor stopped")
    
    async def start(self) -> None:
        """
        Start the entire trading system.
        """
        self.logger.info("Starting Rapid Fire Intraday Trading System...")
        self.system_state.start_time = datetime.now()
        self.system_state.is_running = True
        
        # Setup all components
        await self.setup()
        
        # Start data feed first
        await self.start_data_feed()
        
        # Start processing components
        await self.start_portfolio_tracker()
        await self.start_strategy_engine()
        await self.start_signal_aggregator()
        await self.start_execution_engine()
        await self.start_order_manager()
        
        # Start utility processors
        processor_tasks = [
            asyncio.create_task(self.process_ticks()),
            asyncio.create_task(self.process_fills()),
            asyncio.create_task(self.process_trades()),
            asyncio.create_task(self.check_time_stops()),
            asyncio.create_task(self.monitor_system_health()),
        ]
        
        self.logger.info("All components and processors started")
        
        # Wait for shutdown
        try:
            await self._shutdown_event.wait()
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received, shutting down...")
        finally:
            self._shutdown = True
            
            # Cancel all processor tasks
            for task in processor_tasks:
                task.cancel()
            
            # Stop all components
            stop_tasks = [
                self.data_feed.stop() if self.data_feed else None,
                self.strategy_engine.stop() if self.strategy_engine else None,
                self.signal_aggregator.stop() if self.signal_aggregator else None,
                self.execution_engine.stop() if self.execution_engine else None,
                self.order_manager.stop() if self.order_manager else None,
                self.portfolio_tracker.stop() if self.portfolio_tracker else None,
            ]
            
            await asyncio.gather(*[t for t in stop_tasks if t], return_exceptions=True)
            
            # Wait for processor tasks to complete
            await asyncio.gather(*processor_tasks, return_exceptions=True)
            
            self.system_state.is_running = False
            self.logger.info("Trading system stopped gracefully")
    
    def stop(self) -> None:
        """
        Stop the trading system (synchronous).
        """
        self._shutdown = True
        self._shutdown_event.set()


def handle_signals():
    """
    Handle OS signals for graceful shutdown.
    """
    def signal_handler(signame: str, signum: int) -> None:
        logger = get_logger("signal_handler")
        logger.info(f"Received signal {signame} ({signum}), initiating shutdown...")
        
        # Set shutdown flag for async components
        if hasattr(main_trading_system, '_shutdown'):
            main_trading_system._shutdown = True
        if hasattr(main_trading_system, '_shutdown_event'):
            main_trading_system._shutdown_event.set()
    
    # Register signal handlers
    for signame in ['SIGINT', 'SIGTERM', 'SIGHUP']:
        try:
            signal.signal(
                getattr(signal, signame),
                lambda s, f: signal_handler(signame, s)
            )
        except (AttributeError, ValueError):
            # Some signals may not be available on all platforms
            pass


# Global reference to the trading system
main_trading_system: Optional[TradingSystem] = None


async def run_backtest() -> None:
    """
    Run the system in backtest mode.
    """
    logger = get_logger("backtest")
    logger.info("Starting backtest mode...")
    
    # Create trading system with backtest config
    config_path = "config.yaml"
    
    # Override config for backtest
    from config_loader import ConfigLoader
    config_loader = ConfigLoader(config_path)
    config = config_loader.load_config()
    config.trading.paper_trading = True
    config.trading.mode = "BACKTEST"
    
    # Create and run backtester
    from backtester import Backtester
    backtester = Backtester(config)
    await backtester.run()


async def run_paper_trading() -> None:
    """
    Run the system in paper trading mode.
    """
    logger = get_logger("paper_trading")
    logger.info("Starting paper trading mode...")
    
    trading_system = TradingSystem()
    global main_trading_system
    main_trading_system = trading_system
    
    await trading_system.start()


async def run_live_trading() -> None:
    """
    Run the system in live trading mode.
    """
    logger = get_logger("live_trading")
    logger.info("Starting LIVE trading mode...")
    
    # Safety check
    logger.warning("LIVE MODE: Real money at risk!")
    logger.warning("Ensure all risk parameters are correctly configured!")
    logger.warning("Type 'CONFIRM_LIVE' to continue or Ctrl+C to abort...")
    
    # Add confirmation for live trading
    import sys
    response = input("Confirm LIVE trading (type 'CONFIRM_LIVE'): ")
    if response.strip() != "CONFIRM_LIVE":
        logger.error("Live trading confirmation failed. Aborting.")
        sys.exit(1)
    
    trading_system = TradingSystem()
    global main_trading_system
    main_trading_system = trading_system
    
    await trading_system.start()


def main():
    """
    Main entry point for the trading system.
    
    Usage:
        python main.py backtest    - Run backtesting
        python main.py paper       - Run paper trading
        python main.py live        - Run live trading
    """
    global main_trading_system
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Rapid Fire Intraday Trading System")
    parser.add_argument(
        "mode", 
        choices=["backtest", "paper", "live"],
        help="Trading mode: backtest, paper, or live"
    )
    parser.add_argument(
        "--config", 
        default="config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    # Set up logging level
    if args.debug:
        from .logger import set_log_level
        from .models import LogLevel
        set_log_level(LogLevel.DEBUG)
    
    # Set up signal handlers
    handle_signals()
    
    # Run appropriate mode
    if args.mode == "backtest":
        asyncio.run(run_backtest())
    elif args.mode == "paper":
        asyncio.run(run_paper_trading())
    elif args.mode == "live":
        asyncio.run(run_live_trading())


if __name__ == "__main__":
    main()