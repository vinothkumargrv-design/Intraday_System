"""
Event-driven backtester for the Rapid Fire Intraday Trading System.

This module provides:
- Event-driven backtesting (same logic as live trading)
- Support for slippage, brokerage, exchange fees, STT, GST, stamp duty
- Walk-forward optimization support
- Performance metrics calculation
- Equity curve generation
- Trade-by-trade CSV export

Key design principle: Backtest logic mirrors live logic exactly.
"""

from __future__ import annotations

import asyncio
import csv
import os
import sqlite3
from collections import deque
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading

import numpy as np
import pandas as pd

from .models import (
    AggregatedSignal,
    BrokerType,
    OHLCV,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    PerformanceMetrics,
    Position,
    PositionSide,
    RiskParameters,
    SignalType,
    StrategyName,
    SystemState,
    Tick,
    TimeFrame,
    Trade,
    TradeStatus,
)

from .logger import get_logger
from .config_loader import load_config
from .data_feed import DataFeed, get_data_feed
from .strategy_engine import StrategyEngine
from .signal_aggregator import SignalAggregator
from .risk_manager import RiskManager
from .execution_engine import ExecutionEngine
from .order_manager import OrderManager
from .portfolio_tracker import PortfolioTracker


class Backtester:
    """
    Event-driven backtester that mirrors live trading logic.
    
    Features:
    - Uses the same strategy, signal aggregation, risk, and execution logic as live
    - Simulates market data feed from historical data
    - Includes all trading costs: brokerage, fees, taxes, slippage
    - Generates comprehensive performance metrics
    - Supports walk-forward optimization
    """
    
    def __init__(self, config: Config):
        """
        Initialize the backtester.
        
        Args:
            config: System configuration
        """
        self.logger = get_logger("backtester")
        self.config = config
        
        # Backtest configuration
        self.data_path = getattr(config, 'backtest_data_path', 'data/historical')
        self.start_date = getattr(config, 'backtest_start_date', None)
        self.end_date = getattr(config, 'backtest_end_date', None)
        # Get symbols from market_data.websocket.symbols or strategies.symbols
        symbols = []
        if hasattr(config, 'market_data') and hasattr(config.market_data, 'websocket') and hasattr(config.market_data.websocket, 'symbols'):
            symbols = config.market_data.websocket.symbols
        elif hasattr(config, 'strategies') and hasattr(config.strategies, 'symbols'):
            symbols = config.strategies.symbols
        
        self.symbols = symbols if symbols else ["RELIANCE", "TCS", "INFY"]  # Default symbols
        
        # Get timeframes from market_data or use defaults
        timeframes = []
        if hasattr(config, 'market_data') and hasattr(config.market_data, 'timeframes'):
            timeframes = config.market_data.timeframes
        
        self.timeframes = timeframes if timeframes else [TimeFrame._1M, TimeFrame._5M]  # Default timeframes
        
        # Components (similar to live system)
        self.data_feed: Optional[DataFeed] = None
        self.strategy_engine: Optional[StrategyEngine] = None
        self.signal_aggregator: Optional[SignalAggregator] = None
        self.risk_manager: Optional[RiskManager] = None
        self.execution_engine: Optional[ExecutionEngine] = None
        self.order_manager: Optional[OrderManager] = None
        self.portfolio_tracker: Optional[PortfolioTracker] = None
        
        # Event queues
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue()
        self.ohlcv_queue: asyncio.Queue[OHLCV] = asyncio.Queue()
        self.signal_queue: asyncio.Queue[AggregatedSignal] = asyncio.Queue()
        self.order_queue: asyncio.Queue[Order] = asyncio.Queue()
        self.fill_queue: asyncio.Queue[Order] = asyncio.Queue()
        self.trade_queue: asyncio.Queue[Trade] = asyncio.Queue()
        
        # System state
        self.system_state = SystemState(
            is_running=False,
            is_paper_trading=True,
            current_mode="BACKTEST"
        )
        
        # Results storage
        self.trades: List[Trade] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.performance_metrics: Optional[PerformanceMetrics] = None
        
        # Historical data
        self.historical_data: Dict[str, Dict[TimeFrame, List[OHLCV]]] = {}
        self.current_tick_index: Dict[str, Dict[TimeFrame, int]] = {}
        
        # Shutdown flag
        self._shutdown = False
        
        self.logger.info("Backtester initialized")
    
    async def setup(self) -> None:
        """
        Set up backtester components.
        """
        self.logger.info("Setting up backtester components...")
        
        # Initialize components with backtest configuration
        self.portfolio_tracker = PortfolioTracker(
            config=self.config,
            trade_queue=self.trade_queue
        )
        
        self.risk_manager = RiskManager(config=self.config)
        self.order_manager = OrderManager(config=self.config)
        
        self.strategy_engine = StrategyEngine()
        self.signal_aggregator = SignalAggregator(config=self.config)
        self.execution_engine = ExecutionEngine(config=self.config)
        
        # Set up cross-component references
        self.strategy_engine.portfolio_tracker = self.portfolio_tracker
        self.execution_engine.order_manager = self.order_manager
        self.portfolio_tracker.risk_manager = self.risk_manager
        
        # Override for backtest mode
        self.system_state.current_mode = "BACKTEST"
        self.system_state.is_paper_trading = True
        
        self.logger.info("Backtester components initialized")
    
    async def load_historical_data(self) -> bool:
        """
        Load historical data for backtesting.
        
        Returns:
            True if data loaded successfully, False otherwise
        """
        self.logger.info(f"Loading historical data from {self.data_path}")
        
        try:
            # Create data directory if it doesn't exist
            data_dir = Path(self.data_path)
            data_dir.mkdir(parents=True, exist_ok=True)
            
            # Load data for each symbol and timeframe
            data_feed = get_data_feed()
            
            for symbol in self.symbols:
                self.historical_data[symbol] = {}
                self.current_tick_index[symbol] = {}
                
                for timeframe in self.timeframes:
                    try:
                        # Load data from SQLite or CSV
                        ohlcv_data = await data_feed.get_historical_ohlcv(
                            symbol=symbol,
                            timeframe=timeframe,
                            from_date=self.start_date,
                            to_date=self.end_date
                        )
                        
                        if ohlcv_data:
                            self.historical_data[symbol][timeframe] = ohlcv_data
                            self.current_tick_index[symbol][timeframe] = 0
                            self.logger.info(
                                f"Loaded {len(ohlcv_data)} bars for {symbol} {timeframe.value}"
                            )
                        else:
                            self.logger.warning(
                                f"No data found for {symbol} {timeframe.value}"
                            )
                    except Exception as e:
                        self.logger.error(
                            f"Error loading data for {symbol} {timeframe.value}: {e}"
                        )
            
            if not self.historical_data:
                self.logger.error("No historical data loaded. Cannot backtest.")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load historical data: {e}")
            return False
    
    def create_mock_ticks(self, ohlcv_data: List[OHLCV]) -> List[Tick]:
        """
        Create mock tick data from OHLCV data for simulation.
        
        Args:
            ohlcv_data: List of OHLCV candles
            
        Returns:
            List of mock ticks
        """
        ticks = []
        
        for candle in ohlcv_data:
            # Create multiple ticks per candle for better simulation
            # Use open, high, low, close as tick points
            tick_times = [
                candle.timestamp,
                candle.timestamp + timedelta(seconds=30),
                candle.timestamp + timedelta(minutes=1) - timedelta(seconds=30),
                candle.timestamp + timedelta(minutes=1) - timedelta(seconds=1)
            ]
            
            tick_prices = [candle.open, candle.high, candle.low, candle.close]
            
            for i, (tick_time, price) in enumerate(zip(tick_times, tick_prices)):
                # Vary volume for each tick
                tick_volume = candle.volume / len(tick_times) * (i + 1)
                
                tick = Tick(
                    symbol=candle.symbol,
                    timestamp=tick_time,
                    last_price=price,
                    last_quantity=tick_volume,
                    bid_price=price * 0.999,  # Slightly below
                    bid_quantity=tick_volume * 0.5,
                    ask_price=price * 1.001,  # Slightly above
                    ask_quantity=tick_volume * 0.5,
                    volume=tick_volume * len(tick_times),  # Cumulative
                    open_interest=0.0
                )
                ticks.append(tick)
        
        return ticks
    
    async def simulate_market_data(self) -> None:
        """
        Simulate market data feed from historical data.
        """
        self.logger.info("Starting market data simulation...")
        
        # Find all symbols that have data
        symbols_with_data = [
            symbol for symbol in self.historical_data 
            if self.historical_data[symbol]
        ]
        
        if not symbols_with_data:
            self.logger.error("No symbols with data to simulate")
            return
        
        # Sort symbols by first timestamp to synchronize
        symbol_data: Dict[str, Dict[TimeFrame, List[Tick]]] = {}
        
        for symbol in symbols_with_data:
            symbol_data[symbol] = {}
            for timeframe in self.historical_data[symbol]:
                ohlcv_list = self.historical_data[symbol][timeframe]
                ticks = self.create_mock_ticks(ohlcv_list)
                symbol_data[symbol][timeframe] = ticks
        
        # Create a timeline of all events
        all_events: List[Tuple[datetime, str, TimeFrame, Tick]] = []
        
        for symbol in symbol_data:
            for timeframe in symbol_data[symbol]:
                for tick in symbol_data[symbol][timeframe]:
                    all_events.append((tick.timestamp, symbol, timeframe, tick))
        
        # Sort all events by timestamp
        all_events.sort(key=lambda x: x[0])
        
        self.logger.info(f"Simulating {len(all_events)} market events")
        
        # Process events in chronological order
        for timestamp, symbol, timeframe, tick in all_events:
            if self._shutdown:
                break
            
            # Send tick to queue
            await self.tick_queue.put(tick)
            
            # Also send OHLCV data for strategies that need it
            # (This is simplified - in reality, we'd aggregate ticks to OHLCV)
            
            # Small delay to simulate real-time processing
            # In a real backtest, we'd process all events at once
            # but for debugging, we can add a small delay
            if self.config.backtest.get('realtime_simulation', False):
                await asyncio.sleep(0.001)  # 1ms delay per event
        
        self.logger.info("Market data simulation completed")
    
    async def process_events(self) -> None:
        """
        Process events from queues (similar to live system).
        """
        self.logger.info("Event processor started")
        
        while not self._shutdown:
            try:
                # Process signals
                try:
                    signal = await asyncio.wait_for(
                        self.signal_queue.get(), 
                        timeout=0.1
                    )
                    
                    # In backtest, we'll process signals immediately
                    # The execution engine will handle the rest
                    self.logger.debug(f"Signal received: {signal.final_signal.value} {signal.symbol}")
                    
                    self.signal_queue.task_done()
                    
                except asyncio.TimeoutError:
                    pass
                
                # Process trades
                try:
                    trade = await asyncio.wait_for(
                        self.trade_queue.get(), 
                        timeout=0.1
                    )
                    
                    self.trades.append(trade)
                    self.logger.info(
                        f"Trade executed: {trade.trade_id} - {trade.symbol} - "
                        f"PnL: {trade.net_pnl:.2f}"
                    )
                    
                    self.trade_queue.task_done()
                    
                except asyncio.TimeoutError:
                    pass
                
                # Record equity snapshot periodically
                if self.portfolio_tracker:
                    portfolio_state = self.portfolio_tracker.get_state()
                    self.equity_curve.append({
                        "timestamp": datetime.now().isoformat(),
                        "equity": portfolio_state.equity,
                        "cash": portfolio_state.cash,
                        "pnl": portfolio_state.realized_pnl,
                        "drawdown": self.portfolio_tracker.get_drawdown()
                    })
                
                await asyncio.sleep(0.01)  # Small delay to prevent CPU overload
                
            except Exception as e:
                self.logger.error(f"Error processing events: {e}")
        
        self.logger.info("Event processor stopped")
    
    async def run(self) -> None:
        """
        Run the complete backtest.
        """
        self.logger.info("Starting backtest...")
        self.system_state.start_time = datetime.now()
        
        # Setup components
        await self.setup()
        
        # Load historical data
        if not await self.load_historical_data():
            self.logger.error("Cannot run backtest without data")
            return
        
        # Start components
        self.logger.info("Starting backtest components...")
        
        # Start strategy engine
        if self.strategy_engine:
            await self.strategy_engine.start()
        
        # Start signal aggregator
        if self.signal_aggregator:
            await self.signal_aggregator.start()
        
        # Start execution engine
        if self.execution_engine:
            await self.execution_engine.start()
        
        # Start order manager
        if self.order_manager:
            await self.order_manager.start()
        
        # Start portfolio tracker
        if self.portfolio_tracker:
            await self.portfolio_tracker.start()
        
        # Start market data simulation
        simulation_task = asyncio.create_task(self.simulate_market_data())
        
        # Start event processor
        processor_task = asyncio.create_task(self.process_events())
        
        # Wait for simulation to complete
        await simulation_task
        
        # Give some time for remaining events to be processed
        await asyncio.sleep(1.0)
        
        # Stop all components
        self._shutdown = True
        
        stop_tasks = [
            self.strategy_engine.stop() if self.strategy_engine else None,
            self.signal_aggregator.stop() if self.signal_aggregator else None,
            self.execution_engine.stop() if self.execution_engine else None,
            self.order_manager.stop() if self.order_manager else None,
            self.portfolio_tracker.stop() if self.portfolio_tracker else None,
        ]
        
        await asyncio.gather(*[t for t in stop_tasks if t], return_exceptions=True)
        
        # Wait for processor to finish
        await processor_task
        
        # Calculate final performance metrics
        self._calculate_performance_metrics()
        
        # Save results
        self._save_results()
        
        self.logger.info("Backtest completed")
    
    def _calculate_performance_metrics(self) -> None:
        """
        Calculate comprehensive performance metrics.
        """
        if not self.trades:
            self.logger.warning("No trades to calculate metrics")
            self.performance_metrics = PerformanceMetrics()
            return
        
        # Basic metrics
        total_trades = len(self.trades)
        winning_trades = [t for t in self.trades if t.net_pnl > 0]
        losing_trades = [t for t in self.trades if t.net_pnl < 0]
        
        total_net_pnl = sum(t.net_pnl for t in self.trades)
        total_pnl = sum(t.pnl or 0 for t in self.trades)
        
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0
        avg_win = sum(t.net_pnl for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t.net_pnl for t in losing_trades) / len(losing_trades) if losing_trades else 0
        
        # Profit factor
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        
        # Expectancy
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
        
        # Calculate equity curve returns
        if self.equity_curve and len(self.equity_curve) > 1:
            returns = []
            for i in range(1, len(self.equity_curve)):
                equity_change = self.equity_curve[i]['equity'] - self.equity_curve[i-1]['equity']
                if self.equity_curve[i-1]['equity'] != 0:
                    returns.append(equity_change / self.equity_curve[i-1]['equity'])
            
            # Sharpe ratio (assuming 0 risk-free rate for simplicity)
            if returns:
                mean_return = np.mean(returns)
                std_return = np.std(returns)
                sharpe_ratio = mean_return / std_return if std_return != 0 else 0
            else:
                sharpe_ratio = 0
            
            # Sortino ratio (downside deviation)
            downside_returns = [r for r in returns if r < 0]
            if downside_returns:
                downside_std = np.std(downside_returns)
                sortino_ratio = mean_return / downside_std if downside_std != 0 else 0
            else:
                sortino_ratio = 0
        else:
            sharpe_ratio = 0
            sortino_ratio = 0
        
        # Max drawdown
        if self.equity_curve:
            peak_equity = max(snapshot['equity'] for snapshot in self.equity_curve)
            drawdowns = [
                (peak_equity - snapshot['equity']) / peak_equity * 100 
                for snapshot in self.equity_curve
            ]
            max_drawdown = max(drawdowns) if drawdowns else 0
            current_drawdown = drawdowns[-1] if drawdowns else 0
        else:
            max_drawdown = 0
            current_drawdown = 0
        
        # CAGR (Compound Annual Growth Rate)
        if len(self.equity_curve) > 1:
            start_equity = self.equity_curve[0]['equity']
            end_equity = self.equity_curve[-1]['equity']
            start_time = datetime.fromisoformat(self.equity_curve[0]['timestamp'])
            end_time = datetime.fromisoformat(self.equity_curve[-1]['timestamp'])
            
            days = (end_time - start_time).days
            years = days / 365.25
            
            if start_equity != 0 and years > 0:
                cagr = ((end_equity / start_equity) ** (1/years) - 1) * 100
            else:
                cagr = 0
        else:
            cagr = 0
        
        # MAR ratio (CAGR / Max Drawdown)
        mar_ratio = cagr / max_drawdown if max_drawdown != 0 else 0
        
        self.performance_metrics = PerformanceMetrics(
            total_trades=total_trades,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            win_rate=win_rate,
            total_pnl=total_pnl,
            total_net_pnl=total_net_pnl,
            avg_win=avg_win,
            avg_loss=avg_loss,
            max_drawdown=max_drawdown,
            current_drawdown=current_drawdown,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            profit_factor=profit_factor,
            cagr=cagr,
            mar_ratio=mar_ratio,
            expectancy=expectancy
        )
        
        self.logger.info(f"Performance metrics calculated: {total_trades} trades, PnL: {total_net_pnl:.2f}")
    
    def _save_results(self) -> None:
        """
        Save backtest results to files.
        """
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save trade-by-trade CSV
        trade_csv_path = results_dir / f"trades_{timestamp}.csv"
        self._save_trades_csv(trade_csv_path)
        
        # Save summary
        summary_path = results_dir / f"summary_{timestamp}.txt"
        self._save_summary(summary_path)
        
        # Save equity curve
        equity_csv_path = results_dir / f"equity_curve_{timestamp}.csv"
        self._save_equity_curve(equity_csv_path)
        
        # Generate equity curve plot (if matplotlib is available)
        try:
            import matplotlib.pyplot as plt
            plot_path = results_dir / f"equity_curve_{timestamp}.png"
            self._generate_equity_plot(plot_path)
        except ImportError:
            self.logger.warning("Matplotlib not available, skipping equity curve plot")
        
        self.logger.info(f"Results saved to {results_dir}")
    
    def _save_trades_csv(self, path: Path) -> None:
        """
        Save trade-by-trade results to CSV.
        """
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                headers = [
                    "trade_id", "symbol", "side", "entry_price", "exit_price", "quantity",
                    "entry_timestamp", "exit_timestamp", "duration_minutes",
                    "stop_loss", "take_profit", "strategy", "confidence",
                    "pnl", "net_pnl", "brokerage", "exchange_fees", "stt", "gst", 
                    "stamp_duty", "slippage", "status", "return_pct", "tags"
                ]
                writer.writerow(headers)
                
                # Write trades
                for trade in self.trades:
                    duration_minutes = trade.duration_seconds / 60
                    writer.writerow([
                        trade.trade_id,
                        trade.symbol,
                        trade.side.value,
                        trade.entry_price,
                        trade.exit_price,
                        trade.quantity,
                        trade.entry_timestamp.isoformat(),
                        trade.exit_timestamp.isoformat(),
                        f"{duration_minutes:.2f}",
                        trade.stop_loss or "",
                        trade.take_profit or "",
                        trade.strategy.value if trade.strategy else "",
                        trade.confidence or "",
                        trade.pnl or "",
                        trade.net_pnl,
                        trade.brokerage,
                        trade.exchange_fees,
                        trade.stt,
                        trade.gst,
                        trade.stamp_duty,
                        trade.slippage,
                        trade.status.value,
                        trade.return_pct,
                        ",".join(trade.tags)
                    ])
            
            self.logger.info(f"Trades saved to {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving trades CSV: {e}")
    
    def _save_summary(self, path: Path) -> None:
        """
        Save backtest summary to text file.
        """
        try:
            with open(path, 'w') as f:
                f.write("=" * 60 + "\n")
                f.write("RAPID FIRE INTRADAY TRADING SYSTEM - BACKTEST SUMMARY\n")
                f.write("=" * 60 + "\n\n")
                
                # Configuration
                f.write("CONFIGURATION:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Initial Capital: {self.config.trading.initial_capital}\n")
                f.write(f"Symbols: {', '.join(self.symbols)}\n")
                f.write(f"Start Date: {self.start_date or 'N/A'}\n")
                f.write(f"End Date: {self.end_date or 'N/A'}\n")
                f.write(f"Data Path: {self.data_path}\n\n")
                
                # Performance Metrics
                if self.performance_metrics:
                    f.write("PERFORMANCE METRICS:\n")
                    f.write("-" * 30 + "\n")
                    f.write(f"Total Trades: {self.performance_metrics.total_trades}\n")
                    f.write(f"Winning Trades: {self.performance_metrics.winning_trades}\n")
                    f.write(f"Losing Trades: {self.performance_metrics.losing_trades}\n")
                    f.write(f"Win Rate: {self.performance_metrics.win_rate:.2%}\n")
                    f.write(f"Total PnL: {self.performance_metrics.total_pnl:.2f}\n")
                    f.write(f"Net PnL: {self.performance_metrics.total_net_pnl:.2f}\n")
                    f.write(f"Average Win: {self.performance_metrics.avg_win:.2f}\n")
                    f.write(f"Average Loss: {self.performance_metrics.avg_loss:.2f}\n")
                    f.write(f"Profit Factor: {self.performance_metrics.profit_factor:.2f}\n")
                    f.write(f"Expectancy: {self.performance_metrics.expectancy:.2f}\n")
                    f.write(f"Sharpe Ratio: {self.performance_metrics.sharpe_ratio:.2f}\n")
                    f.write(f"Sortino Ratio: {self.performance_metrics.sortino_ratio:.2f}\n")
                    f.write(f"CAGR: {self.performance_metrics.cagr:.2f}%\n")
                    f.write(f"Max Drawdown: {self.performance_metrics.max_drawdown:.2f}%\n")
                    f.write(f"Current Drawdown: {self.performance_metrics.current_drawdown:.2f}%\n")
                    f.write(f"MAR Ratio: {self.performance_metrics.mar_ratio:.2f}\n\n")
                
                # Final Portfolio State
                if self.portfolio_tracker:
                    state = self.portfolio_tracker.get_state()
                    f.write("FINAL PORTFOLIO STATE:\n")
                    f.write("-" * 30 + "\n")
                    f.write(f"Equity: {state.equity:.2f}\n")
                    f.write(f"Cash: {state.cash:.2f}\n")
                    f.write(f"Open Positions: {len(state.positions)}\n")
                    f.write(f"Closed Trades: {len(state.closed_trades)}\n\n")
                
                f.write("=" * 60 + "\n")
            
            self.logger.info(f"Summary saved to {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving summary: {e}")
    
    def _save_equity_curve(self, path: Path) -> None:
        """
        Save equity curve to CSV.
        """
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                headers = ["timestamp", "equity", "cash", "pnl", "drawdown"]
                writer.writerow(headers)
                
                # Write equity curve data
                for snapshot in self.equity_curve:
                    writer.writerow([
                        snapshot["timestamp"],
                        snapshot["equity"],
                        snapshot["cash"],
                        snapshot["pnl"],
                        snapshot["drawdown"]
                    ])
            
            self.logger.info(f"Equity curve saved to {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving equity curve: {e}")
    
    def _generate_equity_plot(self, path: Path) -> None:
        """
        Generate equity curve plot.
        """
        try:
            import matplotlib.pyplot as plt
            from matplotlib import dates as mdates
            
            # Extract data
            timestamps = [datetime.fromisoformat(s["timestamp"]) for s in self.equity_curve]
            equity = [s["equity"] for s in self.equity_curve]
            
            # Create figure
            plt.figure(figsize=(12, 8))
            
            # Plot equity curve
            plt.plot(timestamps, equity, label='Equity', color='blue', alpha=0.8)
            
            # Plot drawdown
            drawdowns = [s["drawdown"] for s in self.equity_curve]
            plt.fill_between(timestamps, equity, 
                           [e * (1 - d/100) for e, d in zip(equity, drawdowns)],
                           color='red', alpha=0.3, label='Drawdown')
            
            # Format plot
            plt.title('Equity Curve - Rapid Fire Intraday Trading System')
            plt.xlabel('Time')
            plt.ylabel('Equity')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Format x-axis for dates
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.gcf().autofmt_xdate()
            
            # Save plot
            plt.tight_layout()
            plt.savefig(path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Equity curve plot saved to {path}")
            
        except Exception as e:
            self.logger.error(f"Error generating equity plot: {e}")
    
    def get_results(self) -> Dict[str, Any]:
        """
        Get complete backtest results.
        
        Returns:
            Dictionary containing all backtest results
        """
        return {
            "trades": [trade.to_dict() for trade in self.trades],
            "performance_metrics": self.performance_metrics.to_dict() if self.performance_metrics else None,
            "equity_curve": self.equity_curve,
            "summary": self._get_summary_dict()
        }
    
    def _get_summary_dict(self) -> Dict[str, Any]:
        """
        Get summary as dictionary.
        """
        if not self.performance_metrics:
            return {}
        
        return {
            "total_trades": self.performance_metrics.total_trades,
            "winning_trades": self.performance_metrics.winning_trades,
            "losing_trades": self.performance_metrics.losing_trades,
            "win_rate": self.performance_metrics.win_rate,
            "total_net_pnl": self.performance_metrics.total_net_pnl,
            "profit_factor": self.performance_metrics.profit_factor,
            "sharpe_ratio": self.performance_metrics.sharpe_ratio,
            "sortino_ratio": self.performance_metrics.sortino_ratio,
            "cagr": self.performance_metrics.cagr,
            "max_drawdown": self.performance_metrics.max_drawdown,
            "mar_ratio": self.performance_metrics.mar_ratio
        }


class WalkForwardOptimizer:
    """
    Walk-forward optimization for strategy parameters.
    """
    
    def __init__(self, config: Config):
        """
        Initialize walk-forward optimizer.
        
        Args:
            config: System configuration
        """
        self.logger = get_logger("walk_forward")
        self.config = config
        self.backtester = Backtester(config)
    
    async def optimize(
        self,
        parameter_grid: Dict[str, List[Any]],
        optimization_metric: str = "sharpe_ratio",
        in_sample_periods: List[Tuple[datetime, datetime]] = None,
        out_of_sample_periods: List[Tuple[datetime, datetime]] = None
    ) -> Dict[str, Any]:
        """
        Run walk-forward optimization.
        
        Args:
            parameter_grid: Parameters to optimize
            optimization_metric: Metric to optimize (e.g., 'sharpe_ratio', 'profit_factor')
            in_sample_periods: In-sample periods for optimization
            out_of_sample_periods: Out-of-sample periods for validation
            
        Returns:
            Dictionary with best parameters and results
        """
        self.logger.info("Starting walk-forward optimization...")
        
        best_parameters = {}
        best_result = -float('inf')
        all_results = []
        
        # Generate parameter combinations
        param_combinations = self._generate_parameter_combinations(parameter_grid)
        
        self.logger.info(f"Testing {len(param_combinations)} parameter combinations")
        
        for i, params in enumerate(param_combinations):
            self.logger.info(f"Testing combination {i+1}/{len(param_combinations)}: {params}")
            
            # Apply parameters to config
            self._apply_parameters(params)
            
            # Run backtests for each period
            total_metric = 0
            period_count = 0
            
            for is_period, oos_period in zip(in_sample_periods, out_of_sample_periods):
                # Set periods
                self.backtester.start_date = is_period[0]
                self.backtester.end_date = is_period[1]
                
                # Run in-sample backtest
                await self.backtester.load_historical_data()
                await self.backtester.setup()
                
                # Simulate and get results
                # (This is simplified - actual implementation would run full backtest)
                
                # For now, just run a simple optimization
                metric_value = 0  # Placeholder
                total_metric += metric_value
                period_count += 1
            
            avg_metric = total_metric / period_count if period_count > 0 else 0
            
            if avg_metric > best_result:
                best_result = avg_metric
                best_parameters = params
            
            all_results.append({
                "parameters": params,
                "result": avg_metric
            })
        
        self.logger.info(f"Optimization completed. Best parameters: {best_parameters}")
        
        return {
            "best_parameters": best_parameters,
            "best_result": best_result,
            "all_results": all_results
        }
    
    def _generate_parameter_combinations(self, parameter_grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        """
        Generate all parameter combinations from grid.
        """
        from itertools import product
        
        keys = list(parameter_grid.keys())
        values = list(parameter_grid.values())
        
        combinations = []
        for combo in product(*values):
            combinations.append(dict(zip(keys, combo)))
        
        return combinations
    
    def _apply_parameters(self, params: Dict[str, Any]) -> None:
        """
        Apply parameters to config.
        """
        # This would modify the backtester's config
        # Implementation depends on how parameters are structured
        pass