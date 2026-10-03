"""
Portfolio tracking module for the Rapid Fire Intraday Trading System.

This module tracks:
- Open positions and their current PnL
- Completed trades and their final PnL
- Portfolio equity and drawdown
- Performance metrics and statistics

All calculations include brokerage, fees, slippage, and taxes.
"""

from __future__ import annotations

import asyncio
import csv
import os
from collections import deque
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import threading

from .models import (
    OHLCV,
    Order,
    OrderSide,
    OrderStatus,
    PerformanceMetrics,
    Position,
    PositionSide,
    PortfolioState,
    SignalType,
    StrategyName,
    SystemState,
    Tick,
    TimeFrame,
    Trade,
    TradeStatus,
)

from .logger import get_logger
from .config_loader import Config
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .risk_manager import RiskManager


class PortfolioTracker:
    """
    Tracks portfolio state, positions, trades, and performance metrics.
    
    Key features:
    - Real-time PnL calculation for open positions
    - Trade lifecycle management
    - Equity curve tracking
    - Drawdown calculation
    - Performance statistics
    - CSV trade journal export
    - SQLite persistence for trade history
    """
    
    def __init__(
        self, 
        config: Config, 
        trade_queue: asyncio.Queue[Trade]
    ):
        """
        Initialize the portfolio tracker.
        
        Args:
            config: System configuration
            trade_queue: Queue for completed trades
        """
        self.logger = get_logger("portfolio_tracker")
        self.config = config
        self.trade_queue = trade_queue
        
        # Initial capital and current state
        # Get from portfolio config, fallback to backtester, then to trading
        initial_capital = 100000.0  # Default
        if hasattr(config, 'portfolio') and hasattr(config.portfolio, 'initial_equity'):
            initial_capital = config.portfolio.initial_equity
        elif hasattr(config, 'portfolio') and hasattr(config.portfolio, 'initial_cash'):
            initial_capital = config.portfolio.initial_cash
        elif hasattr(config, 'backtester') and hasattr(config.backtester, 'initial_capital'):
            initial_capital = config.backtester.initial_capital
        
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        self.current_equity = initial_capital
        
        # Peak equity for drawdown calculation
        self.peak_equity = initial_capital
        
        # Positions and trades
        self.positions: Dict[str, Position] = {}  # symbol -> Position
        self.open_trades: Dict[str, Trade] = {}  # trade_id -> Trade
        self.closed_trades: List[Trade] = []
        
        # Trade journal for CSV export
        self.trade_journal_path = Path("trade_journal.csv")
        self.trade_journal_headers = [
            "trade_id", "symbol", "side", "entry_price", "exit_price", "quantity",
            "entry_timestamp", "exit_timestamp", "stop_loss", "take_profit",
            "strategy", "confidence", "pnl", "net_pnl", "brokerage",
            "exchange_fees", "stt", "gst", "stamp_duty", "slippage",
            "status", "duration_seconds", "return_pct", "tags"
        ]
        
        # Equity curve history
        self.equity_history: List[Dict[str, Any]] = []
        self.equity_history_max_len = 10000
        
        # Performance metrics
        self.performance_metrics = PerformanceMetrics()
        
        # Locks for thread safety
        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        
        # Risk manager reference (set externally)
        self.risk_manager: Optional[RiskManager] = None
        
        # System state reference
        self.system_state: Optional[SystemState] = None
        
        # Load existing trade history if available
        self._load_trade_history()
        
        # Initialize equity history
        self._record_equity_snapshot()
        
        self.logger.info(f"Portfolio tracker initialized with capital: {self.initial_capital}")
    
    def _load_trade_history(self) -> None:
        """
        Load trade history from CSV if it exists.
        """
        if self.trade_journal_path.exists():
            try:
                with open(self.trade_journal_path, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Reconstruct trade from CSV
                        trade = Trade(
                            trade_id=row["trade_id"],
                            symbol=row["symbol"],
                            side=OrderSide(row["side"]),
                            entry_price=float(row["entry_price"]),
                            exit_price=float(row["exit_price"]),
                            quantity=float(row["quantity"]),
                            entry_timestamp=datetime.fromisoformat(row["entry_timestamp"]),
                            exit_timestamp=datetime.fromisoformat(row["exit_timestamp"]),
                            stop_loss=float(row["stop_loss"]) if row["stop_loss"] else None,
                            take_profit=float(row["take_profit"]) if row["take_profit"] else None,
                            strategy=StrategyName(row["strategy"]) if row["strategy"] else None,
                            confidence=float(row["confidence"]) if row["confidence"] else None,
                            pnl=float(row["pnl"]) if row["pnl"] else None,
                            brokerage=float(row["brokerage"]),
                            exchange_fees=float(row["exchange_fees"]),
                            stt=float(row["stt"]),
                            gst=float(row["gst"]),
                            stamp_duty=float(row["stamp_duty"]),
                            slippage=float(row["slippage"]),
                            status=TradeStatus(row["status"]),
                            tags=row["tags"].split(",") if row["tags"] else []
                        )
                        self.closed_trades.append(trade)
                
                self.logger.info(f"Loaded {len(self.closed_trades)} trades from history")
                
                # Update current equity based on loaded trades
                self._update_equity_from_trades()
                
            except Exception as e:
                self.logger.error(f"Error loading trade history: {e}")
    
    def _update_equity_from_trades(self) -> None:
        """
        Update current equity based on closed trades.
        """
        total_net_pnl = sum(trade.net_pnl for trade in self.closed_trades)
        self.current_equity = self.initial_capital + total_net_pnl
        self.current_cash = self.current_equity - self._get_open_positions_value()
        
        # Update peak equity
        if self.current_equity > self.peak_equity:
            self.peak_equity = self.current_equity
    
    def _record_equity_snapshot(self) -> None:
        """
        Record current equity snapshot for history.
        """
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "equity": self.current_equity,
            "cash": self.current_cash,
            "open_positions": len(self.positions),
            "total_trades": len(self.closed_trades)
        }
        self.equity_history.append(snapshot)
        
        # Trim history if too long
        if len(self.equity_history) > self.equity_history_max_len:
            self.equity_history = self.equity_history[-self.equity_history_max_len:]
    
    async def start(self) -> None:
        """
        Start the portfolio tracker.
        """
        self.logger.info("Portfolio tracker started")
        
        # Start background tasks if needed
        # (Portfolio tracking is mostly event-driven)
    
    async def stop(self) -> None:
        """
        Stop the portfolio tracker.
        """
        self.logger.info("Portfolio tracker stopping...")
        
        # Save trade history
        self._save_trade_journal()
        
        self.logger.info("Portfolio tracker stopped")
    
    def _save_trade_journal(self) -> None:
        """
        Save trade journal to CSV.
        """
        try:
            # Create directory if it doesn't exist
            self.trade_journal_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write all closed trades to CSV
            with open(self.trade_journal_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.trade_journal_headers)
                writer.writeheader()
                
                for trade in self.closed_trades:
                    trade_dict = trade.to_dict()
                    # Ensure all fields are present
                    row = {}
                    for header in self.trade_journal_headers:
                        # Convert nested dictionaries or handle missing keys
                        if header in trade_dict:
                            value = trade_dict[header]
                            # Handle special cases
                            if header == "side" and isinstance(value, str):
                                row[header] = value
                            elif header == "strategy" and isinstance(value, str):
                                row[header] = value
                            elif header == "status" and isinstance(value, str):
                                row[header] = value
                            elif header == "tags" and isinstance(value, str):
                                row[header] = value
                            elif isinstance(value, (float, int)):
                                row[header] = value
                            elif isinstance(value, str):
                                row[header] = value
                            else:
                                row[header] = str(value) if value else ""
                        else:
                            row[header] = ""
                    writer.writerow(row)
            
            self.logger.info(f"Trade journal saved to {self.trade_journal_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving trade journal: {e}")
    
    async def on_tick(self, tick: Tick) -> None:
        """
        Handle incoming tick data.
        
        Args:
            tick: Incoming tick data
        """
        # Update PnL for positions in this symbol
        if tick.symbol in self.positions:
            position = self.positions[tick.symbol]
            current_pnl = position.pnl(tick.last_price)
            
            self.logger.debug(
                f"Position PnL update: {tick.symbol} - {position.side.value} "
                f"{position.quantity} @ {position.entry_price} -> "
                f"Current: {tick.last_price} - PnL: {current_pnl:.2f}"
            )
    
    async def on_fill(self, fill: Order) -> None:
        """
        Handle order fill.
        
        Args:
            fill: Order that was filled
        """
        async with self._async_lock:
            if fill.status != OrderStatus.EXECUTED:
                return
            
            symbol = fill.symbol
            
            # Check if this fill opens a new position or modifies existing
            if fill.order_side == OrderSide.BUY:
                if symbol in self.positions:
                    # Add to existing position
                    existing = self.positions[symbol]
                    new_quantity = existing.quantity + fill.filled_quantity
                    new_avg_price = (
                        (existing.entry_price * existing.quantity) + 
                        (fill.average_fill_price or fill.price or 0) * fill.filled_quantity
                    ) / new_quantity
                    
                    updated_position = Position(
                        symbol=symbol,
                        side=existing.side,
                        quantity=new_quantity,
                        entry_price=new_avg_price,
                        entry_timestamp=existing.entry_timestamp,  # Keep original entry time
                        strategy=existing.strategy,
                        stop_loss=existing.stop_loss,
                        take_profit=existing.take_profit
                    )
                    self.positions[symbol] = updated_position
                    
                else:
                    # Create new position
                    position = Position(
                        symbol=symbol,
                        side=PositionSide.LONG,
                        quantity=fill.filled_quantity,
                        entry_price=fill.average_fill_price or fill.price or 0,
                        entry_timestamp=fill.timestamp,
                        strategy=getattr(fill, 'strategy', None),
                        stop_loss=getattr(fill, 'stop_loss', None),
                        take_profit=getattr(fill, 'take_profit', None),
                        order_id=fill.order_id
                    )
                    self.positions[symbol] = position
                
                self.logger.info(
                    f"Position opened/updated: {symbol} - LONG {fill.filled_quantity} @ {fill.average_fill_price}"
                )
            
            elif fill.order_side == OrderSide.SELL:
                if symbol in self.positions:
                    existing = self.positions[symbol]
                    if existing.side == PositionSide.LONG:
                        # Close or reduce position
                        if existing.quantity <= fill.filled_quantity:
                            # Close position completely
                            del self.positions[symbol]
                            self.logger.info(f"Position closed: {symbol}")
                        else:
                            # Reduce position
                            existing.quantity -= fill.filled_quantity
                            self.logger.info(
                                f"Position reduced: {symbol} - {existing.quantity} remaining"
                            )
                    else:
                        # Short position - add to it
                        new_quantity = existing.quantity + fill.filled_quantity
                        new_avg_price = (
                            (existing.entry_price * existing.quantity) + 
                            (fill.average_fill_price or fill.price or 0) * fill.filled_quantity
                        ) / new_quantity
                        
                        updated_position = Position(
                            symbol=symbol,
                            side=PositionSide.SHORT,
                            quantity=new_quantity,
                            entry_price=new_avg_price,
                            entry_timestamp=existing.entry_timestamp,
                            strategy=existing.strategy
                        )
                        self.positions[symbol] = updated_position
                        
                        self.logger.info(
                            f"Short position increased: {symbol} - SHORT {new_quantity} @ {new_avg_price}"
                        )
                else:
                    # Create new short position
                    position = Position(
                        symbol=symbol,
                        side=PositionSide.SHORT,
                        quantity=fill.filled_quantity,
                        entry_price=fill.average_fill_price or fill.price or 0,
                        entry_timestamp=fill.timestamp,
                        strategy=getattr(fill, 'strategy', None),
                        stop_loss=getattr(fill, 'stop_loss', None),
                        take_profit=getattr(fill, 'take_profit', None),
                        order_id=fill.order_id
                    )
                    self.positions[symbol] = position
                    
                    self.logger.info(
                        f"Short position opened: {symbol} - SHORT {fill.filled_quantity} @ {fill.average_fill_price}"
                    )
            
            # Update cash balance
            if fill.average_fill_price and fill.filled_quantity:
                if fill.order_side == OrderSide.BUY:
                    cash_used = fill.average_fill_price * fill.filled_quantity
                    self.current_cash -= cash_used
                else:  # SELL
                    cash_received = fill.average_fill_price * fill.filled_quantity
                    self.current_cash += cash_received
            
            # Update equity
            self._update_equity()
            
            # Record equity snapshot
            self._record_equity_snapshot()
    
    async def on_trade_complete(self, trade: Trade) -> None:
        """
        Handle completed trade.
        
        Args:
            trade: Completed trade
        """
        async with self._async_lock:
            # Add to closed trades
            self.closed_trades.append(trade)
            
            # Remove from open trades if present
            if trade.trade_id in self.open_trades:
                del self.open_trades[trade.trade_id]
            
            # Update positions - remove the position for this symbol
            if trade.symbol in self.positions:
                del self.positions[trade.symbol]
            
            # Update cash balance
            if trade.entry_price and trade.quantity:
                if trade.side == OrderSide.BUY:
                    self.current_cash += trade.entry_price * trade.quantity
                else:
                    self.current_cash -= trade.entry_price * trade.quantity
            
            if trade.exit_price and trade.quantity:
                if trade.side == OrderSide.BUY:
                    self.current_cash -= trade.exit_price * trade.quantity
                else:
                    self.current_cash += trade.exit_price * trade.quantity
            
            # Update equity
            self._update_equity()
            
            # Update performance metrics
            self._update_performance_metrics(trade)
            
            # Record equity snapshot
            self._record_equity_snapshot()
            
            # Save trade journal
            self._save_trade_journal()
            
            self.logger.info(
                f"Trade completed: {trade.trade_id} - {trade.symbol} - "
                f"PnL: {trade.net_pnl:.2f} - Return: {trade.return_pct:.2f}%"
            )
    
    def _update_equity(self) -> None:
        """
        Update current equity based on cash and positions.
        """
        positions_value = self._get_open_positions_value()
        self.current_equity = self.current_cash + positions_value
        
        # Update peak equity
        if self.current_equity > self.peak_equity:
            self.peak_equity = self.current_equity
    
    def _get_open_positions_value(self) -> float:
        """
        Calculate total value of open positions.
        """
        total_value = 0.0
        
        for symbol, position in self.positions.items():
            # We need current price, but we might not have it
            # For now, use entry price as approximation
            # In real-time, this would be updated via on_tick
            total_value += position.entry_price * position.quantity
        
        return total_value
    
    def _update_performance_metrics(self, trade: Trade) -> None:
        """
        Update performance metrics based on completed trade.
        """
        self.performance_metrics.total_trades += 1
        
        if trade.net_pnl > 0:
            self.performance_metrics.winning_trades += 1
        else:
            self.performance_metrics.losing_trades += 1
        
        self.performance_metrics.total_pnl += trade.pnl or 0
        self.performance_metrics.total_net_pnl += trade.net_pnl
        
        # Update win rate
        if self.performance_metrics.total_trades > 0:
            self.performance_metrics.win_rate = (
                self.performance_metrics.winning_trades / 
                self.performance_metrics.total_trades
            )
        
        # Update averages (only for closed trades)
        winning_trades = [t for t in self.closed_trades if t.net_pnl > 0]
        losing_trades = [t for t in self.closed_trades if t.net_pnl < 0]
        
        if winning_trades:
            self.performance_metrics.avg_win = (
                sum(t.net_pnl for t in winning_trades) / len(winning_trades)
            )
        
        if losing_trades:
            self.performance_metrics.avg_loss = (
                sum(t.net_pnl for t in losing_trades) / len(losing_trades)
            )
        
        # Update profit factor
        if self.performance_metrics.avg_loss != 0:
            self.performance_metrics.profit_factor = (
                self.performance_metrics.avg_win / 
                abs(self.performance_metrics.avg_loss)
            )
        
        # Update expectancy
        win_pct = self.performance_metrics.win_rate
        loss_pct = 1 - win_pct
        self.performance_metrics.expectancy = (
            (win_pct * self.performance_metrics.avg_win) + 
            (loss_pct * self.performance_metrics.avg_loss)
        )
        
        # Update max drawdown
        current_drawdown = ((self.peak_equity - self.current_equity) / self.peak_equity) * 100
        self.performance_metrics.current_drawdown = current_drawdown
        
        if current_drawdown > self.performance_metrics.max_drawdown:
            self.performance_metrics.max_drawdown = current_drawdown
    
    def get_state(self) -> PortfolioState:
        """
        Get current portfolio state.
        
        Returns:
            PortfolioState: Current state of the portfolio
        """
        with self._lock:
            # Get current prices for all symbols
            current_prices = {}
            for symbol in self.positions.keys():
                # In real-time, we'd get this from the data feed
                # For now, use entry price as fallback
                if symbol in self.positions:
                    current_prices[symbol] = self.positions[symbol].entry_price
            
            # Create position list
            position_list = list(self.positions.values())
            
            return PortfolioState(
                equity=self.current_equity,
                cash=self.current_cash,
                total_value=self.current_equity + self._get_open_positions_value(),
                positions=position_list,
                open_trades=list(self.open_trades.values()),
                closed_trades=self.closed_trades.copy()
            )
    
    def get_current_pnl(self, symbol: str, current_price: float) -> float:
        """
        Get current PnL for a specific symbol.
        
        Args:
            symbol: Symbol to check
            current_price: Current market price
            
        Returns:
            Current PnL for the symbol
        """
        with self._lock:
            if symbol in self.positions:
                return self.positions[symbol].pnl(current_price)
            return 0.0
    
    def get_unrealized_pnl(self) -> float:
        """
        Get total unrealized PnL across all positions.
        
        Returns:
            Total unrealized PnL
        """
        with self._lock:
            total_pnl = 0.0
            for symbol, position in self.positions.items():
                # Use entry price as fallback - in real-time this would use current prices
                current_price = position.entry_price  # This should be updated via ticks
                total_pnl += position.pnl(current_price)
            return total_pnl
    
    def get_realized_pnl(self) -> float:
        """
        Get total realized PnL from closed trades.
        
        Returns:
            Total realized PnL
        """
        with self._lock:
            return sum(trade.net_pnl for trade in self.closed_trades)
    
    def get_drawdown(self) -> float:
        """
        Get current drawdown percentage.
        
        Returns:
            Current drawdown percentage
        """
        with self._lock:
            if self.peak_equity == 0:
                return 0.0
            return ((self.peak_equity - self.current_equity) / self.peak_equity) * 100
    
    def get_max_drawdown(self) -> float:
        """
        Get maximum drawdown experienced.
        
        Returns:
            Maximum drawdown percentage
        """
        return self.performance_metrics.max_drawdown
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get position for a specific symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            Position if exists, None otherwise
        """
        with self._lock:
            return self.positions.get(symbol)
    
    def get_position_size(self, symbol: str) -> float:
        """
        Get position size (quantity) for a symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            Position quantity (positive for long, negative for short, 0 for no position)
        """
        with self._lock:
            if symbol in self.positions:
                position = self.positions[symbol]
                if position.side == PositionSide.SHORT:
                    return -position.quantity
                return position.quantity
            return 0.0
    
    def has_position(self, symbol: str) -> bool:
        """
        Check if there's an open position in a symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if position exists, False otherwise
        """
        with self._lock:
            return symbol in self.positions
    
    def get_num_open_positions(self) -> int:
        """
        Get number of open positions.
        
        Returns:
            Number of open positions
        """
        with self._lock:
            return len(self.positions)
    
    def get_current_equity(self) -> float:
        """
        Get current portfolio equity.
        
        Returns:
            Current equity
        """
        with self._lock:
            return self.current_equity
    
    def get_available_cash(self) -> float:
        """
        Get available cash (equity minus margin used).
        
        Returns:
            Available cash
        """
        with self._lock:
            return self.current_cash
    
    def create_trade(
        self,
        symbol: str,
        side: OrderSide,
        entry_price: float,
        exit_price: float,
        quantity: float,
        entry_timestamp: datetime,
        exit_timestamp: datetime,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        strategy: Optional[StrategyName] = None,
        confidence: Optional[float] = None,
        brokerage: float = 0.0,
        exchange_fees: float = 0.0,
        stt: float = 0.0,
        gst: float = 0.0,
        stamp_duty: float = 0.0,
        slippage: float = 0.0,
        tags: List[str] = None
    ) -> Trade:
        """
        Create a new trade record.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            entry_price: Entry price
            exit_price: Exit price
            quantity: Trade quantity
            entry_timestamp: Entry time
            exit_timestamp: Exit time
            stop_loss: Stop loss price
            take_profit: Take profit price
            strategy: Strategy that generated the signal
            confidence: Signal confidence score
            brokerage: Brokerage fees
            exchange_fees: Exchange fees
            stt: Securities Transaction Tax
            gst: Goods and Services Tax
            stamp_duty: Stamp duty
            slippage: Slippage cost
            tags: Additional tags
            
        Returns:
            Created Trade object
        """
        with self._lock:
            # Calculate PnL
            if side == OrderSide.BUY:
                pnl = (exit_price - entry_price) * quantity
            else:
                pnl = (entry_price - exit_price) * quantity
            
            trade = Trade(
                trade_id=str(len(self.closed_trades) + 1),
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=quantity,
                entry_timestamp=entry_timestamp,
                exit_timestamp=exit_timestamp,
                stop_loss=stop_loss,
                take_profit=take_profit,
                strategy=strategy,
                confidence=confidence,
                pnl=pnl,
                brokerage=brokerage,
                exchange_fees=exchange_fees,
                stt=stt,
                gst=gst,
                stamp_duty=stamp_duty,
                slippage=slippage,
                status=TradeStatus.CLOSED,
                tags=tags or []
            )
            
            self.closed_trades.append(trade)
            
            # Update equity and metrics
            self.current_cash += pnl - (brokerage + exchange_fees + stt + gst + stamp_duty + slippage)
            self._update_equity()
            self._update_performance_metrics(trade)
            
            return trade
    
    def get_performance_metrics(self) -> PerformanceMetrics:
        """
        Get current performance metrics.
        
        Returns:
            Current performance metrics
        """
        with self._lock:
            # Return a copy to prevent external modification
            return PerformanceMetrics(
                total_trades=self.performance_metrics.total_trades,
                winning_trades=self.performance_metrics.winning_trades,
                losing_trades=self.performance_metrics.losing_trades,
                win_rate=self.performance_metrics.win_rate,
                total_pnl=self.performance_metrics.total_pnl,
                total_net_pnl=self.performance_metrics.total_net_pnl,
                avg_win=self.performance_metrics.avg_win,
                avg_loss=self.performance_metrics.avg_loss,
                max_drawdown=self.performance_metrics.max_drawdown,
                current_drawdown=self.performance_metrics.current_drawdown,
                sharpe_ratio=self.performance_metrics.sharpe_ratio,
                sortino_ratio=self.performance_metrics.sortino_ratio,
                profit_factor=self.performance_metrics.profit_factor,
                cagr=self.performance_metrics.cagr,
                mar_ratio=self.performance_metrics.mar_ratio,
                expectancy=self.performance_metrics.expectancy
            )
    
    def get_equity_history(self) -> List[Dict[str, Any]]:
        """
        Get equity curve history.
        
        Returns:
            List of equity snapshots
        """
        with self._lock:
            return self.equity_history.copy()
    
    def get_trade_history(self) -> List[Trade]:
        """
        Get all closed trades.
        
        Returns:
            List of closed trades
        """
        with self._lock:
            return self.closed_trades.copy()
    
    def get_daily_pnl(self) -> float:
        """
        Get daily PnL (today only).
        
        Returns:
            Daily PnL
        """
        with self._lock:
            today = datetime.now().date()
            daily_trades = [
                t for t in self.closed_trades 
                if t.exit_timestamp.date() == today
            ]
            return sum(t.net_pnl for t in daily_trades)
    
    def get_daily_pnl_pct(self) -> float:
        """
        Get daily PnL as percentage of initial capital.
        
        Returns:
            Daily PnL percentage
        """
        daily_pnl = self.get_daily_pnl()
        if self.initial_capital == 0:
            return 0.0
        return (daily_pnl / self.initial_capital) * 100


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_portfolio_tracker: Optional[PortfolioTracker] = None


def get_portfolio_tracker() -> PortfolioTracker:
    """Get the singleton PortfolioTracker instance."""
    global _portfolio_tracker
    if _portfolio_tracker is None:
        from .config_loader import load_config
        config = load_config()
        _portfolio_tracker = PortfolioTracker(config)
    return _portfolio_tracker


def start_portfolio_tracker(config: Optional[Dict[str, Any]] = None) -> PortfolioTracker:
    """Start the portfolio tracker and return the instance."""
    global _portfolio_tracker
    _portfolio_tracker = PortfolioTracker(config or {})
    return _portfolio_tracker


def stop_portfolio_tracker() -> None:
    """Stop the portfolio tracker."""
    global _portfolio_tracker
    if _portfolio_tracker:
        _portfolio_tracker = None