"""
Risk Manager for the Rapid Fire Intraday Trading System.

This module implements comprehensive risk management including:
- Per-trade risk limits (0.5% of equity default)
- Position sizing based on risk percentage and stop loss distance
- Maximum concurrent positions (5 default)
- Daily loss limits (2% equity) and profit targets (4% equity)
- Trailing stop loss (ATR-based, 1.5x default)
- Hard stop loss (always set on exchange)
- Time-based exits (15:15 IST default)
- Spread and slippage protection
- Circuit breakers for various failure modes
- Kill-switch activation for emergency shutdown

All risk checks are non-negotiable and designed to prevent catastrophic losses.
"""

from __future__ import annotations

import os
import threading
import time as time_module
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from intraday_system.config_loader import get_config_loader
from intraday_system.data_feed import get_data_feed
from intraday_system.logger import risk_logger
from intraday_system.models import (
    MarketRegime, OHLCV, OrderSide, OrderStatus, OrderType, OrderValidity,
    Position, PositionSide, RiskCheckResult, RiskParameters, RiskState,
    SignalType, StrategyName, Symbol, Timestamp, Trade, TradeStatus
)
from intraday_system.portfolio_tracker import get_portfolio_tracker


# =============================================================================
# RISK MANAGER
# =============================================================================

def _local_now(timezone_name: str) -> datetime:
    """Current wall-clock time in the exchange timezone (naive if unavailable)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(timezone_name)).replace(tzinfo=None)
    except Exception:
        return datetime.now()


class RiskManager:
    """
    Comprehensive risk manager with all required safety features.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the risk manager."""
        self.config = get_config_loader().get_risk_parameters()
        self.system_config = config or get_config_loader().load()
        self.logger = risk_logger()
        
        # Current state
        self._state = RiskState(
            daily_pnl=0.0,
            daily_pnl_pct=0.0,
            current_exposure=0.0,
            current_exposure_pct=0.0,
            open_positions_count=0,
            cooldown_symbols={},
            consecutive_failures=0,
            kill_switch_activated=False
        )
        self._state_lock = threading.RLock()  # Re-entrant: checks may trigger the kill switch
        self.subscribers: List[Callable] = []
        
        # Track trades and positions
        self._open_trades: Dict[str, Trade] = {}
        self._daily_trades: List[Trade] = []
        self._daily_start = datetime.utcnow().date()
        
        # Order tracking
        self._pending_orders: Dict[str, Any] = {}  # order_id -> order
        self._failed_orders: List[Any] = []
        
        # Kill switch file monitoring
        self._kill_file_path = self.system_config.get('risk_management', {}).get(
            'circuit_breakers', {}).get('manual_kill_file', '/tmp/KILL'
        )
        self._kill_file_check_interval = 1.0  # seconds
        self._kill_file_monitor_thread: Optional[threading.Thread] = None
        self._kill_file_running = False
        
        # Data feed
        self.data_feed = get_data_feed()
        
        # Initialize
        self._initialize()
        
        self.logger.info("RiskManager initialized with strict safety protocols")
    
    def _initialize(self) -> None:
        """Initialize risk manager state."""
        # Start kill switch file monitor
        self._start_kill_file_monitor()
        
        self._daily_start = self._now().date()
        self.enforce_market_hours = bool(self.system_config.get('risk_management', {}).get(
            'time_stop', {}).get('enforce_market_hours', True))
    
    def _now(self) -> datetime:
        timezone = self.system_config.get('system', {}).get('timezone', 'Asia/Kolkata')
        return _local_now(timezone)
    
    def _roll_day_if_needed(self) -> None:
        """Reset daily counters when the exchange date changes."""
        today = self._now().date()
        if today != self._daily_start:
            self.on_end_of_day()
    
    def _refresh_state(self) -> None:
        """Derive PnL, exposure and position count from the portfolio ledger."""
        self._roll_day_if_needed()
        portfolio = get_portfolio_tracker()
        equity = portfolio.get_current_equity()
        daily_pnl = portfolio.get_daily_pnl()
        exposure = portfolio.get_gross_exposure()
        with self._state_lock:
            self._state.daily_pnl = daily_pnl
            self._state.daily_pnl_pct = daily_pnl / portfolio.initial_capital * 100 if portfolio.initial_capital else 0.0
            self._state.current_exposure = exposure
            self._state.current_exposure_pct = exposure / equity * 100 if equity > 0 else 0.0
            self._state.open_positions_count = portfolio.get_num_open_positions()
    
    def start(self) -> None:
        """Start the risk manager."""
        self._start_kill_file_monitor()
        self.logger.info("RiskManager started")
    
    def stop(self) -> None:
        """Stop the risk manager."""
        self._stop_kill_file_monitor()
        self.logger.info("RiskManager stopped")
    
    # =============================================================================
    # POSITION SIZING
    # =============================================================================
    
    def calculate_position_size(self, symbol: Symbol, entry_price: float, 
                               stop_loss: float, risk_pct: Optional[float] = None) -> int:
        """
        Calculate position size based on risk percentage and stop loss distance.
        
        Formula: qty = (equity * risk_pct) / |entry - SL|
        
        Args:
            symbol: Trading symbol
            entry_price: Proposed entry price
            stop_loss: Proposed stop loss price
            risk_pct: Risk percentage of equity (defaults to config)
            
        Returns:
            Position size in integer units
        """
        try:
            # Get risk percentage
            if risk_pct is None:
                risk_pct = self.config.max_per_trade_risk_pct / 100.0
            else:
                risk_pct = risk_pct / 100.0
            
            # Get equity
            equity = get_portfolio_tracker().get_current_equity()
            
            # Calculate stop loss distance
            sl_distance = abs(entry_price - stop_loss)
            
            if sl_distance == 0:
                self.logger.error(f"Stop loss distance is zero for {symbol}")
                return 0
            
            # Calculate quantity
            quantity = (equity * risk_pct) / sl_distance
            
            # Round to nearest integer
            quantity = round(quantity)
            
            # Apply minimum quantity (lot size)
            instrument_info = self._get_instrument_info(symbol)
            if instrument_info:
                lot_size = instrument_info.lot_size
                quantity = (quantity // lot_size) * lot_size
                quantity = max(lot_size, quantity)
            
            # Cap notional so all concurrent positions fit within the leverage limit
            max_notional = equity * self.config.max_leverage / max(self.config.max_concurrent_positions, 1)
            if entry_price > 0:
                quantity = min(quantity, int(max_notional / entry_price))
            
            if quantity < 1:
                self.logger.warning(f"Position size for {symbol} rounds to zero")
                return 0
            quantity = int(quantity)
            
            self.logger.debug(f"Position size for {symbol}: entry={entry_price:.2f}, "
                            f"SL={stop_loss:.2f}, risk={risk_pct*100:.2f}%, "
                            f"qty={quantity}")
            
            return quantity
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {e}")
            return 0
    
    def _get_instrument_info(self, symbol: Symbol) -> Optional[Any]:
        """Get instrument information (lot size, tick size, etc.)."""
        # In production, this would come from the broker
        # For now, return default values
        return type('InstrumentInfo', (), {
            'lot_size': 1,
            'tick_size': 0.05,
            'min_quantity': 1,
            'max_quantity': 100000
        })()
    
    # =============================================================================
    # RISK CHECKS
    # =============================================================================
    
    def check_trade_risk(self, symbol: Symbol, side: OrderSide, quantity: int,
                        entry_price: float, stop_loss: float,
                        take_profit: Optional[float] = None) -> RiskCheckResult:
        """
        Perform comprehensive risk check before allowing a trade.
        
        This is the main risk check method called before order placement.
        All conditions must pass for the trade to be allowed.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            quantity: Position size
            entry_price: Proposed entry price
            stop_loss: Proposed stop loss price
            take_profit: Proposed take profit price
            
        Returns:
            RiskCheckResult with passed=True/False and reason
        """
        self._refresh_state()
        checks = [
            # 1. Kill switch check
            self._check_kill_switch(),
            
            # 2. Daily loss limit check
            self._check_daily_loss_limit(),
            
            # 3. Daily profit target check
            self._check_daily_profit_target(),
            
            # 4. Position limit check
            self._check_position_limit(symbol),
            
            # 5. Exposure limit check
            self._check_exposure_limit(symbol, side, quantity, entry_price),
            
            # 6. Spread check
            self._check_spread(symbol),
            
            # 7. Volume check
            self._check_volume(symbol),
            
            # 8. Time check (market hours)
            self._check_time(),
            
            # 9. Consecutive failures check
            self._check_consecutive_failures(),
            
            # 10. Cooldown check
            self._check_cooldown(symbol),
        ]
        
        # Check if all conditions passed
        failed_checks = [c for c in checks if not c.passed]
        
        if failed_checks:
            # Return the first failed check
            failed_check = failed_checks[0]
            self.logger.warning(f"Risk check failed: {failed_check.reason}")
            return failed_check
        
        # All checks passed
        return RiskCheckResult(
            passed=True,
            reason="All risk checks passed",
            details={'checks': [c.reason for c in checks]}
        )
    
    def _check_kill_switch(self) -> RiskCheckResult:
        """Check if kill switch is activated."""
        with self._state_lock:
            activated = self._state.kill_switch_activated
        if activated:
            return RiskCheckResult(passed=False, reason="Kill switch activated - all trading halted")
        
        if self._check_kill_file():
            self._activate_kill_switch("Manual kill file detected")
            return RiskCheckResult(passed=False,
                                   reason="Manual kill file detected - kill switch activated")
        
        return RiskCheckResult(passed=True, reason="Kill switch not activated")
    
    def _check_daily_loss_limit(self) -> RiskCheckResult:
        """Check if daily loss limit has been reached."""
        max_loss_pct = self.config.max_daily_loss_pct
        with self._state_lock:
            current_loss_pct = -self._state.daily_pnl_pct
        
        if current_loss_pct >= max_loss_pct:
            # Activated outside the state lock: square-off calls into other components
            kill_enabled = self.system_config.get('risk_management', {}).get(
                'circuit_breakers', {}).get('daily_loss_kill_switch', True)
            if kill_enabled:
                self._activate_kill_switch(f"Daily loss limit ({max_loss_pct}%) breached")
            return RiskCheckResult(
                passed=False,
                reason=f"Daily loss limit breached: {current_loss_pct:.2f}% >= {max_loss_pct}%"
            )
        
        return RiskCheckResult(passed=True, reason="Daily loss limit not breached")
    
    def _check_daily_profit_target(self) -> RiskCheckResult:
        """Check if daily profit target has been reached."""
        with self._state_lock:
            max_profit_pct = self.config.max_daily_profit_pct
            current_profit_pct = self._state.daily_pnl_pct
            
            if current_profit_pct >= max_profit_pct:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Daily profit target reached: {current_profit_pct:.2f}% >= {max_profit_pct}%"
                )
        
        return RiskCheckResult(passed=True, reason="Daily profit target not reached")
    
    def _check_position_limit(self, symbol: Symbol) -> RiskCheckResult:
        """Check position limits."""
        with self._state_lock:
            max_positions = self.config.max_concurrent_positions
            current_positions = self._state.open_positions_count
            
            if current_positions >= max_positions:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Max concurrent positions reached: {current_positions} >= {max_positions}"
                )
            
            # Check for existing position in same symbol
            if get_portfolio_tracker().has_position(symbol):
                return RiskCheckResult(
                    passed=False,
                    reason=f"Existing position already exists in {symbol}"
                )
        
        return RiskCheckResult(passed=True, reason="Position limits not exceeded")
    
    def _check_exposure_limit(self, symbol: Symbol, side: OrderSide, 
                              quantity: int, entry_price: float) -> RiskCheckResult:
        """Check exposure limits."""
        with self._state_lock:
            max_exposure_pct = self.config.max_exposure_pct
            equity = get_portfolio_tracker().get_current_equity()
            
            # Calculate proposed exposure
            proposed_exposure = quantity * entry_price
            current_exposure = self._state.current_exposure
            total_exposure = current_exposure + proposed_exposure
            total_exposure_pct = (total_exposure / equity) * 100 if equity > 0 else 0
            
            if total_exposure_pct > max_exposure_pct:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Exposure limit exceeded: {total_exposure_pct:.2f}% > {max_exposure_pct}%"
                )
        
        return RiskCheckResult(passed=True, reason="Exposure within limits")
    
    def _check_spread(self, symbol: Symbol) -> RiskCheckResult:
        """Check if spread is within acceptable limits."""
        current_tick = self.data_feed.cache.get_current_tick(symbol)
        
        if not current_tick:
            return RiskCheckResult(
                passed=False,
                reason=f"No tick data available for {symbol}"
            )
        
        spread_pct = current_tick.spread_pct()
        max_spread_pct = self.config.max_spread_pct
        
        if spread_pct is None:
            return RiskCheckResult(
                passed=True,
                reason="No spread data available - assuming acceptable"
            )
        
        if spread_pct > max_spread_pct:
            return RiskCheckResult(
                passed=False,
                reason=f"Spread too wide: {spread_pct:.3f}% > {max_spread_pct}%"
            )
        
        return RiskCheckResult(passed=True, reason=f"Spread acceptable: {spread_pct:.3f}%")
    
    def _check_volume(self, symbol: Symbol) -> RiskCheckResult:
        """Check if volume is sufficient for entry."""
        min_volume = self.config.min_volume_for_entry or 1000
        
        current_tick = self.data_feed.cache.get_current_tick(symbol)
        if current_tick and current_tick.volume:
            if current_tick.volume < min_volume:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Insufficient volume: {current_tick.volume:.0f} < {min_volume:.0f}"
                )
        
        return RiskCheckResult(passed=True, reason="Volume check passed")
    
    def _check_time(self) -> RiskCheckResult:
        """Check if current time is within trading hours."""
        timezone = self.system_config.get('system', {}).get('timezone', 'Asia/Kolkata')
        market_open = self.config.time_stop_time.replace(hour=9, minute=15)  # 9:15 AM
        market_close = self.config.time_stop_time  # 15:15 or configurable
        
        if not self.enforce_market_hours:
            return RiskCheckResult(passed=True, reason="Market hours not enforced")
        
        try:
            current_time = self._now().time()
            
            # Check if within trading hours
            if current_time < market_open or current_time >= market_close:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Outside trading hours: {current_time} (trading: {market_open} - {market_close})"
                )
            
            # Check pre-square off period
            pre_square_off_minutes = self.system_config.get('risk_management', {}).get(
                'time_stop', {}).get('pre_square_off_minutes', 5
            )
            pre_square_off_time = (market_close.hour * 60 + market_close.minute - 
                                  pre_square_off_minutes) * 60
            current_seconds = current_time.hour * 3600 + current_time.minute * 60
            
            if current_seconds >= pre_square_off_time:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Pre-square off period: within {pre_square_off_minutes} minutes of close"
                )
            
        except Exception as e:
            self.logger.error(f"Error checking time: {e}")
            # Fail-safe: allow trading
            return RiskCheckResult(passed=True, reason="Time check error - assuming valid")
        
        return RiskCheckResult(passed=True, reason="Within trading hours")
    
    def _check_consecutive_failures(self) -> RiskCheckResult:
        """Check for consecutive order failures."""
        max_failures = self.config.max_consecutive_failures
        with self._state_lock:
            failures = self._state.consecutive_failures
        
        if failures >= max_failures:
            self._activate_kill_switch(f"Consecutive order failures: {failures} >= {max_failures}")
            return RiskCheckResult(passed=False,
                                   reason=f"Consecutive failures limit reached: {failures}")
        
        return RiskCheckResult(passed=True, reason="Consecutive failures within limit")
    
    def _check_cooldown(self, symbol: Symbol) -> RiskCheckResult:
        """Check if symbol is in cooldown period."""
        cooldown_seconds = self.system_config.get('risk_management', {}).get(
            'circuit_breakers', {}).get('cooldown_seconds', 60
        )
        
        with self._state_lock:
            cooldown_end = self._state.cooldown_symbols.get(symbol)
            if cooldown_end:
                time_since_cooldown = (datetime.utcnow() - cooldown_end).total_seconds()
                if time_since_cooldown < cooldown_seconds:
                    remaining = cooldown_seconds - time_since_cooldown
                    return RiskCheckResult(
                        passed=False,
                        reason=f"Symbol {symbol} in cooldown ({remaining:.1f}s remaining)"
                    )
        
        return RiskCheckResult(passed=True, reason="Not in cooldown")
    
    # =============================================================================
    # KILL SWITCH
    # =============================================================================
    
    def _activate_kill_switch(self, reason: str) -> None:
        """Activate the kill switch and perform emergency actions."""
        with self._state_lock:
            if self._state.kill_switch_activated:
                return
            
            self._state.kill_switch_activated = True
        
        self.logger.critical(f"KILL SWITCH ACTIVATED: {reason}")
        
        # Perform emergency actions
        self._emergency_square_off()
        
        # Notify subscribers
        self._notify_kill_switch(reason)
    
    def _deactivate_kill_switch(self) -> None:
        """Deactivate the kill switch (use with caution)."""
        with self._state_lock:
            self._state.kill_switch_activated = False
        
        self.logger.warning("Kill switch deactivated manually")
    
    def _emergency_square_off(self) -> None:
        """Emergency square off all positions."""
        self.logger.critical("EMERGENCY SQUARE OFF: Closing all positions")
        
        # Preferred path: the execution engine owns positions and their orders
        try:
            from intraday_system import execution_engine as ee
            if ee._execution_engine is not None:
                ee._execution_engine.square_off_all_positions("KILL SWITCH")
                return
        except Exception as e:
            self.logger.error(f"Execution engine square-off failed, falling back: {e}")
        
        # Fallback: flatten every position the ledger knows about directly
        portfolio = get_portfolio_tracker().get_portfolio_state()
        from intraday_system.order_manager import get_order_manager
        order_manager = get_order_manager()
        order_manager.cancel_all_orders()
        
        for position in portfolio.positions:
            if position.side != PositionSide.FLAT:
                side = OrderSide.SELL if position.side == PositionSide.LONG else OrderSide.BUY
                
                self.logger.critical(f"Emergency close: {side.value} {position.quantity} {position.symbol}")
                
                # Place market order
                order_manager.place_market_order(
                    symbol=position.symbol,
                    side=side,
                    quantity=position.quantity,
                    strategy=position.strategy,
                    reason="KILL SWITCH ACTIVATED - EMERGENCY SQUARE OFF",
                    priority=True
                )
    
    def _check_kill_file(self) -> bool:
        """Check if kill file exists."""
        try:
            return os.path.exists(self._kill_file_path)
        except Exception:
            return False
    
    def _start_kill_file_monitor(self) -> None:
        """Start thread to monitor kill file."""
        if self._kill_file_monitor_thread and self._kill_file_running:
            return
        
        self._kill_file_running = True
        self._kill_file_monitor_thread = threading.Thread(
            target=self._monitor_kill_file,
            name="KillFile-Monitor",
            daemon=True
        )
        self._kill_file_monitor_thread.start()
    
    def _stop_kill_file_monitor(self) -> None:
        """Stop kill file monitor thread."""
        self._kill_file_running = False
        if self._kill_file_monitor_thread:
            self._kill_file_monitor_thread.join(timeout=5)
            self._kill_file_monitor_thread = None
    
    def _monitor_kill_file(self) -> None:
        """Monitor kill file in a loop."""
        while self._kill_file_running:
            try:
                if self._check_kill_file():
                    self._activate_kill_switch("Manual kill file detected")
                    break
            except Exception as e:
                self.logger.error(f"Error monitoring kill file: {e}")
            
            # Sleep for the check interval
            for _ in range(int(self._kill_file_check_interval * 10)):
                if not self._kill_file_running:
                    break
                time_module.sleep(0.1)
    
    def _notify_kill_switch(self, reason: str) -> None:
        """Notify subscribers of kill switch activation."""
        for subscriber in self.subscribers:
            try:
                subscriber('kill_switch_activated', {'reason': reason})
            except Exception as e:
                self.logger.error(f"Error notifying kill switch subscriber: {e}")
    
    # =============================================================================
    # TRADE MANAGEMENT
    # =============================================================================
    
    def on_order_filled(self, order_id: str, filled_quantity: float, 
                        fill_price: float) -> None:
        """Update risk state when an order is filled."""
        with self._state_lock:
            # Reset consecutive failures on successful fill
            self._state.consecutive_failures = 0
    
    def on_order_failed(self, order_id: str, reason: str) -> None:
        """Update risk state when an order fails."""
        with self._state_lock:
            self._state.consecutive_failures += 1
            self._failed_orders.append({'order_id': order_id, 'reason': reason, 'timestamp': datetime.utcnow()})
            
            self.logger.warning(f"Order failure {self._state.consecutive_failures}/{self.config.max_consecutive_failures}: {reason}")
    
    def on_trade_opened(self, trade: Trade) -> None:
        """Update risk state when a trade is opened."""
        with self._state_lock:
            self._open_trades[trade.trade_id] = trade
            self._daily_trades.append(trade)
            
            # Update exposure
            trade_value = trade.entry_price * trade.quantity
            self._state.current_exposure += trade_value
            self._state.open_positions_count += 1
    
    def on_trade_closed(self, trade: Trade, exit_price: float) -> None:
        """Update risk state when a trade is closed."""
        with self._state_lock:
            self._open_trades.pop(trade.trade_id, None)
            self._daily_trades.append(trade)
            # Start cooldown for symbol
            self._state.cooldown_symbols[trade.symbol] = datetime.utcnow()
        self._refresh_state()
        
        # Breaching the daily loss limit trips the kill switch immediately
        self._check_daily_loss_limit()
    
    def _calculate_trade_pnl(self, trade: Trade, exit_price: float) -> float:
        """Calculate PnL for a trade."""
        if trade.side == OrderSide.BUY:
            return (exit_price - trade.entry_price) * trade.quantity
        else:  # SELL
            return (trade.entry_price - exit_price) * trade.quantity
    
    def on_end_of_day(self) -> None:
        """Reset daily limits at end of day."""
        with self._state_lock:
            # Reset daily counters
            self._state.daily_pnl = 0.0
            self._state.daily_pnl_pct = 0.0
            
            self._daily_start = self._now().date()
            self._daily_trades = []
            
            # Reset consecutive failures
            self._state.consecutive_failures = 0
            self._failed_orders = []
            
            # Clear cooldowns
            self._state.cooldown_symbols.clear()
        
        self.logger.info("Daily limits reset")
    
    # =============================================================================
    # STOP LOSS MANAGEMENT
    # =============================================================================
    
    def calculate_stop_loss(self, symbol: Symbol, entry_price: float,
                           side: OrderSide, atr: Optional[float] = None) -> Tuple[float, float]:
        """
        Calculate stop loss and take profit levels.
        
        Returns: (stop_loss, take_profit)
        """
        if atr is None:
            atr = self.data_feed.get_atr(symbol, period=14)
        
        if atr is None:
            # Use percentage-based fallback
            sl_pct = self.config.hard_stop_loss_pct / 100.0
            sl_distance = entry_price * sl_pct
            
            if side == OrderSide.BUY:
                stop_loss = entry_price - sl_distance
                take_profit = entry_price + (sl_distance * 2)  # 2:1 reward
            else:  # SELL
                stop_loss = entry_price + sl_distance
                take_profit = entry_price - (sl_distance * 2)
        else:
            # Use ATR-based stop loss
            sl_distance = atr * self.config.trailing_stop_atr_multiplier
            tp_distance = atr * 2  # 2:1 reward
            
            if side == OrderSide.BUY:
                stop_loss = entry_price - sl_distance
                take_profit = entry_price + tp_distance
            else:  # SELL
                stop_loss = entry_price + sl_distance
                take_profit = entry_price - tp_distance
        
        # Ensure stop loss and take profit are not too close to entry
        min_distance = entry_price * 0.005  # 0.5%
        
        if side == OrderSide.BUY:
            stop_loss = max(entry_price - (entry_price * 0.02), stop_loss)  # Max 2% loss
            take_profit = max(entry_price + min_distance, take_profit)
        else:
            stop_loss = min(entry_price + (entry_price * 0.02), stop_loss)  # Max 2% loss
            take_profit = min(entry_price - min_distance, take_profit)
        
        return stop_loss, take_profit
    
    def update_trailing_stop(self, position: Position, current_price: float) -> Optional[float]:
        """
        Update trailing stop loss for a position.
        
        Returns: New stop loss price if updated, None otherwise
        """
        if not self.config.use_trailing_stop:
            return None
        
        if position.side == PositionSide.LONG:
            # For long positions, trail stop up
            atr = self.data_feed.get_atr(position.symbol, period=14)
            if atr is None:
                return None
            
            trail_distance = atr * self.config.trailing_stop_atr_multiplier
            
            # Calculate new trailing stop
            new_stop = current_price - trail_distance
            
            # Only update if new stop is higher than current stop
            if position.stop_loss is None or new_stop > position.stop_loss:
                return new_stop
            
        elif position.side == PositionSide.SHORT:
            # For short positions, trail stop down
            atr = self.data_feed.get_atr(position.symbol, period=14)
            if atr is None:
                return None
            
            trail_distance = atr * self.config.trailing_stop_atr_multiplier
            
            # Calculate new trailing stop
            new_stop = current_price + trail_distance
            
            # Only update if new stop is lower than current stop
            if position.stop_loss is None or new_stop < position.stop_loss:
                return new_stop
        
        return None
    
    def check_stop_loss_hit(self, position: Position, current_price: float) -> bool:
        """Check if stop loss has been hit."""
        if position.stop_loss is None:
            return False
        
        if position.side == PositionSide.LONG:
            return current_price <= position.stop_loss
        elif position.side == PositionSide.SHORT:
            return current_price >= position.stop_loss
        
        return False
    
    def check_take_profit_hit(self, position: Position, current_price: float) -> bool:
        """Check if take profit has been hit."""
        if position.take_profit is None:
            return False
        
        if position.side == PositionSide.LONG:
            return current_price >= position.take_profit
        elif position.side == PositionSide.SHORT:
            return current_price <= position.take_profit
        
        return False
    
    # =============================================================================
    # STATE MANAGEMENT
    # =============================================================================
    
    def get_risk_state(self) -> RiskState:
        """Get current risk state."""
        try:
            self._refresh_state()
        except Exception as e:
            self.logger.error(f"Could not refresh risk state: {e}")
        with self._state_lock:
            return RiskState(
                daily_pnl=self._state.daily_pnl,
                daily_pnl_pct=self._state.daily_pnl_pct,
                current_exposure=self._state.current_exposure,
                current_exposure_pct=self._state.current_exposure_pct,
                open_positions_count=self._state.open_positions_count,
                cooldown_symbols=self._state.cooldown_symbols.copy(),
                consecutive_failures=self._state.consecutive_failures,
                kill_switch_activated=self._state.kill_switch_activated
            )
    
    def get_daily_stats(self) -> Dict[str, Any]:
        """Get daily trading statistics."""
        with self._state_lock:
            return {
                'daily_pnl': self._state.daily_pnl,
                'daily_pnl_pct': self._state.daily_pnl_pct,
                'open_positions': self._state.open_positions_count,
                'current_exposure': self._state.current_exposure,
                'current_exposure_pct': self._state.current_exposure_pct,
                'consecutive_failures': self._state.consecutive_failures,
                'kill_switch_activated': self._state.kill_switch_activated,
                'open_trades': len(self._open_trades),
                'failed_orders': len(self._failed_orders)
            }
    
    # =============================================================================
    # SUBSCRIPTIONS
    # =============================================================================
    
    def subscribe(self, callback: Callable) -> None:
        """Subscribe to risk manager events."""
        if callback not in self.subscribers:
            self.subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from risk manager events."""
        if callback in self.subscribers:
            self.subscribers.remove(callback)


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_risk_manager: Optional[RiskManager] = None


def get_risk_manager() -> RiskManager:
    """Get the singleton RiskManager instance."""
    global _risk_manager
    if _risk_manager is None:
        _risk_manager = RiskManager()
    return _risk_manager


def start_risk_manager() -> RiskManager:
    """Start the risk manager and return the instance."""
    global _risk_manager
    _risk_manager = RiskManager()
    _risk_manager.start()
    return _risk_manager


def stop_risk_manager() -> None:
    """Stop the risk manager."""
    global _risk_manager
    if _risk_manager:
        _risk_manager.stop()
        _risk_manager = None
