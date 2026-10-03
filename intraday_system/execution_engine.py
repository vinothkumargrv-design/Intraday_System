"""
Execution Engine for the Rapid Fire Intraday Trading System.

This module handles the execution logic including:
- Entry and exit logic based on aggregated signals
- Trailing stop loss management
- Time-based exits (square off at 15:15 IST)
- Order placement coordination with OrderManager
- Position lifecycle management
- Trade journaling and performance tracking

Key Features:
- Event-driven execution based on aggregated signals
- Automatic position management (open, update, close)
- Trailing stop loss implementation
- Time-based square off
- Integration with PortfolioTracker and RiskManager
- Comprehensive logging and observability
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .config_loader import get_config_loader
from .logger import execution_logger
from .models import (
    AggregatedSignal, MarketRegime, OHLCV, Order, OrderId, OrderSide,
    OrderStatus, Position, PositionSide, RiskCheckResult, SignalType,
    StrategyName, Symbol, TimeFrame, Timestamp, Trade, TradeStatus
)
from .data_feed import get_data_feed
from .order_manager import get_order_manager
from .portfolio_tracker import get_portfolio_tracker
from .risk_manager import get_risk_manager
from .signal_aggregator import get_signal_aggregator


# =============================================================================
# EXECUTION ENGINE
# =============================================================================

class ExecutionEngine:
    """
    Handles trade execution based on aggregated signals and manages position lifecycle.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the execution engine."""
        self.config = config or get_config_loader().load()
        self.logger = execution_logger()
        
        # Component references
        self.data_feed = get_data_feed()
        self.order_manager = get_order_manager()
        self.portfolio_tracker = get_portfolio_tracker()
        self.risk_manager = get_risk_manager()
        self.signal_aggregator = get_signal_aggregator()
        
        # Execution state
        self._open_positions: Dict[str, Position] = {}  # position_id -> Position
        self._active_trades: Dict[str, Trade] = {}  # trade_id -> Trade
        self._position_counter = 0
        self._trade_counter = 0
        self._state_lock = threading.Lock()
        
        # Order tracking
        self._signal_to_order: Dict[str, OrderId] = {}  # signal_id -> order_id
        self._position_to_brackets: Dict[str, List[str]] = {}  # position_id -> [bracket_order_ids]
        
        # Subscribers
        self.subscribers: List[Callable] = []
        
        # Threading
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Configuration
        self.square_off_time = self._parse_time(
            self.config.get('risk_management', {}).get('time_stop', {}).get('square_off_time', '15:15')
        )
        self.pre_square_off_minutes = self.config.get('risk_management', {}).get(
            'time_stop', {}).get('pre_square_off_minutes', 5
        )
        self.square_off_enabled = self.config.get('risk_management', {}).get(
            'time_stop', {}).get('enabled', True)
        
        # Market regime tracking
        self._market_regimes: Dict[Symbol, MarketRegime] = {}
        
        self.logger.info("ExecutionEngine initialized")
    
    def _parse_time(self, time_str: str) -> time:
        """Parse time string to time object."""
        try:
            if isinstance(time_str, time):
                return time_str
            hour, minute = map(int, time_str.split(':'))
            return time(hour, minute)
        except Exception:
            return time(15, 15)  # Default to 15:15
    
    def start(self) -> None:
        """Start the execution engine."""
        if self._running:
            return
        
        self._running = True
        
        # Subscribe to aggregated signals
        self.signal_aggregator.subscribe(self.on_aggregated_signal)
        
        # Start monitoring thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_positions,
            name="ExecutionEngine-Thread",
            daemon=True
        )
        self._monitor_thread.start()
        
        self.logger.info("ExecutionEngine started")
    
    def stop(self) -> None:
        """Stop the execution engine."""
        self._running = False
        
        # Unsubscribe from signals
        self.signal_aggregator.unsubscribe(self.on_aggregated_signal)
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
            self._monitor_thread = None
        
        self.logger.info("ExecutionEngine stopped")
    
    def _monitor_positions(self) -> None:
        """Monitor open positions in a background thread."""
        while self._running:
            try:
                # Check for stop loss and take profit hits
                self._check_stop_losses()
                self._check_take_profits()
                self._check_trailing_stops()
                
                # Check for time-based square off
                if self.square_off_enabled:
                    self._check_square_off_time()
                
                # Small sleep
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
                time.sleep(5)
    
    # =============================================================================
    # SIGNAL HANDLING
    # =============================================================================
    
    def on_aggregated_signal(self, signal: AggregatedSignal) -> None:
        """Handle an aggregated signal from the signal aggregator."""
        self.logger.log_signal('execution_engine', signal.to_dict())
        
        try:
            # Check if we can execute this signal
            if not self._can_execute_signal(signal):
                return
            
            # Execute the signal
            if signal.final_signal == SignalType.BUY:
                self._execute_buy_signal(signal)
            elif signal.final_signal == SignalType.SELL:
                self._execute_sell_signal(signal)
            
        except Exception as e:
            self.logger.error(f"Error executing signal for {signal.symbol}: {e}")
    
    def _can_execute_signal(self, signal: AggregatedSignal) -> bool:
        """Check if we can execute a signal."""
        # Check kill switch
        if self.risk_manager.get_risk_state().kill_switch_activated:
            self.logger.warning("Cannot execute signal - kill switch activated")
            return False
        
        # Check if we already have a position in this symbol
        with self._state_lock:
            for position in self._open_positions.values():
                if position.symbol == signal.symbol and position.side != PositionSide.FLAT:
                    self.logger.warning(f"Cannot execute signal for {signal.symbol} - position already open")
                    return False
        
        # Check risk limits
        risk_result = self.risk_manager.check_trade_risk(
            symbol=signal.symbol,
            side=OrderSide.BUY if signal.final_signal == SignalType.BUY else OrderSide.SELL,
            quantity=1,  # Placeholder - actual quantity will be calculated later
            entry_price=0,  # Placeholder
            stop_loss=0  # Placeholder
        )
        
        if not risk_result.passed:
            self.logger.warning(f"Cannot execute signal - risk check failed: {risk_result.reason}")
            return False
        
        return True
    
    def _execute_buy_signal(self, signal: AggregatedSignal) -> None:
        """Execute a BUY signal."""
        symbol = signal.symbol
        
        # Get current price
        current_price = self._get_current_price(symbol)
        if current_price is None:
            self.logger.error(f"Cannot execute BUY for {symbol} - no price data")
            return
        
        # Calculate stop loss and take profit
        stop_loss, take_profit = self._calculate_sl_tp(signal, current_price, OrderSide.BUY)
        
        # Calculate position size
        quantity = self.risk_manager.calculate_position_size(
            symbol=symbol,
            entry_price=current_price,
            stop_loss=stop_loss
        )
        
        if quantity <= 0:
            self.logger.error(f"Cannot execute BUY for {symbol} - invalid quantity: {quantity}")
            return
        
        # Check risk again with actual quantity
        risk_result = self.risk_manager.check_trade_risk(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit
        )
        
        if not risk_result.passed:
            self.logger.warning(f"Cannot execute BUY - risk check failed: {risk_result.reason}")
            return
        
        # Create and place bracket order
        self._place_entry_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            signal=signal,
            strategy=self._get_dominant_strategy(signal)
        )
    
    def _execute_sell_signal(self, signal: AggregatedSignal) -> None:
        """Execute a SELL signal."""
        symbol = signal.symbol
        
        # Get current price
        current_price = self._get_current_price(symbol)
        if current_price is None:
            self.logger.error(f"Cannot execute SELL for {symbol} - no price data")
            return
        
        # Calculate stop loss and take profit
        stop_loss, take_profit = self._calculate_sl_tp(signal, current_price, OrderSide.SELL)
        
        # Calculate position size
        quantity = self.risk_manager.calculate_position_size(
            symbol=symbol,
            entry_price=current_price,
            stop_loss=stop_loss
        )
        
        if quantity <= 0:
            self.logger.error(f"Cannot execute SELL for {symbol} - invalid quantity: {quantity}")
            return
        
        # Check risk again with actual quantity
        risk_result = self.risk_manager.check_trade_risk(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit
        )
        
        if not risk_result.passed:
            self.logger.warning(f"Cannot execute SELL - risk check failed: {risk_result.reason}")
            return
        
        # Create and place bracket order
        self._place_entry_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            signal=signal,
            strategy=self._get_dominant_strategy(signal)
        )
    
    def _place_entry_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                           entry_price: float, stop_loss: float, take_profit: float,
                           signal: AggregatedSignal, strategy: Optional[StrategyName] = None) -> None:
        """Place entry order and create position."""
        try:
            # Determine order type (MARKET or LIMIT from config)
            order_type = self._get_entry_order_type()
            
            if order_type == OrderType.MARKET:
                # Place market order
                order = self.order_manager.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    strategy=strategy,
                    reason=f"Execution from signal: {signal.final_signal.value}"
                )
            else:
                # Place limit order
                order = self.order_manager.place_limit_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=entry_price,
                    strategy=strategy,
                    reason=f"Execution from signal: {signal.final_signal.value}"
                )
            
            if not order:
                self.logger.error(f"Failed to place entry order for {symbol} {side.value}")
                return
            
            # Create position
            position_id = self._generate_position_id()
            position = Position(
                symbol=symbol,
                side=PositionSide.LONG if side == OrderSide.BUY else PositionSide.SHORT,
                quantity=quantity,
                entry_price=entry_price,
                entry_timestamp=datetime.utcnow(),
                strategy=strategy,
                stop_loss=stop_loss,
                take_profit=take_profit,
                order_id=order.order_id
            )
            
            # Store position
            with self._state_lock:
                self._open_positions[position_id] = position
                self._position_to_brackets[position_id] = []
            
            # Create trade record (to be updated when filled)
            trade_id = self._generate_trade_id()
            trade = Trade(
                trade_id=trade_id,
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                exit_price=0,
                quantity=quantity,
                entry_timestamp=datetime.utcnow(),
                exit_timestamp=datetime.utcnow(),
                stop_loss=stop_loss,
                take_profit=take_profit,
                strategy=strategy,
                confidence=signal.net_confidence,
                status=TradeStatus.OPEN,
                tags=[strategy.value if strategy else 'UNKNOWN']
            )
            
            with self._state_lock:
                self._active_trades[trade_id] = trade
                self._signal_to_order[signal.symbol] = order.order_id
            
            # Link position to trade
            position.order_id = order.order_id
            
            # Register for order updates
            self.order_manager.subscribe(self._on_order_update)
            
            # Log the execution
            self.logger.info(f"ENTRY ORDER PLACED: {side.value} {quantity} {symbol} @ {entry_price:.2f} "
                           f"(SL: {stop_loss:.2f}, TP: {take_profit:.2f}, Strategy: {strategy.value if strategy else 'UNKNOWN'})")
            
            # Notify subscribers
            self._notify_execution('entry_placed', {
                'position_id': position_id,
                'trade_id': trade_id,
                'order_id': order.order_id,
                'symbol': symbol,
                'side': side.value,
                'quantity': quantity,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit
            })
            
        except Exception as e:
            self.logger.error(f"Error placing entry order: {e}")
    
    def _get_entry_order_type(self) -> OrderType:
        """Get the configured entry order type."""
        entry_config = self.config.get('execution', {}).get('entry', {})
        order_type_str = entry_config.get('order_type', 'MARKET')
        
        order_types = {
            'MARKET': OrderType.MARKET,
            'LIMIT': OrderType.LIMIT
        }
        
        return order_types.get(order_type_str.upper(), OrderType.MARKET)
    
    def _get_current_price(self, symbol: Symbol) -> Optional[float]:
        """Get current price for a symbol."""
        tick = self.data_feed.cache.get_current_tick(symbol)
        if tick:
            return tick.last_price
        
        ohlcv = self.data_feed.cache.get_current_ohlcv(symbol, TimeFrame._1M)
        if ohlcv:
            return ohlcv.close
        
        return None
    
    def _calculate_sl_tp(self, signal: AggregatedSignal, current_price: float,
                        side: OrderSide) -> Tuple[float, float]:
        """Calculate stop loss and take profit based on signal and current price."""
        # Get suggested SL/TP from the signal
        suggested_sl = None
        suggested_tp = None
        
        for strategy_signal in signal.contributing_strategies:
            if strategy_signal.suggested_sl is not None:
                suggested_sl = strategy_signal.suggested_sl
            if strategy_signal.suggested_tp is not None:
                suggested_tp = strategy_signal.suggested_tp
        
        # If no suggestions from strategies, use risk manager defaults
        if suggested_sl is None or suggested_tp is None:
            suggested_sl, suggested_tp = self.risk_manager.calculate_stop_loss(
                symbol=signal.symbol,
                entry_price=current_price,
                side=side
            )
        
        # Ensure SL and TP are valid
        if side == OrderSide.BUY:
            if suggested_sl >= current_price:
                # SL must be below entry for BUY
                sl_distance = abs(current_price - (suggested_sl or current_price))
                suggested_sl = current_price - sl_distance
            if suggested_tp <= current_price:
                # TP must be above entry for BUY
                tp_distance = abs((suggested_tp or current_price) - current_price)
                suggested_tp = current_price + tp_distance
        else:  # SELL
            if suggested_sl <= current_price:
                # SL must be above entry for SELL
                sl_distance = abs(current_price - (suggested_sl or current_price))
                suggested_sl = current_price + sl_distance
            if suggested_tp >= current_price:
                # TP must be below entry for SELL
                tp_distance = abs((suggested_tp or current_price) - current_price)
                suggested_tp = current_price - tp_distance
        
        # Ensure minimum distance from entry
        min_distance = current_price * 0.005  # 0.5%
        if side == OrderSide.BUY:
            suggested_sl = min(suggested_sl, current_price - min_distance)
            suggested_tp = max(suggested_tp, current_price + min_distance)
        else:
            suggested_sl = max(suggested_sl, current_price + min_distance)
            suggested_tp = min(suggested_tp, current_price - min_distance)
        
        return suggested_sl, suggested_tp
    
    def _get_dominant_strategy(self, signal: AggregatedSignal) -> Optional[StrategyName]:
        """Get the dominant strategy from an aggregated signal."""
        if not signal.contributing_strategies:
            return None
        
        # Find strategy with highest confidence
        dominant = max(signal.contributing_strategies, key=lambda x: x.confidence)
        return dominant.strategy
    
    # =============================================================================
    # ORDER UPDATES
    # =============================================================================
    
    def _on_order_update(self, event_type: str, data: Any) -> None:
        """Handle order updates from OrderManager."""
        try:
            if event_type == 'order_placed':
                self._handle_order_placed(data)
            elif event_type == 'order_update':
                self._handle_order_status_update(data)
            elif event_type == 'order_failed':
                self._handle_order_failed(data)
            elif event_type == 'order_cancelled':
                self._handle_order_cancelled(data)
            elif event_type == 'order_modified':
                self._handle_order_modified(data)
                
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}")
    
    def _handle_order_placed(self, order: Order) -> None:
        """Handle order placed event."""
        # Find the position and trade associated with this order
        with self._state_lock:
            position_id = None
            for pos_id, position in self._open_positions.items():
                if position.order_id == order.order_id:
                    position_id = pos_id
                    break
            
            trade_id = None
            for t_id, trade in self._active_trades.items():
                if trade.entry_price == order.price and trade.symbol == order.symbol:
                    trade_id = t_id
                    break
        
        if position_id and trade_id:
            self._notify_execution('order_placed', {
                'position_id': position_id,
                'trade_id': trade_id,
                'order': order.to_dict()
            })
    
    def _handle_order_status_update(self, data: Dict[str, Any]) -> None:
        """Handle order status update."""
        order_id = data.get('order_id')
        status = data.get('status')
        
        if not order_id or not status:
            return
        
        # Get the order
        order = self.order_manager.get_order(order_id)
        if not order:
            return
        
        # Check if this is an entry order fill
        if status == OrderStatus.COMPLETED.value and order.parent_order_id is None:
            self._handle_entry_filled(order)
        
        # Check if this is a stop loss or take profit hit
        elif status == OrderStatus.COMPLETED.value:
            self._handle_exit_filled(order)
    
    def _handle_entry_filled(self, order: Order) -> None:
        """Handle entry order fill."""
        # Find the position
        with self._state_lock:
            position_id = None
            for pos_id, position in self._open_positions.items():
                if position.order_id == order.order_id:
                    position_id = pos_id
                    break
            
            if not position_id:
                return
            
            position = self._open_positions[position_id]
        
        # Update position
        position.entry_price = order.average_fill_price or order.price or order.trigger_price or 0
        position.entry_timestamp = order.timestamp
        
        # Place bracket orders (SL and TP)
        if self._should_use_bracket_orders():
            self._place_bracket_orders(position, order)
        
        # Update trade
        with self._state_lock:
            trade_id = None
            for t_id, trade in self._active_trades.items():
                if trade.symbol == order.symbol and trade.side == order.order_side:
                    trade_id = t_id
                    break
            
            if trade_id:
                trade = self._active_trades[trade_id]
                trade.entry_price = position.entry_price
                trade.entry_timestamp = position.entry_timestamp
        
        # Notify portfolio tracker
        self.portfolio_tracker.on_position_opened(position)
        
        # Log the fill
        self.logger.info(f"ENTRY FILLED: {order.symbol} {order.order_side.value} {order.filled_quantity} @ {order.average_fill_price:.2f}")
        
        # Notify subscribers
        self._notify_execution('entry_filled', {
            'position_id': position_id,
            'trade_id': trade_id,
            'order': order.to_dict(),
            'position': position.to_dict()
        })
    
    def _place_bracket_orders(self, position: Position, entry_order: Order) -> None:
        """Place SL and TP orders for a position."""
        if not position.stop_loss or not position.take_profit:
            return
        
        # Calculate SL and TP quantities (same as entry)
        sl_quantity = position.quantity
        tp_quantity = position.quantity
        
        # Place SL order
        sl_side = OrderSide.SELL if position.side == PositionSide.LONG else OrderSide.BUY
        sl_order = self.order_manager.place_sl_order(
            symbol=position.symbol,
            side=sl_side,
            quantity=sl_quantity,
            trigger_price=position.stop_loss,
            strategy=position.strategy,
            parent_order_id=entry_order.order_id,
            reason=f"SL for position {position.symbol}"
        )
        
        # Place TP order
        tp_side = OrderSide.SELL if position.side == PositionSide.LONG else OrderSide.BUY
        tp_order = self.order_manager.place_limit_order(
            symbol=position.symbol,
            side=tp_side,
            quantity=tp_quantity,
            price=position.take_profit,
            strategy=position.strategy,
            parent_order_id=entry_order.order_id,
            reason=f"TP for position {position.symbol}"
        )
        
        if sl_order and tp_order:
            self.logger.info(f"Bracket orders placed: SL={position.stop_loss:.2f}, TP={position.take_profit:.2f}")
    
    def _should_use_bracket_orders(self) -> bool:
        """Check if bracket orders should be used."""
        return self.config.get('execution', {}).get('exits', {}).get('use_bracket_orders', True)
    
    def _handle_exit_filled(self, order: Order) -> None:
        """Handle stop loss or take profit order fill."""
        # Find the position associated with this exit order
        parent_order_id = order.parent_order_id
        if not parent_order_id:
            return
        
        # Find the position
        with self._state_lock:
            position_id = None
            for pos_id, position in self._open_positions.items():
                if position.order_id == parent_order_id:
                    position_id = pos_id
                    break
            
            if not position_id:
                return
            
            position = self._open_positions[position_id]
        
        # Determine if this is SL or TP
        is_sl = order.order_type in [OrderType.SL, OrderType.SL_M]
        is_tp = order.order_type == OrderType.LIMIT and order.parent_order_id == parent_order_id
        
        # Update position
        if is_sl:
            position.stop_loss = order.trigger_price or order.price or position.stop_loss
            exit_reason = "STOP LOSS HIT"
            exit_type = "SL"
        elif is_tp:
            position.take_profit = order.price or position.take_profit
            exit_reason = "TAKE PROFIT HIT"
            exit_type = "TP"
        else:
            exit_reason = "EXIT ORDER FILLED"
            exit_type = "EXIT"
        
        # Close the position
        self._close_position(position, order.average_fill_price or order.price or 0, exit_reason, exit_type)
        
        # Start cooldown for symbol
        self.signal_aggregator.start_cooldown(position.symbol)
    
    def _handle_order_failed(self, data: Dict[str, Any]) -> None:
        """Handle order failure."""
        order_id = data.get('order_id')
        reason = data.get('reason', 'Unknown')
        
        self.logger.error(f"Order failed: {order_id} - {reason}")
        
        # Check if this is an entry order
        with self._state_lock:
            for position_id, position in list(self._open_positions.items()):
                if position.order_id == order_id:
                    # Remove the position since entry failed
                    del self._open_positions[position_id]
                    
                    # Also remove associated trade
                    for trade_id, trade in list(self._active_trades.items()):
                        if trade.symbol == position.symbol and trade.entry_price == position.entry_price:
                            del self._active_trades[trade_id]
                            break
                    
                    break
        
        # Notify subscribers
        self._notify_execution('order_failed', data)
    
    def _handle_order_cancelled(self, data: Dict[str, Any]) -> None:
        """Handle order cancellation."""
        order_id = data.get('order_id')
        self.logger.info(f"Order cancelled: {order_id}")
        
        # Notify subscribers
        self._notify_execution('order_cancelled', data)
    
    def _handle_order_modified(self, data: Dict[str, Any]) -> None:
        """Handle order modification."""
        order_id = data.get('order_id')
        self.logger.info(f"Order modified: {order_id}")
        
        # Notify subscribers
        self._notify_execution('order_modified', data)
    
    # =============================================================================
    # POSITION MANAGEMENT
    # =============================================================================
    
    def _close_position(self, position: Position, exit_price: float, 
                        reason: str, exit_type: str) -> None:
        """Close a position and update trade record."""
        position_id = None
        with self._state_lock:
            for pos_id, pos in self._open_positions.items():
                if pos == position:
                    position_id = pos_id
                    break
            
            if not position_id:
                return
        
        # Calculate PnL
        pnl = self._calculate_position_pnl(position, exit_price)
        
        # Create trade record
        trade_id = None
        with self._state_lock:
            for t_id, trade in self._active_trades.items():
                if trade.symbol == position.symbol and trade.side.value == position.side.value:
                    trade_id = t_id
                    break
        
        if trade_id:
            with self._state_lock:
                trade = self._active_trades[trade_id]
                trade.exit_price = exit_price
                trade.exit_timestamp = datetime.utcnow()
                trade.pnl = pnl
                trade.status = TradeStatus.CLOSED
                trade.tags.append(reason)
                trade.tags.append(exit_type)
        
        # Update position
        position.side = PositionSide.FLAT
        
        # Remove from open positions
        with self._state_lock:
            if position_id in self._open_positions:
                del self._open_positions[position_id]
        
        # Notify portfolio tracker
        self.portfolio_tracker.on_position_closed(position, exit_price)
        
        # Notify risk manager
        if trade_id and trade_id in self._active_trades:
            self.risk_manager.on_trade_closed(self._active_trades[trade_id], exit_price)
        
        # Log the close
        self.logger.info(f"POSITION CLOSED: {position.symbol} {position.side.value} {position.quantity} "
                       f"Entry: {position.entry_price:.2f} Exit: {exit_price:.2f} "
                       f"PnL: {pnl:.2f} Reason: {reason}")
        
        # Notify subscribers
        self._notify_execution('position_closed', {
            'position_id': position_id,
            'trade_id': trade_id,
            'symbol': position.symbol,
            'side': position.side.value,
            'quantity': position.quantity,
            'entry_price': position.entry_price,
            'exit_price': exit_price,
            'pnl': pnl,
            'reason': reason,
            'exit_type': exit_type
        })
    
    def _calculate_position_pnl(self, position: Position, exit_price: float) -> float:
        """Calculate PnL for a position."""
        if position.side == PositionSide.LONG:
            return (exit_price - position.entry_price) * position.quantity
        elif position.side == PositionSide.SHORT:
            return (position.entry_price - exit_price) * position.quantity
        return 0.0
    
    # =============================================================================
    # STOP LOSS AND TAKE PROFIT CHECKS
    # =============================================================================
    
    def _check_stop_losses(self) -> None:
        """Check if any stop losses have been hit."""
        with self._state_lock:
            positions = list(self._open_positions.values())
        
        for position in positions:
            if position.side == PositionSide.FLAT:
                continue
            
            current_price = self._get_current_price(position.symbol)
            if current_price is None:
                continue
            
            if self.risk_manager.check_stop_loss_hit(position, current_price):
                self.logger.warning(f"STOP LOSS HIT: {position.symbol} at {current_price:.2f}")
                self._close_position(position, current_price, "STOP LOSS HIT", "SL")
    
    def _check_take_profits(self) -> None:
        """Check if any take profits have been hit."""
        with self._state_lock:
            positions = list(self._open_positions.values())
        
        for position in positions:
            if position.side == PositionSide.FLAT:
                continue
            
            current_price = self._get_current_price(position.symbol)
            if current_price is None:
                continue
            
            if self.risk_manager.check_take_profit_hit(position, current_price):
                self.logger.info(f"TAKE PROFIT HIT: {position.symbol} at {current_price:.2f}")
                self._close_position(position, current_price, "TAKE PROFIT HIT", "TP")
    
    def _check_trailing_stops(self) -> None:
        """Check and update trailing stop losses."""
        with self._state_lock:
            positions = list(self._open_positions.values())
        
        for position in positions:
            if position.side == PositionSide.FLAT:
                continue
            
            current_price = self._get_current_price(position.symbol)
            if current_price is None:
                continue
            
            # Check if we should update trailing stop
            new_stop = self.risk_manager.update_trailing_stop(position, current_price)
            
            if new_stop is not None:
                position.stop_loss = new_stop
                self.logger.info(f"TRAILING STOP UPDATED: {position.symbol} -> {new_stop:.2f}")
                
                # Update SL order
                self._update_sl_order(position, new_stop)
    
    def _update_sl_order(self, position: Position, new_stop: float) -> None:
        """Update stop loss order for a position."""
        # Find the SL order for this position
        bracket_orders = self._position_to_brackets.get(position.symbol, [])
        
        for order_id in bracket_orders:
            order = self.order_manager.get_order(order_id)
            if order and order.order_type in [OrderType.SL, OrderType.SL_M]:
                # Modify the SL order
                self.order_manager.modify_order(
                    order_id=order_id,
                    trigger_price=new_stop
                )
                break
    
    # =============================================================================
    # TIME-BASED SQUARE OFF
    # =============================================================================
    
    def _check_square_off_time(self) -> None:
        """Check if it's time to square off all positions."""
        timezone = self.system_config.get('system', {}).get('timezone', 'Asia/Kolkata')
        
        try:
            import pytz
            tz = pytz.timezone(timezone)
            now = datetime.now(tz)
            current_time = now.time()
            
            # Check if we've reached square off time
            if current_time >= self.square_off_time:
                self.logger.warning(f"SQUARE OFF TIME REACHED: {current_time} >= {self.square_off_time}")
                self.square_off_all_positions()
                return
            
            # Check if we're in pre-square off period
            pre_square_off_time = (self.square_off_time.hour * 60 + self.square_off_time.minute - 
                                  self.pre_square_off_minutes) * 60
            current_seconds = current_time.hour * 3600 + current_time.minute * 60
            
            if current_seconds >= pre_square_off_time:
                self.logger.info("Entering pre-square off period")
                # Start closing positions gradually
                self._gradual_square_off()
                
        except Exception as e:
            self.logger.error(f"Error checking square off time: {e}")
    
    def square_off_all_positions(self, reason: str = "END OF DAY") -> None:
        """Square off all open positions immediately."""
        with self._state_lock:
            positions = list(self._open_positions.values())
        
        for position in positions:
            if position.side == PositionSide.FLAT:
                continue
            
            current_price = self._get_current_price(position.symbol)
            if current_price is None:
                continue
            
            self._close_position(position, current_price, reason, "SQUARE_OFF")
        
        self.logger.info(f"All positions squared off: {reason}")
    
    def _gradual_square_off(self) -> None:
        """Gradually square off positions during pre-square off period."""
        with self._state_lock:
            positions = list(self._open_positions.values())
        
        # Close positions with losses first (to free up margin)
        losing_positions = []
        winning_positions = []
        
        for position in positions:
            if position.side == PositionSide.FLAT:
                continue
            
            current_price = self._get_current_price(position.symbol)
            if current_price is None:
                continue
            
            pnl = self._calculate_position_pnl(position, current_price)
            
            if pnl < 0:
                losing_positions.append(position)
            else:
                winning_positions.append(position)
        
        # Close losing positions first
        for position in losing_positions:
            current_price = self._get_current_price(position.symbol)
            if current_price:
                self._close_position(position, current_price, "PRE-SQUARE OFF (Loss)", "SQUARE_OFF")
        
        # Close some winning positions
        if winning_positions:
            position_to_close = winning_positions[0]  # Close the first winning position
            current_price = self._get_current_price(position_to_close.symbol)
            if current_price:
                self._close_position(position_to_close, current_price, "PRE-SQUARE OFF (Profit)", "SQUARE_OFF")
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def _generate_position_id(self) -> str:
        """Generate a unique position ID."""
        with self._state_lock:
            self._position_counter += 1
            return f"POS-{datetime.utcnow().strftime('%Y%m%d')}-{self._position_counter}"
    
    def _generate_trade_id(self) -> str:
        """Generate a unique trade ID."""
        with self._state_lock:
            self._trade_counter += 1
            return f"TRADE-{datetime.utcnow().strftime('%Y%m%d')}-{self._trade_counter}"
    
    def get_open_positions(self) -> List[Position]:
        """Get all open positions."""
        with self._state_lock:
            return list(self._open_positions.values())
    
    def get_open_position(self, position_id: str) -> Optional[Position]:
        """Get a specific open position."""
        with self._state_lock:
            return self._open_positions.get(position_id)
    
    def get_active_trades(self) -> List[Trade]:
        """Get all active trades."""
        with self._state_lock:
            return list(self._active_trades.values())
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        with self._state_lock:
            open_positions = len(self._open_positions)
            active_trades = len(self._active_trades)
        
        return {
            'open_positions': open_positions,
            'active_trades': active_trades,
            'square_off_time': self.square_off_time.isoformat(),
            'pre_square_off_minutes': self.pre_square_off_minutes,
            'square_off_enabled': self.square_off_enabled
        }
    
    # =============================================================================
    # NOTIFICATIONS
    # =============================================================================
    
    def subscribe(self, callback: Callable) -> None:
        """Subscribe to execution events."""
        self.subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from execution events."""
        if callback in self.subscribers:
            self.subscribers.remove(callback)
    
    def _notify_execution(self, event_type: str, data: Dict[str, Any]) -> None:
        """Notify subscribers of execution events."""
        for subscriber in self.subscribers:
            try:
                subscriber(event_type, data)
            except Exception as e:
                self.logger.error(f"Error notifying execution subscriber: {e}")


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_execution_engine: Optional[ExecutionEngine] = None


def get_execution_engine() -> ExecutionEngine:
    """Get the singleton ExecutionEngine instance."""
    global _execution_engine
    if _execution_engine is None:
        _execution_engine = ExecutionEngine()
    return _execution_engine


def start_execution_engine() -> ExecutionEngine:
    """Start the execution engine and return the instance."""
    global _execution_engine
    _execution_engine = ExecutionEngine()
    _execution_engine.start()
    return _execution_engine


def stop_execution_engine() -> None:
    """Stop the execution engine."""
    global _execution_engine
    if _execution_engine:
        _execution_engine.stop()
        _execution_engine = None
