"""
Order Manager for the Rapid Fire Intraday Trading System.

This module manages all aspects of order placement, tracking, and management:
- Order placement with retry logic and exponential backoff
- Partial fill handling with SL quantity adjustment
- Order types: MARKET, LIMIT, SL, SL-M, OCO, BRACKET (where supported)
- Idempotency keys to prevent duplicate orders
- Order status tracking and reconciliation
- Integration with multiple brokers

Key Features:
- Async order placement with automatic retries
- Bracket order support for built-in stop loss and take profit
- Partial fill management
- Order validation and risk checks before placement
- Comprehensive logging and observability
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import hashlib

from .config_loader import get_config_loader
from .logger import order_logger
from .models import (
    BracketOrder, BrokerConfig, Order, OrderId, OrderResponse, OrderSide, OrderStatus,
    OrderType, OrderValidity, PositionSide, SignalType, StrategyName,
    Symbol, Timestamp, Trade, TradeStatus
)
from .brokers.base_broker import BaseBroker, BrokerError
from .brokers.broker_factory import get_broker
from .data_feed import get_data_feed
from .risk_manager import get_risk_manager
from .portfolio_tracker import get_portfolio_tracker


# =============================================================================
# ORDER MANAGER
# =============================================================================

class OrderManager:
    """
    Manages all order-related operations including placement, tracking, and management.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the order manager."""
        self.config = config or get_config_loader().load()
        self.logger = order_logger()
        
        # Broker connection
        self.broker: Optional[BaseBroker] = None
        self._init_broker()
        
        # Order storage and tracking
        self._orders: Dict[OrderId, Order] = {}
        self._pending_orders: Dict[OrderId, Order] = {}
        self._completed_orders: Dict[OrderId, Order] = {}
        self._failed_orders: Dict[OrderId, Order] = {}
        self._order_lock = threading.Lock()
        
        # Bracket orders tracking
        self._bracket_orders: Dict[OrderId, BracketOrder] = {}
        
        # Idempotency keys
        self._idempotency_keys: Dict[str, OrderId] = {}
        self._idempotency_lock = threading.Lock()
        
        # Order retry configuration
        self.max_retries = self.config.get('execution', {}).get('retries', {}).get('max_retries', 3)
        self.base_retry_delay = self.config.get('execution', {}).get('retries', {}).get('base_delay_ms', 100) / 1000.0
        self.max_retry_delay = self.config.get('execution', {}).get('retries', {}).get('max_delay_ms', 5000) / 1000.0
        self.retry_multiplier = self.config.get('execution', {}).get('retries', {}).get('exponential_backoff', True)
        
        # Timeout configuration
        self.fallback_timeout = self.config.get('execution', {}).get('entry', {}).get('fallback_timeout_seconds', 2)
        self.use_fallback_to_market = self.config.get('execution', {}).get('entry', {}).get('use_fallback_to_market', True)
        
        # Order types configuration
        self.use_bracket_orders = self.config.get('execution', {}).get('exits', {}).get('use_bracket_orders', True)
        self.use_oco_orders = self.config.get('execution', {}).get('exits', {}).get('use_oco_orders', False)
        
        # Default order validity
        self.default_validity = OrderValidity.MIS
        if self.broker:
            validity_map = {
                'ZERODHA': OrderValidity.MIS,
                'ALPACA': OrderValidity.DAY,
                'INTERACTIVE_BROKERS': OrderValidity.DAY
            }
            broker_type = get_config_loader().get_broker_config().broker_type.value
            self.default_validity = validity_map.get(broker_type, OrderValidity.MIS)
        
        # Subscribers
        self.subscribers: List[Callable] = []
        
        # Threading
        self._order_thread: Optional[threading.Thread] = None
        self._running = False
        
        self.logger.info(f"OrderManager initialized with broker: {type(self.broker).__name__}")
    
    def _init_broker(self) -> None:
        """Initialize broker connection."""
        try:
            broker_config = get_config_loader().get_broker_config()
            self.broker = get_broker(broker_config.broker_type.value, broker_config.to_dict())
            
            # Connect to broker
            if self.broker:
                self.broker.connect()
                self.logger.info(f"Connected to broker: {broker_config.broker_type.value}")
        except Exception as e:
            self.logger.error(f"Failed to initialize broker: {e}")
            # Fall back to paper trading broker
            broker_type_str = self.config.get('broker', {}).get('active', 'PAPER')
            self.broker = get_broker(
                broker_type=broker_type_str,
                config={'paper': {}}
            )
    
    def start(self) -> None:
        """Start the order manager."""
        if self._running:
            return
        
        self._running = True
        
        # Start order monitoring thread
        self._order_thread = threading.Thread(
            target=self._monitor_orders,
            name="OrderManager-Thread",
            daemon=True
        )
        self._order_thread.start()
        
        self.logger.info("OrderManager started")
    
    def stop(self) -> None:
        """Stop the order manager."""
        self._running = False
        
        if self._order_thread:
            self._order_thread.join(timeout=10)
            self._order_thread = None
        
        # Disconnect broker
        if self.broker:
            try:
                self.broker.disconnect()
            except Exception as e:
                self.logger.error(f"Error disconnecting broker: {e}")
        
        self.logger.info("OrderManager stopped")
    
    def _monitor_orders(self) -> None:
        """Monitor order status in a background thread."""
        while self._running:
            try:
                # Check order statuses
                self._update_order_statuses()
                
                # Small sleep to prevent CPU overload
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error in order monitoring: {e}")
                time.sleep(5)
    
    def _update_order_statuses(self) -> None:
        """Update status of all pending orders."""
        if not self.broker:
            return
        
        with self._order_lock:
            pending_order_ids = list(self._pending_orders.keys())
        
        for order_id in pending_order_ids:
            try:
                # Get order status from broker
                order = self._pending_orders[order_id]
                if not self.broker:
                    continue
                
                broker_order_id = order.broker_order_id
                if broker_order_id:
                    status = self.broker.get_order_status(broker_order_id)
                    
                    if status:
                        self._update_order_status(order_id, status)
                        
                        # If completed, remove from pending
                        if status in [OrderStatus.COMPLETED, OrderStatus.REJECTED, OrderStatus.CANCELLED]:
                            with self._order_lock:
                                if order_id in self._pending_orders:
                                    del self._pending_orders[order_id]
                                    
                                if status == OrderStatus.COMPLETED:
                                    self._completed_orders[order_id] = self._orders[order_id]
                                elif status in [OrderStatus.REJECTED, OrderStatus.CANCELLED]:
                                    self._failed_orders[order_id] = self._orders[order_id]
                            
                            # Notify subscribers
                            self._notify_order_update(order_id, status)
            except Exception as e:
                self.logger.error(f"Error updating order status for {order_id}: {e}")
    
    def _update_order_status(self, order_id: OrderId, status: OrderStatus) -> None:
        """Update the status of an order."""
        with self._order_lock:
            if order_id in self._orders:
                self._orders[order_id].status = status
                
                # Update bracket orders
                if order_id in self._bracket_orders:
                    bracket = self._bracket_orders[order_id]
                    if order_id == bracket.entry_order.order_id:
                        bracket.entry_order.status = status
                    elif order_id == bracket.sl_order.order_id:
                        bracket.sl_order.status = status
                    elif bracket.tp_order and order_id == bracket.tp_order.order_id:
                        bracket.tp_order.status = status
    
    # =============================================================================
    # ORDER PLACEMENT
    # =============================================================================
    
    def place_order(self, symbol: Symbol, side: OrderSide, order_type: OrderType,
                    quantity: float, price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    validity: Optional[OrderValidity] = None,
                    strategy: Optional[StrategyName] = None,
                    parent_order_id: Optional[OrderId] = None,
                    reason: str = "", priority: bool = False) -> Optional[Order]:
        """
        Place an order with the broker.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            order_type: MARKET, LIMIT, SL, SL-M, etc.
            quantity: Order quantity
            price: Price for LIMIT orders
            trigger_price: Trigger price for SL orders
            validity: Order validity (DAY, IOC, MIS, etc.)
            strategy: Strategy that generated this order
            parent_order_id: Parent order ID for bracket orders
            reason: Reason for placing the order
            priority: High priority order (bypasses some checks)
            
        Returns:
            Order object if successful, None otherwise
        """
        try:
            # Generate unique order ID
            order_id = self._generate_order_id()
            
            # Check for duplicate order (idempotency)
            if self._is_duplicate_order(order_id, symbol, side, quantity):
                self.logger.warning(f"Duplicate order detected for {symbol} {side.value} {quantity}")
                return None
            
            # Set default validity
            if validity is None:
                validity = self.default_validity
            
            # Create order object
            order = Order(
                order_id=order_id,
                symbol=symbol,
                order_side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                trigger_price=trigger_price,
                status=OrderStatus.PLACED,
                timestamp=datetime.utcnow(),
                validity=validity,
                strategy=strategy,
                parent_order_id=parent_order_id,
                idempotency_key=self._generate_idempotency_key(symbol, side, quantity, price)
            )
            
            # Store order
            with self._order_lock:
                self._orders[order_id] = order
                self._pending_orders[order_id] = order
            
            # Log the order
            self.logger.log_order('order_manager', order.to_dict())
            
            # Place order with broker (async)
            asyncio.create_task(self._place_order_async(order, reason, priority))
            
            return order
            
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return None
    
    async def _place_order_async(self, order: Order, reason: str, priority: bool) -> None:
        """Place order asynchronously with retry logic."""
        if not self.broker:
            self._handle_order_failure(order.order_id, "No broker connection")
            return
        
        # Perform risk checks (unless priority)
        if not priority:
            risk_result = self._perform_risk_checks(order)
            if not risk_result.passed:
                self._handle_order_failure(order.order_id, risk_result.reason)
                return
        
        # Try to place the order
        for attempt in range(self.max_retries + 1):
            try:
                # Place order with broker
                response = await self._place_with_broker(order)
                
                if response.success:
                    # Order placed successfully
                    with self._order_lock:
                        self._orders[order.order_id].broker_order_id = response.broker_order_id
                        self._orders[order.order_id].status = OrderStatus.PLACED
                    
                    self.logger.info(f"Order placed successfully: {order.order_id} -> {response.broker_order_id}")
                    self._notify_order_placed(order)
                    return
                else:
                    # Order placement failed
                    error_msg = response.error_message or f"Unknown error: {response.error_code}"
                    self.logger.warning(f"Order placement failed (attempt {attempt + 1}/{self.max_retries + 1}): {error_msg}")
                    
                    # Check if error is retryable
                    if self._is_retryable_error(response.error_code or "") and attempt < self.max_retries:
                        # Calculate delay with exponential backoff
                        delay = self._calculate_retry_delay(attempt)
                        self.logger.info(f"Retrying in {delay:.2f}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        # Non-retryable error
                        self._handle_order_failure(order.order_id, error_msg)
                        return
                        
            except BrokerError as e:
                self.logger.error(f"Broker error placing order (attempt {attempt + 1}): {e}")
                
                if attempt < self.max_retries:
                    delay = self._calculate_retry_delay(attempt)
                    await asyncio.sleep(delay)
                    continue
                else:
                    self._handle_order_failure(order.order_id, str(e))
                    return
                    
            except Exception as e:
                self.logger.error(f"Unexpected error placing order: {e}")
                self._handle_order_failure(order.order_id, str(e))
                return
        
        # All retries exhausted
        self._handle_order_failure(order.order_id, f"Max retries ({self.max_retries}) exhausted")
    
    async def _place_with_broker(self, order: Order) -> OrderResponse:
        """Place order with the broker."""
        if not self.broker:
            return OrderResponse(success=False, error_message="No broker connection")
        
        try:
            # Convert order to broker-specific format
            broker_order = self._convert_to_broker_order(order)
            
            # Place order
            response = await self.broker.place_order(
                symbol=order.symbol,
                side=order.order_side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                trigger_price=order.trigger_price,
                validity=order.validity,
                idempotency_key=order.idempotency_key
            )
            
            return OrderResponse(
                success=response.get('success', False),
                order_id=order.order_id,
                broker_order_id=response.get('order_id'),
                error_code=response.get('error_code'),
                error_message=response.get('error_message', response.get('message', 'Unknown error'))
            )
            
        except Exception as e:
            return OrderResponse(
                success=False,
                error_message=str(e)
            )
    
    def _convert_to_broker_order(self, order: Order) -> Dict[str, Any]:
        """Convert order to broker-specific format."""
        # This method can be overridden for broker-specific requirements
        return {
            'symbol': order.symbol,
            'side': order.order_side.value,
            'type': order.order_type.value,
            'quantity': order.quantity,
            'price': order.price,
            'trigger_price': order.trigger_price,
            'validity': order.validity.value,
            'idempotency_key': order.idempotency_key
        }
    
    def _is_retryable_error(self, error_code: str) -> bool:
        """Check if an error is retryable."""
        retryable_errors = self.config.get('execution', {}).get('retries', {}).get('retryable_errors', []) or [
            "TIMEOUT", "NETWORK_ERROR", "RATE_LIMIT", "TEMPORARY_ERROR",
            "500", "502", "503", "504", "429"
        ]
        
        return error_code.upper() in [e.upper() for e in retryable_errors]
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff."""
        if not self.retry_multiplier:
            return self.base_retry_delay
        
        delay = self.base_retry_delay * (self.retry_multiplier ** attempt)
        return min(delay, self.max_retry_delay)
    
    def _handle_order_failure(self, order_id: OrderId, reason: str) -> None:
        """Handle order placement failure."""
        with self._order_lock:
            if order_id in self._orders:
                self._orders[order_id].status = OrderStatus.REJECTED
                
                if order_id in self._pending_orders:
                    del self._pending_orders[order_id]
                
                self._failed_orders[order_id] = self._orders[order_id]
        
        # Notify risk manager
        risk_manager = get_risk_manager()
        risk_manager.on_order_failed(order_id, reason)
        
        # Notify subscribers
        self._notify_order_failure(order_id, reason)
        
        self.logger.error(f"Order failed: {order_id} - {reason}")
    
    def _perform_risk_checks(self, order: Order) -> Any:
        """Perform risk checks before order placement."""
        risk_manager = get_risk_manager()
        
        # Get current price for spread check
        current_price = self.data_feed.cache.get_current_tick(order.symbol)
        entry_price = order.price or (current_price.last_price if current_price else 0)
        
        # Perform comprehensive risk check
        risk_result = risk_manager.check_trade_risk(
            symbol=order.symbol,
            side=order.order_side,
            quantity=order.quantity,
            entry_price=entry_price,
            stop_loss=order.trigger_price or entry_price,  # Use trigger_price as SL
            take_profit=None
        )
        
        return risk_result
    
    # =============================================================================
    # BRACKET ORDERS
    # =============================================================================
    
    def place_bracket_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                           entry_price: float, stop_loss: float, take_profit: float,
                           strategy: Optional[StrategyName] = None,
                           reason: str = "") -> Optional[BracketOrder]:
        """
        Place a bracket order (entry + SL + TP).
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            quantity: Order quantity
            entry_price: Entry price
            stop_loss: Stop loss price
            take_profit: Take profit price
            strategy: Strategy that generated this order
            reason: Reason for placing the order
            
        Returns:
            BracketOrder object if successful, None otherwise
        """
        try:
            if not self.use_bracket_orders:
                # Fall back to individual orders
                return self._place_individual_orders(symbol, side, quantity, 
                                                     entry_price, stop_loss, take_profit, strategy, reason)
            
            # Generate order IDs
            entry_order_id = self._generate_order_id()
            sl_order_id = self._generate_order_id()
            tp_order_id = self._generate_order_id()
            
            # Create entry order
            entry_order = self.place_order(
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=entry_price,
                validity=self.default_validity,
                strategy=strategy,
                reason=reason
            )
            
            if not entry_order:
                self.logger.error("Failed to place entry order for bracket")
                return None
            
            # Create stop loss order
            sl_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
            sl_order = self.place_order(
                symbol=symbol,
                side=sl_side,
                order_type=self._get_sl_order_type(),
                quantity=quantity,
                price=stop_loss,
                trigger_price=stop_loss,
                validity=OrderValidity.MIS,
                strategy=strategy,
                parent_order_id=entry_order_id,
                reason=f"SL for bracket order {entry_order_id}"
            )
            
            if not sl_order:
                self.logger.error("Failed to place SL order for bracket")
                # Cancel entry order
                self.cancel_order(entry_order_id)
                return None
            
            # Create take profit order
            tp_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
            tp_order = self.place_order(
                symbol=symbol,
                side=tp_side,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=take_profit,
                validity=OrderValidity.MIS,
                strategy=strategy,
                parent_order_id=entry_order_id,
                reason=f"TP for bracket order {entry_order_id}"
            )
            
            if not tp_order:
                self.logger.error("Failed to place TP order for bracket")
                # Cancel entry and SL orders
                self.cancel_order(entry_order_id)
                self.cancel_order(sl_order_id)
                return None
            
            # Create bracket order
            bracket = BracketOrder(
                entry_order=entry_order,
                sl_order=sl_order,
                tp_order=tp_order
            )
            
            # Store bracket order
            with self._order_lock:
                self._bracket_orders[entry_order_id] = bracket
                self._bracket_orders[sl_order_id] = bracket
                self._bracket_orders[tp_order_id] = bracket
            
            self.logger.info(f"Bracket order placed: {entry_order_id} (Entry: {entry_price}, SL: {stop_loss}, TP: {take_profit})")
            
            return bracket
            
        except Exception as e:
            self.logger.error(f"Error placing bracket order: {e}")
            return None
    
    def _get_sl_order_type(self) -> OrderType:
        """Get the appropriate SL order type for the current broker."""
        broker_type = get_config_loader().get_broker_config().broker_type.value
        
        if broker_type == 'ZERODHA':
            return OrderType.SL_M  # SL-M for Zerodha
        else:
            return OrderType.SL
    
    def _place_individual_orders(self, symbol: Symbol, side: OrderSide, quantity: float,
                                entry_price: float, stop_loss: float, take_profit: float,
                                strategy: Optional[StrategyName] = None,
                                reason: str = "") -> Optional[BracketOrder]:
        """Place individual orders instead of a bracket order."""
        # Place entry order
        entry_order = self.place_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=entry_price,
            validity=self.default_validity,
            strategy=strategy,
            reason=reason
        )
        
        if not entry_order:
            return None
        
        # Place SL and TP orders
        sl_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
        tp_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
        
        sl_order = self.place_order(
            symbol=symbol,
            side=sl_side,
            order_type=self._get_sl_order_type(),
            quantity=quantity,
            price=stop_loss,
            trigger_price=stop_loss,
            validity=OrderValidity.MIS,
            strategy=strategy,
            parent_order_id=entry_order.order_id,
            reason=f"SL for order {entry_order.order_id}"
        )
        
        tp_order = self.place_order(
            symbol=symbol,
            side=tp_side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=take_profit,
            validity=OrderValidity.MIS,
            strategy=strategy,
            parent_order_id=entry_order.order_id,
            reason=f"TP for order {entry_order.order_id}"
        )
        
        if entry_order and sl_order and tp_order:
            bracket = BracketOrder(
                entry_order=entry_order,
                sl_order=sl_order,
                tp_order=tp_order
            )
            return bracket
        
        return None
    
    # =============================================================================
    # MARKET ORDERS
    # =============================================================================
    
    def place_market_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                          strategy: Optional[StrategyName] = None,
                          reason: str = "", priority: bool = False) -> Optional[Order]:
        """
        Place a market order.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            quantity: Order quantity
            strategy: Strategy that generated this order
            reason: Reason for placing the order
            priority: High priority order
            
        Returns:
            Order object if successful, None otherwise
        """
        return self.place_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            validity=self.default_validity,
            strategy=strategy,
            reason=reason,
            priority=priority
        )
    
    def place_limit_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                         price: float, strategy: Optional[StrategyName] = None,
                         reason: str = "", priority: bool = False) -> Optional[Order]:
        """
        Place a limit order.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            quantity: Order quantity
            price: Limit price
            strategy: Strategy that generated this order
            reason: Reason for placing the order
            priority: High priority order
            
        Returns:
            Order object if successful, None otherwise
        """
        return self.place_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            validity=self.default_validity,
            strategy=strategy,
            reason=reason,
            priority=priority
        )
    
    def place_sl_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                      trigger_price: float, strategy: Optional[StrategyName] = None,
                      reason: str = "", priority: bool = False) -> Optional[Order]:
        """
        Place a stop loss order.
        
        Args:
            symbol: Trading symbol
            side: BUY or SELL
            quantity: Order quantity
            trigger_price: Trigger price
            strategy: Strategy that generated this order
            reason: Reason for placing the order
            priority: High priority order
            
        Returns:
            Order object if successful, None otherwise
        """
        return self.place_order(
            symbol=symbol,
            side=side,
            order_type=self._get_sl_order_type(),
            quantity=quantity,
            trigger_price=trigger_price,
            validity=OrderValidity.MIS,
            strategy=strategy,
            reason=reason,
            priority=priority
        )
    
    # =============================================================================
    # ORDER MANAGEMENT
    # =============================================================================
    
    def cancel_order(self, order_id: OrderId) -> bool:
        """Cancel an order."""
        try:
            if not self.broker:
                return False
            
            with self._order_lock:
                order = self._orders.get(order_id)
                if not order:
                    return False
                
                broker_order_id = order.broker_order_id
            
            if not broker_order_id:
                # Order was never placed with broker
                with self._order_lock:
                    if order_id in self._pending_orders:
                        del self._pending_orders[order_id]
                    if order_id in self._orders:
                        self._orders[order_id].status = OrderStatus.CANCELLED
                return True
            
            # Cancel with broker
            success = self.broker.cancel_order(broker_order_id)
            
            if success:
                with self._order_lock:
                    if order_id in self._pending_orders:
                        del self._pending_orders[order_id]
                    if order_id in self._orders:
                        self._orders[order_id].status = OrderStatus.CANCELLED
                
                # Update bracket orders
                if order_id in self._bracket_orders:
                    bracket = self._bracket_orders[order_id]
                    if order_id == bracket.entry_order.order_id:
                        bracket.entry_order.status = OrderStatus.CANCELLED
                    elif order_id == bracket.sl_order.order_id:
                        bracket.sl_order.status = OrderStatus.CANCELLED
                    elif bracket.tp_order and order_id == bracket.tp_order.order_id:
                        bracket.tp_order.status = OrderStatus.CANCELLED
                
                self._notify_order_cancelled(order_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    def cancel_all_orders(self) -> int:
        """Cancel all open orders."""
        count = 0
        
        with self._order_lock:
            order_ids = list(self._pending_orders.keys())
        
        for order_id in order_ids:
            if self.cancel_order(order_id):
                count += 1
        
        self.logger.info(f"Cancelled {count} orders")
        return count
    
    def modify_order(self, order_id: OrderId, quantity: Optional[float] = None,
                    price: Optional[float] = None, trigger_price: Optional[float] = None) -> bool:
        """Modify an existing order."""
        try:
            if not self.broker:
                return False
            
            with self._order_lock:
                order = self._orders.get(order_id)
                if not order:
                    return False
                
                broker_order_id = order.broker_order_id
            
            if not broker_order_id:
                return False
            
            # Modify with broker
            success = self.broker.modify_order(
                order_id=broker_order_id,
                quantity=quantity,
                price=price,
                trigger_price=trigger_price
            )
            
            if success:
                with self._order_lock:
                    if quantity is not None:
                        self._orders[order_id].quantity = quantity
                    if price is not None:
                        self._orders[order_id].price = price
                    if trigger_price is not None:
                        self._orders[order_id].trigger_price = trigger_price
                
                self._notify_order_modified(order_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error modifying order {order_id}: {e}")
            return False
    
    # =============================================================================
    # PARTIAL FILL HANDLING
    # =============================================================================
    
    def update_order_fill(self, order_id: OrderId, filled_quantity: float, 
                         fill_price: float, remaining_quantity: float) -> None:
        """Update order with partial fill information."""
        with self._order_lock:
            order = self._orders.get(order_id)
            if not order:
                return
            
            # Update fill information
            order.filled_quantity += filled_quantity
            if order.average_fill_price is None:
                order.average_fill_price = fill_price
            else:
                # Weighted average
                total_filled = order.filled_quantity
                order.average_fill_price = (
                    (order.average_fill_price * (total_filled - filled_quantity)) + 
                    (fill_price * filled_quantity)
                ) / total_filled
            
            # Update remaining quantity
            order.quantity = remaining_quantity + order.filled_quantity
            
            # Check if order is completed
            if order.is_completed:
                order.status = OrderStatus.COMPLETED
                if order_id in self._pending_orders:
                    del self._pending_orders[order_id]
                self._completed_orders[order_id] = order
                
                # Notify portfolio tracker
                portfolio_tracker = get_portfolio_tracker()
                portfolio_tracker.on_order_filled(order, fill_price, filled_quantity)
                
                # Notify risk manager
                risk_manager = get_risk_manager()
                risk_manager.on_order_filled(order_id, filled_quantity, fill_price)
            
            # Update bracket orders if this is part of a bracket
            if order_id in self._bracket_orders:
                bracket = self._bracket_orders[order_id]
                if order_id == bracket.entry_order.order_id:
                    bracket.entry_order = order
                elif order_id == bracket.sl_order.order_id:
                    bracket.sl_order = order
                elif bracket.tp_order and order_id == bracket.tp_order.order_id:
                    bracket.tp_order = order
        
        self._notify_order_update(order_id, order.status)
    
    def adjust_sl_quantity(self, bracket_order: BracketOrder, filled_quantity: float) -> bool:
        """Adjust stop loss quantity for partial fills."""
        try:
            # Only adjust SL if entry order has partial fill
            entry_order = bracket_order.entry_order
            if not entry_order.is_completed and entry_order.filled_quantity > 0:
                # Calculate remaining quantity
                remaining_qty = entry_order.quantity - entry_order.filled_quantity
                
                # Modify SL order quantity
                success = self.modify_order(
                    order_id=bracket_order.sl_order.order_id,
                    quantity=remaining_qty
                )
                
                if success:
                    # Also modify TP order if it exists
                    if bracket_order.tp_order:
                        self.modify_order(
                            order_id=bracket_order.tp_order.order_id,
                            quantity=remaining_qty
                        )
                    
                    self.logger.info(f"Adjusted SL quantity to {remaining_qty} for partial fill")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error adjusting SL quantity: {e}")
            return False
    
    # =============================================================================
    # ORDER QUERY
    # =============================================================================
    
    def get_order(self, order_id: OrderId) -> Optional[Order]:
        """Get an order by ID."""
        with self._order_lock:
            return self._orders.get(order_id)
    
    def get_orders_by_symbol(self, symbol: Symbol) -> List[Order]:
        """Get all orders for a symbol."""
        with self._order_lock:
            return [order for order in self._orders.values() if order.symbol == symbol]
    
    def get_orders_by_strategy(self, strategy: StrategyName) -> List[Order]:
        """Get all orders for a strategy."""
        with self._order_lock:
            return [order for order in self._orders.values() if order.strategy == strategy]
    
    def get_pending_orders(self) -> List[Order]:
        """Get all pending orders."""
        with self._order_lock:
            return list(self._pending_orders.values())
    
    def get_completed_orders(self) -> List[Order]:
        """Get all completed orders."""
        with self._order_lock:
            return list(self._completed_orders.values())
    
    def get_failed_orders(self) -> List[Order]:
        """Get all failed orders."""
        with self._order_lock:
            return list(self._failed_orders.values())
    
    def get_bracket_order(self, order_id: OrderId) -> Optional[BracketOrder]:
        """Get a bracket order by any order ID."""
        with self._order_lock:
            return self._bracket_orders.get(order_id)
    
    # =============================================================================
    # TIMEOUT HANDLING
    # =============================================================================
    
    def check_order_timeouts(self) -> None:
        """Check for orders that have timed out."""
        with self._order_lock:
            current_time = datetime.utcnow()
            
            for order_id, order in list(self._pending_orders.items()):
                # Only check LIMIT orders (MARKET orders should execute immediately)
                if order.order_type == OrderType.LIMIT:
                    time_elapsed = (current_time - order.timestamp).total_seconds()
                    
                    if time_elapsed > self.fallback_timeout and self.use_fallback_to_market:
                        self.logger.warning(f"Order {order_id} timed out after {time_elapsed:.1f}s - converting to MARKET")
                        
                        # Cancel the LIMIT order
                        self.cancel_order(order_id)
                        
                        # Place a MARKET order instead
                        self.place_market_order(
                            symbol=order.symbol,
                            side=order.order_side,
                            quantity=order.quantity - order.filled_quantity,
                            strategy=order.strategy,
                            reason=f"Fallback to MARKET from timed out LIMIT order {order_id}",
                            priority=True
                        )
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def _generate_order_id(self) -> OrderId:
        """Generate a unique order ID."""
        return f"ORD-{datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')}-{uuid.uuid4().hex[:8]}"
    
    def _generate_idempotency_key(self, symbol: Symbol, side: OrderSide, 
                                  quantity: float, price: Optional[float] = None) -> str:
        """Generate an idempotency key for an order."""
        key_data = f"{symbol}-{side.value}-{quantity}"
        if price is not None:
            key_data += f"-{price}"
        
        # Add timestamp for uniqueness
        key_data += f"-{datetime.utcnow().timestamp()}"
        
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_duplicate_order(self, order_id: OrderId, symbol: Symbol, 
                           side: OrderSide, quantity: float) -> bool:
        """Check if an order is a duplicate."""
        with self._idempotency_lock:
            # Check for existing orders with same symbol, side, and similar quantity
            for existing_order_id, existing_order in self._orders.items():
                if (existing_order.symbol == symbol and 
                    existing_order.order_side == side and
                    abs(existing_order.quantity - quantity) < (quantity * 0.01) and  # Within 1%
                    existing_order.status == OrderStatus.PENDING):
                    return True
        
        return False
    
    def get_order_stats(self) -> Dict[str, Any]:
        """Get statistics about orders."""
        with self._order_lock:
            total_orders = len(self._orders)
            pending = len(self._pending_orders)
            completed = len(self._completed_orders)
            failed = len(self._failed_orders)
            bracket_orders = len(self._bracket_orders)
        
        return {
            'total_orders': total_orders,
            'pending_orders': pending,
            'completed_orders': completed,
            'failed_orders': failed,
            'bracket_orders': bracket_orders,
            'max_retries': self.max_retries,
            'use_fallback_to_market': self.use_fallback_to_market,
            'fallback_timeout': self.fallback_timeout
        }
    
    # =============================================================================
    # NOTIFICATIONS
    # =============================================================================
    
    def subscribe(self, callback: Callable) -> None:
        """Subscribe to order events."""
        self.subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from order events."""
        if callback in self.subscribers:
            self.subscribers.remove(callback)
    
    def _notify_order_placed(self, order: Order) -> None:
        """Notify subscribers that an order was placed."""
        for subscriber in self.subscribers:
            try:
                subscriber('order_placed', order)
            except Exception as e:
                self.logger.error(f"Error notifying order placed: {e}")
    
    def _notify_order_update(self, order_id: OrderId, status: OrderStatus) -> None:
        """Notify subscribers of order status update."""
        for subscriber in self.subscribers:
            try:
                subscriber('order_update', {'order_id': order_id, 'status': status.value})
            except Exception as e:
                self.logger.error(f"Error notifying order update: {e}")
    
    def _notify_order_failure(self, order_id: OrderId, reason: str) -> None:
        """Notify subscribers of order failure."""
        for subscriber in self.subscribers:
            try:
                subscriber('order_failed', {'order_id': order_id, 'reason': reason})
            except Exception as e:
                self.logger.error(f"Error notifying order failure: {e}")
    
    def _notify_order_cancelled(self, order_id: OrderId) -> None:
        """Notify subscribers of order cancellation."""
        for subscriber in self.subscribers:
            try:
                subscriber('order_cancelled', {'order_id': order_id})
            except Exception as e:
                self.logger.error(f"Error notifying order cancelled: {e}")
    
    def _notify_order_modified(self, order_id: OrderId) -> None:
        """Notify subscribers of order modification."""
        for subscriber in self.subscribers:
            try:
                subscriber('order_modified', {'order_id': order_id})
            except Exception as e:
                self.logger.error(f"Error notifying order modified: {e}")


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_order_manager: Optional[OrderManager] = None


def get_order_manager() -> OrderManager:
    """Get the singleton OrderManager instance."""
    global _order_manager
    if _order_manager is None:
        _order_manager = OrderManager()
    return _order_manager


def start_order_manager() -> OrderManager:
    """Start the order manager and return the instance."""
    global _order_manager
    _order_manager = OrderManager()
    _order_manager.start()
    return _order_manager


def stop_order_manager() -> None:
    """Stop the order manager."""
    global _order_manager
    if _order_manager:
        _order_manager.stop()
        _order_manager = None
