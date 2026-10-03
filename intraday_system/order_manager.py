"""
Order Manager for the Rapid Fire Intraday Trading System.

Responsibilities:
- Place MARKET / LIMIT / SL / SL-M orders with retries and exponential backoff
- Track every order's lifecycle and reconcile fills from the broker
- Fall back from an unfilled LIMIT entry to MARKET after a timeout
- Idempotency keys so a retried request can never create a duplicate order
- Publish order events to subscribers (the execution engine)

Broker calls are coroutines; they run on a dedicated event-loop thread owned by
the order manager, so the public API is synchronous and safe to call from any
thread (data-feed callbacks, monitor threads, signal handlers).

Events published to subscribers ``callback(event_type, data)``:
    order_placed     Order
    order_update     {'order_id', 'status'}          (fills, cancels, rejects)
    order_failed     {'order_id', 'reason'}
    order_cancelled  {'order_id'}
    order_modified   {'order_id'}
    order_replaced   {'old_order_id', 'new_order_id'} (LIMIT -> MARKET fallback)
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .brokers.base_broker import BaseBroker, BrokerError
from .brokers.broker_factory import get_broker
from .config_loader import get_config_loader
from .logger import order_logger
from .models import (
    BracketOrder, Order, OrderId, OrderResponse, OrderSide, OrderStatus,
    OrderType, OrderValidity, StrategyName, Symbol
)

TERMINAL_STATUSES = (OrderStatus.COMPLETED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


class OrderManager:
    """Manages order placement, tracking and broker reconciliation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or get_config_loader().load()
        self.logger = order_logger()

        execution = self.config.get('execution', {}) or {}
        retries = execution.get('retries', {}) or {}
        entry = execution.get('entry', {}) or {}
        exits = execution.get('exits', {}) or {}

        self.max_retries = int(retries.get('max_retries', 3)) if retries.get('enabled', True) else 0
        self.base_retry_delay = retries.get('base_delay_ms', 100) / 1000.0
        self.max_retry_delay = retries.get('max_delay_ms', 5000) / 1000.0
        self.exponential_backoff = bool(retries.get('exponential_backoff', True))
        self.retryable_errors = [e.upper() for e in (retries.get('retryable_errors') or [
            "TIMEOUT", "NETWORK_ERROR", "RATE_LIMIT", "TEMPORARY_ERROR", "429", "500", "502", "503", "504"])]
        self.fallback_timeout = float(entry.get('fallback_timeout_seconds', 2))
        self.use_fallback_to_market = bool(entry.get('use_fallback_to_market', True))
        self.use_bracket_orders = bool(exits.get('use_bracket_orders', True))
        self.broker_timeout = float(execution.get('broker_timeout_seconds', 10))
        self.poll_interval = float(execution.get('order_poll_interval_seconds', 0.5))

        broker_name = str(self.config.get('broker', {}).get('active', 'PAPER')).upper()
        self.broker_name = broker_name
        self.default_validity = OrderValidity.MIS if broker_name in ('ZERODHA', 'PAPER') else OrderValidity.DAY

        # Order storage
        self._orders: Dict[OrderId, Order] = {}
        self._pending_orders: Dict[OrderId, Order] = {}
        self._completed_orders: Dict[OrderId, Order] = {}
        self._failed_orders: Dict[OrderId, Order] = {}
        self._bracket_orders: Dict[OrderId, BracketOrder] = {}
        self._fallback_eligible: Dict[OrderId, float] = {}   # order_id -> placed monotonic time
        # old order_id -> replacement order_id (None while the replacement is being placed)
        self._replacements: Dict[OrderId, Optional[OrderId]] = {}
        self._idempotency_keys: Dict[str, OrderId] = {}
        self._order_lock = threading.RLock()

        self.subscribers: List[Callable] = []

        # Broker event loop
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever,
                                             name="OrderManager-BrokerLoop", daemon=True)
        self._loop_thread.start()

        self.broker: Optional[BaseBroker] = None
        self._init_broker()

        self._order_thread: Optional[threading.Thread] = None
        self._running = False

        self.logger.info(f"OrderManager initialized with broker: {type(self.broker).__name__}")

    # =========================================================================
    # BROKER PLUMBING
    # =========================================================================

    def _call(self, coro, timeout: Optional[float] = None) -> Any:
        """Run a broker coroutine on the broker loop and wait for the result."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout or self.broker_timeout)

    def _init_broker(self) -> None:
        broker_section = dict(self.config.get('broker', {}) or {})
        try:
            self.broker = get_broker(self.broker_name, broker_section)
        except BrokerError as e:
            if self.broker_name != 'PAPER':
                # Never silently trade a different account type
                raise
            self.logger.error(f"Failed to create paper broker: {e}")
            raise

        # Paper fills follow the live data feed's prices
        if hasattr(self.broker, 'price_provider'):
            from .data_feed import get_data_feed
            data_feed = get_data_feed()

            def last_price(symbol: Symbol) -> Optional[float]:
                tick = data_feed.cache.get_current_tick(symbol)
                return tick.last_price if tick else None

            self.broker.price_provider = last_price

        if not self._call(self.broker.connect(), timeout=60):
            raise BrokerError(f"Could not connect to broker {self.broker_name}", broker=self.broker_name)
        self.logger.info(f"Connected to broker: {self.broker_name}")

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._order_thread = threading.Thread(target=self._monitor_orders,
                                              name="OrderManager-Thread", daemon=True)
        self._order_thread.start()
        self.logger.info("OrderManager started")

    def stop(self) -> None:
        self._running = False
        if self._order_thread:
            self._order_thread.join(timeout=10)
            self._order_thread = None
        if self.broker:
            try:
                self._call(self.broker.disconnect())
            except Exception as e:
                self.logger.error(f"Error disconnecting broker: {e}")
        self.logger.info("OrderManager stopped")

    def shutdown(self) -> None:
        """Stop everything including the broker event loop thread."""
        self.stop()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=5)

    def _monitor_orders(self) -> None:
        while self._running:
            try:
                self.poll_orders()
                self.check_order_timeouts()
            except Exception as e:
                self.logger.error(f"Error in order monitoring: {e}")
            time.sleep(self.poll_interval)

    # =========================================================================
    # ORDER PLACEMENT
    # =========================================================================

    def place_order(self, symbol: Symbol, side: OrderSide, order_type: OrderType,
                    quantity: float, price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    validity: Optional[OrderValidity] = None,
                    strategy: Optional[StrategyName] = None,
                    parent_order_id: Optional[OrderId] = None,
                    reason: str = "", priority: bool = False,
                    fallback_to_market: bool = False) -> Optional[Order]:
        """
        Place an order and return it, or None if it was rejected.

        ``priority`` orders (exits, emergency square-off) skip the kill-switch
        gate so that risk-reducing orders can always be sent.
        """
        quantity = float(int(quantity))
        if quantity <= 0:
            self.logger.error(f"Refusing order with non-positive quantity for {symbol}")
            return None

        if not priority:
            from .risk_manager import get_risk_manager
            if get_risk_manager().get_risk_state().kill_switch_activated:
                self.logger.warning(f"Order for {symbol} blocked: kill switch active")
                return None

        order = Order(
            order_id=self._generate_order_id(),
            symbol=symbol,
            order_side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            status=OrderStatus.PENDING,
            timestamp=datetime.utcnow(),
            validity=validity or self.default_validity,
            strategy=strategy,
            parent_order_id=parent_order_id,
            idempotency_key=self._generate_idempotency_key(symbol, side, quantity, price),
        )
        with self._order_lock:
            self._orders[order.order_id] = order
            self._pending_orders[order.order_id] = order
            self._idempotency_keys[order.idempotency_key] = order.order_id

        self.logger.log_order('order_manager', order.to_dict())
        response = self._submit_with_retries(order)

        if not response.success:
            self._handle_order_failure(order.order_id, response.error_message or "Order rejected")
            return None

        with self._order_lock:
            order.broker_order_id = response.broker_order_id
            order.status = OrderStatus.PLACED
            if fallback_to_market and order_type == OrderType.LIMIT and self.use_fallback_to_market:
                self._fallback_eligible[order.order_id] = time.monotonic()

        self.logger.info(f"Order placed: {order.order_id} -> {response.broker_order_id} "
                         f"{side.value} {quantity:g} {symbol} {order_type.value} ({reason})")
        self._notify('order_placed', order)

        # Market orders usually fill immediately; reconcile right away
        self.refresh_order(order.order_id)
        return order

    def _submit_with_retries(self, order: Order) -> OrderResponse:
        last_error = "Unknown error"
        for attempt in range(self.max_retries + 1):
            try:
                response = self._call(self.broker.place_order(order))
                if response.success:
                    return response
                last_error = response.error_message or str(response.error_code)
                retryable = (response.error_code or "").upper() in self.retryable_errors
            except BrokerError as e:
                last_error, retryable = str(e), e.retryable
            except Exception as e:
                last_error, retryable = f"{type(e).__name__}: {e}", True

            if not retryable or attempt >= self.max_retries:
                break
            delay = self._calculate_retry_delay(attempt)
            self.logger.warning(f"Order {order.order_id} attempt {attempt + 1} failed ({last_error}); "
                                f"retrying in {delay:.2f}s")
            time.sleep(delay)
        return OrderResponse(success=False, order_id=order.order_id, error_message=last_error)

    def _calculate_retry_delay(self, attempt: int) -> float:
        if not self.exponential_backoff:
            return self.base_retry_delay
        return min(self.base_retry_delay * (2 ** attempt), self.max_retry_delay)

    def _handle_order_failure(self, order_id: OrderId, reason: str) -> None:
        with self._order_lock:
            order = self._orders.get(order_id)
            if order:
                order.status = OrderStatus.REJECTED
                self._pending_orders.pop(order_id, None)
                self._failed_orders[order_id] = order
        try:
            from .risk_manager import get_risk_manager
            get_risk_manager().on_order_failed(order_id, reason)
        except Exception as e:
            self.logger.error(f"Risk manager notification failed: {e}")
        self.logger.error(f"Order failed: {order_id} - {reason}")
        self._notify('order_failed', {'order_id': order_id, 'reason': reason})

    def place_market_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                           strategy: Optional[StrategyName] = None, reason: str = "",
                           priority: bool = False,
                           parent_order_id: Optional[OrderId] = None) -> Optional[Order]:
        return self.place_order(symbol, side, OrderType.MARKET, quantity, strategy=strategy,
                                parent_order_id=parent_order_id, reason=reason, priority=priority)

    def place_limit_order(self, symbol: Symbol, side: OrderSide, quantity: float, price: float,
                          strategy: Optional[StrategyName] = None, reason: str = "",
                          priority: bool = False, parent_order_id: Optional[OrderId] = None,
                          fallback_to_market: bool = False) -> Optional[Order]:
        return self.place_order(symbol, side, OrderType.LIMIT, quantity, price=price,
                                strategy=strategy, parent_order_id=parent_order_id, reason=reason,
                                priority=priority, fallback_to_market=fallback_to_market)

    def place_sl_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                       trigger_price: float, strategy: Optional[StrategyName] = None,
                       reason: str = "", priority: bool = True,
                       parent_order_id: Optional[OrderId] = None) -> Optional[Order]:
        return self.place_order(symbol, side, OrderType.SL_M, quantity, trigger_price=trigger_price,
                                strategy=strategy, parent_order_id=parent_order_id, reason=reason,
                                priority=priority)

    def place_bracket_order(self, symbol: Symbol, side: OrderSide, quantity: float,
                            entry_price: float, stop_loss: float, take_profit: float,
                            strategy: Optional[StrategyName] = None,
                            reason: str = "") -> Optional[BracketOrder]:
        """
        Entry plus protective SL and TP orders. SL/TP are placed only after the
        entry is accepted; if either fails, everything is cancelled.
        """
        entry = self.place_limit_order(symbol, side, quantity, entry_price, strategy, reason)
        if not entry:
            return None
        exit_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
        sl = self.place_sl_order(symbol, exit_side, quantity, stop_loss, strategy,
                                 f"SL for {entry.order_id}", parent_order_id=entry.order_id)
        tp = self.place_limit_order(symbol, exit_side, quantity, take_profit, strategy,
                                    f"TP for {entry.order_id}", priority=True,
                                    parent_order_id=entry.order_id) if sl else None
        if not sl or not tp:
            for o in (entry, sl, tp):
                if o:
                    self.cancel_order(o.order_id)
            return None
        bracket = BracketOrder(entry_order=entry, sl_order=sl, tp_order=tp)
        with self._order_lock:
            for o in (entry, sl, tp):
                self._bracket_orders[o.order_id] = bracket
        return bracket

    # =========================================================================
    # RECONCILIATION
    # =========================================================================

    def refresh_order(self, order_id: OrderId) -> Optional[Order]:
        """Pull the broker's view of an order and publish any status change."""
        with self._order_lock:
            order = self._orders.get(order_id)
            if order is None or not order.broker_order_id or order.status in TERMINAL_STATUSES:
                return order
            broker_order_id = order.broker_order_id

        try:
            broker_view = self._call(self.broker.get_order(broker_order_id))
            status = broker_view.status if broker_view else self._call(
                self.broker.get_order_status(broker_order_id))
        except Exception as e:
            self.logger.error(f"Could not refresh order {order_id}: {e}")
            return order

        changed = False
        with self._order_lock:
            if broker_view is not None:
                if broker_view.filled_quantity != order.filled_quantity:
                    order.filled_quantity = broker_view.filled_quantity
                    changed = True
                if broker_view.average_fill_price:
                    order.average_fill_price = broker_view.average_fill_price
            if status and status != order.status:
                order.status = status
                changed = True
            if order.status in TERMINAL_STATUSES:
                self._pending_orders.pop(order_id, None)
                self._fallback_eligible.pop(order_id, None)
                target = self._completed_orders if order.status == OrderStatus.COMPLETED else self._failed_orders
                target[order_id] = order

        if changed:
            if order.status == OrderStatus.COMPLETED:
                try:
                    from .risk_manager import get_risk_manager
                    get_risk_manager().on_order_filled(order_id, order.filled_quantity,
                                                       order.average_fill_price or 0.0)
                except Exception as e:
                    self.logger.error(f"Risk manager notification failed: {e}")
                self.logger.info(f"Order filled: {order_id} {order.order_side.value} "
                                 f"{order.filled_quantity:g} {order.symbol} @ {order.average_fill_price:.2f}")
            self._notify('order_update', {'order_id': order_id, 'status': order.status.value})
        return order

    def poll_orders(self) -> None:
        with self._order_lock:
            pending = list(self._pending_orders.keys())
        for order_id in pending:
            self.refresh_order(order_id)

    def check_order_timeouts(self) -> None:
        """Convert LIMIT entries that did not fill in time into MARKET orders."""
        now = time.monotonic()
        with self._order_lock:
            expired = [oid for oid, placed in self._fallback_eligible.items()
                       if now - placed > self.fallback_timeout]
        for order_id in expired:
            with self._order_lock:
                self._fallback_eligible.pop(order_id, None)
                order = self._orders.get(order_id)
            if order is None or order.status in TERMINAL_STATUSES:
                continue
            # Announce the replacement *before* cancelling so that listeners
            # seeing the cancellation know a successor order is coming.
            with self._order_lock:
                self._replacements[order_id] = None
            if not self.cancel_order(order_id):
                with self._order_lock:
                    self._replacements.pop(order_id, None)
                continue  # Probably filled in the meantime
            remaining = order.quantity - order.filled_quantity
            replacement = None
            if remaining > 0:
                self.logger.warning(f"LIMIT order {order_id} unfilled after {self.fallback_timeout}s - "
                                    f"falling back to MARKET")
                replacement = self.place_market_order(order.symbol, order.order_side, remaining,
                                                      order.strategy, f"Fallback for {order_id}",
                                                      priority=True, parent_order_id=order.parent_order_id)
            with self._order_lock:
                if replacement:
                    self._replacements[order_id] = replacement.order_id
                else:
                    self._replacements.pop(order_id, None)
            if replacement:
                self._notify('order_replaced', {'old_order_id': order_id,
                                                'new_order_id': replacement.order_id})

    # =========================================================================
    # CANCEL / MODIFY
    # =========================================================================

    def cancel_order(self, order_id: OrderId) -> bool:
        with self._order_lock:
            order = self._orders.get(order_id)
            if order is None or order.status in TERMINAL_STATUSES:
                return False
            broker_order_id = order.broker_order_id

        success = True
        if broker_order_id:
            try:
                success = bool(self._call(self.broker.cancel_order(broker_order_id)))
            except Exception as e:
                self.logger.error(f"Error cancelling order {order_id}: {e}")
                success = False
        if not success:
            # Maybe it filled; make sure our view is current
            self.refresh_order(order_id)
            return False

        with self._order_lock:
            order.status = OrderStatus.CANCELLED
            self._pending_orders.pop(order_id, None)
            self._fallback_eligible.pop(order_id, None)
            self._failed_orders[order_id] = order
        self._notify('order_cancelled', {'order_id': order_id})
        return True

    def cancel_all_orders(self) -> int:
        with self._order_lock:
            order_ids = list(self._pending_orders.keys())
        count = sum(1 for oid in order_ids if self.cancel_order(oid))
        self.logger.info(f"Cancelled {count} orders")
        return count

    def modify_order(self, order_id: OrderId, quantity: Optional[float] = None,
                     price: Optional[float] = None, trigger_price: Optional[float] = None) -> bool:
        with self._order_lock:
            order = self._orders.get(order_id)
            if order is None or not order.broker_order_id or order.status in TERMINAL_STATUSES:
                return False
            broker_order_id = order.broker_order_id
        try:
            response = self._call(self.broker.modify_order(broker_order_id, quantity=quantity,
                                                           price=price, trigger_price=trigger_price))
            success = response.success if isinstance(response, OrderResponse) else bool(response)
        except Exception as e:
            self.logger.error(f"Error modifying order {order_id}: {e}")
            return False
        if success:
            with self._order_lock:
                if quantity is not None:
                    order.quantity = quantity
                if price is not None:
                    order.price = price
                if trigger_price is not None:
                    order.trigger_price = trigger_price
            self._notify('order_modified', {'order_id': order_id})
            self.refresh_order(order_id)
        return success

    # =========================================================================
    # QUERIES
    # =========================================================================

    def replacement_status(self, order_id: OrderId) -> Tuple[bool, Optional[OrderId]]:
        """
        (is_being_or_was_replaced, replacement_order_id) for an order cancelled
        by the LIMIT -> MARKET fallback. The id is None while still in progress.
        """
        with self._order_lock:
            if order_id not in self._replacements:
                return False, None
            return True, self._replacements[order_id]
    
    def get_order(self, order_id: OrderId) -> Optional[Order]:
        with self._order_lock:
            return self._orders.get(order_id)

    def get_orders_by_symbol(self, symbol: Symbol) -> List[Order]:
        with self._order_lock:
            return [o for o in self._orders.values() if o.symbol == symbol]

    def get_orders_by_strategy(self, strategy: StrategyName) -> List[Order]:
        with self._order_lock:
            return [o for o in self._orders.values() if o.strategy == strategy]

    def get_pending_orders(self) -> List[Order]:
        with self._order_lock:
            return list(self._pending_orders.values())

    def get_completed_orders(self) -> List[Order]:
        with self._order_lock:
            return list(self._completed_orders.values())

    def get_failed_orders(self) -> List[Order]:
        with self._order_lock:
            return list(self._failed_orders.values())

    def get_bracket_order(self, order_id: OrderId) -> Optional[BracketOrder]:
        with self._order_lock:
            return self._bracket_orders.get(order_id)

    def get_order_stats(self) -> Dict[str, Any]:
        with self._order_lock:
            return {
                'total_orders': len(self._orders),
                'pending_orders': len(self._pending_orders),
                'completed_orders': len(self._completed_orders),
                'failed_orders': len(self._failed_orders),
                'bracket_orders': len(self._bracket_orders),
                'max_retries': self.max_retries,
                'use_fallback_to_market': self.use_fallback_to_market,
                'fallback_timeout': self.fallback_timeout,
            }

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def _generate_order_id(self) -> OrderId:
        return f"ORD-{datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')}-{uuid.uuid4().hex[:8]}"

    def _generate_idempotency_key(self, symbol: Symbol, side: OrderSide,
                                  quantity: float, price: Optional[float] = None) -> str:
        key_data = f"{symbol}-{side.value}-{quantity}-{price}-{uuid.uuid4().hex}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    # =========================================================================
    # NOTIFICATIONS
    # =========================================================================

    def subscribe(self, callback: Callable) -> None:
        if callback not in self.subscribers:
            self.subscribers.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        if callback in self.subscribers:
            self.subscribers.remove(callback)

    def _notify(self, event_type: str, data: Any) -> None:
        for subscriber in list(self.subscribers):
            try:
                subscriber(event_type, data)
            except Exception as e:
                self.logger.error(f"Error notifying {event_type}: {e}")


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
    order_manager = get_order_manager()
    order_manager.start()
    return order_manager


def stop_order_manager() -> None:
    """Stop the order manager."""
    global _order_manager
    if _order_manager:
        _order_manager.shutdown()
        _order_manager = None
