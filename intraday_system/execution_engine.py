"""
Execution Engine for the Rapid Fire Intraday Trading System.

Turns aggregated signals into positions and manages each position until it is
flat again:

    signal -> risk checks -> size -> entry order
           -> (fill) protective SL + TP orders at the broker (OCO handled here)
           -> trailing stop / time stop / kill switch -> exit order
           -> (fill) trade record -> PortfolioTracker, RiskManager, cooldown

Each position is a small state machine (PENDING_ENTRY -> OPEN -> EXITING ->
closed). State transitions are driven by the *actual* order states held by the
OrderManager, and ``_sync`` is idempotent, so order events, the monitor thread
and synchronous fills can all safely trigger it.

Every exit sends a real order to the broker: resting SL/TP orders are
cancelled first and a MARKET order closes the position. If cancelling fails
because the bracket order already filled, that fill is used instead.
"""

from __future__ import annotations

import itertools
import threading
import time as time_module
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from statistics import median
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config_loader import get_config_loader, parse_time
from .costs import CostModel
from .data_feed import get_data_feed
from .logger import execution_logger
from .models import (
    AggregatedSignal, Order, OrderId, OrderSide, OrderStatus, OrderType, Position,
    PositionSide, SignalType, StrategyName, Symbol, TimeFrame, Trade, TradeStatus
)
from .order_manager import get_order_manager
from .portfolio_tracker import get_portfolio_tracker
from .risk_manager import _local_now, get_risk_manager
from .signal_aggregator import get_signal_aggregator

PENDING_ENTRY = "PENDING_ENTRY"
OPEN = "OPEN"
EXITING = "EXITING"
CLOSED = "CLOSED"

DEAD_STATUSES = (OrderStatus.CANCELLED, OrderStatus.REJECTED)


@dataclass
class ManagedPosition:
    """A position owned by the execution engine, with its orders."""
    position_id: str
    symbol: Symbol
    side: OrderSide                      # Entry side
    planned_quantity: int
    stop_loss: float
    take_profit: float
    strategy: Optional[StrategyName]
    confidence: float
    contributors: List[str]
    entry_order_ids: List[OrderId] = field(default_factory=list)
    state: str = PENDING_ENTRY
    quantity: float = 0.0
    entry_price: float = 0.0
    entry_time: Optional[datetime] = None
    initial_risk: float = 0.0
    best_price: float = 0.0
    sl_order_id: Optional[OrderId] = None
    tp_order_id: Optional[OrderId] = None
    exit_order_id: Optional[OrderId] = None
    exit_reason: str = ""
    exit_attempts: int = 0

    @property
    def is_long(self) -> bool:
        return self.side == OrderSide.BUY

    @property
    def exit_side(self) -> OrderSide:
        return OrderSide.SELL if self.is_long else OrderSide.BUY

    def to_position(self) -> Position:
        return Position(
            symbol=self.symbol,
            side=PositionSide.LONG if self.is_long else PositionSide.SHORT,
            quantity=self.quantity,
            entry_price=self.entry_price,
            entry_timestamp=self.entry_time or datetime.now(),
            strategy=self.strategy,
            stop_loss=self.stop_loss,
            take_profit=self.take_profit,
            order_id=self.entry_order_ids[0] if self.entry_order_ids else None,
        )


class ExecutionEngine:
    """Handles trade execution and the position lifecycle."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or get_config_loader().load()
        self.system_config = self.config
        self.logger = execution_logger()

        self.data_feed = get_data_feed()
        self.order_manager = get_order_manager()
        self.portfolio_tracker = get_portfolio_tracker()
        self.risk_manager = get_risk_manager()
        self.signal_aggregator = get_signal_aggregator()
        self.risk_params = get_config_loader().get_risk_parameters()
        self.costs = CostModel.from_config(self.config)
        self.costs.simulate_slippage = False  # Live fills already include slippage

        risk_cfg = self.config.get('risk_management', {}) or {}
        time_stop = risk_cfg.get('time_stop', {}) or {}
        execution = self.config.get('execution', {}) or {}
        self.square_off_enabled = bool(time_stop.get('enabled', True))
        self.enforce_market_hours = bool(time_stop.get('enforce_market_hours', True))
        self.square_off_time: time = parse_time(time_stop.get('square_off_time', '15:15'))
        self.pre_square_off_minutes = int(time_stop.get('pre_square_off_minutes', 5))
        self.no_entry_after: time = (datetime.combine(datetime.today(), self.square_off_time)
                                     - timedelta(minutes=self.pre_square_off_minutes)).time()
        self.timezone = (self.config.get('system', {}) or {}).get('timezone', 'Asia/Kolkata')
        self.use_bracket_orders = bool((execution.get('exits', {}) or {}).get('use_bracket_orders', True))
        self.entry_order_type = str((execution.get('entry', {}) or {}).get('order_type', 'MARKET')).upper()
        self.max_exit_attempts = 5

        self._positions: Dict[str, ManagedPosition] = {}
        self._order_index: Dict[OrderId, str] = {}   # order_id -> position_id
        self._lock = threading.RLock()
        self._ids = itertools.count(1)
        self._closed_trades: List[Trade] = []

        self.subscribers: List[Callable] = []
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        self._squared_off_today: Optional[datetime] = None

        self.logger.info("ExecutionEngine initialized")

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.order_manager.subscribe(self._on_order_update)
        self.signal_aggregator.subscribe(self.on_aggregated_signal)
        self._monitor_thread = threading.Thread(target=self._monitor_positions,
                                                name="ExecutionEngine-Thread", daemon=True)
        self._monitor_thread.start()
        self.logger.info("ExecutionEngine started")

    def stop(self, square_off: bool = True) -> None:
        """Stop the engine; by default flatten everything first (intraday system)."""
        self.signal_aggregator.unsubscribe(self.on_aggregated_signal)
        if square_off:
            self.square_off_all_positions("SYSTEM SHUTDOWN")
            deadline = time_module.monotonic() + 10
            while self.get_open_positions() and time_module.monotonic() < deadline:
                self.order_manager.poll_orders()
                self._sync_all()
                time_module.sleep(0.2)
            remaining = self.get_open_positions()
            if remaining:
                self.logger.critical(f"{len(remaining)} position(s) still open at shutdown: "
                                     f"{[p.symbol for p in remaining]}")
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None
        self.order_manager.unsubscribe(self._on_order_update)
        self.logger.info("ExecutionEngine stopped")

    def _monitor_positions(self) -> None:
        while self._running:
            try:
                self._sync_all()
                self._manage_open_positions()
                self._check_square_off_time()
            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
            time_module.sleep(1)

    # =========================================================================
    # ENTRY
    # =========================================================================

    def _now(self) -> datetime:
        return _local_now(self.timezone)

    def on_aggregated_signal(self, signal: AggregatedSignal) -> None:
        """Handle an aggregated signal from the signal aggregator."""
        if signal.final_signal not in (SignalType.BUY, SignalType.SELL):
            return
        self.logger.log_signal('execution_engine', signal.to_dict())
        try:
            with self._lock:
                if not self._can_enter(signal.symbol):
                    return
                self._enter(signal)
        except Exception as e:
            self.logger.error(f"Error executing signal for {signal.symbol}: {e}")

    def _can_enter(self, symbol: Symbol) -> bool:
        if not self._running:
            return False
        if any(mp.symbol == symbol for mp in self._positions.values()):
            self.logger.debug(f"Signal for {symbol} ignored - position already active")
            return False
        if len(self._positions) >= self.risk_params.max_concurrent_positions:
            self.logger.debug("Signal ignored - max concurrent positions reached")
            return False
        if self.enforce_market_hours and self._now().time() >= self.no_entry_after:
            self.logger.debug(f"Signal for {symbol} ignored - no new entries after {self.no_entry_after}")
            return False
        return True

    def _enter(self, signal: AggregatedSignal) -> None:
        symbol = signal.symbol
        side = OrderSide.BUY if signal.final_signal == SignalType.BUY else OrderSide.SELL
        price = self._get_current_price(symbol)
        if price is None:
            self.logger.error(f"Cannot enter {symbol} - no price data")
            return

        stop_loss, take_profit = self._calculate_sl_tp(signal, price, side)
        quantity = self.risk_manager.calculate_position_size(symbol, price, stop_loss)
        if quantity <= 0:
            self.logger.warning(f"Cannot enter {symbol} - position size is zero")
            return

        risk = self.risk_manager.check_trade_risk(symbol, side, quantity, price, stop_loss, take_profit)
        if not risk.passed:
            self.logger.warning(f"Entry for {symbol} blocked by risk check: {risk.reason}")
            return

        agreeing = [s for s in signal.contributing_strategies if s.signal == signal.final_signal]
        lead = max(agreeing or signal.contributing_strategies, key=lambda s: s.confidence)
        mp = ManagedPosition(
            position_id=f"POS-{datetime.utcnow():%Y%m%d}-{next(self._ids)}",
            symbol=symbol, side=side, planned_quantity=quantity,
            stop_loss=stop_loss, take_profit=take_profit,
            strategy=lead.strategy, confidence=signal.net_confidence,
            contributors=sorted(s.strategy.value for s in agreeing),
            initial_risk=abs(price - stop_loss), best_price=price,
        )
        self._positions[mp.position_id] = mp

        reason = f"Entry {side.value} from {', '.join(mp.contributors)}"
        if self.entry_order_type == 'LIMIT':
            order = self.order_manager.place_limit_order(symbol, side, quantity, round(price, 2),
                                                         mp.strategy, reason, fallback_to_market=True)
        else:
            order = self.order_manager.place_market_order(symbol, side, quantity, mp.strategy, reason)

        if order is None:
            self.logger.error(f"Entry order for {symbol} was rejected")
            del self._positions[mp.position_id]
            return

        self._track_order(mp, order.order_id)
        mp.entry_order_ids.append(order.order_id)
        self.logger.info(f"ENTRY ORDER: {side.value} {quantity} {symbol} ~{price:.2f} "
                         f"SL {stop_loss:.2f} TP {take_profit:.2f} [{', '.join(mp.contributors)}]")
        self._notify('entry_placed', {'position_id': mp.position_id, 'symbol': symbol,
                                      'side': side.value, 'quantity': quantity,
                                      'stop_loss': stop_loss, 'take_profit': take_profit})
        self._sync(mp)

    def _calculate_sl_tp(self, signal: AggregatedSignal, price: float,
                         side: OrderSide) -> Tuple[float, float]:
        """
        Stop: median of the agreeing strategies' suggestions when on the right
        side, capped at the hard stop and at least 0.1% away. Target: median
        suggestion if valid, else 2R. Mirrors the backtester.
        """
        direction = 1 if side == OrderSide.BUY else -1
        agreeing = [s for s in signal.contributing_strategies if s.signal == signal.final_signal]
        stops = [s.suggested_sl for s in agreeing
                 if s.suggested_sl and (s.suggested_sl - price) * direction < 0]
        targets = [s.suggested_tp for s in agreeing
                   if s.suggested_tp and (s.suggested_tp - price) * direction > 0]

        hard_distance = price * self.risk_params.hard_stop_loss_pct / 100
        distance = min(abs(price - median(stops)), hard_distance) if stops else hard_distance
        distance = max(distance, price * self.risk_params.min_stop_loss_pct / 100)
        stop_loss = round(price - direction * distance, 2)
        target = median(targets) if targets else price + direction * 2 * distance
        if abs(target - price) < distance:  # Never accept less than 1R
            target = price + direction * distance
        take_profit = round(target, 2)
        return stop_loss, take_profit

    # =========================================================================
    # ORDER EVENTS / STATE MACHINE
    # =========================================================================

    def _track_order(self, mp: ManagedPosition, order_id: OrderId) -> None:
        self._order_index[order_id] = mp.position_id

    def _on_order_update(self, event_type: str, data: Any) -> None:
        try:
            if event_type == 'order_replaced':
                with self._lock:
                    position_id = self._order_index.get(data['old_order_id'])
                    mp = self._positions.get(position_id) if position_id else None
                    if mp is None:
                        return
                    new_id = data['new_order_id']
                    self._track_order(mp, new_id)
                    if data['old_order_id'] in mp.entry_order_ids and new_id not in mp.entry_order_ids:
                        mp.entry_order_ids.append(new_id)
                    elif data['old_order_id'] == mp.exit_order_id:
                        mp.exit_order_id = new_id
                    self._sync(mp)
                return

            order_id = data.order_id if isinstance(data, Order) else data.get('order_id')
            with self._lock:
                position_id = self._order_index.get(order_id)
                mp = self._positions.get(position_id) if position_id else None
                if mp is not None:
                    self._sync(mp)
        except Exception as e:
            self.logger.error(f"Error handling order event {event_type}: {e}")

    def _sync_all(self) -> None:
        with self._lock:
            for mp in list(self._positions.values()):
                self._sync(mp)

    def _sync(self, mp: ManagedPosition) -> None:
        """Advance a position's state machine from its orders' real states."""
        if mp.state == PENDING_ENTRY:
            self._sync_entry(mp)
        if mp.state == OPEN:
            self._sync_brackets(mp)
        if mp.state == EXITING:
            self._sync_exit(mp)

    def _sync_entry(self, mp: ManagedPosition) -> None:
        # Follow LIMIT -> MARKET fallbacks so a cancelled entry is not mistaken for a dead one
        while True:
            replaced, new_id = self.order_manager.replacement_status(mp.entry_order_ids[-1])
            if not replaced:
                break
            if new_id is None:
                return  # Replacement is being placed; wait for it
            if new_id in mp.entry_order_ids:
                break
            mp.entry_order_ids.append(new_id)
            self._track_order(mp, new_id)
        
        orders = [self.order_manager.get_order(oid) for oid in mp.entry_order_ids]
        orders = [o for o in orders if o is not None]
        if not orders:
            return
        latest = orders[-1]
        if latest.status not in (OrderStatus.COMPLETED,) + DEAD_STATUSES:
            return  # Still working

        filled = sum(o.filled_quantity for o in orders)
        if filled <= 0:
            self.logger.warning(f"Entry for {mp.symbol} not filled ({latest.status.value}); dropping")
            self._forget(mp)
            return

        mp.quantity = filled
        mp.entry_price = sum(o.filled_quantity * (o.average_fill_price or 0) for o in orders) / filled
        mp.entry_time = datetime.now()
        mp.best_price = mp.entry_price
        # Keep the planned stop distance relative to the actual fill
        direction = 1 if mp.is_long else -1
        mp.stop_loss = round(mp.entry_price - direction * mp.initial_risk, 2)
        mp.state = OPEN

        self.portfolio_tracker.on_position_opened(mp.to_position())
        self.logger.info(f"ENTRY FILLED: {mp.side.value} {mp.quantity:g} {mp.symbol} @ {mp.entry_price:.2f}")
        self._notify('entry_filled', {'position_id': mp.position_id, 'symbol': mp.symbol,
                                      'quantity': mp.quantity, 'price': mp.entry_price})

        if self.use_bracket_orders:
            self._place_brackets(mp)

    def _place_brackets(self, mp: ManagedPosition) -> None:
        sl = self.order_manager.place_sl_order(mp.symbol, mp.exit_side, mp.quantity, mp.stop_loss,
                                               mp.strategy, f"SL {mp.position_id}",
                                               parent_order_id=mp.entry_order_ids[0])
        if sl is None:
            # Without a protective stop at the broker, exit immediately
            self.logger.critical(f"Could not place stop loss for {mp.symbol} - exiting position")
            self._exit(mp, "SL_PLACEMENT_FAILED")
            return
        mp.sl_order_id = sl.order_id
        self._track_order(mp, sl.order_id)

        tp = self.order_manager.place_limit_order(mp.symbol, mp.exit_side, mp.quantity, mp.take_profit,
                                                  mp.strategy, f"TP {mp.position_id}", priority=True,
                                                  parent_order_id=mp.entry_order_ids[0])
        if tp is not None:
            mp.tp_order_id = tp.order_id
            self._track_order(mp, tp.order_id)
        else:
            self.logger.warning(f"Could not place take-profit for {mp.symbol}; target managed in software")

    def _sync_brackets(self, mp: ManagedPosition) -> None:
        for order_id, reason, sibling in ((mp.sl_order_id, "STOP_LOSS", mp.tp_order_id),
                                          (mp.tp_order_id, "TAKE_PROFIT", mp.sl_order_id)):
            if not order_id:
                continue
            order = self.order_manager.get_order(order_id)
            if order is None:
                continue
            if order.status == OrderStatus.COMPLETED:
                # The filled bracket order is the exit; cancel its sibling (OCO)
                mp.state = EXITING
                mp.exit_reason = reason
                mp.exit_order_id = order_id
                if sibling:
                    self.order_manager.cancel_order(sibling)
                self._sync_exit(mp)
                return
            if order.status in DEAD_STATUSES:
                self.logger.warning(f"{reason} order for {mp.symbol} is {order.status.value}; "
                                    f"managing it in software")
                if order_id == mp.sl_order_id:
                    mp.sl_order_id = None
                else:
                    mp.tp_order_id = None

    def _sync_exit(self, mp: ManagedPosition) -> None:
        if not mp.exit_order_id:
            return
        order = self.order_manager.get_order(mp.exit_order_id)
        if order is None:
            return
        if order.status == OrderStatus.COMPLETED:
            self._finalize(mp, order.average_fill_price or self._get_current_price(mp.symbol) or mp.entry_price)
        elif order.status in DEAD_STATUSES:
            self.logger.critical(f"Exit order for {mp.symbol} {order.status.value}; retrying")
            mp.exit_order_id = None
            self._send_exit_order(mp)

    # =========================================================================
    # EXITS
    # =========================================================================

    def _exit(self, mp: ManagedPosition, reason: str) -> None:
        """Close an OPEN position: cancel brackets, then send a MARKET exit."""
        if mp.state == PENDING_ENTRY:
            # Cancel the working entry; anything already filled is exited when it resolves
            for oid in mp.entry_order_ids:
                self.order_manager.cancel_order(oid)
            self._sync_entry(mp)
            if mp.state != OPEN:
                return
        if mp.state != OPEN:
            return

        mp.state = EXITING
        mp.exit_reason = reason
        for oid in (mp.sl_order_id, mp.tp_order_id):
            if oid and not self.order_manager.cancel_order(oid):
                order = self.order_manager.get_order(oid)
                if order is not None and order.status == OrderStatus.COMPLETED:
                    # The bracket filled first; that is our exit
                    mp.exit_reason = "STOP_LOSS" if oid == mp.sl_order_id else "TAKE_PROFIT"
                    mp.exit_order_id = oid
                    other = mp.tp_order_id if oid == mp.sl_order_id else mp.sl_order_id
                    if other:
                        self.order_manager.cancel_order(other)
                    self._sync_exit(mp)
                    return
        self._send_exit_order(mp)

    def _send_exit_order(self, mp: ManagedPosition) -> None:
        if mp.exit_attempts >= self.max_exit_attempts:
            self.logger.critical(f"GAVE UP exiting {mp.symbol} after {mp.exit_attempts} attempts - "
                                 f"MANUAL INTERVENTION REQUIRED")
            return
        mp.exit_attempts += 1
        order = self.order_manager.place_market_order(mp.symbol, mp.exit_side, mp.quantity, mp.strategy,
                                                      f"Exit {mp.position_id}: {mp.exit_reason}",
                                                      priority=True,
                                                      parent_order_id=mp.entry_order_ids[0])
        if order is None:
            self.logger.critical(f"Exit order for {mp.symbol} rejected (attempt {mp.exit_attempts})")
            return
        mp.exit_order_id = order.order_id
        self._track_order(mp, order.order_id)
        self._sync_exit(mp)

    def _finalize(self, mp: ManagedPosition, exit_price: float) -> None:
        """Record the completed trade and release the position."""
        if mp.state == CLOSED:
            return
        mp.state = CLOSED
        direction = 1 if mp.is_long else -1
        gross = (exit_price - mp.entry_price) * mp.quantity * direction
        if mp.is_long:
            buy_value, sell_value = mp.entry_price * mp.quantity, exit_price * mp.quantity
        else:
            buy_value, sell_value = exit_price * mp.quantity, mp.entry_price * mp.quantity
        charges = self.costs.charges(buy_value, sell_value)
        status = {"STOP_LOSS": TradeStatus.STOPPED_OUT, "TIME_STOP": TradeStatus.TIMED_OUT}.get(
            mp.exit_reason, TradeStatus.CLOSED)

        trade = Trade(
            trade_id=mp.position_id.replace("POS", "TRD"), symbol=mp.symbol, side=mp.side,
            entry_price=mp.entry_price, exit_price=exit_price, quantity=mp.quantity,
            entry_timestamp=mp.entry_time or datetime.now(), exit_timestamp=datetime.now(),
            stop_loss=mp.stop_loss, take_profit=mp.take_profit, strategy=mp.strategy,
            confidence=mp.confidence, pnl=gross, status=status,
            tags=[mp.exit_reason] + mp.contributors, **charges)
        self._closed_trades.append(trade)
        self._forget(mp)

        self.portfolio_tracker.on_trade_closed(trade)
        self.risk_manager.on_trade_closed(trade, exit_price)
        self.signal_aggregator.start_cooldown(mp.symbol)
        self.logger.info(f"POSITION CLOSED: {mp.symbol} {mp.side.value} {mp.quantity:g} "
                         f"{mp.entry_price:.2f} -> {exit_price:.2f} net {trade.net_pnl:.2f} ({mp.exit_reason})")
        self._notify('position_closed', trade.to_dict())

    def _forget(self, mp: ManagedPosition) -> None:
        self._positions.pop(mp.position_id, None)

    # =========================================================================
    # POSITION MANAGEMENT
    # =========================================================================

    def _manage_open_positions(self) -> None:
        with self._lock:
            for mp in list(self._positions.values()):
                if mp.state != OPEN:
                    continue
                price = self._get_current_price(mp.symbol)
                if price is None:
                    continue

                # Software stop / target when no working broker order covers it
                if mp.sl_order_id is None and ((mp.is_long and price <= mp.stop_loss) or
                                               (not mp.is_long and price >= mp.stop_loss)):
                    self._exit(mp, "STOP_LOSS")
                    continue
                if mp.tp_order_id is None and ((mp.is_long and price >= mp.take_profit) or
                                               (not mp.is_long and price <= mp.take_profit)):
                    self._exit(mp, "TAKE_PROFIT")
                    continue

                self._update_trailing_stop(mp, price)

    def _update_trailing_stop(self, mp: ManagedPosition, price: float) -> None:
        if not self.risk_params.use_trailing_stop:
            return
        mp.best_price = max(mp.best_price, price) if mp.is_long else min(mp.best_price, price)
        favourable = (mp.best_price - mp.entry_price) if mp.is_long else (mp.entry_price - mp.best_price)
        if favourable < self.risk_params.trailing_activation_r * mp.initial_risk:
            return
        atr = self.data_feed.get_atr(mp.symbol, period=14)
        if not atr:
            return
        trail = atr * self.risk_params.trailing_stop_atr_multiplier
        new_stop = round(mp.best_price - trail if mp.is_long else mp.best_price + trail, 2)
        improves = new_stop > mp.stop_loss if mp.is_long else new_stop < mp.stop_loss
        if not improves:
            return
        if mp.sl_order_id and not self.order_manager.modify_order(mp.sl_order_id, trigger_price=new_stop):
            return
        self.logger.info(f"TRAILING STOP: {mp.symbol} {mp.stop_loss:.2f} -> {new_stop:.2f}")
        mp.stop_loss = new_stop

    def _check_square_off_time(self) -> None:
        if not (self.square_off_enabled and self.enforce_market_hours):
            return
        now = self._now()
        if now.time() >= self.square_off_time and self._positions:
            if self._squared_off_today != now.date():
                self.logger.warning(f"SQUARE OFF TIME REACHED ({self.square_off_time})")
            self._squared_off_today = now.date()
            self.square_off_all_positions("TIME_STOP")

    def square_off_all_positions(self, reason: str = "END OF DAY") -> None:
        """Exit every position and cancel every working entry."""
        with self._lock:
            for mp in list(self._positions.values()):
                try:
                    self._exit(mp, reason)
                except Exception as e:
                    self.logger.error(f"Error squaring off {mp.symbol}: {e}")
        self.logger.info(f"Square off requested: {reason}")

    # =========================================================================
    # QUERIES
    # =========================================================================

    def _get_current_price(self, symbol: Symbol) -> Optional[float]:
        tick = self.data_feed.cache.get_current_tick(symbol)
        if tick:
            return tick.last_price
        bar = self.data_feed.cache.get_current_ohlcv(symbol, TimeFrame._1M)
        return bar.close if bar else None

    def get_open_positions(self) -> List[Position]:
        with self._lock:
            return [mp.to_position() for mp in self._positions.values() if mp.state in (OPEN, EXITING)]

    def get_active_trades(self) -> List[ManagedPosition]:
        with self._lock:
            return list(self._positions.values())

    def get_closed_trades(self) -> List[Trade]:
        with self._lock:
            return list(self._closed_trades)

    def get_execution_stats(self) -> Dict[str, Any]:
        with self._lock:
            states: Dict[str, int] = {}
            for mp in self._positions.values():
                states[mp.state] = states.get(mp.state, 0) + 1
            return {
                'positions_by_state': states,
                'closed_trades': len(self._closed_trades),
                'square_off_time': self.square_off_time.isoformat(),
                'no_entry_after': self.no_entry_after.isoformat(),
                'square_off_enabled': self.square_off_enabled,
            }

    # =========================================================================
    # NOTIFICATIONS
    # =========================================================================

    def subscribe(self, callback: Callable) -> None:
        if callback not in self.subscribers:
            self.subscribers.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        if callback in self.subscribers:
            self.subscribers.remove(callback)

    def _notify(self, event_type: str, data: Dict[str, Any]) -> None:
        for subscriber in list(self.subscribers):
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
    engine = get_execution_engine()
    engine.start()
    return engine


def stop_execution_engine() -> None:
    """Stop the execution engine."""
    global _execution_engine
    if _execution_engine:
        _execution_engine.stop()
        _execution_engine = None
