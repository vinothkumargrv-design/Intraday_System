"""
Paper Trading Broker for the Rapid Fire Intraday Trading System.

Simulates an exchange-connected broker without risking money:

- MARKET orders fill immediately at the current price plus slippage.
- LIMIT orders fill once the market trades at or through the limit price.
- SL / SL-M orders trigger when the market crosses the trigger price and then
  fill like a market order.
- Net positions and cash are tracked per symbol.

Prices come from a ``price_provider`` callable (normally the live data feed's
last traded price) so that paper fills track the same market the strategies
see. Resting orders are re-evaluated whenever their status is queried, so the
order manager's polling loop drives fills.
"""

from __future__ import annotations

import random
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from intraday_system.brokers.base_broker import (
    BaseBroker, BrokerError, BrokerType, ConnectionStatus, OrderBook
)
from intraday_system.models import (
    BracketOrder, OHLCV, Order, OrderId, OrderResponse, OrderSide, OrderStatus,
    OrderType, Position, PositionSide, Symbol, Tick
)

OPEN_STATUSES = (OrderStatus.PENDING, OrderStatus.PLACED, OrderStatus.PARTIALLY_FILLED)


@dataclass
class PaperBrokerConfig:
    """Configuration for the paper broker."""
    initial_balance: float = 100000.0
    simulated_slippage: float = 0.0005   # 0.05% adverse slippage on market fills
    fill_probability: float = 1.0        # Probability a marketable limit fills per check
    seed: Optional[int] = None


@dataclass
class _NetPosition:
    quantity: float = 0.0                # +long / -short
    average_price: float = 0.0
    realized_pnl: float = 0.0


class PaperBroker(BaseBroker):
    """Paper trading broker implementation."""

    def __init__(self, broker_type: BrokerType, config: Dict[str, Any]):
        super().__init__(broker_type, config)
        paper_cfg = dict(config.get('paper', {}) or {})
        known = PaperBrokerConfig.__dataclass_fields__.keys()
        self.paper_config = PaperBrokerConfig(**{k: v for k, v in paper_cfg.items() if k in known})

        self._rng = random.Random(self.paper_config.seed)
        self._cash = self.paper_config.initial_balance
        self._orders: Dict[OrderId, Order] = {}
        self._positions: Dict[Symbol, _NetPosition] = {}
        self._last_prices: Dict[Symbol, float] = {}
        self._broker_lock = threading.RLock()

        # Callable returning the latest price for a symbol (set by OrderManager)
        self.price_provider: Optional[Callable[[Symbol], Optional[float]]] = None

    # =========================================================================
    # PRICES
    # =========================================================================

    def set_price(self, symbol: Symbol, price: float) -> None:
        """Manually set a price (used when no price provider is attached)."""
        with self._broker_lock:
            self._last_prices[symbol] = price
        self._process_resting_orders(symbol)

    def _price(self, symbol: Symbol) -> Optional[float]:
        price = None
        if self.price_provider is not None:
            try:
                price = self.price_provider(symbol)
            except Exception:
                price = None
        if price is not None:
            self._last_prices[symbol] = price
            return price
        return self._last_prices.get(symbol)

    def _slipped(self, price: float, side: OrderSide) -> float:
        slip = self.paper_config.simulated_slippage
        return price * (1 + slip) if side == OrderSide.BUY else price * (1 - slip)

    # =========================================================================
    # CONNECTION
    # =========================================================================

    async def connect(self) -> bool:
        self.connection_status = ConnectionStatus.CONNECTED
        for callback in self._connection_callbacks:
            callback(self.connection_status)
        return True

    async def disconnect(self) -> None:
        self.connection_status = ConnectionStatus.DISCONNECTED
        for callback in self._connection_callbacks:
            callback(self.connection_status)

    async def is_connected(self) -> bool:
        return self.connection_status == ConnectionStatus.CONNECTED

    # =========================================================================
    # MARKET DATA
    # =========================================================================

    async def get_market_data(self, symbols: List[Symbol]) -> Dict[Symbol, Tick]:
        result = {}
        for symbol in symbols:
            price = self._price(symbol)
            if price is not None:
                result[symbol] = Tick(symbol=symbol, timestamp=datetime.now(),
                                      last_price=price, last_quantity=0)
        return result

    async def get_ohlcv_data(self, symbol: Symbol, timeframe: str,
                             from_date: datetime, to_date: datetime) -> List[OHLCV]:
        return []  # Paper broker has no history; the data feed provides it

    async def get_order_book(self, symbol: Symbol, depth: int = 5) -> OrderBook:
        price = self._price(symbol) or 0.0
        step = price * 0.0001
        return OrderBook(symbol=symbol,
                         bids=[(price - step * (i + 1), 100.0) for i in range(depth)],
                         asks=[(price + step * (i + 1), 100.0) for i in range(depth)])

    # =========================================================================
    # ORDERS
    # =========================================================================

    async def place_order(self, order: Order) -> OrderResponse:
        if self.connection_status != ConnectionStatus.CONNECTED:
            raise BrokerError("Not connected", broker="paper", retryable=True)
        if order.quantity <= 0:
            return OrderResponse(success=False, order_id=order.order_id,
                                 error_code="INVALID_QUANTITY",
                                 error_message=f"Invalid quantity {order.quantity}")
        if order.order_type == OrderType.LIMIT and not order.price:
            return OrderResponse(success=False, order_id=order.order_id,
                                 error_code="INVALID_PRICE", error_message="LIMIT order needs a price")
        if order.order_type in (OrderType.SL, OrderType.SL_M) and not order.trigger_price:
            return OrderResponse(success=False, order_id=order.order_id,
                                 error_code="INVALID_TRIGGER", error_message="SL order needs a trigger price")

        broker_order_id = self.generate_order_id()
        stored = replace(order, broker_order_id=broker_order_id, status=OrderStatus.PLACED,
                         filled_quantity=0.0, average_fill_price=None)
        with self._broker_lock:
            self._orders[broker_order_id] = stored

        self._try_fill(stored, on_arrival=True)
        return OrderResponse(success=True, order_id=order.order_id, broker_order_id=broker_order_id)

    async def place_bracket_order(self, bracket_order: BracketOrder) -> Tuple[OrderResponse, OrderResponse, OrderResponse]:
        entry = await self.place_order(bracket_order.entry_order)
        sl = await self.place_order(bracket_order.sl_order)
        tp = await self.place_order(bracket_order.tp_order) if bracket_order.tp_order else \
            OrderResponse(success=False, error_message="No TP order")
        return entry, sl, tp

    def _try_fill(self, order: Order, on_arrival: bool = False) -> None:
        """
        Fill an open order if the market allows it.
        
        A LIMIT order that is marketable on arrival takes the market price;
        one that was resting fills at its own limit price when the market
        trades through it.
        """
        if order.status not in OPEN_STATUSES:
            return
        price = self._price(order.symbol)
        if price is None:
            return

        fill_price: Optional[float] = None
        buy = order.order_side == OrderSide.BUY
        if order.order_type == OrderType.MARKET:
            fill_price = self._slipped(price, order.order_side)
        elif order.order_type == OrderType.LIMIT:
            marketable = price <= order.price if buy else price >= order.price
            if marketable and self._rng.random() <= self.paper_config.fill_probability:
                if on_arrival:
                    fill_price = min(price, order.price) if buy else max(price, order.price)
                else:
                    fill_price = order.price
        elif order.order_type in (OrderType.SL, OrderType.SL_M):
            triggered = price >= order.trigger_price if buy else price <= order.trigger_price
            if triggered:
                fill_price = self._slipped(price, order.order_side)
                if order.order_type == OrderType.SL and order.price:
                    # Stop-limit: don't fill worse than the limit
                    if (buy and fill_price > order.price) or (not buy and fill_price < order.price):
                        return
        if fill_price is None:
            return

        with self._broker_lock:
            quantity = order.quantity - order.filled_quantity
            order.filled_quantity = order.quantity
            order.average_fill_price = fill_price
            order.status = OrderStatus.COMPLETED
            self._apply_fill(order.symbol, order.order_side, quantity, fill_price)

        for callback in list(self._fill_callbacks.values()):
            try:
                callback(OrderResponse(success=True, order_id=order.order_id,
                                       broker_order_id=order.broker_order_id))
            except Exception:
                pass

    def _apply_fill(self, symbol: Symbol, side: OrderSide, quantity: float, price: float) -> None:
        pos = self._positions.setdefault(symbol, _NetPosition())
        signed = quantity if side == OrderSide.BUY else -quantity
        self._cash -= signed * price

        if pos.quantity == 0 or (pos.quantity > 0) == (signed > 0):
            # Opening or adding
            total = pos.quantity + signed
            pos.average_price = (pos.average_price * abs(pos.quantity) + price * quantity) / abs(total)
            pos.quantity = total
        else:
            # Reducing / closing / flipping
            closing = min(abs(signed), abs(pos.quantity))
            direction = 1 if pos.quantity > 0 else -1
            pos.realized_pnl += (price - pos.average_price) * closing * direction
            pos.quantity += signed
            if abs(pos.quantity) < 1e-9:
                pos.quantity, pos.average_price = 0.0, 0.0
            elif (pos.quantity > 0) != (direction > 0):
                pos.average_price = price  # Flipped direction

    def _process_resting_orders(self, symbol: Optional[Symbol] = None) -> None:
        with self._broker_lock:
            orders = [o for o in self._orders.values()
                      if o.status in OPEN_STATUSES and (symbol is None or o.symbol == symbol)]
        for order in orders:
            self._try_fill(order)

    async def modify_order(self, order_id: OrderId, quantity: Optional[float] = None,
                           price: Optional[float] = None, trigger_price: Optional[float] = None,
                           stop_loss: Optional[float] = None,
                           take_profit: Optional[float] = None) -> OrderResponse:
        with self._broker_lock:
            order = self._orders.get(order_id)
            if order is None or order.status not in OPEN_STATUSES:
                return OrderResponse(success=False, order_id=order_id,
                                     error_code="NOT_MODIFIABLE",
                                     error_message=f"Order {order_id} not open")
            if quantity is not None:
                order.quantity = quantity
            if price is not None:
                order.price = price
            if trigger_price is not None or stop_loss is not None:
                order.trigger_price = trigger_price if trigger_price is not None else stop_loss
        self._try_fill(order)
        return OrderResponse(success=True, order_id=order.order_id, broker_order_id=order_id)

    async def cancel_order(self, order_id: OrderId) -> bool:
        with self._broker_lock:
            order = self._orders.get(order_id)
            if order is None or order.status not in OPEN_STATUSES:
                return False
            order.status = OrderStatus.CANCELLED
            return True

    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> int:
        with self._broker_lock:
            open_ids = [oid for oid, o in self._orders.items()
                        if o.status in OPEN_STATUSES and (symbol is None or o.symbol == symbol)]
        count = 0
        for oid in open_ids:
            if await self.cancel_order(oid):
                count += 1
        return count

    async def get_order(self, order_id: OrderId) -> Optional[Order]:
        """Broker's current view of an order (re-evaluates resting orders)."""
        with self._broker_lock:
            order = self._orders.get(order_id)
        if order is None:
            return None
        self._try_fill(order)
        with self._broker_lock:
            return replace(order)

    async def get_order_status(self, order_id: OrderId) -> OrderStatus:
        order = await self.get_order(order_id)
        return order.status if order else OrderStatus.REJECTED

    async def get_open_orders(self) -> List[Order]:
        with self._broker_lock:
            return [replace(o) for o in self._orders.values() if o.status in OPEN_STATUSES]

    # =========================================================================
    # ACCOUNT
    # =========================================================================

    async def get_positions(self) -> List[Position]:
        with self._broker_lock:
            items = [(s, p) for s, p in self._positions.items() if p.quantity != 0]
        return [Position(symbol=s, side=PositionSide.LONG if p.quantity > 0 else PositionSide.SHORT,
                         quantity=abs(p.quantity), entry_price=p.average_price,
                         entry_timestamp=datetime.now())
                for s, p in items]

    async def get_position(self, symbol: Symbol) -> Optional[Position]:
        for position in await self.get_positions():
            if position.symbol == symbol:
                return position
        return None

    def net_quantity(self, symbol: Symbol) -> float:
        """Signed net position (+long / -short)."""
        with self._broker_lock:
            pos = self._positions.get(symbol)
            return pos.quantity if pos else 0.0

    async def get_account_balance(self) -> Dict[str, Any]:
        with self._broker_lock:
            market_value = sum(p.quantity * (self._last_prices.get(s) or p.average_price)
                               for s, p in self._positions.items())
            realized = sum(p.realized_pnl for p in self._positions.values())
            return {
                "cash": self._cash,
                "equity": self._cash + market_value,
                "realized_pnl": realized,
                "currency": "INR",
            }

    async def get_trade_history(self, symbol: Optional[Symbol] = None,
                                from_date: Optional[datetime] = None,
                                to_date: Optional[datetime] = None) -> List[Any]:
        with self._broker_lock:
            return [replace(o) for o in self._orders.values()
                    if o.status == OrderStatus.COMPLETED and (symbol is None or o.symbol == symbol)]

    async def convert_to_market_order(self, order_id: OrderId) -> OrderResponse:
        with self._broker_lock:
            order = self._orders.get(order_id)
            if order is None or order.status not in OPEN_STATUSES:
                raise BrokerError(f"Order {order_id} not open", broker="paper")
            order.order_type = OrderType.MARKET
        self._try_fill(order)
        return OrderResponse(success=True, order_id=order.order_id, broker_order_id=order_id)

    # =========================================================================
    # FEATURES
    # =========================================================================

    def supports_bracket_orders(self) -> bool:
        return True

    def supports_oco_orders(self) -> bool:
        return False

    def get_max_orders_per_second(self) -> int:
        return 100
