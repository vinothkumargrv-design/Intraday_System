"""
Paper Trading Broker Implementation for the Rapid Fire Intraday Trading System.

This module provides a paper trading broker that simulates order execution
without connecting to a real broker. It's used for testing and validation
before going live.
"""

import asyncio
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import time
import uuid

from intraday_system.brokers.base_broker import (
    BaseBroker, BrokerError, BrokerType, ConnectionStatus, OrderBook
)
from intraday_system.models import (
    BracketOrder, OHLCV, Order, OrderId, OrderResponse, 
    OrderSide, OrderStatus, OrderType, OrderValidity,
    Position, Symbol, Tick, Timestamp
)


@dataclass
class PaperBrokerConfig:
    """Configuration for Paper Broker."""
    initial_balance: float = 100000.0
    simulated_slippage: float = 0.0005  # 0.05%
    simulated_spread: float = 0.001   # 0.1%
    simulated_latency: float = 0.1     # 100ms
    fill_probability: float = 0.95      # 95% chance of fill
    partial_fill_probability: float = 0.1  # 10% chance of partial fill
    
    # For generating simulated market data
    volatility: float = 0.01
    drift: float = 0.001
    
    debug: bool = False


class PaperBroker(BaseBroker):
    """
    Paper trading broker implementation.
    
    Features:
    - Simulates market orders with configurable slippage
    - Simulates limit orders with fill probability
    - Tracks positions and PnL
    - Supports bracket orders
    - No real money at risk
    """
    
    def __init__(self, broker_type: BrokerType, config: Dict[str, Any]):
        """Initialize Paper broker."""
        super().__init__(broker_type, config)
        
        self.config = PaperBrokerConfig(**config.get('paper', {}))
        
        # Account state
        self._balance: float = self.config.initial_balance
        self._used_margin: float = 0.0
        self._total_exposure: float = 0.0
        
        # Positions: {symbol: Position}
        self._positions: Dict[Symbol, Position] = {}
        
        # Orders: {order_id: Order}
        self._orders: Dict[OrderId, Order] = {}
        
        # Market data simulation
        self._symbol_prices: Dict[Symbol, float] = {}  # Current price for each symbol
        self._symbol_volatility: Dict[Symbol, float] = {}  # Volatility for each symbol
        
        # WebSocket simulation
        self._websocket_enabled: bool = False
        self._tick_callbacks: List[Callable[[Tick], None]] = []
        self._stop_simulation: bool = False
        self._simulation_thread: Optional[threading.Thread] = None
        
        # Locks
        self._lock = threading.Lock()
        
        # Initialize with some default symbols
        self._initialize_default_prices()
    
    def _initialize_default_prices(self) -> None:
        """Initialize default prices for common symbols."""
        default_symbols = [
            Symbol("NIFTY"),
            Symbol("BANKNIFTY"),
            Symbol("RELIANCE"),
            Symbol("TCS"),
            Symbol("INFY"),
            Symbol("HDFCBANK"),
            Symbol("ICICIBANK"),
            Symbol("SBIN"),
        ]
        
        for symbol in default_symbols:
            # Generate random price between 100 and 2000
            price = 100 + (hash(symbol.value) % 1900)
            self._symbol_prices[symbol] = float(price)
            self._symbol_volatility[symbol] = self.config.volatility * (0.5 + hash(symbol.value) % 100 / 200)
    
    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================
    
    async def connect(self) -> bool:
        """Establish connection."""
        self.connection_status = ConnectionStatus.CONNECTED
        
        # Notify callbacks
        for callback in self._connection_callbacks:
            callback(self.connection_status)
        
        return True
    
    async def disconnect(self) -> None:
        """Disconnect."""
        self._stop_simulation = True
        
        if self._simulation_thread:
            self._simulation_thread.join(timeout=2.0)
            self._simulation_thread = None
        
        self.connection_status = ConnectionStatus.DISCONNECTED
        
        # Notify callbacks
        for callback in self._connection_callbacks:
            callback(self.connection_status)
    
    async def is_connected(self) -> bool:
        """Check if connected."""
        return self.connection_status == ConnectionStatus.CONNECTED
    
    # =========================================================================
    # MARKET DATA
    # =========================================================================
    
    async def get_market_data(self, symbols: List[Symbol]) -> Dict[Symbol, Tick]:
        """Get current market data for multiple symbols."""
        result = {}
        
        for symbol in symbols:
            if symbol not in self._symbol_prices:
                # Initialize new symbol
                self._symbol_prices[symbol] = 100.0 + (hash(symbol.value) % 1900)
                self._symbol_volatility[symbol] = self.config.volatility
            
            # Generate a tick
            tick = self._generate_tick(symbol)
            result[symbol] = tick
        
        return result
    
    def _generate_tick(self, symbol: Symbol) -> Tick:
        """Generate a simulated tick for a symbol."""
        base_price = self._symbol_prices[symbol]
        volatility = self._symbol_volatility.get(symbol, self.config.volatility)
        
        # Simulate price movement
        import random
        price_change = base_price * volatility * (random.random() - 0.5) * 2
        new_price = base_price + price_change
        
        # Update stored price
        self._symbol_prices[symbol] = new_price
        
        # Calculate spread
        spread = new_price * self.config.simulated_spread
        bid_price = new_price - spread / 2
        ask_price = new_price + spread / 2
        
        return Tick(
            symbol=symbol,
            timestamp=Timestamp(datetime.utcnow().isoformat()),
            last_price=new_price,
            last_quantity=100,
            volume=10000,
            bid_price=bid_price,
            bid_quantity=50,
            ask_price=ask_price,
            ask_quantity=50,
            open_price=base_price * 0.99,
            high_price=base_price * 1.01,
            low_price=base_price * 0.99,
            close_price=base_price,
            change=new_price - base_price,
            change_percent=(new_price - base_price) / base_price * 100,
            average_price=base_price,
            oi=0,
            oi_day_high=0,
            oi_day_low=0,
            is_index=symbol.value in ["NIFTY", "BANKNIFTY"]
        )
    
    async def get_ohlcv_data(self, symbol: Symbol, timeframe: str,
                           from_date: datetime, to_date: datetime) -> List[OHLCV]:
        """Get historical OHLCV data."""
        if symbol not in self._symbol_prices:
            self._symbol_prices[symbol] = 100.0 + (hash(symbol.value) % 1900)
        
        ohlcv_list = []
        current_time = from_date
        base_price = self._symbol_prices[symbol]
        
        while current_time < to_date:
            # Generate simulated OHLCV bar
            volatility = self._symbol_volatility.get(symbol, self.config.volatility)
            
            open_price = base_price
            high_price = open_price * (1 + volatility * 2)
            low_price = open_price * (1 - volatility * 2)
            close_price = open_price * (1 + volatility * (0.5 - 0.5))  # Random walk
            volume = 1000 + hash(str(current_time)) % 5000
            
            ohlcv_list.append(OHLCV(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=Timestamp(current_time.isoformat()),
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
                oi=0
            ))
            
            # Update base price for next bar
            base_price = close_price
            
            # Move to next bar
            if timeframe == "1m":
                current_time += timedelta(minutes=1)
            elif timeframe == "5m":
                current_time += timedelta(minutes=5)
            elif timeframe == "15m":
                current_time += timedelta(minutes=15)
            elif timeframe == "1h":
                current_time += timedelta(hours=1)
            else:
                current_time += timedelta(minutes=1)
        
        return ohlcv_list
    
    async def get_order_book(self, symbol: Symbol, depth: int = 5) -> OrderBook:
        """Get order book for a symbol."""
        base_price = self._symbol_prices.get(symbol, 100.0)
        spread = base_price * self.config.simulated_spread
        
        bids = []
        asks = []
        
        for i in range(depth):
            bids.append((base_price - spread * (i + 1) / (depth * 2), 100 * (depth - i)))
            asks.append((base_price + spread * (i + 1) / (depth * 2), 100 * (depth - i)))
        
        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=Timestamp(datetime.utcnow().isoformat())
        )
    
    # =========================================================================
    # ORDER MANAGEMENT
    # =========================================================================
    
    async def place_order(self, order: Order) -> OrderResponse:
        """Place a new order."""
        if not self.connection_status == ConnectionStatus.CONNECTED:
            raise BrokerError("Not connected", retryable=True)
        
        # Generate order ID
        order_id = self.generate_order_id()
        
        # Get current price for the symbol
        current_price = self._symbol_prices.get(order.symbol, 100.0)
        
        # Simulate order execution
        status, filled_quantity, executed_price = self._simulate_execution(
            order, current_price
        )
        
        # Create order response
        order_response = OrderResponse(
            order_id=order_id,
            status=status,
            filled_quantity=filled_quantity,
            pending_quantity=order.quantity - filled_quantity,
            average_price=executed_price,
            executed_price=executed_price,
            message=f"Order {status.value}"
        )
        
        # Store the order
        stored_order = Order(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=order.price or executed_price,
            order_type=order.order_type,
            order_validity=order.order_validity,
            status=status,
            filled_quantity=filled_quantity,
            pending_quantity=order.quantity - filled_quantity,
            average_price=executed_price,
            trigger_price=order.trigger_price,
            tag=order.tag or "paper"
        )
        
        with self._lock:
            self._orders[order_id] = stored_order
        
        # If filled, update positions
        if filled_quantity > 0:
            await self._update_position(stored_order, filled_quantity, executed_price)
        
        # Simulate latency
        await asyncio.sleep(self.config.simulated_latency)
        
        # Notify callbacks
        if order_id in self._order_callbacks:
            self._order_callbacks[order_id](order_response)
        
        if filled_quantity > 0 and order_id in self._fill_callbacks:
            self._fill_callbacks[order_id](order_response)
        
        return order_response
    
    def _simulate_execution(self, order: Order, current_price: float) -> Tuple[OrderStatus, float, float]:
        """Simulate order execution."""
        import random
        
        random.seed(time.time() * 1000)
        
        # Market orders are always filled (with slippage)
        if order.order_type == OrderType.MARKET:
            executed_price = self._calculate_slippage(current_price, order.side)
            return OrderStatus.FILLED, order.quantity, executed_price
        
        # Limit orders may not be filled
        if order.price:
            if order.side == OrderSide.BUY and order.price < current_price:
                # Buy limit below market - may not fill
                if random.random() < self.config.fill_probability:
                    executed_price = min(order.price, self._calculate_slippage(current_price, order.side))
                    fill_qty = order.quantity
                    
                    # Partial fill
                    if random.random() < self.config.partial_fill_probability:
                        fill_qty = order.quantity * (0.5 + random.random() * 0.5)  # 50-100%
                    
                    return OrderStatus.FILLED, fill_qty, executed_price
                else:
                    return OrderStatus.OPEN, 0.0, 0.0
                    
            elif order.side == OrderSide.SELL and order.price > current_price:
                # Sell limit above market - may not fill
                if random.random() < self.config.fill_probability:
                    executed_price = max(order.price, self._calculate_slippage(current_price, order.side))
                    fill_qty = order.quantity
                    
                    # Partial fill
                    if random.random() < self.config.partial_fill_probability:
                        fill_qty = order.quantity * (0.5 + random.random() * 0.5)  # 50-100%
                    
                    return OrderStatus.FILLED, fill_qty, executed_price
                else:
                    return OrderStatus.OPEN, 0.0, 0.0
            else:
                # Limit order at market price or better - always fill
                executed_price = self._calculate_slippage(current_price, order.side)
                return OrderStatus.FILLED, order.quantity, executed_price
        
        # Default to market execution
        executed_price = self._calculate_slippage(current_price, order.side)
        return OrderStatus.FILLED, order.quantity, executed_price
    
    def _calculate_slippage(self, price: float, side: OrderSide) -> float:
        """Calculate execution price with slippage."""
        import random
        
        slippage_percent = self.config.simulated_slippage
        
        if side == OrderSide.BUY:
            # Buying at slightly higher price
            slippage_factor = 1 + slippage_percent * (0.5 + random.random())
        else:
            # Selling at slightly lower price
            slippage_factor = 1 - slippage_percent * (0.5 + random.random())
        
        return price * slippage_factor
    
    async def _update_position(self, order: Order, filled_quantity: float, executed_price: float) -> None:
        """Update position after order execution."""
        with self._lock:
            existing_position = self._positions.get(order.symbol)
            
            if existing_position:
                # Update existing position
                if order.side == OrderSide.BUY:
                    new_quantity = existing_position.quantity + filled_quantity
                    new_average_price = (
                        existing_position.average_price * existing_position.quantity +
                        executed_price * filled_quantity
                    ) / new_quantity if new_quantity > 0 else 0
                else:  # SELL
                    new_quantity = existing_position.quantity - filled_quantity
                    # Average price remains the same for sells
                    new_average_price = existing_position.average_price
                
                position = Position(
                    symbol=order.symbol,
                    quantity=new_quantity,
                    average_price=new_average_price,
                    last_price=executed_price,
                    value=new_quantity * executed_price,
                    pnl=(executed_price - new_average_price) * new_quantity,
                    side=PositionSide.LONG if new_quantity > 0 else PositionSide.SHORT if new_quantity < 0 else PositionSide.FLAT
                )
                self._positions[order.symbol] = position
                
                # Update account balance
                if order.side == OrderSide.BUY:
                    self._balance -= executed_price * filled_quantity
                    self._used_margin += executed_price * filled_quantity
                else:  # SELL
                    self._balance += executed_price * filled_quantity
                    self._used_margin -= executed_price * filled_quantity
                    
            else:
                # Create new position
                position = Position(
                    symbol=order.symbol,
                    quantity=filled_quantity if order.side == OrderSide.BUY else -filled_quantity,
                    average_price=executed_price,
                    last_price=executed_price,
                    value=abs(filled_quantity) * executed_price,
                    pnl=0.0,  # No PnL for new position
                    side=PositionSide.LONG if order.side == OrderSide.BUY else PositionSide.SHORT
                )
                self._positions[order.symbol] = position
                
                # Update account balance
                if order.side == OrderSide.BUY:
                    self._balance -= executed_price * filled_quantity
                    self._used_margin += executed_price * filled_quantity
                else:  # SELL
                    self._balance += executed_price * filled_quantity
                    self._used_margin -= executed_price * filled_quantity
    
    async def place_bracket_order(self, bracket_order: BracketOrder) -> Tuple[OrderResponse, OrderResponse, OrderResponse]:
        """Place a bracket order (entry + SL + TP)."""
        # Create and place the three orders
        entry_order_response = await self.place_order(
            Order(
                order_id=OrderId(""),  # Will be generated
                symbol=bracket_order.symbol,
                side=bracket_order.side,
                quantity=bracket_order.quantity,
                price=bracket_order.price,
                order_type=bracket_order.order_type,
                order_validity=OrderValidity.INTRADAY,
                status=OrderStatus.OPEN,
                tag=bracket_order.tag or "bracket_entry"
            )
        )
        
        # Create SL and TP orders (as limit orders)
        sl_order_response = await self.place_order(
            Order(
                order_id=OrderId(""),
                symbol=bracket_order.symbol,
                side=OrderSide.SELL if bracket_order.side == OrderSide.BUY else OrderSide.BUY,
                quantity=bracket_order.quantity,
                price=bracket_order.stop_loss,
                order_type=OrderType.SL,
                order_validity=OrderValidity.INTRADAY,
                status=OrderStatus.OPEN,
                tag=bracket_order.tag or "bracket_sl"
            )
        )
        
        tp_order_response = await self.place_order(
            Order(
                order_id=OrderId(""),
                symbol=bracket_order.symbol,
                side=OrderSide.SELL if bracket_order.side == OrderSide.BUY else OrderSide.BUY,
                quantity=bracket_order.quantity,
                price=bracket_order.take_profit,
                order_type=OrderType.LIMIT,
                order_validity=OrderValidity.INTRADAY,
                status=OrderStatus.OPEN,
                tag=bracket_order.tag or "bracket_tp"
            )
        )
        
        return (entry_order_response, sl_order_response, tp_order_response)
    
    async def modify_order(self, order_id: OrderId,
                          quantity: Optional[float] = None,
                          price: Optional[float] = None,
                          stop_loss: Optional[float] = None,
                          take_profit: Optional[float] = None) -> OrderResponse:
        """Modify an existing order."""
        with self._lock:
            order = self._orders.get(order_id)
            if not order:
                raise BrokerError(f"Order {order_id} not found")
            
            # Update order
            if quantity is not None:
                order.quantity = quantity
            if price is not None:
                order.price = price
            if stop_loss is not None:
                order.trigger_price = stop_loss
            
            return OrderResponse(
                order_id=order_id,
                status=order.status,
                message="Order modified"
            )
    
    async def cancel_order(self, order_id: OrderId) -> bool:
        """Cancel an order."""
        with self._lock:
            if order_id in self._orders:
                order = self._orders[order_id]
                if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.TRIGGERED]:
                    order.status = OrderStatus.CANCELLED
                    del self._orders[order_id]
                    return True
                else:
                    return False
            return False
    
    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> int:
        """Cancel all orders."""
        with self._lock:
            cancelled_count = 0
            
            if symbol:
                # Cancel orders for specific symbol
                orders_to_cancel = [
                    order_id for order_id, order in self._orders.items()
                    if order.symbol == symbol and 
                    order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.TRIGGERED]
                ]
            else:
                # Cancel all orders
                orders_to_cancel = [
                    order_id for order_id, order in self._orders.items()
                    if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.TRIGGERED]
                ]
            
            for order_id in orders_to_cancel:
                self._orders[order_id].status = OrderStatus.CANCELLED
                del self._orders[order_id]
                cancelled_count += 1
            
            return cancelled_count
    
    async def get_order_status(self, order_id: OrderId) -> OrderStatus:
        """Get order status."""
        with self._lock:
            order = self._orders.get(order_id)
            if order:
                return order.status
            return OrderStatus.UNKNOWN
    
    async def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        with self._lock:
            return [
                order for order in self._orders.values()
                if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.TRIGGERED]
            ]
    
    async def get_positions(self) -> List[Position]:
        """Get current positions."""
        with self._lock:
            return list(self._positions.values())
    
    async def get_position(self, symbol: Symbol) -> Optional[Position]:
        """Get position for a specific symbol."""
        with self._lock:
            return self._positions.get(symbol)
    
    async def get_account_balance(self) -> Dict[str, Any]:
        """Get account balance."""
        with self._lock:
            return {
                "balance": self._balance,
                "used_margin": self._used_margin,
                "available_margin": self._balance - self._used_margin,
                "total_exposure": self._total_exposure,
                "currency": "INR"
            }
    
    async def get_trade_history(self, symbol: Optional[Symbol] = None,
                               from_date: Optional[datetime] = None,
                               to_date: Optional[datetime] = None) -> List[Any]:
        """Get trade history."""
        # In paper trading, we don't track trade history
        return []
    
    async def convert_to_market_order(self, order_id: OrderId) -> OrderResponse:
        """Convert a limit order to market order."""
        with self._lock:
            if order_id in self._orders:
                order = self._orders[order_id]
                if order.order_type == OrderType.LIMIT:
                    order.order_type = OrderType.MARKET
                    # Recalculate execution
                    current_price = self._symbol_prices.get(order.symbol, 100.0)
                    executed_price = self._calculate_slippage(current_price, order.side)
                    order.filled_quantity = order.quantity
                    order.status = OrderStatus.FILLED
                    order.average_price = executed_price
                    
                    # Update position
                    await self._update_position(order, order.quantity, executed_price)
                    
                    return OrderResponse(
                        order_id=order_id,
                        status=OrderStatus.FILLED,
                        message="Order converted to market and executed"
                    )
        
        raise BrokerError(f"Order {order_id} not found or cannot be converted")
    
    # =========================================================================
    # SUBSCRIPTION MANAGEMENT
    # =========================================================================
    
    def subscribe(self, symbol: Symbol) -> None:
        """Subscribe to live data for a symbol."""
        # Initialize price if not exists
        if symbol not in self._symbol_prices:
            self._symbol_prices[symbol] = 100.0 + (hash(symbol.value) % 1900)
            self._symbol_volatility[symbol] = self.config.volatility
    
    def unsubscribe(self, symbol: Symbol) -> None:
        """Unsubscribe from live data for a symbol."""
        # In paper trading, we don't need to do anything
        pass
    
    def register_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Register callback for tick data."""
        with self._lock:
            if callback not in self._tick_callbacks:
                self._tick_callbacks.append(callback)
    
    def unregister_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Unregister tick callback."""
        with self._lock:
            if callback in self._tick_callbacks:
                self._tick_callbacks.remove(callback)
    
    # =========================================================================
    # WEBSOCKET SIMULATION
    # =========================================================================
    
    async def start_simulation(self) -> None:
        """Start simulated WebSocket data generation."""
        self._stop_simulation = False
        self._simulation_thread = threading.Thread(
            target=self._simulation_loop,
            daemon=True
        )
        self._simulation_thread.start()
    
    def _simulation_loop(self) -> None:
        """Generate simulated market data in a loop."""
        import time
        
        while not self._stop_simulation:
            try:
                # Generate ticks for all subscribed symbols
                with self._lock:
                    symbols = list(self._symbol_prices.keys())
                
                for symbol in symbols:
                    tick = self._generate_tick(symbol)
                    
                    # Notify callbacks
                    for callback in self._tick_callbacks:
                        try:
                            callback(tick)
                        except:
                            pass
                
                # Sleep for a bit
                time.sleep(0.1)
                
            except Exception:
                time.sleep(1.0)
    
    # =========================================================================
    # OVERIDDEN PROPERTIES
    # =========================================================================
    
    def supports_bracket_orders(self) -> bool:
        """Check if broker supports bracket orders."""
        return True
    
    def supports_oco_orders(self) -> bool:
        """Check if broker supports OCO orders."""
        return True
    
    def get_max_orders_per_second(self) -> int:
        """Get maximum orders per second."""
        return 100  # Paper trading has no rate limits
    
    def get_min_order_quantity(self, symbol: Symbol) -> float:
        """Get minimum order quantity."""
        return 1.0
    
    def get_lot_size(self, symbol: Symbol) -> float:
        """Get lot size for a symbol."""
        return 1.0
    
    def get_tick_size(self, symbol: Symbol) -> float:
        """Get tick size for a symbol."""
        return 0.05