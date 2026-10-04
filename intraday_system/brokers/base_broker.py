"""
Base Broker Interface for the Rapid Fire Intraday Trading System.

This module defines the abstract base class that all broker implementations
must implement. It provides a unified interface for order placement, market
data access, and account management across different brokers.
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import threading

from intraday_system.models import (
    BracketOrder, OHLCV, Order, OrderId, OrderResponse, 
    OrderSide, OrderStatus, OrderType, OrderValidity,
    Position, Symbol, Tick, Timestamp
)


class BrokerError(Exception):
    """Base exception for broker-related errors."""
    
    def __init__(self, message: str, broker: str = "unknown", 
                 order_id: Optional[OrderId] = None, 
                 retryable: bool = False):
        super().__init__(message)
        self.message = message
        self.broker = broker
        self.order_id = order_id
        self.retryable = retryable
        self.timestamp = datetime.utcnow().isoformat()
    
    def __str__(self) -> str:
        parts = [f"BrokerError: {self.message}"]
        if self.broker:
            parts.append(f"(Broker: {self.broker})")
        if self.order_id:
            parts.append(f"(Order ID: {self.order_id})")
        if self.retryable:
            parts.append("(Retryable)")
        return " ".join(parts)


class ConnectionStatus(Enum):
    """Status of broker connection."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class BrokerType(Enum):
    """Supported broker types."""
    ZERODHA = "zerodha"
    ALPACA = "alpaca"
    INTERACTIVE_BROKERS = "interactive_brokers"
    PAPER = "paper"
    SIMULATION = "simulation"


@dataclass
class OrderBook:
    """Represents the current order book for a symbol."""
    symbol: Symbol
    bids: List[Tuple[float, float]] = field(default_factory=list)  # [(price, quantity), ...]
    asks: List[Tuple[float, float]] = field(default_factory=list)  # [(price, quantity), ...]
    timestamp: Timestamp = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    @property
    def best_bid(self) -> Optional[float]:
        """Get the best bid price."""
        return self.bids[0][0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[float]:
        """Get the best ask price."""
        return self.asks[0][0] if self.asks else None
    
    @property
    def bid_ask_spread(self) -> float:
        """Get the bid-ask spread."""
        bid = self.best_bid
        ask = self.best_ask
        if bid is not None and ask is not None:
            return ask - bid
        return 0.0


class BaseBroker(ABC):
    """
    Abstract base class for all broker implementations.
    
    All brokers must implement the methods defined in this class to ensure
    compatibility with the trading system.
    """
    
    def __init__(self, broker_type: BrokerType, config: Dict[str, Any]):
        """Initialize the broker."""
        self.broker_type = broker_type
        self.config = config
        self.connection_status = ConnectionStatus.DISCONNECTED
        self._lock = threading.Lock()
        self._order_callbacks: Dict[OrderId, Callable] = {}
        self._fill_callbacks: Dict[OrderId, Callable] = {}
        self._connection_callbacks: List[Callable] = []
        self._order_id_counter = 0
        self._last_order_id: Optional[OrderId] = None
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the broker API."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the broker API."""
        pass
    
    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if the broker is connected."""
        return self.connection_status == ConnectionStatus.CONNECTED
    
    @abstractmethod
    async def get_market_data(self, symbols: List[Symbol]) -> Dict[Symbol, Tick]:
        """Get current market data (LTP) for multiple symbols."""
        pass
    
    @abstractmethod
    async def get_ohlcv_data(self, symbol: Symbol, timeframe: str, 
                           from_date: datetime, to_date: datetime) -> List[OHLCV]:
        """Get historical OHLCV data for a symbol."""
        pass
    
    @abstractmethod
    async def get_order_book(self, symbol: Symbol, depth: int = 5) -> OrderBook:
        """Get the order book for a symbol."""
        pass
    
    @abstractmethod
    async def place_order(self, order: Order) -> OrderResponse:
        """Place a new order."""
        pass
    
    @abstractmethod
    async def place_bracket_order(self, bracket_order: BracketOrder) -> Tuple[OrderResponse, OrderResponse, OrderResponse]:
        """Place a bracket order (entry + SL + TP)."""
        pass
    
    @abstractmethod
    async def modify_order(self, order_id: OrderId, 
                          quantity: Optional[float] = None,
                          price: Optional[float] = None,
                          stop_loss: Optional[float] = None,
                          take_profit: Optional[float] = None) -> OrderResponse:
        """Modify an existing order."""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: OrderId) -> bool:
        """Cancel an order."""
        pass
    
    @abstractmethod
    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> int:
        """Cancel all orders (optionally for a specific symbol)."""
        pass
    
    @abstractmethod
    async def get_order_status(self, order_id: OrderId) -> OrderStatus:
        """Get the status of an order."""
        pass
    
    async def get_order(self, order_id: OrderId) -> Optional[Order]:
        """
        Broker's current view of an order, including filled quantity and
        average fill price. Brokers should override this; the default only
        reports the status.
        """
        return None
    
    @abstractmethod
    async def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        pass
    
    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Get current positions."""
        pass
    
    @abstractmethod
    async def get_position(self, symbol: Symbol) -> Optional[Position]:
        """Get position for a specific symbol."""
        pass
    
    @abstractmethod
    async def get_account_balance(self) -> Dict[str, Any]:
        """Get current account balance and margins."""
        pass
    
    @abstractmethod
    async def get_trade_history(self, symbol: Optional[Symbol] = None,
                               from_date: Optional[datetime] = None,
                               to_date: Optional[datetime] = None) -> List[Any]:
        """Get trade/fill history."""
        pass
    
    @abstractmethod
    async def convert_to_market_order(self, order_id: OrderId) -> OrderResponse:
        """Convert a limit order to market order."""
        pass
    
    # =========================================================================
    # CALLBACK REGISTRATION
    # =========================================================================
    
    def register_order_callback(self, order_id: OrderId, 
                               callback: Callable[[OrderResponse], None]) -> None:
        """Register a callback for order status updates."""
        with self._lock:
            self._order_callbacks[order_id] = callback
    
    def register_fill_callback(self, order_id: OrderId, 
                               callback: Callable[[OrderResponse], None]) -> None:
        """Register a callback for order fills."""
        with self._lock:
            self._fill_callbacks[order_id] = callback
    
    def register_connection_callback(self, callback: Callable[[ConnectionStatus], None]) -> None:
        """Register a callback for connection status changes."""
        with self._lock:
            self._connection_callbacks.append(callback)
    
    def unregister_order_callback(self, order_id: OrderId) -> None:
        """Unregister order callback."""
        with self._lock:
            self._order_callbacks.pop(order_id, None)
    
    def unregister_fill_callback(self, order_id: OrderId) -> None:
        """Unregister fill callback."""
        with self._lock:
            self._fill_callbacks.pop(order_id, None)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def generate_order_id(self) -> OrderId:
        """Generate a unique order ID."""
        with self._lock:
            self._order_id_counter += 1
            order_id = f"{self.broker_type.value}_{self._order_id_counter}_{int(datetime.utcnow().timestamp() * 1000)}"
            self._last_order_id = order_id
            return order_id
    
    def get_connection_status(self) -> ConnectionStatus:
        """Get current connection status."""
        return self.connection_status
    
    def get_broker_name(self) -> str:
        """Get broker name."""
        return self.broker_type.value
    
    # =========================================================================
    # SUPPORTED FEATURES
    # =========================================================================
    
    def supports_bracket_orders(self) -> bool:
        """Check if broker supports bracket orders."""
        return False
    
    def supports_oco_orders(self) -> bool:
        """Check if broker supports OCO orders."""
        return False
    
    def supports_gtc_orders(self) -> bool:
        """Check if broker supports GTC orders."""
        return False
    
    def supports_iceberg_orders(self) -> bool:
        """Check if broker supports iceberg orders."""
        return False
    
    def get_max_orders_per_second(self) -> int:
        """Get maximum orders per second allowed by broker."""
        return 5  # Default conservative value
    
    def get_min_order_quantity(self, symbol: Symbol) -> float:
        """Get minimum order quantity for a symbol."""
        return 1.0
    
    def get_lot_size(self, symbol: Symbol) -> float:
        """Get lot size for a symbol."""
        return 1.0
    
    def get_tick_size(self, symbol: Symbol) -> float:
        """Get tick size for a symbol."""
        return 0.05  # Default for Indian markets
    
    def get_symbol_info(self, symbol: Symbol) -> Dict[str, Any]:
        """Get information about a symbol."""
        return {
            'symbol': symbol,
            'exchange': 'NSE',
            'instrument_type': 'EQ',
            'lot_size': self.get_lot_size(symbol),
            'tick_size': self.get_tick_size(symbol),
            'min_quantity': self.get_min_order_quantity(symbol)
        }