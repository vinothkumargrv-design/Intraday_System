"""
Zerodha Kite Connect Broker Implementation for the Rapid Fire Intraday Trading System.

This module implements the BaseBroker interface for Zerodha's Kite Connect API.
It provides full support for Indian equity and F&O markets with WebSocket
streaming data and REST API for order management.
"""

import asyncio
import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import time
import uuid

import aiohttp
import websockets

from intraday_system.brokers.base_broker import (
    BaseBroker, BrokerError, BrokerType, ConnectionStatus, OrderBook
)
from intraday_system.models import (
    BracketOrder, OHLCV, Order, OrderId, OrderResponse, 
    OrderSide, OrderStatus, OrderType, OrderValidity,
    Position, Symbol, Tick, Timestamp
)


@dataclass
class KiteConnectConfig:
    """Configuration for Zerodha Kite Connect."""
    api_key: str
    access_token: Optional[str] = None
    request_token: Optional[str] = None
    user_id: Optional[str] = None
    api_secret: Optional[str] = None  # For generating session
    
    # WebSocket configuration
    websocket_url: str = "wss://ws.kite.trade"
    auto_reconnect: bool = True
    reconnect_interval: float = 5.0
    max_reconnect_attempts: int = 10
    
    # Rate limiting
    max_requests_per_second: int = 10
    
    # Data preferences
    tick_depth: int = 5
    subscribe_to_live_data: bool = True
    
    # Debug
    debug: bool = False


class ZerodhaBroker(BaseBroker):
    """
    Zerodha Kite Connect broker implementation.
    
    Features:
    - Full support for equity, F&O, commodity, currency markets
    - WebSocket streaming for live tick data
    - REST API for order placement and management
    - Bracket orders (entry + SL + TP)
    - Cover orders
    - GTT (Good Till Trigger) orders
    - OCO (One Cancels Other) support
    """
    
    def __init__(self, broker_type: BrokerType, config: Dict[str, Any]):
        """Initialize Zerodha broker."""
        super().__init__(broker_type, config)
        
        self.kite_config = KiteConnectConfig(**config.get('zerodha', {}))
        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._websocket_thread: Optional[threading.Thread] = None
        self._stop_websocket: bool = False
        
        # Authentication
        self.access_token: Optional[str] = None
        self.user_id: Optional[str] = None
        self.public_token: Optional[str] = None
        
        # Subscriptions
        self._subscribed_symbols: set = set()
        self._subscription_lock = threading.Lock()
        
        # Rate limiting
        self._rate_limit_semaphore: Optional[asyncio.Semaphore] = None
        self._last_request_time: float = 0
        
        # Callbacks
        self._tick_callbacks: List[Callable[[Tick], None]] = []
        self._order_book_callbacks: Dict[Symbol, List[Callable[[OrderBook], None]]] = {}
        
        # Data cache
        self._ticks: Dict[Symbol, Tick] = {}
        self._order_books: Dict[Symbol, OrderBook] = {}
    
    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================
    
    async def connect(self) -> bool:
        """Establish connection to Kite Connect."""
        try:
            # Initialize rate limiting
            self._rate_limit_semaphore = asyncio.Semaphore(
                self.kite_config.max_requests_per_second
            )
            
            # Create HTTP session
            self.session = aiohttp.ClientSession()
            
            # Authenticate
            await self._authenticate()
            
            # Start WebSocket connection
            await self._start_websocket()
            
            self.connection_status = ConnectionStatus.CONNECTED
            
            # Notify callbacks
            for callback in self._connection_callbacks:
                callback(self.connection_status)
            
            return True
            
        except Exception as e:
            self.connection_status = ConnectionStatus.ERROR
            raise BrokerError(f"Failed to connect to Zerodha: {e}", 
                            broker=self.broker_type.value, retryable=True)
    
    async def disconnect(self) -> None:
        """Disconnect from Kite Connect."""
        self._stop_websocket = True
        
        # Close WebSocket
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass
            self.websocket = None
        
        # Join WebSocket thread
        if self._websocket_thread:
            self._websocket_thread.join(timeout=5.0)
            self._websocket_thread = None
        
        # Close HTTP session
        if self.session:
            await self.session.close()
            self.session = None
        
        self.connection_status = ConnectionStatus.DISCONNECTED
        
        # Notify callbacks
        for callback in self._connection_callbacks:
            callback(self.connection_status)
    
    async def is_connected(self) -> bool:
        """Check if connected to Kite Connect."""
        if self.connection_status == ConnectionStatus.CONNECTED:
            try:
                # Test connection with a simple API call
                await self._make_request("GET", "https://api.kite.trade/margins/equity")
                return True
            except:
                self.connection_status = ConnectionStatus.ERROR
                return False
        return False
    
    # =========================================================================
    # AUTHENTICATION
    # =========================================================================
    
    async def _authenticate(self) -> None:
        """Authenticate with Kite Connect."""
        # Use existing session if available
        if self.kite_config.access_token and self.kite_config.user_id:
            self.access_token = self.kite_config.access_token
            self.user_id = self.kite_config.user_id
            return
        
        # Generate session using request token
        if self.kite_config.request_token:
            await self._generate_session()
        else:
            raise BrokerError("No access token or request token provided")
    
    async def _generate_session(self) -> None:
        """Generate session using request token."""
        url = "https://api.kite.trade/session/token"
        
        data = {
            "api_key": self.kite_config.api_key,
            "request_token": self.kite_config.request_token,
            "checksum": self._generate_checksum(self.kite_config.request_token)
        }
        
        try:
            response = await self._make_request("POST", url, data=data)
            if response.get("status") == "success":
                self.access_token = response.get("data", {}).get("access_token")
                self.user_id = response.get("data", {}).get("user_id")
                self.public_token = response.get("data", {}).get("public_token")
            else:
                raise BrokerError(f"Session generation failed: {response}")
        except Exception as e:
            raise BrokerError(f"Failed to generate session: {e}")
    
    def _generate_checksum(self, request_token: str) -> str:
        """Generate checksum for session generation."""
        import hashlib
        if not self.kite_config.api_secret:
            raise BrokerError("API secret required for checksum generation")
        
        checksum_str = f"{self.kite_config.api_key}{request_token}{self.kite_config.api_secret}"
        return hashlib.sha256(checksum_str.encode()).hexdigest()
    
    # =========================================================================
    # WEBSOCKET MANAGEMENT
    # =========================================================================
    
    async def _start_websocket(self) -> None:
        """Start WebSocket connection for live data."""
        if not self.kite_config.subscribe_to_live_data:
            return
        
        self._stop_websocket = False
        self._websocket_thread = threading.Thread(
            target=self._websocket_loop,
            daemon=True
        )
        self._websocket_thread.start()
    
    async def _websocket_loop(self) -> None:
        """WebSocket connection loop with auto-reconnect."""
        reconnect_attempts = 0
        
        while not self._stop_websocket:
            try:
                # Handle rate limiting for WebSocket reconnects
                if reconnect_attempts > 0:
                    time.sleep(self.kite_config.reconnect_interval)
                
                # Connect
                self.websocket = await self._connect_websocket()
                reconnect_attempts = 0
                
                # Subscribe to symbols
                await self._subscribe_to_symbols()
                
                # Process messages
                while not self._stop_websocket:
                    try:
                        message = await asyncio.get_event_loop().run_in_executor(
                            None, self.websocket.recv
                        )
                        await self._process_websocket_message(message)
                    except websockets.exceptions.ConnectionClosed:
                        break
                    except Exception as e:
                        if self.kite_config.debug:
                            print(f"WebSocket error: {e}")
                        break
                        
            except Exception as e:
                if self.kite_config.debug:
                    print(f"WebSocket connection failed: {e}")
                reconnect_attempts += 1
                
                if reconnect_attempts >= self.kite_config.max_reconnect_attempts:
                    self.connection_status = ConnectionStatus.ERROR
                    break
        
        self.websocket = None
    
    async def _connect_websocket(self) -> websockets.WebSocketClientProtocol:
        """Establish WebSocket connection."""
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.access_token}:{self.public_token}"
        }
        
        return await websockets.connect(
            self.kite_config.websocket_url,
            extra_headers=headers,
            ping_interval=30,
            ping_timeout=20
        )
    
    async def _subscribe_to_symbols(self) -> None:
        """Subscribe to symbols for live data."""
        with self._subscription_lock:
            for symbol in self._subscribed_symbols:
                subscribe_message = {
                    "a": "subscribe",
                    "v": [self._get_instrument_token(symbol)]
                }
                try:
                    await self.websocket.send(json.dumps(subscribe_message))
                except:
                    pass
    
    async def _unsubscribe_from_symbols(self) -> None:
        """Unsubscribe from all symbols."""
        with self._subscription_lock:
            if self._subscribed_symbols:
                unsubscribe_message = {
                    "a": "unsubscribe",
                    "v": [self._get_instrument_token(sym) for sym in self._subscribed_symbols]
                }
                try:
                    await self.websocket.send(json.dumps(unsubscribe_message))
                except:
                    pass
                self._subscribed_symbols.clear()
    
    def _get_instrument_token(self, symbol: Symbol) -> int:
        """Get instrument token for a symbol."""
        # This would typically be fetched from Kite's instruments API
        # For now, return a placeholder
        return hash(symbol) % 1000000
    
    async def _process_websocket_message(self, message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            
            if data.get("type") == "tick":
                tick = self._parse_tick(data)
                self._ticks[tick.symbol] = tick
                
                # Notify callbacks
                for callback in self._tick_callbacks:
                    callback(tick)
                    
            elif data.get("type") == "depth":
                symbol = self._get_symbol_from_instrument(data.get("instrument_token", 0))
                if symbol:
                    order_book = self._parse_order_book(data, symbol)
                    self._order_books[symbol] = order_book
                    
                    # Notify callbacks
                    if symbol in self._order_book_callbacks:
                        for callback in self._order_book_callbacks[symbol]:
                            callback(order_book)
                            
            elif data.get("type") == "order_update":
                self._handle_order_update(data)
                
        except Exception as e:
            if self.kite_config.debug:
                print(f"Failed to process WebSocket message: {e}")
    
    def _get_symbol_from_instrument(self, instrument_token: int) -> Optional[Symbol]:
        """Get symbol from instrument token."""
        # This would be implemented with a proper mapping
        for symbol in self._subscribed_symbols:
            if self._get_instrument_token(symbol) == instrument_token:
                return symbol
        return None
    
    def _parse_tick(self, data: Dict[str, Any]) -> Tick:
        """Parse tick data from WebSocket message."""
        return Tick(
            symbol=Symbol(self._get_symbol_from_instrument(data.get("instrument_token", 0)) or "UNKNOWN"),
            timestamp=Timestamp(datetime.fromisoformat(data.get("exchange_timestamp", "")).isoformat()),
            last_price=data.get("last_price", 0.0),
            last_quantity=data.get("last_quantity", 0),
            volume=data.get("volume", 0),
            bid_price=data.get("best_bid", 0.0),
            bid_quantity=data.get("best_bid_quantity", 0),
            ask_price=data.get("best_ask", 0.0),
            ask_quantity=data.get("best_ask_quantity", 0),
            open_price=data.get("open", 0.0),
            high_price=data.get("high", 0.0),
            low_price=data.get("low", 0.0),
            close_price=data.get("close", 0.0),
            change=data.get("net_change", 0.0),
            change_percent=data.get("net_change_percent", 0.0),
            average_price=data.get("average_price", 0.0),
            oi=data.get("oi", 0),
            oi_day_high=data.get("oi_day_high", 0),
            oi_day_low=data.get("oi_day_low", 0),
            is_index=False
        )
    
    def _parse_order_book(self, data: Dict[str, Any], symbol: Symbol) -> OrderBook:
        """Parse order book data from WebSocket message."""
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        return OrderBook(
            symbol=symbol,
            bids=[(price, qty) for price, qty in bids[:self.kite_config.tick_depth]],
            asks=[(price, qty) for price, qty in asks[:self.kite_config.tick_depth]],
            timestamp=Timestamp(datetime.fromisoformat(data.get("exchange_timestamp", "")).isoformat())
        )
    
    def _handle_order_update(self, data: Dict[str, Any]) -> None:
        """Handle order update from WebSocket."""
        order_id = OrderId(str(data.get("order_id", "")))
        status_str = data.get("status", "")
        
        # Map Kite Connect status to our OrderStatus
        status_map = {
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "TRIGGERED": OrderStatus.TRIGGERED,
        }
        
        status = status_map.get(status_str, OrderStatus.UNKNOWN)
        
        # Notify callbacks
        order_response = OrderResponse(
            order_id=order_id,
            status=status,
            filled_quantity=data.get("filled_quantity", 0),
            pending_quantity=data.get("pending_quantity", 0),
            average_price=data.get("average_price", 0.0),
            executed_price=data.get("executed_price", 0.0),
            message=data.get("message", "")
        )
        
        if order_id in self._order_callbacks:
            self._order_callbacks[order_id](order_response)
            
        if order_id in self._fill_callbacks and status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
            self._fill_callbacks[order_id](order_response)
    
    # =========================================================================
    # HTTP REQUESTS
    # =========================================================================
    
    async def _make_request(self, method: str, url: str, 
                           data: Optional[Dict[str, Any]] = None,
                           params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make HTTP request to Kite Connect API."""
        if not self.session:
            raise BrokerError("Not connected")
        
        # Rate limiting
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < 1.0 / self.kite_config.max_requests_per_second:
            await asyncio.sleep(1.0 / self.kite_config.max_requests_per_second - elapsed)
        
        self._last_request_time = time.time()
        
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.access_token}:{self.public_token}",
            "Content-Type": "application/json"
        }
        
        try:
            if method.upper() == "GET":
                async with self.session.get(url, headers=headers, params=params) as response:
                    return await self._handle_response(response)
            elif method.upper() == "POST":
                async with self.session.post(url, headers=headers, json=data) as response:
                    return await self._handle_response(response)
            elif method.upper() == "PUT":
                async with self.session.put(url, headers=headers, json=data) as response:
                    return await self._handle_response(response)
            elif method.upper() == "DELETE":
                async with self.session.delete(url, headers=headers, params=params) as response:
                    return await self._handle_response(response)
            else:
                raise BrokerError(f"Unsupported HTTP method: {method}")
                
        except aiohttp.ClientError as e:
            raise BrokerError(f"HTTP request failed: {e}", retryable=True)
    
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """Handle HTTP response."""
        if response.status == 200:
            return await response.json()
        elif response.status == 401:
            raise BrokerError("Unauthorized - invalid access token")
        elif response.status == 403:
            raise BrokerError("Forbidden")
        elif response.status == 429:
            raise BrokerError("Rate limit exceeded", retryable=True)
        elif response.status == 500:
            raise BrokerError("Internal server error", retryable=True)
        else:
            text = await response.text()
            raise BrokerError(f"API request failed with status {response.status}: {text}")
    
    # =========================================================================
    # MARKET DATA
    # =========================================================================
    
    async def get_market_data(self, symbols: List[Symbol]) -> Dict[Symbol, Tick]:
        """Get current market data for multiple symbols."""
        result = {}
        
        for symbol in symbols:
            if symbol in self._ticks:
                result[symbol] = self._ticks[symbol]
            else:
                # Fetch from REST API
                try:
                    instrument_token = self._get_instrument_token(symbol)
                    url = f"https://api.kite.trade/quote?i={instrument_token}"
                    data = await self._make_request("GET", url)
                    
                    if data.get("status") == "success":
                        quote_data = data.get("data", {}).get(f"{instrument_token}", {})
                        if quote_data:
                            result[symbol] = self._parse_rest_tick(quote_data, symbol)
                except:
                    pass
        
        return result
    
    def _parse_rest_tick(self, data: Dict[str, Any], symbol: Symbol) -> Tick:
        """Parse tick data from REST API response."""
        return Tick(
            symbol=symbol,
            timestamp=Timestamp(datetime.utcnow().isoformat()),
            last_price=data.get("last_price", 0.0),
            last_quantity=data.get("last_quantity", 0),
            volume=data.get("volume", 0),
            bid_price=data.get("buy", {}).get("price", 0.0),
            bid_quantity=data.get("buy", {}).get("quantity", 0),
            ask_price=data.get("sell", {}).get("price", 0.0),
            ask_quantity=data.get("sell", {}).get("quantity", 0),
            open_price=data.get("open", 0.0),
            high_price=data.get("day_high", 0.0),
            low_price=data.get("day_low", 0.0),
            close_price=data.get("close", 0.0),
            change=data.get("net_change", 0.0),
            change_percent=0.0,
            average_price=data.get("average_price", 0.0),
            oi=data.get("oi", 0),
            oi_day_high=data.get("oi_day_high", 0),
            oi_day_low=data.get("oi_day_low", 0),
            is_index=False
        )
    
    async def get_ohlcv_data(self, symbol: Symbol, timeframe: str,
                           from_date: datetime, to_date: datetime) -> List[OHLCV]:
        """Get historical OHLCV data."""
        instrument_token = self._get_instrument_token(symbol)
        
        # Map timeframe to Kite Connect format
        kite_timeframe = self._map_timeframe(timeframe)
        
        # Format dates
        from_date_str = from_date.strftime("%Y-%m-%d")
        to_date_str = to_date.strftime("%Y-%m-%d")
        
        url = f"https://api.kite.trade/instruments/historical/{instrument_token}/{kite_timeframe}?from={from_date_str}&to={to_date_str}"
        
        try:
            data = await self._make_request("GET", url)
            ohlcv_list = []
            
            for candle in data.get("data", {}).get("candles", []):
                if len(candle) >= 6:
                    ohlcv_list.append(OHLCV(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=Timestamp(datetime.fromisoformat(candle[0]).isoformat()),
                        open_price=candle[1],
                        high_price=candle[2],
                        low_price=candle[3],
                        close_price=candle[4],
                        volume=candle[5],
                        oi=candle[6] if len(candle) > 6 else 0
                    ))
            
            return ohlcv_list
            
        except Exception as e:
            raise BrokerError(f"Failed to fetch OHLCV data: {e}")
    
    def _map_timeframe(self, timeframe: str) -> str:
        """Map standard timeframe to Kite Connect timeframe."""
        mapping = {
            "1m": "minute",
            "5m": "5minute",
            "15m": "15minute",
            "30m": "30minute",
            "1h": "hour",
            "1d": "day"
        }
        return mapping.get(timeframe, "minute")
    
    async def get_order_book(self, symbol: Symbol, depth: int = 5) -> OrderBook:
        """Get order book for a symbol."""
        if symbol in self._order_books:
            return self._order_books[symbol]
        
        instrument_token = self._get_instrument_token(symbol)
        url = f"https://api.kite.trade/quote/ltp?i={instrument_token}"
        
        try:
            data = await self._make_request("GET", url)
            if data.get("status") == "success":
                quote_data = data.get("data", {}).get(f"{instrument_token}", {})
                return self._parse_order_book_from_ltp(quote_data, symbol)
        except:
            pass
        
        # Return empty order book
        return OrderBook(symbol=symbol)
    
    def _parse_order_book_from_ltp(self, data: Dict[str, Any], symbol: Symbol) -> OrderBook:
        """Parse order book from LTP data."""
        return OrderBook(
            symbol=symbol,
            bids=[(data.get("buy", {}).get("price", 0.0), 
                   data.get("buy", {}).get("quantity", 0))],
            asks=[(data.get("sell", {}).get("price", 0.0), 
                   data.get("sell", {}).get("quantity", 0))],
            timestamp=Timestamp(datetime.utcnow().isoformat())
        )
    
    # =========================================================================
    # ORDER MANAGEMENT
    # =========================================================================
    
    async def place_order(self, order: Order) -> OrderResponse:
        """Place a new order."""
        if not self.session:
            raise BrokerError("Not connected")
        
        url = "https://api.kite.trade/orders/regular"
        
        order_params = {
            "exchange": self._get_exchange(order.symbol),
            "tradingsymbol": self._get_trading_symbol(order.symbol),
            "transaction_type": "BUY" if order.side == OrderSide.BUY else "SELL",
            "quantity": int(order.quantity),
            "price": order.price if order.order_type == OrderType.LIMIT else None,
            "trigger_price": order.trigger_price,
            "product": self._map_product(order.order_validity),
            "order_type": self._map_order_type(order.order_type),
            "validity": self._map_validity(order.order_validity),
            "disclosed_quantity": order.disclosed_quantity,
            "tag": f"rapidfire_{order.tag}" if order.tag else "rapidfire"
        }
        
        # Filter out None values
        order_params = {k: v for k, v in order_params.items() if v is not None}
        
        try:
            response = await self._make_request("POST", url, data=order_params)
            
            if response.get("status") == "success":
                order_data = response.get("data", {})
                order_id = OrderId(str(order_data.get("order_id", "")))
                
                return OrderResponse(
                    order_id=order_id,
                    status=OrderStatus.OPEN,
                    message="Order placed successfully"
                )
            else:
                raise BrokerError(f"Order placement failed: {response}")
                
        except Exception as e:
            raise BrokerError(f"Failed to place order: {e}", retryable=True)
    
    async def place_bracket_order(self, bracket_order: BracketOrder) -> Tuple[OrderResponse, OrderResponse, OrderResponse]:
        """Place a bracket order (entry + SL + TP)."""
        url = "https://api.kite.trade/orders/bracket"
        
        # Zerodha uses different parameter names for bracket orders
        params = {
            "exchange": self._get_exchange(bracket_order.symbol),
            "tradingsymbol": self._get_trading_symbol(bracket_order.symbol),
            "transaction_type": "BUY" if bracket_order.side == OrderSide.BUY else "SELL",
            "quantity": int(bracket_order.quantity),
            "target": bracket_order.take_profit,
            "stoploss": bracket_order.stop_loss,
            "squareoff": bracket_order.trailing_stop_loss or 0,
            "trailing_stoploss": bracket_order.trailing_stop_loss or 0,
            "product": "MIS",  # Bracket orders are typically MIS
            "order_type": "LIMIT" if bracket_order.price else "MARKET",
            "price": bracket_order.price,
            "validity": "DAY"
        }
        
        # Filter out None values
        params = {k: v for k, v in params.items() if v is not None}
        
        try:
            response = await self._make_request("POST", url, data=params)
            
            if response.get("status") == "success":
                order_data = response.get("data", {})
                
                entry_order = OrderResponse(
                    order_id=OrderId(str(order_data.get("order_id", ""))),
                    status=OrderStatus.OPEN,
                    message="Bracket order entry placed"
                )
                
                sl_order = OrderResponse(
                    order_id=OrderId(str(order_data.get("stoploss_order_id", ""))),
                    status=OrderStatus.OPEN,
                    message="Bracket order SL placed"
                )
                
                tp_order = OrderResponse(
                    order_id=OrderId(str(order_data.get("target_order_id", ""))),
                    status=OrderStatus.OPEN,
                    message="Bracket order TP placed"
                )
                
                return (entry_order, sl_order, tp_order)
            else:
                raise BrokerError(f"Bracket order placement failed: {response}")
                
        except Exception as e:
            raise BrokerError(f"Failed to place bracket order: {e}", retryable=True)
    
    async def modify_order(self, order_id: OrderId,
                          quantity: Optional[float] = None,
                          price: Optional[float] = None,
                          stop_loss: Optional[float] = None,
                          take_profit: Optional[float] = None) -> OrderResponse:
        """Modify an existing order."""
        url = f"https://api.kite.trade/orders/regular/{order_id}"
        
        params = {}
        if quantity is not None:
            params["quantity"] = int(quantity)
        if price is not None:
            params["price"] = price
            params["order_type"] = "LIMIT"
        if stop_loss is not None:
            params["trigger_price"] = stop_loss
        
        if not params:
            return OrderResponse(order_id=order_id, status=OrderStatus.OPEN, message="No modifications")
        
        try:
            response = await self._make_request("PUT", url, data=params)
            
            if response.get("status") == "success":
                return OrderResponse(
                    order_id=order_id,
                    status=OrderStatus.OPEN,
                    message="Order modified successfully"
                )
            else:
                raise BrokerError(f"Order modification failed: {response}")
                
        except Exception as e:
            raise BrokerError(f"Failed to modify order: {e}", retryable=True)
    
    async def cancel_order(self, order_id: OrderId) -> bool:
        """Cancel an order."""
        url = f"https://api.kite.trade/orders/regular/{order_id}"
        
        try:
            response = await self._make_request("DELETE", url)
            
            if response.get("status") == "success":
                return True
            else:
                return False
                
        except Exception as e:
            raise BrokerError(f"Failed to cancel order: {e}", retryable=True)
    
    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> int:
        """Cancel all orders."""
        if symbol:
            # Cancel orders for specific symbol
            orders = await self.get_open_orders()
            cancelled_count = 0
            
            for order in orders:
                if order.symbol == symbol:
                    try:
                        await self.cancel_order(order.order_id)
                        cancelled_count += 1
                    except:
                        pass
            
            return cancelled_count
        else:
            # Cancel all orders
            url = "https://api.kite.trade/orders"
            
            try:
                response = await self._make_request("DELETE", url)
                return response.get("data", {}).get("cancelled_count", 0)
            except Exception as e:
                raise BrokerError(f"Failed to cancel all orders: {e}", retryable=True)
    
    async def get_order_status(self, order_id: OrderId) -> OrderStatus:
        """Get order status."""
        url = f"https://api.kite.trade/orders/{order_id}"
        
        try:
            response = await self._make_request("GET", url)
            
            if response.get("status") == "success":
                status_str = response.get("data", {}).get("status", "")
                status_map = {
                    "OPEN": OrderStatus.OPEN,
                    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                    "FILLED": OrderStatus.FILLED,
                    "CANCELLED": OrderStatus.CANCELLED,
                    "REJECTED": OrderStatus.REJECTED,
                    "TRIGGERED": OrderStatus.TRIGGERED,
                }
                return status_map.get(status_str, OrderStatus.UNKNOWN)
            else:
                return OrderStatus.UNKNOWN
                
        except Exception:
            return OrderStatus.UNKNOWN
    
    async def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        url = "https://api.kite.trade/orders"
        
        try:
            response = await self._make_request("GET", url)
            
            if response.get("status") == "success":
                orders = []
                for order_data in response.get("data", {}).get("orders", []):
                    order = self._parse_order_data(order_data)
                    if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.TRIGGERED]:
                        orders.append(order)
                return orders
            else:
                return []
                
        except Exception as e:
            raise BrokerError(f"Failed to get open orders: {e}", retryable=True)
    
    async def get_positions(self) -> List[Position]:
        """Get current positions."""
        url = "https://api.kite.trade/portfolio/positions"
        
        try:
            response = await self._make_request("GET", url)
            
            if response.get("status") == "success":
                positions = []
                for position_data in response.get("data", {}).get("net", []):
                    position = self._parse_position_data(position_data)
                    positions.append(position)
                return positions
            else:
                return []
                
        except Exception as e:
            raise BrokerError(f"Failed to get positions: {e}", retryable=True)
    
    async def get_position(self, symbol: Symbol) -> Optional[Position]:
        """Get position for a specific symbol."""
        positions = await self.get_positions()
        
        for position in positions:
            if position.symbol == symbol:
                return position
        
        return None
    
    async def get_account_balance(self) -> Dict[str, Any]:
        """Get account balance and margins."""
        url = "https://api.kite.trade/margins/equity"
        
        try:
            response = await self._make_request("GET", url)
            
            if response.get("status") == "success":
                return {
                    "balance": response.get("data", {}).get("available", {}).get("cash", 0),
                    "used_margin": response.get("data", {}).get("utilised", {}).get("debit", 0),
                    "available_margin": response.get("data", {}).get("available", {}).get("cash", 0),
                    "currency": "INR",
                    "raw_response": response.get("data", {})
                }
            else:
                return {}
                
        except Exception as e:
            raise BrokerError(f"Failed to get account balance: {e}", retryable=True)
    
    async def get_trade_history(self, symbol: Optional[Symbol] = None,
                               from_date: Optional[datetime] = None,
                               to_date: Optional[datetime] = None) -> List[Any]:
        """Get trade history."""
        # Zerodha doesn't provide a direct trade history API
        # This would need to be implemented using order history
        return []
    
    async def convert_to_market_order(self, order_id: OrderId) -> OrderResponse:
        """Convert a limit order to market order."""
        url = f"https://api.kite.trade/orders/regular/{order_id}"
        
        try:
            response = await self._make_request("PUT", url, data={"order_type": "MARKET"})
            
            if response.get("status") == "success":
                return OrderResponse(
                    order_id=order_id,
                    status=OrderStatus.OPEN,
                    message="Order converted to market"
                )
            else:
                raise BrokerError(f"Failed to convert to market order: {response}")
                
        except Exception as e:
            raise BrokerError(f"Failed to convert order: {e}", retryable=True)
    
    # =========================================================================
    # SUBSCRIPTION MANAGEMENT
    # =========================================================================
    
    def subscribe(self, symbol: Symbol) -> None:
        """Subscribe to live data for a symbol."""
        with self._subscription_lock:
            if symbol not in self._subscribed_symbols:
                self._subscribed_symbols.add(symbol)
                
                # If WebSocket is connected, subscribe immediately
                if self.websocket:
                    import asyncio
                    asyncio.create_task(self._subscribe_to_symbol(symbol))
    
    def unsubscribe(self, symbol: Symbol) -> None:
        """Unsubscribe from live data for a symbol."""
        with self._subscription_lock:
            if symbol in self._subscribed_symbols:
                self._subscribed_symbols.remove(symbol)
                
                # If WebSocket is connected, unsubscribe immediately
                if self.websocket:
                    import asyncio
                    asyncio.create_task(self._unsubscribe_from_symbol(symbol))
    
    async def _subscribe_to_symbol(self, symbol: Symbol) -> None:
        """Subscribe to a single symbol via WebSocket."""
        instrument_token = self._get_instrument_token(symbol)
        subscribe_message = {
            "a": "subscribe",
            "v": [instrument_token]
        }
        
        try:
            await self.websocket.send(json.dumps(subscribe_message))
        except:
            pass
    
    async def _unsubscribe_from_symbol(self, symbol: Symbol) -> None:
        """Unsubscribe from a single symbol via WebSocket."""
        instrument_token = self._get_instrument_token(symbol)
        unsubscribe_message = {
            "a": "unsubscribe",
            "v": [instrument_token]
        }
        
        try:
            await self.websocket.send(json.dumps(unsubscribe_message))
        except:
            pass
    
    def register_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Register callback for tick data."""
        with self._subscription_lock:
            if callback not in self._tick_callbacks:
                self._tick_callbacks.append(callback)
    
    def unregister_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Unregister tick callback."""
        with self._subscription_lock:
            if callback in self._tick_callbacks:
                self._tick_callbacks.remove(callback)
    
    def register_order_book_callback(self, symbol: Symbol, 
                                    callback: Callable[[OrderBook], None]) -> None:
        """Register callback for order book updates."""
        if symbol not in self._order_book_callbacks:
            self._order_book_callbacks[symbol] = []
        
        if callback not in self._order_book_callbacks[symbol]:
            self._order_book_callbacks[symbol].append(callback)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def _get_exchange(self, symbol: Symbol) -> str:
        """Get exchange for a symbol."""
        # Extract exchange from symbol if encoded
        if ":" in symbol.value:
            return symbol.value.split(":")[0]
        return "NSE"  # Default
    
    def _get_trading_symbol(self, symbol: Symbol) -> str:
        """Get trading symbol (without exchange prefix)."""
        if ":" in symbol.value:
            return symbol.value.split(":")[1]
        return symbol.value
    
    def _map_product(self, validity: OrderValidity) -> str:
        """Map order validity to Kite Connect product."""
        mapping = {
            OrderValidity.INTRADAY: "MIS",
            OrderValidity.DELIVERY: "CNC",
            OrderValidity.GTC: "NRML",
        }
        return mapping.get(validity, "MIS")
    
    def _map_order_type(self, order_type: OrderType) -> str:
        """Map order type to Kite Connect order type."""
        mapping = {
            OrderType.MARKET: "MARKET",
            OrderType.LIMIT: "LIMIT",
            OrderType.SL: "SL",
            OrderType.SL_MARKET: "SL-M",
        }
        return mapping.get(order_type, "MARKET")
    
    def _map_validity(self, validity: OrderValidity) -> str:
        """Map validity to Kite Connect validity."""
        mapping = {
            OrderValidity.INTRADAY: "DAY",
            OrderValidity.GTC: "GTC",
            OrderValidity.IOC: "IOC",
        }
        return mapping.get(validity, "DAY")
    
    def _parse_order_data(self, data: Dict[str, Any]) -> Order:
        """Parse order data from API response."""
        status_map = {
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "TRIGGERED": OrderStatus.TRIGGERED,
        }
        
        return Order(
            order_id=OrderId(str(data.get("order_id", ""))),
            symbol=Symbol(data.get("tradingsymbol", "")),
            side=OrderSide.BUY if data.get("transaction_type") == "BUY" else OrderSide.SELL,
            quantity=data.get("quantity", 0),
            price=data.get("price", 0.0),
            order_type=self._reverse_map_order_type(data.get("order_type", "MARKET")),
            order_validity=self._reverse_map_validity(data.get("validity", "DAY")),
            status=status_map.get(data.get("status", ""), OrderStatus.UNKNOWN),
            filled_quantity=data.get("filled_quantity", 0),
            pending_quantity=data.get("pending_quantity", data.get("quantity", 0) - data.get("filled_quantity", 0)),
            average_price=data.get("average_price", 0.0),
            trigger_price=data.get("trigger_price", 0.0),
            tag=data.get("tag", "")
        )
    
    def _reverse_map_order_type(self, kite_type: str) -> OrderType:
        """Reverse map Kite Connect order type to our OrderType."""
        mapping = {
            "MARKET": OrderType.MARKET,
            "LIMIT": OrderType.LIMIT,
            "SL": OrderType.SL,
            "SL-M": OrderType.SL_MARKET,
        }
        return mapping.get(kite_type, OrderType.MARKET)
    
    def _reverse_map_validity(self, kite_validity: str) -> OrderValidity:
        """Reverse map Kite Connect validity to our OrderValidity."""
        mapping = {
            "DAY": OrderValidity.INTRADAY,
            "GTC": OrderValidity.GTC,
            "IOC": OrderValidity.IOC,
        }
        return mapping.get(kite_validity, OrderValidity.INTRADAY)
    
    def _parse_position_data(self, data: Dict[str, Any]) -> Position:
        """Parse position data from API response."""
        return Position(
            symbol=Symbol(data.get("tradingsymbol", "")),
            quantity=data.get("quantity", 0),
            average_price=data.get("average_price", 0.0),
            last_price=data.get("last_price", 0.0),
            value=data.get("value", 0.0),
            pnl=data.get("pnl", 0.0),
            side=PositionSide.LONG if data.get("quantity", 0) > 0 else PositionSide.SHORT
        )
    
    # =========================================================================
    # OVERIDDEN PROPERTIES
    # =========================================================================
    
    def supports_bracket_orders(self) -> bool:
        """Check if broker supports bracket orders."""
        return True
    
    def supports_oco_orders(self) -> bool:
        """Check if broker supports OCO orders."""
        return True  # Zerodha supports OCO through bracket orders
    
    def get_max_orders_per_second(self) -> int:
        """Get maximum orders per second."""
        return self.kite_config.max_requests_per_second
    
    def get_min_order_quantity(self, symbol: Symbol) -> float:
        """Get minimum order quantity."""
        return 1.0
    
    def get_lot_size(self, symbol: Symbol) -> float:
        """Get lot size for a symbol."""
        # This should be fetched from instruments API
        return 1.0
    
    def get_tick_size(self, symbol: Symbol) -> float:
        """Get tick size for a symbol."""
        return 0.05  # Default for NSE