"""
WebSocket and REST data feed for the Rapid Fire Intraday Trading System.

This module provides real-time market data through WebSocket connections and
historical data through REST APIs. It handles tick data, OHLCV candles,
resampling, gap handling, and data validation.

Key Features:
- WebSocket connection management with auto-reconnect
- Tick data aggregation to multiple timeframes
- Rolling window management for OHLCV data
- Data validation and gap filling
- SQLite storage for historical data
- Async/await support for non-blocking operations
"""

import asyncio
import json
import os
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import threading

import aiohttp
import numpy as np
import pandas as pd

from intraday_system.config_loader import get_config_loader, parse_timeframe
from intraday_system.logger import data_feed_logger, get_logger
from intraday_system.models import (
    OHLCV, OrderId, Symbol, Tick, TimeFrame, Timestamp
)


# =============================================================================
# DATA STORAGE AND CACHE
# =============================================================================

@dataclass
class DataCache:
    """In-memory cache for market data."""
    
    # Raw tick data: {symbol: deque(Tick)}
    ticks: Dict[Symbol, deque] = field(default_factory=dict)
    
    # OHLCV data: {symbol: {timeframe: deque(OHLCV)}}
    ohlcv: Dict[Symbol, Dict[TimeFrame, deque]] = field(default_factory=dict)
    
    # Latest data: {symbol: Tick}
    latest_ticks: Dict[Symbol, Tick] = field(default_factory=dict)
    
    # Latest OHLCV: {symbol: {timeframe: OHLCV}}
    latest_ohlcv: Dict[Symbol, Dict[TimeFrame, OHLCV]] = field(default_factory=dict)
    
    # Last update times: {symbol: Timestamp}
    last_update: Dict[Symbol, Timestamp] = field(default_factory=dict)
    
    # Data locks: {symbol: threading.Lock}
    locks: Dict[Symbol, threading.Lock] = field(default_factory=dict)
    
    # Buffer sizes
    tick_buffer_size: int = 10000
    ohlcv_buffer_size: int = 5000
    
    def get_lock(self, symbol: Symbol) -> threading.Lock:
        """Get or create a lock for a symbol."""
        if symbol not in self.locks:
            self.locks[symbol] = threading.Lock()
        return self.locks[symbol]
    
    def get_ticks(self, symbol: Symbol, limit: Optional[int] = None) -> List[Tick]:
        """Get tick data for a symbol."""
        with self.get_lock(symbol):
            if symbol not in self.ticks:
                return []
            ticks = list(self.ticks[symbol])
            return ticks[-limit:] if limit else ticks
    
    def get_ohlcv(self, symbol: Symbol, timeframe: TimeFrame, 
                  limit: Optional[int] = None) -> List[OHLCV]:
        """Get OHLCV data for a symbol and timeframe."""
        with self.get_lock(symbol):
            if symbol not in self.ohlcv or timeframe not in self.ohlcv[symbol]:
                return []
            ohlcv_data = list(self.ohlcv[symbol][timeframe])
            return ohlcv_data[-limit:] if limit else ohlcv_data
    
    def add_tick(self, tick: Tick) -> None:
        """Add a tick to the cache."""
        symbol = tick.symbol
        with self.get_lock(symbol):
            # Initialize if not exists
            if symbol not in self.ticks:
                self.ticks[symbol] = deque(maxlen=self.tick_buffer_size)
            
            # Add tick
            self.ticks[symbol].append(tick)
            self.latest_ticks[symbol] = tick
            self.last_update[symbol] = tick.timestamp
    
    def add_ohlcv(self, ohlcv: OHLCV) -> None:
        """Add OHLCV data to the cache."""
        symbol = ohlcv.symbol
        timeframe = ohlcv.timeframe
        
        with self.get_lock(symbol):
            # Initialize if not exists
            if symbol not in self.ohlcv:
                self.ohlcv[symbol] = {}
            if timeframe not in self.ohlcv[symbol]:
                self.ohlcv[symbol][timeframe] = deque(maxlen=self.ohlcv_buffer_size)
            
            # Add OHLCV
            self.ohlcv[symbol][timeframe].append(ohlcv)
            self.latest_ohlcv.setdefault(symbol, {})[timeframe] = ohlcv
    
    def get_current_ohlcv(self, symbol: Symbol, timeframe: TimeFrame) -> Optional[OHLCV]:
        """Get the most recent OHLCV for a symbol and timeframe."""
        with self.get_lock(symbol):
            if symbol in self.latest_ohlcv and timeframe in self.latest_ohlcv[symbol]:
                return self.latest_ohlcv[symbol][timeframe]
            return None
    
    def get_current_tick(self, symbol: Symbol) -> Optional[Tick]:
        """Get the most recent tick for a symbol."""
        with self.get_lock(symbol):
            return self.latest_ticks.get(symbol)
    
    def clear_symbol(self, symbol: Symbol) -> None:
        """Clear all data for a symbol."""
        with self.get_lock(symbol):
            if symbol in self.ticks:
                del self.ticks[symbol]
            if symbol in self.ohlcv:
                del self.ohlcv[symbol]
            if symbol in self.latest_ticks:
                del self.latest_ticks[symbol]
            if symbol in self.latest_ohlcv:
                del self.latest_ohlcv[symbol]
            if symbol in self.last_update:
                del self.last_update[symbol]


@dataclass
class Subscriber:
    """Subscriber to data feed events."""
    callback: Callable
    symbol: Optional[Symbol] = None
    data_type: str = "all"  # 'tick', 'ohlcv', 'all'


class DataFeed:
    """Main data feed class managing WebSocket and REST data."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the data feed."""
        self.config = config or get_config_loader().load()
        self.logger = data_feed_logger()
        
        # Data cache
        self.cache = DataCache()
        
        # SQLite database for historical storage
        self.db_path = self.config.get('system', {}).get('candle_db_path', 'data/candles.sqlite')
        self._init_db()
        
        # WebSocket configuration
        self.ws_config = self.config.get('market_data', {}).get('websocket', {})
        self.symbols = self._get_symbols()
        
        # Connection state
        self.ws_connected = False
        self.ws_reconnect_attempts = 0
        self.ws_last_ping = datetime.utcnow()
        
        # Subscribers
        self.subscribers: List[Subscriber] = []
        
        # Threading
        self._thread: Optional[threading.Thread] = None
        self._running = False
        
        # Event loop for async operations
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
        # Aggregation settings
        self.aggregation_interval_ms = self.ws_config.get('tick_aggregation_interval_ms', 100)
        self._last_aggregation_time = 0
        
        # Bars currently being built from ticks: {(symbol, timeframe): OHLCV}
        self._building_bars: Dict[Tuple[Symbol, TimeFrame], OHLCV] = {}
        self._bar_timeframes = [TimeFrame._1M, TimeFrame._5M]
        self._last_cum_volume: Dict[Symbol, float] = {}
        
        # Persisting every tick to SQLite is expensive; off by default
        self.store_ticks = bool(self.config.get('market_data', {}).get('store_ticks', False))
        
        # Load historical data
        self._load_historical_data()
        
        self.logger.info(f"DataFeed initialized with {len(self.symbols)} symbols")
    
    def _get_symbols(self) -> List[Symbol]:
        """Get symbols from configuration."""
        symbols = self.ws_config.get('symbols', [])
        if isinstance(symbols, str):
            return [s.strip() for s in symbols.split(',') if s.strip()]
        return symbols
    
    def _init_db(self) -> None:
        """Initialize SQLite database for candle storage."""
        try:
            # Create directory if it doesn't exist
            db_dir = self.db_path.rsplit('/', 1)[0]
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create candles table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS candles (
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL NOT NULL,
                        PRIMARY KEY (symbol, timestamp, timeframe)
                    )
                ''')
                
                # Create ticks table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ticks (
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        last_price REAL NOT NULL,
                        last_quantity REAL,
                        bid_price REAL,
                        bid_quantity REAL,
                        ask_price REAL,
                        ask_quantity REAL,
                        volume REAL,
                        open_interest REAL,
                        PRIMARY KEY (symbol, timestamp)
                    )
                ''')
                
                # Create indexes
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_candles_symbol_timeframe 
                    ON candles (symbol, timeframe)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_candles_timestamp 
                    ON candles (timestamp)
                ''')
                
                conn.commit()
            
            self.logger.info(f"SQLite database initialized at {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
    
    def _load_historical_data(self) -> None:
        """Load historical data from database into cache."""
        try:
            # For each symbol and timeframe, load recent data
            for symbol in self.symbols:
                for timeframe in [TimeFrame._1M, TimeFrame._5M]:
                    ohlcv_data = self._get_ohlcv_from_db(
                        symbol, timeframe, limit=1000
                    )
                    for ohlcv in ohlcv_data:
                        self.cache.add_ohlcv(ohlcv)
            
            self.logger.info("Historical data loaded into cache")
        except Exception as e:
            self.logger.error(f"Failed to load historical data: {e}")
    
    def start(self) -> None:
        """Start the data feed."""
        if self._running:
            self.logger.warning("Data feed already running")
            return
        
        self._running = True
        
        # Start the main thread
        self._thread = threading.Thread(
            target=self._run,
            name="DataFeed-Thread",
            daemon=True
        )
        self._thread.start()
        
        self.logger.info("Data feed started")
    
    def stop(self) -> None:
        """Stop the data feed."""
        self._running = False
        
        if getattr(self, '_zerodha', None) is not None:
            self._zerodha.stop_ticker()
        
        # Close WebSocket connection
        if hasattr(self, '_ws_client') and self._ws_client:
            try:
                self._ws_client.close()
            except Exception as e:
                self.logger.error(f"Error closing WebSocket: {e}")
        
        # Wait for thread to finish
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        
        # Close event loop
        if self._loop:
            try:
                self._loop.close()
            except Exception as e:
                self.logger.error(f"Error closing event loop: {e}")
        
        self.logger.info("Data feed stopped")
    
    def _run(self) -> None:
        """Main data feed loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        try:
            self._loop.run_until_complete(self._main_async())
        except Exception as e:
            self.logger.error(f"Data feed loop terminated: {e}")
        
        finally:
            # Clean up
            if hasattr(self, '_ws_client') and self._ws_client:
                try:
                    self._loop.run_until_complete(self._ws_client.close())
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket in thread: {e}")
    
    async def _main_async(self) -> None:
        """Start the feed, then supervise it until stopped."""
        await self._start_websocket()
        while self._running:
            try:
                await self._process_aggregations()
                self._check_websocket_health()
            except Exception as e:
                self.logger.error(f"Error in data feed loop: {e}")
            await asyncio.sleep(0.1)
        
        # Let background tasks (mock generator, listeners) finish
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _start_websocket(self) -> None:
        """Start WebSocket connection based on broker type."""
        broker_type = self.config.get('broker', {}).get('active', 'PAPER')
        
        if broker_type == 'ZERODHA':
            await self._start_zerodha_websocket()
        elif broker_type == 'ALPACA':
            await self._start_alpaca_websocket()
        elif broker_type == 'INTERACTIVE_BROKERS':
            self.logger.warning("Interactive Brokers WebSocket not implemented yet")
        else:
            # Paper trading - use mock data
            self.logger.info("Running in paper trading mode - using mock data")
            await self._start_mock_websocket()
    
    async def _start_zerodha_websocket(self) -> None:
        """Stream live ticks from Zerodha's KiteTicker into handle_tick()."""
        from intraday_system.brokers.zerodha_broker import ZerodhaMarketData
        
        def on_status(connected: bool) -> None:
            self.ws_connected = connected
            self.logger.info(f"Zerodha ticker {'connected' if connected else 'disconnected'}")
        
        try:
            self._zerodha = ZerodhaMarketData.from_config(self.config)
            self._zerodha.start_ticker(self.symbols, self.handle_tick, on_status)
            self.ws_reconnect_attempts = 0
            self.logger.info(f"Zerodha ticker started for {len(self.symbols)} symbols")
        except Exception as e:
            self.logger.error(f"Failed to start Zerodha ticker: {e}")
            self.ws_connected = False
    
    async def _start_alpaca_websocket(self) -> None:
        """Start Alpaca WebSocket."""
        try:
            broker_config = self.config.get('broker', {}).get('alpaca', {})
            api_key = broker_config.get('api_key')
            api_secret = broker_config.get('api_secret')
            
            if not api_key or not api_secret:
                self.logger.error("Alpaca API key and secret are required")
                return
            
            data_ws_url = broker_config.get('data_websocket_url', 
                                            'wss://stream.data.alpaca.co')
            
            self._ws_client = await self._connect_websocket(
                data_ws_url,
                self.symbols,
                {
                    "action": "auth",
                    "key": api_key,
                    "secret": api_secret
                }
            )
            
            self.ws_connected = True
            self.ws_reconnect_attempts = 0
            self.logger.info(f"Alpaca WebSocket connected with {len(self.symbols)} symbols")
            
        except Exception as e:
            self.logger.error(f"Failed to start Alpaca WebSocket: {e}")
            self.ws_connected = False
    
    async def _start_mock_websocket(self) -> None:
        """Start mock WebSocket for paper trading."""
        self.logger.info("Starting mock WebSocket for paper trading")
        self.ws_connected = True
        
        # Start mock data generator
        asyncio.create_task(self._generate_mock_data())
    
    async def _connect_websocket(self, url: str, symbols: List[str], 
                               auth_data: Optional[Dict[str, Any]] = None) -> Any:
        """Connect to WebSocket and subscribe to symbols."""
        import websockets
        
        # Build subscription message
        if 'kite' in url:
            # Zerodha format
            symbol_tokens = self._get_zerodha_tokens(symbols)
            subscribe_msg = json.dumps({
                "a": "subscribe",
                "v": symbol_tokens
            })
        else:
            # Generic format
            subscribe_msg = json.dumps({
                "action": "subscribe",
                "symbols": symbols
            })
        
        try:
            # Connect with authentication
            headers = None
            if auth_data:
                headers = {"Authorization": f"Bearer {auth_data.get('access_token')}"}
            
            websocket = await websockets.connect(
                url,
                extra_headers=headers
            )
            
            # Send authentication if needed
            if auth_data:
                auth_msg = json.dumps(auth_data)
                await websocket.send(auth_msg)
            
            # Send subscription
            await websocket.send(subscribe_msg)
            
            # Start listening
            asyncio.create_task(self._listen_websocket(websocket))
            
            return websocket
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            self.ws_connected = False
            raise
    
    def _get_zerodha_tokens(self, symbols: List[str]) -> List[str]:
        """Convert symbols to Zerodha instrument tokens."""
        # In production, this would query Kite Connect for instrument tokens
        # For now, return mock tokens
        return [f"NSE:{symbol}" for symbol in symbols]
    
    async def _listen_websocket(self, websocket: Any) -> None:
        """Listen to WebSocket messages."""
        import websockets
        try:
            async for message in websocket:
                await self._process_websocket_message(message)
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
            self.ws_connected = False
            self._schedule_reconnect()
        except Exception as e:
            self.logger.error(f"WebSocket error: {e}")
            self.ws_connected = False
            self._schedule_reconnect()
    
    async def _process_websocket_message(self, message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            
            # Handle different message types
            if isinstance(data, list):
                # Zerodha format: ["type", "data"]
                msg_type = data[0]
                msg_data = data[1] if len(data) > 1 else {}
                
                if msg_type == "tick":
                    await self._process_tick(msg_data)
                elif msg_type == "order_update":
                    self._notify_subscribers('order_update', msg_data.get('tradingsymbol', ''), msg_data)
                elif msg_type == "connect":
                    self.logger.info(f"WebSocket connected: {msg_data}")
                    self.ws_connected = True
                    self.ws_reconnect_attempts = 0
                    
            elif isinstance(data, dict):
                # Generic format
                if 'event' in data:
                    if data['event'] == 'subscription':
                        self.logger.info(f"Subscribed: {data}")
                    elif data['event'] == 'error':
                        self.logger.error(f"WebSocket error: {data}")
                elif 'T' in data or 'type' in data:
                    # Alpaca tick format
                    await self._process_tick(data)
                else:
                    self.logger.debug(f"Unhandled message: {data}")
            
            # Update last ping time
            self.ws_last_ping = datetime.utcnow()
            
        except json.JSONDecodeError:
            self.logger.debug(f"Non-JSON message: {message}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
    async def _process_tick(self, tick_data: Dict[str, Any]) -> None:
        """Process tick data and create Tick object."""
        try:
            # Parse tick data based on broker format
            if 'tradable' in tick_data:
                # Zerodha format
                symbol = tick_data.get('tradable', 'UNKNOWN')
                timestamp = datetime.fromisoformat(
                    tick_data.get('exchange_timestamp', datetime.utcnow().isoformat())
                )
                
                tick = Tick(
                    symbol=symbol,
                    timestamp=timestamp,
                    last_price=float(tick_data.get('last_price', 0)),
                    last_quantity=float(tick_data.get('last_quantity', 0)),
                    bid_price=float(tick_data.get('bid', 0)) if tick_data.get('bid') else None,
                    bid_quantity=float(tick_data.get('bid_quantity', 0)) if tick_data.get('bid_quantity') else None,
                    ask_price=float(tick_data.get('ask', 0)) if tick_data.get('ask') else None,
                    ask_quantity=float(tick_data.get('ask_quantity', 0)) if tick_data.get('ask_quantity') else None,
                    volume=float(tick_data.get('volume', 0)) if tick_data.get('volume') else None,
                    open_interest=float(tick_data.get('oi', 0)) if tick_data.get('oi') else None
                )
            else:
                # Generic/Alpaca format
                symbol = tick_data.get('S', tick_data.get('symbol', 'UNKNOWN'))
                timestamp = datetime.fromisoformat(
                    tick_data.get('T', tick_data.get('timestamp', datetime.utcnow().isoformat()))
                )
                
                tick = Tick(
                    symbol=symbol,
                    timestamp=timestamp,
                    last_price=float(tick_data.get('p', tick_data.get('price', 0))),
                    last_quantity=float(tick_data.get('s', tick_data.get('size', 0))),
                    bid_price=float(tick_data.get('bp', 0)) if tick_data.get('bp') else None,
                    bid_quantity=float(tick_data.get('bs', 0)) if tick_data.get('bs') else None,
                    ask_price=float(tick_data.get('ap', 0)) if tick_data.get('ap') else None,
                    ask_quantity=float(tick_data.get('as', 0)) if tick_data.get('as') else None,
                    volume=float(tick_data.get('v', 0)) if tick_data.get('v') else None
                )
            
            self.handle_tick(tick)
            
        except Exception as e:
            self.logger.error(f"Error processing tick data: {e}")
    
    def handle_tick(self, tick: Tick) -> None:
        """
        Single entry point for every incoming tick (live or mock).
        
        Caches and optionally stores the tick, notifies tick subscribers and
        updates the in-progress bars, emitting each bar once it completes.
        """
        self.ws_last_ping = datetime.utcnow()  # Any tick proves the feed is alive
        self.cache.add_tick(tick)
        if self.store_ticks:
            self._store_tick(tick)
        self._notify_subscribers('tick', tick.symbol, tick)
        self._update_bars(tick)
    
    def _update_bars(self, tick: Tick) -> None:
        """Incrementally build 1m/5m bars; emit completed bars to subscribers."""
        # Exchange feeds report cumulative day volume; derive per-tick volume
        quantity = tick.last_quantity or 0.0
        if tick.volume is not None:
            previous = self._last_cum_volume.get(tick.symbol)
            self._last_cum_volume[tick.symbol] = tick.volume
            if previous is not None and tick.volume >= previous:
                quantity = tick.volume - previous
        
        for timeframe in self._bar_timeframes:
            minutes = 5 if timeframe == TimeFrame._5M else 1
            ts = tick.timestamp
            bucket = ts.replace(minute=ts.minute - ts.minute % minutes, second=0, microsecond=0)
            key = (tick.symbol, timeframe)
            bar = self._building_bars.get(key)
            
            if bar is not None and bucket > bar.timestamp:
                self._emit_bar(bar)
                bar = None
            elif bar is not None and bucket < bar.timestamp:
                continue  # Out-of-order tick for an already-closed bar
            
            if bar is None:
                self._building_bars[key] = OHLCV(
                    symbol=tick.symbol, timestamp=bucket, open=tick.last_price,
                    high=tick.last_price, low=tick.last_price, close=tick.last_price,
                    volume=quantity, timeframe=timeframe)
            else:
                bar.high = max(bar.high, tick.last_price)
                bar.low = min(bar.low, tick.last_price)
                bar.close = tick.last_price
                bar.volume += quantity
    
    def _emit_bar(self, bar: OHLCV) -> None:
        """Publish a completed bar."""
        self.cache.add_ohlcv(bar)
        self._store_ohlcv(bar)
        self._notify_subscribers('ohlcv', bar.symbol, bar)
    
    def _resample_ticks_to_ohlcv(self, ticks: List[Tick], timeframe: TimeFrame) -> Optional[OHLCV]:
        """Resample ticks to OHLCV for the given timeframe."""
        if not ticks:
            return None
        
        try:
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame([
                {
                    'timestamp': tick.timestamp,
                    'price': tick.last_price,
                    'volume': tick.last_quantity or 0,
                    'bid': tick.bid_price,
                    'ask': tick.ask_price
                }
                for tick in ticks
            ])
            
            # Ensure dataframe is not empty
            if df.empty:
                return None
            
            # Convert timestamp to datetime if it's not already
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Set timestamp as index
            df.set_index('timestamp', inplace=True)
            
            # Determine the timeframe in minutes
            if timeframe == TimeFrame.TICK:
                return None
            elif timeframe == TimeFrame._1M:
                freq = '1T'  # 1 minute
            elif timeframe == TimeFrame._5M:
                freq = '5T'  # 5 minutes
            elif timeframe == TimeFrame._15M:
                freq = '15T'  # 15 minutes
            elif timeframe == TimeFrame._1H:
                freq = '1H'  # 1 hour
            else:
                freq = '1T'
            
            # Resample to OHLCV
            ohlcv_df = df['price'].resample(freq).agg([
                ('open', 'first'),
                ('high', 'max'),
                ('low', 'min'),
                ('close', 'last')
            ])
            
            # Calculate volume
            volume_df = df['volume'].resample(freq).sum()
            ohlcv_df['volume'] = volume_df.values
            
            # Reset index
            ohlcv_df.reset_index(inplace=True)
            
            # Get the most recent bar
            if len(ohlcv_df) > 0:
                latest = ohlcv_df.iloc[-1]
                
                return OHLCV(
                    symbol=ticks[0].symbol,
                    timestamp=latest['timestamp'].to_pydatetime(),
                    open=float(latest['open']),
                    high=float(latest['high']),
                    low=float(latest['low']),
                    close=float(latest['close']),
                    volume=float(latest['volume']),
                    timeframe=timeframe
                )
            
        except Exception as e:
            self.logger.error(f"Error resampling ticks: {e}")
        
        return None
    
    def _check_websocket_health(self) -> None:
        """Check WebSocket connection health and reconnect if needed."""
        if not self._running:
            return
        if str(self.config.get('broker', {}).get('active', 'PAPER')).upper() == 'ZERODHA':
            return  # KiteTicker reconnects itself; quiet periods outside market hours are normal
        
        max_disconnect_time = self.ws_config.get('websocket_disconnect_timeout_seconds', 10)
        current_time = datetime.utcnow()
        
        if (current_time - self.ws_last_ping).total_seconds() > max_disconnect_time:
            self.logger.warning("WebSocket connection timed out")
            self.ws_connected = False
            self._schedule_reconnect()
    
    def _schedule_reconnect(self) -> None:
        """Schedule WebSocket reconnection."""
        max_attempts = self.ws_config.get('max_reconnect_attempts', 5)
        delay = self.ws_config.get('reconnect_delay_seconds', 3)
        
        if self.ws_reconnect_attempts >= max_attempts:
            self.logger.error("Max WebSocket reconnection attempts reached")
            return
        
        self.ws_reconnect_attempts += 1
        self.logger.info(f"Reconnecting WebSocket (attempt {self.ws_reconnect_attempts}/{max_attempts})")
        
        # Schedule reconnection in a new thread
        def reconnect():
            try:
                if self._loop:
                    asyncio.run_coroutine_threadsafe(self._start_websocket(), self._loop)
            except Exception as e:
                self.logger.error(f"Reconnection failed: {e}")
        
        threading.Timer(delay, reconnect).start()
    
    async def _generate_mock_data(self) -> None:
        """Generate mock market data (random walk ticks) for paper trading."""
        import random
        
        rng = random.Random()
        prices: Dict[Symbol, float] = {}
        cumulative_volume: Dict[Symbol, float] = {}
        tick_interval = float(self.config.get('market_data', {}).get('mock_tick_interval_seconds', 0.25))
        
        while self._running:
            try:
                for symbol in self.symbols:
                    if symbol not in prices:
                        current_tick = self.cache.get_current_tick(symbol)
                        prices[symbol] = current_tick.last_price if current_tick else \
                            float(100 + sum(map(ord, symbol)) * 7 % 2900)
                    
                    # Small Gaussian step (~0.03% per tick)
                    previous = prices[symbol]
                    new_price = max(0.05, previous * (1 + rng.gauss(0, 0.0003)))
                    prices[symbol] = new_price
                    
                    quantity = float(rng.randint(10, 500))
                    cumulative_volume[symbol] = cumulative_volume.get(symbol, 0.0) + quantity
                    
                    # Tick rule: upticks trade at the ask, downticks at the bid
                    half_spread = new_price * 0.0002
                    if new_price >= previous:
                        bid, ask = new_price - 2 * half_spread, new_price
                    else:
                        bid, ask = new_price, new_price + 2 * half_spread
                    
                    self.handle_tick(Tick(
                        symbol=symbol,
                        timestamp=datetime.now(),
                        last_price=round(new_price, 2),
                        last_quantity=quantity,
                        bid_price=round(bid, 2),
                        bid_quantity=float(rng.randint(100, 2000)),
                        ask_price=round(ask, 2),
                        ask_quantity=float(rng.randint(100, 2000)),
                        volume=cumulative_volume[symbol]
                    ))
                
                await asyncio.sleep(tick_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error generating mock data: {e}")
                await asyncio.sleep(1)
    
    async def _process_aggregations(self) -> None:
        """Process periodic aggregations."""
        # This is called from the main loop
        pass
    
    def _store_tick(self, tick: Tick) -> None:
        """Store tick in SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR IGNORE INTO ticks (
                        symbol, timestamp, last_price, last_quantity,
                        bid_price, bid_quantity, ask_price, ask_quantity,
                        volume, open_interest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tick.symbol,
                    tick.timestamp.isoformat(),
                    tick.last_price,
                    tick.last_quantity,
                    tick.bid_price,
                    tick.bid_quantity,
                    tick.ask_price,
                    tick.ask_quantity,
                    tick.volume,
                    tick.open_interest
                ))
                
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error storing tick: {e}")
    
    def _store_ohlcv(self, ohlcv: OHLCV) -> None:
        """Store OHLCV in SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO candles (
                        symbol, timestamp, timeframe, open, high, low, close, volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ohlcv.symbol,
                    ohlcv.timestamp.isoformat(),
                    ohlcv.timeframe.value,
                    ohlcv.open,
                    ohlcv.high,
                    ohlcv.low,
                    ohlcv.close,
                    ohlcv.volume
                ))
                
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error storing OHLCV: {e}")
    
    # =============================================================================
    # SUBSCRIPTION MANAGEMENT
    # =============================================================================
    
    def subscribe(self, callback: Callable, symbol: Optional[Symbol] = None,
                  data_type: str = "all") -> None:
        """Subscribe to data feed events."""
        subscriber = Subscriber(
            callback=callback,
            symbol=symbol,
            data_type=data_type
        )
        self.subscribers.append(subscriber)
        self.logger.debug(f"New subscriber: {callback.__name__} for {data_type}")
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from data feed events."""
        self.subscribers = [
            s for s in self.subscribers 
            if s.callback != callback
        ]
    
    def _notify_subscribers(self, event_type: str, symbol: Symbol, data: Any) -> None:
        """Notify all subscribers of an event."""
        for subscriber in self.subscribers:
            # Check if subscriber is interested in this event
            if subscriber.data_type != "all" and subscriber.data_type != event_type:
                continue
            
            # Check if subscriber is interested in this symbol
            if subscriber.symbol and subscriber.symbol != symbol:
                continue
            
            try:
                subscriber.callback(symbol, data)
            except Exception as e:
                self.logger.error(f"Error in subscriber {subscriber.callback.__name__}: {e}")
    
    # =============================================================================
    # REST DATA API
    # =============================================================================
    
    async def get_historical_ohlcv(self, symbol: Symbol, timeframe: TimeFrame,
                                  from_date: Optional[Timestamp] = None,
                                  to_date: Optional[Timestamp] = None,
                                  limit: Optional[int] = None) -> List[OHLCV]:
        """Get historical OHLCV data from REST API or database."""
        # First try to get from database
        db_data = self._get_ohlcv_from_db(symbol, timeframe, from_date, to_date, limit)
        if db_data:
            return db_data
        
        # If not in database, try to fetch from REST API
        try:
            api_data = await self._fetch_historical_from_api(
                symbol, timeframe, from_date, to_date, limit
            )
            if api_data:
                # Store in database for future use
                for ohlcv in api_data:
                    self._store_ohlcv(ohlcv)
                return api_data
        except Exception as e:
            self.logger.error(f"Failed to fetch historical data: {e}")
        
        return []
    
    def _get_ohlcv_from_db(self, symbol: Symbol, timeframe: TimeFrame,
                           from_date: Optional[Timestamp] = None,
                           to_date: Optional[Timestamp] = None,
                           limit: Optional[int] = None) -> List[OHLCV]:
        """Get OHLCV data from SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = '''
                    SELECT symbol, timestamp, timeframe, open, high, low, close, volume
                    FROM candles 
                    WHERE symbol = ? AND timeframe = ?
                '''
                params = [symbol, timeframe.value]
                
                # Add date filters
                if from_date:
                    query += " AND timestamp >= ?"
                    params.append(from_date.isoformat())
                if to_date:
                    query += " AND timestamp <= ?"
                    params.append(to_date.isoformat())
                
                # Add ordering and limit
                query += " ORDER BY timestamp DESC"
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                
                cursor.execute(query, params)
                
                # Convert to OHLCV objects
                ohlcv_list = []
                for row in cursor.fetchall():
                    ohlcv_list.append(OHLCV(
                        symbol=row[0],
                        timestamp=datetime.fromisoformat(row[1]),
                        timeframe=parse_timeframe(row[2]),
                        open=row[3],
                        high=row[4],
                        low=row[5],
                        close=row[6],
                        volume=row[7]
                    ))
                
                # Return in chronological order
                return list(reversed(ohlcv_list))
                
        except Exception as e:
            self.logger.error(f"Error querying database for OHLCV: {e}")
            return []
    
    async def _fetch_historical_from_api(self, symbol: Symbol, timeframe: TimeFrame,
                                        from_date: Optional[Timestamp] = None,
                                        to_date: Optional[Timestamp] = None,
                                        limit: Optional[int] = None) -> List[OHLCV]:
        """Fetch historical data from broker REST API."""
        broker_type = self.config.get('broker', {}).get('active', 'PAPER')
        
        if broker_type == 'ZERODHA':
            return await self._fetch_zerodha_historical(symbol, timeframe, from_date, to_date, limit)
        elif broker_type == 'ALPACA':
            return await self._fetch_alpaca_historical(symbol, timeframe, from_date, to_date, limit)
        else:
            # Generate mock historical data for paper trading
            return self._generate_mock_historical(symbol, timeframe, limit or 100)
    
    async def _fetch_zerodha_historical(self, symbol: Symbol, timeframe: TimeFrame,
                                       from_date: Optional[Timestamp] = None,
                                       to_date: Optional[Timestamp] = None,
                                       limit: Optional[int] = None) -> List[OHLCV]:
        """Fetch historical candles from Zerodha."""
        from intraday_system.brokers.zerodha_broker import ZerodhaMarketData
        try:
            market_data = getattr(self, '_zerodha', None) or ZerodhaMarketData.from_config(self.config)
            to = to_date or datetime.now()
            frm = from_date or (to - timedelta(days=5))
            loop = asyncio.get_running_loop()
            bars = await loop.run_in_executor(None, market_data.historical, symbol, timeframe, frm, to)
            return bars[-limit:] if limit else bars
        except Exception as e:
            self.logger.error(f"Failed to fetch Zerodha history for {symbol}: {e}")
            return []
    
    async def _fetch_alpaca_historical(self, symbol: Symbol, timeframe: TimeFrame,
                                       from_date: Optional[Timestamp] = None,
                                       to_date: Optional[Timestamp] = None,
                                       limit: Optional[int] = None) -> List[OHLCV]:
        """Fetch historical data from Alpaca API."""
        try:
            broker_config = self.config.get('broker', {}).get('alpaca', {})
            api_key = broker_config.get('api_key')
            api_secret = broker_config.get('api_secret')
            base_url = broker_config.get('base_url', 'https://paper-api.alpaca.markets')
            
            if not api_key or not api_secret:
                self.logger.error("Alpaca API credentials required")
                return []
            
            # Convert timeframe
            timeframe_map = {
                TimeFrame._1M: "1Min",
                TimeFrame._5M: "5Min",
                TimeFrame._15M: "15Min",
                TimeFrame._1H: "1H"
            }
            alpaca_timeframe = timeframe_map.get(timeframe, "1Min")
            
            # Calculate date range
            to = to_date or datetime.utcnow()
            from_dt = from_date or (to - timedelta(days=30))
            
            # Build URL
            url = f"{base_url}/v2/stocks/{symbol}/bars"
            params = {
                'timeframe': alpaca_timeframe,
                'start': from_dt.isoformat() + 'Z',
                'end': to.isoformat() + 'Z',
                'limit': limit or 1000
            }
            
            # Make request
            headers = {
                'APCA-API-KEY-ID': api_key,
                'APCA-API-SECRET-KEY': api_secret
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Convert to OHLCV
                        ohlcv_list = []
                        for bar in data.get('bars', []):
                            ohlcv_list.append(OHLCV(
                                symbol=symbol,
                                timestamp=datetime.fromisoformat(bar['t'].replace('Z', '+00:00')),
                                timeframe=timeframe,
                                open=bar['o'],
                                high=bar['h'],
                                low=bar['l'],
                                close=bar['c'],
                                volume=bar['v']
                            ))
                        
                        return ohlcv_list
                    else:
                        self.logger.error(f"Alpaca API error: {response.status}")
                        return []
                        
        except Exception as e:
            self.logger.error(f"Failed to fetch Alpaca historical data: {e}")
            return []
    
    def _generate_mock_historical(self, symbol: Symbol, timeframe: TimeFrame,
                                   count: int = 100) -> List[OHLCV]:
        """Generate mock historical data."""
        import random
        
        base_price = random.uniform(100, 1000)
        ohlcv_list = []
        current_price = base_price
        
        for i in range(count):
            # Generate random price movement
            change_pct = random.uniform(-1.0, 1.0)
            price_change = current_price * (change_pct / 100)
            current_price = max(0.05, current_price + price_change)
            
            # Generate random volume
            volume = random.randint(100, 10000)
            
            # Create OHLCV
            timestamp = datetime.utcnow() - timedelta(minutes=count-i)
            
            # Add some randomness to OHLC
            spread = current_price * 0.01  # 1% spread
            open_price = current_price + random.uniform(-spread, spread)
            high_price = max(open_price, current_price + random.uniform(0, spread))
            low_price = min(open_price, current_price - random.uniform(0, spread))
            
            ohlcv_list.append(OHLCV(
                symbol=symbol,
                timestamp=timestamp,
                timeframe=timeframe,
                open=round(open_price, 2),
                high=round(high_price, 2),
                low=round(low_price, 2),
                close=round(current_price, 2),
                volume=volume
            ))
        
        return ohlcv_list
    
    # =============================================================================
    # DATA VALIDATION AND GAP HANDLING
    # =============================================================================
    
    def validate_data(self, symbol: Symbol, timeframe: TimeFrame) -> bool:
        """Validate data integrity for a symbol and timeframe."""
        ohlcv_data = self.cache.get_ohlcv(symbol, timeframe)
        
        if not ohlcv_data:
            self.logger.warning(f"No data available for {symbol} {timeframe.value}")
            return False
        
        # Check for gaps
        gaps = self._detect_gaps(ohlcv_data)
        if gaps:
            self.logger.warning(f"Data gaps detected for {symbol} {timeframe.value}: {gaps}")
            # Fill gaps
            self._fill_gaps(symbol, timeframe, gaps)
            return True
        
        return True
    
    def _detect_gaps(self, ohlcv_data: List[OHLCV]) -> List[Tuple[Timestamp, Timestamp]]:
        """Detect gaps in OHLCV data."""
        if len(ohlcv_data) < 2:
            return []
        
        gaps = []
        timeframe_map = {
            TimeFrame._1M: timedelta(minutes=1),
            TimeFrame._5M: timedelta(minutes=5),
            TimeFrame._15M: timedelta(minutes=15),
            TimeFrame._1H: timedelta(hours=1)
        }
        
        expected_interval = timeframe_map.get(ohlcv_data[0].timeframe, timedelta(minutes=1))
        
        for i in range(1, len(ohlcv_data)):
            prev_time = ohlcv_data[i-1].timestamp
            curr_time = ohlcv_data[i].timestamp
            
            expected_time = prev_time + expected_interval
            
            if curr_time > expected_time + timedelta(seconds=10):  # Allow 10 seconds tolerance
                gaps.append((expected_time, curr_time))
        
        return gaps
    
    def _fill_gaps(self, symbol: Symbol, timeframe: TimeFrame, 
                   gaps: List[Tuple[Timestamp, Timestamp]]) -> None:
        """Fill gaps in OHLCV data."""
        for gap_start, gap_end in gaps:
            # For each missing candle, create a placeholder
            current_time = gap_start
            
            while current_time < gap_end:
                # Create a placeholder OHLCV
                prev_ohlcv = self.cache.get_current_ohlcv(symbol, timeframe)
                if prev_ohlcv:
                    placeholder = OHLCV(
                        symbol=symbol,
                        timestamp=current_time,
                        timeframe=timeframe,
                        open=prev_ohlcv.close,
                        high=prev_ohlcv.close,
                        low=prev_ohlcv.close,
                        close=prev_ohlcv.close,
                        volume=0
                    )
                    self.cache.add_ohlcv(placeholder)
                    self._store_ohlcv(placeholder)
                
                # Move to next timeframe
                timeframe_map = {
                    TimeFrame._1M: timedelta(minutes=1),
                    TimeFrame._5M: timedelta(minutes=5),
                    TimeFrame._15M: timedelta(minutes=15),
                    TimeFrame._1H: timedelta(hours=1)
                }
                interval = timeframe_map.get(timeframe, timedelta(minutes=1))
                current_time += interval
    
    def handle_out_of_order_tick(self, tick: Tick) -> bool:
        """Handle out-of-order tick data."""
        current_tick = self.cache.get_current_tick(tick.symbol)
        
        if current_tick and tick.timestamp < current_tick.timestamp:
            tolerance = self.config.get('market_data', {}).get('validation', {}).get(
                'out_of_order_tolerance_ms', 100
            )
            time_diff = (current_tick.timestamp - tick.timestamp).total_seconds() * 1000
            
            if time_diff <= tolerance:
                # Within tolerance, ignore or adjust
                self.logger.debug(f"Out-of-order tick ignored (within tolerance): {tick.symbol}")
                return False
            else:
                # Too far out of order, discard
                self.logger.warning(f"Out-of-order tick discarded: {tick.symbol} {tick.timestamp}")
                return False
        
        return True
    
    def get_data_quality_score(self, symbol: Symbol, timeframe: TimeFrame) -> float:
        """Get data quality score (0.0 to 1.0) for a symbol and timeframe."""
        ohlcv_data = self.cache.get_ohlcv(symbol, timeframe, limit=100)
        
        if not ohlcv_data:
            return 0.0
        
        # Check for gaps
        gaps = self._detect_gaps(ohlcv_data)
        gap_penalty = min(1.0, len(gaps) * 0.05)  # 5% penalty per gap
        
        # Check data recency
        latest = ohlcv_data[-1]
        # Feed timestamps are exchange-local wall-clock time
        time_since_update = (datetime.now() - latest.timestamp).total_seconds()
        max_allowed_age = 300  # 5 minutes
        age_penalty = min(1.0, time_since_update / max_allowed_age)
        
        # Check volume consistency
        volumes = [ohlcv.volume for ohlcv in ohlcv_data]
        if volumes:
            avg_volume = sum(volumes) / len(volumes)
            zero_volume_count = sum(1 for v in volumes if v == 0)
            volume_penalty = min(1.0, zero_volume_count / len(volumes))
        else:
            volume_penalty = 0.0
        
        # Calculate overall score
        score = 1.0 - (gap_penalty * 0.4 + age_penalty * 0.3 + volume_penalty * 0.3)
        return max(0.0, score)
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def get_current_prices(self) -> Dict[Symbol, float]:
        """Get current prices for all symbols."""
        prices = {}
        for symbol in self.symbols:
            tick = self.cache.get_current_tick(symbol)
            if tick:
                prices[symbol] = tick.last_price
            else:
                ohlcv = self.cache.get_current_ohlcv(symbol, TimeFrame._1M)
                if ohlcv:
                    prices[symbol] = ohlcv.close
        return prices
    
    def get_vwap(self, symbol: Symbol, period: int = 50) -> Optional[float]:
        """Calculate VWAP for a symbol."""
        ticks = self.cache.get_ticks(symbol, limit=period * 10)
        
        if not ticks:
            return None
        
        total_volume = 0
        total_value = 0
        
        for tick in ticks:
            if tick.last_price and tick.last_quantity:
                total_value += tick.last_price * tick.last_quantity
                total_volume += tick.last_quantity
        
        return total_value / total_volume if total_volume > 0 else None
    
    def get_atr(self, symbol: Symbol, period: int = 14, timeframe: TimeFrame = TimeFrame._1M) -> Optional[float]:
        """Calculate Average True Range for a symbol."""
        ohlcv_data = self.cache.get_ohlcv(symbol, timeframe, limit=period + 1)
        
        if len(ohlcv_data) < period:
            return None
        
        true_ranges = []
        
        for i in range(1, len(ohlcv_data)):
            prev = ohlcv_data[i-1]
            curr = ohlcv_data[i]
            
            tr = max(
                curr.high - curr.low,
                abs(curr.high - prev.close),
                abs(curr.low - prev.close)
            )
            true_ranges.append(tr)
        
        return sum(true_ranges[-period:]) / period if true_ranges else None
    
    def get_ema(self, data: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average."""
        if len(data) < period:
            return None
        
        weights = np.exp(np.linspace(-1., 0., period))
        weights /= weights.sum()
        
        return float(np.convolve(data[-period:], weights, mode='valid')[0])
    
    def get_sma(self, data: List[float], period: int) -> Optional[float]:
        """Calculate Simple Moving Average."""
        if len(data) < period:
            return None
        return sum(data[-period:]) / period
    
    def get_rsi(self, data: List[float], period: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index."""
        if len(data) < period + 1:
            return None
        
        deltas = np.diff(data)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = sum(gains[-period:]) / period if len(gains) >= period else None
        avg_loss = sum(losses[-period:]) / period if len(losses) >= period else None
        
        if avg_loss == 0:
            return 100.0
        if avg_gain is None or avg_loss is None:
            return None
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_data_feed: Optional[DataFeed] = None


def get_data_feed() -> DataFeed:
    """Get the singleton DataFeed instance."""
    global _data_feed
    if _data_feed is None:
        _data_feed = DataFeed()
    return _data_feed


def start_data_feed() -> DataFeed:
    """Start the data feed and return the instance."""
    global _data_feed
    _data_feed = DataFeed()
    _data_feed.start()
    return _data_feed


def stop_data_feed() -> None:
    """Stop the data feed."""
    global _data_feed
    if _data_feed:
        _data_feed.stop()
        _data_feed = None
