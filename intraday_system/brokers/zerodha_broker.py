"""
Zerodha Kite Connect broker for the Rapid Fire Intraday Trading System.

Built on the official ``kiteconnect`` SDK (``pip install kiteconnect``), which
implements Kite's REST API and binary WebSocket ticker protocol.

Authentication: Kite access tokens are valid for one trading day and are
obtained through Zerodha's browser login. Provide either

* ``broker.zerodha.access_token`` (or env ``ZERODHA_ACCESS_TOKEN``), or
* ``request_token`` + ``api_secret`` to exchange for an access token on connect.

The SDK is synchronous; calls are run in a thread executor so the async
broker interface does not block the order manager's event loop.

NOTE: This adapter maps the system's order model onto Kite's API, but it has
not been exercised against a live Kite account in this repository's test
suite. Validate it with small quantities before trading real size.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

from intraday_system.brokers.base_broker import (
    BaseBroker, BrokerError, BrokerType, ConnectionStatus, OrderBook
)
from intraday_system.models import (
    BracketOrder, OHLCV, Order, OrderId, OrderResponse, OrderSide, OrderStatus,
    OrderType, OrderValidity, Position, PositionSide, Symbol, Tick, TimeFrame
)


def _require_kiteconnect():
    try:
        import kiteconnect  # noqa: F401
        return kiteconnect
    except ImportError as e:
        raise BrokerError("The 'kiteconnect' package is required for Zerodha "
                          "(pip install kiteconnect)", broker="zerodha") from e


@dataclass
class KiteConnectConfig:
    """Configuration for Zerodha Kite Connect."""
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""
    request_token: str = ""
    exchange: str = "NSE"
    default_product: str = "MIS"
    variety: str = "regular"
    tag: str = "rapidfire"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KiteConnectConfig":
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: ("" if v is None else v) for k, v in (data or {}).items() if k in known})


# Kite order status -> system status (partial fills handled separately)
_STATUS_MAP = {
    "COMPLETE": OrderStatus.COMPLETED,
    "CANCELLED": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED,
}

_ORDER_TYPE_MAP = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.SL: "SL",
    OrderType.SL_M: "SL-M",
}


def split_symbol(symbol: Symbol, default_exchange: str = "NSE") -> Tuple[str, str]:
    """'NSE:INFY' -> ('NSE', 'INFY'); 'INFY' -> (default_exchange, 'INFY')."""
    if ":" in symbol:
        exchange, tradingsymbol = symbol.split(":", 1)
        return exchange, tradingsymbol
    return default_exchange, symbol


class ZerodhaBroker(BaseBroker):
    """Zerodha Kite Connect broker implementation."""

    def __init__(self, broker_type: BrokerType, config: Dict[str, Any]):
        super().__init__(broker_type, config)
        self.kite_config = KiteConnectConfig.from_dict(config.get('zerodha', {}))
        if not self.kite_config.api_key:
            raise BrokerError("broker.zerodha.api_key is required", broker="zerodha")
        kiteconnect = _require_kiteconnect()
        self.kite = kiteconnect.KiteConnect(api_key=self.kite_config.api_key)
        self._order_meta: Dict[str, Order] = {}
        self._meta_lock = threading.Lock()

    async def _run(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, partial(func, *args, **kwargs))
        except BrokerError:
            raise
        except Exception as e:
            # kiteconnect raises NetworkException / DataException for transient faults
            retryable = type(e).__name__ in ("NetworkException", "DataException", "GeneralException")
            raise BrokerError(f"{type(e).__name__}: {e}", broker="zerodha", retryable=retryable) from e

    # =========================================================================
    # CONNECTION
    # =========================================================================

    async def connect(self) -> bool:
        self.connection_status = ConnectionStatus.CONNECTING
        cfg = self.kite_config
        if not cfg.access_token and cfg.request_token and cfg.api_secret:
            session = await self._run(self.kite.generate_session, cfg.request_token,
                                      api_secret=cfg.api_secret)
            cfg.access_token = session["access_token"]
        if not cfg.access_token:
            self.connection_status = ConnectionStatus.ERROR
            raise BrokerError("Zerodha access_token missing (or request_token + api_secret)",
                              broker="zerodha")
        self.kite.set_access_token(cfg.access_token)
        await self._run(self.kite.profile)  # Validates the token
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

    def _instrument(self, symbol: Symbol) -> str:
        exchange, tradingsymbol = split_symbol(symbol, self.kite_config.exchange)
        return f"{exchange}:{tradingsymbol}"

    async def get_market_data(self, symbols: List[Symbol]) -> Dict[Symbol, Tick]:
        keys = {self._instrument(s): s for s in symbols}
        quotes = await self._run(self.kite.ltp, list(keys))
        return {keys[k]: Tick(symbol=keys[k], timestamp=datetime.now(),
                              last_price=float(v["last_price"]), last_quantity=0)
                for k, v in quotes.items()}

    async def get_ohlcv_data(self, symbol: Symbol, timeframe: str,
                             from_date: datetime, to_date: datetime) -> List[OHLCV]:
        md = ZerodhaMarketData(self.kite, self.kite_config.exchange)
        return await self._run(md.historical, symbol, timeframe, from_date, to_date)

    async def get_order_book(self, symbol: Symbol, depth: int = 5) -> OrderBook:
        key = self._instrument(symbol)
        quote = (await self._run(self.kite.quote, [key]))[key]
        book = quote.get("depth", {})
        return OrderBook(symbol=symbol,
                         bids=[(b["price"], b["quantity"]) for b in book.get("buy", [])[:depth]],
                         asks=[(a["price"], a["quantity"]) for a in book.get("sell", [])[:depth]])

    # =========================================================================
    # ORDERS
    # =========================================================================

    def _product(self, validity: OrderValidity) -> str:
        return {OrderValidity.NRML: "NRML", OrderValidity.CNS: "CNC"}.get(
            validity, self.kite_config.default_product)

    async def place_order(self, order: Order) -> OrderResponse:
        exchange, tradingsymbol = split_symbol(order.symbol, self.kite_config.exchange)
        params: Dict[str, Any] = dict(
            variety=self.kite_config.variety,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type="BUY" if order.order_side == OrderSide.BUY else "SELL",
            quantity=int(order.quantity),
            product=self._product(order.validity),
            order_type=_ORDER_TYPE_MAP.get(order.order_type, "MARKET"),
            validity="IOC" if order.validity == OrderValidity.IOC else "DAY",
            tag=self.kite_config.tag[:20],
        )
        if order.order_type in (OrderType.LIMIT, OrderType.SL) and order.price:
            params["price"] = round(order.price, 2)
        if order.order_type in (OrderType.SL, OrderType.SL_M):
            params["trigger_price"] = round(order.trigger_price, 2)

        try:
            broker_order_id = await self._run(self.kite.place_order, **params)
        except BrokerError as e:
            return OrderResponse(success=False, order_id=order.order_id,
                                 error_code="TEMPORARY_ERROR" if e.retryable else "REJECTED",
                                 error_message=str(e))
        with self._meta_lock:
            self._order_meta[str(broker_order_id)] = order
        return OrderResponse(success=True, order_id=order.order_id, broker_order_id=str(broker_order_id))

    async def place_bracket_order(self, bracket_order: BracketOrder) -> Tuple[OrderResponse, OrderResponse, OrderResponse]:
        # Kite discontinued bracket orders; the execution engine places SL/TP itself
        entry = await self.place_order(bracket_order.entry_order)
        sl = await self.place_order(bracket_order.sl_order)
        tp = await self.place_order(bracket_order.tp_order) if bracket_order.tp_order else \
            OrderResponse(success=False, error_message="No TP order")
        return entry, sl, tp

    async def modify_order(self, order_id: OrderId, quantity: Optional[float] = None,
                           price: Optional[float] = None, trigger_price: Optional[float] = None,
                           stop_loss: Optional[float] = None,
                           take_profit: Optional[float] = None) -> OrderResponse:
        params: Dict[str, Any] = {}
        if quantity is not None:
            params["quantity"] = int(quantity)
        if price is not None:
            params["price"] = round(price, 2)
        trigger = trigger_price if trigger_price is not None else stop_loss
        if trigger is not None:
            params["trigger_price"] = round(trigger, 2)
        if not params:
            return OrderResponse(success=True, broker_order_id=order_id)
        try:
            await self._run(self.kite.modify_order, self.kite_config.variety, order_id, **params)
        except BrokerError as e:
            return OrderResponse(success=False, broker_order_id=order_id, error_message=str(e))
        return OrderResponse(success=True, broker_order_id=order_id)

    async def cancel_order(self, order_id: OrderId) -> bool:
        try:
            await self._run(self.kite.cancel_order, self.kite_config.variety, order_id)
            return True
        except BrokerError:
            return False

    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> int:
        count = 0
        for order in await self.get_open_orders():
            if (symbol is None or order.symbol == symbol) and await self.cancel_order(order.broker_order_id):
                count += 1
        return count

    def _to_order(self, data: Dict[str, Any]) -> Order:
        broker_order_id = str(data.get("order_id"))
        with self._meta_lock:
            meta = self._order_meta.get(broker_order_id)
        filled = float(data.get("filled_quantity") or 0)
        status = _STATUS_MAP.get(data.get("status", ""))
        if status is None:
            status = OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.PLACED
        order_type = {v: k for k, v in _ORDER_TYPE_MAP.items()}.get(data.get("order_type"), OrderType.MARKET)
        return Order(
            order_id=meta.order_id if meta else broker_order_id,
            symbol=meta.symbol if meta else data.get("tradingsymbol", ""),
            order_side=OrderSide.BUY if data.get("transaction_type") == "BUY" else OrderSide.SELL,
            order_type=order_type,
            quantity=float(data.get("quantity") or 0),
            price=data.get("price") or None,
            trigger_price=data.get("trigger_price") or None,
            status=status,
            filled_quantity=filled,
            average_fill_price=float(data["average_price"]) if data.get("average_price") else None,
            broker_order_id=broker_order_id,
        )

    async def get_order(self, order_id: OrderId) -> Optional[Order]:
        history = await self._run(self.kite.order_history, order_id)
        return self._to_order(history[-1]) if history else None

    async def get_order_status(self, order_id: OrderId) -> OrderStatus:
        order = await self.get_order(order_id)
        return order.status if order else OrderStatus.REJECTED

    async def get_open_orders(self) -> List[Order]:
        orders = [self._to_order(o) for o in await self._run(self.kite.orders)]
        return [o for o in orders if o.status in (OrderStatus.PLACED, OrderStatus.PARTIALLY_FILLED)]

    # =========================================================================
    # ACCOUNT
    # =========================================================================

    async def get_positions(self) -> List[Position]:
        positions = (await self._run(self.kite.positions)).get("net", [])
        return [Position(symbol=p["tradingsymbol"],
                         side=PositionSide.LONG if p["quantity"] > 0 else PositionSide.SHORT,
                         quantity=abs(float(p["quantity"])), entry_price=float(p["average_price"]),
                         entry_timestamp=datetime.now())
                for p in positions if p.get("quantity")]

    async def get_position(self, symbol: Symbol) -> Optional[Position]:
        _, tradingsymbol = split_symbol(symbol, self.kite_config.exchange)
        for position in await self.get_positions():
            if position.symbol == tradingsymbol:
                return position
        return None

    async def get_account_balance(self) -> Dict[str, Any]:
        equity = (await self._run(self.kite.margins)).get("equity", {})
        return {"cash": equity.get("available", {}).get("live_balance"),
                "net": equity.get("net"), "currency": "INR"}

    async def get_trade_history(self, symbol: Optional[Symbol] = None,
                                from_date: Optional[datetime] = None,
                                to_date: Optional[datetime] = None) -> List[Any]:
        trades = await self._run(self.kite.trades)
        if symbol:
            _, tradingsymbol = split_symbol(symbol, self.kite_config.exchange)
            trades = [t for t in trades if t.get("tradingsymbol") == tradingsymbol]
        return trades

    async def convert_to_market_order(self, order_id: OrderId) -> OrderResponse:
        try:
            await self._run(self.kite.modify_order, self.kite_config.variety, order_id,
                            order_type="MARKET")
        except BrokerError as e:
            return OrderResponse(success=False, broker_order_id=order_id, error_message=str(e))
        return OrderResponse(success=True, broker_order_id=order_id)

    def supports_bracket_orders(self) -> bool:
        return False

    def get_max_orders_per_second(self) -> int:
        return 10


# =============================================================================
# MARKET DATA (ticker + history)
# =============================================================================

class ZerodhaMarketData:
    """Live ticks via KiteTicker and historical candles via Kite REST."""

    INTERVALS = {"1m": "minute", "5m": "5minute", "15m": "15minute", "1h": "60minute"}

    def __init__(self, kite: Any, exchange: str = "NSE"):
        self.kite = kite
        self.exchange = exchange
        self._tokens: Optional[Dict[str, int]] = None
        self._ticker = None

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "ZerodhaMarketData":
        kiteconnect = _require_kiteconnect()
        cfg = KiteConnectConfig.from_dict(config.get('broker', {}).get('zerodha', {}))
        if not cfg.api_key or not cfg.access_token:
            raise BrokerError("Zerodha api_key and access_token are required for market data",
                              broker="zerodha")
        kite = kiteconnect.KiteConnect(api_key=cfg.api_key)
        kite.set_access_token(cfg.access_token)
        return cls(kite, cfg.exchange)

    def instrument_tokens(self, symbols: List[Symbol]) -> Dict[int, Symbol]:
        """Map instrument tokens to our symbols (loads the instrument dump once)."""
        if self._tokens is None:
            self._tokens = {}
            for exchange in {split_symbol(s, self.exchange)[0] for s in symbols}:
                for inst in self.kite.instruments(exchange):
                    self._tokens[f"{exchange}:{inst['tradingsymbol']}"] = int(inst["instrument_token"])
        result = {}
        for symbol in symbols:
            exchange, tradingsymbol = split_symbol(symbol, self.exchange)
            token = self._tokens.get(f"{exchange}:{tradingsymbol}")
            if token is not None:
                result[token] = symbol
        return result

    def historical(self, symbol: Symbol, timeframe: Any, from_date: datetime,
                   to_date: datetime) -> List[OHLCV]:
        tf_value = timeframe.value if isinstance(timeframe, TimeFrame) else str(timeframe)
        tokens = {v: k for k, v in self.instrument_tokens([symbol]).items()}
        if symbol not in tokens:
            return []
        rows = self.kite.historical_data(tokens[symbol], from_date, to_date,
                                         self.INTERVALS.get(tf_value, "minute"))
        tf = TimeFrame(tf_value) if tf_value in {t.value for t in TimeFrame} else TimeFrame._1M
        return [OHLCV(symbol=symbol, timestamp=row["date"].replace(tzinfo=None), open=row["open"],
                      high=row["high"], low=row["low"], close=row["close"],
                      volume=row.get("volume", 0), timeframe=tf)
                for row in rows]

    def start_ticker(self, symbols: List[Symbol], on_tick: Callable[[Tick], None],
                     on_status: Optional[Callable[[bool], None]] = None) -> None:
        """Stream full-mode ticks for `symbols` on a background thread."""
        from kiteconnect import KiteTicker
        token_map = self.instrument_tokens(symbols)
        if not token_map:
            raise BrokerError("None of the configured symbols were found in the Kite instrument list",
                              broker="zerodha")
        ticker = KiteTicker(self.kite.api_key, self.kite.access_token)
        tokens = list(token_map)

        def handle_ticks(ws, ticks):
            for t in ticks:
                symbol = token_map.get(t.get("instrument_token"))
                if symbol is None:
                    continue
                depth = t.get("depth") or {}
                bid = (depth.get("buy") or [{}])[0]
                ask = (depth.get("sell") or [{}])[0]
                ts = t.get("exchange_timestamp") or t.get("last_trade_time") or datetime.now()
                on_tick(Tick(symbol=symbol, timestamp=ts.replace(tzinfo=None),
                             last_price=float(t["last_price"]),
                             last_quantity=float(t.get("last_traded_quantity") or 0),
                             bid_price=bid.get("price") or None, bid_quantity=bid.get("quantity"),
                             ask_price=ask.get("price") or None, ask_quantity=ask.get("quantity"),
                             volume=t.get("volume_traded"), open_interest=t.get("oi")))

        def handle_connect(ws, response):
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)
            if on_status:
                on_status(True)

        def handle_close(ws, code, reason):
            if on_status:
                on_status(False)

        ticker.on_ticks = handle_ticks
        ticker.on_connect = handle_connect
        ticker.on_close = handle_close
        ticker.connect(threaded=True)
        self._ticker = ticker

    def stop_ticker(self) -> None:
        if self._ticker is not None:
            try:
                self._ticker.close()
            except Exception:
                pass
            self._ticker = None
