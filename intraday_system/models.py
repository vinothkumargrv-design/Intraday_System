"""
Core data models and types for the Rapid Fire Intraday Trading System.

This module contains all dataclasses, enums, and type definitions used throughout
the trading system to ensure type safety and maintain clean interfaces between components.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import decimal


# =============================================================================
# ENUMS AND TYPE DEFINITIONS
# =============================================================================

class SignalType(Enum):
    """Signal types for trading decisions."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class OrderType(Enum):
    """Supported order types."""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SL_M = "SL-M"
    OCO = "OCO"
    BRACKET = "BRACKET"


class OrderStatus(Enum):
    """Order lifecycle statuses."""
    PENDING = "PENDING"
    PLACED = "PLACED"
    EXECUTED = "EXECUTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"


class OrderSide(Enum):
    """Buy or sell side of an order."""
    BUY = "BUY"
    SELL = "SELL"


class OrderValidity(Enum):
    """Order validity types."""
    DAY = "DAY"
    IOC = "IOC"
    MIS = "MIS"
    NRML = "NRML"
    CNS = "CNS"


class PositionSide(Enum):
    """Position direction."""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class StrategyName(Enum):
    """Available strategy identifiers."""
    ORB = "ORB"  # Opening Range Breakout
    VWAP_MR = "VWAP_MR"  # VWAP Mean Reversion
    MOMENTUM_BURST = "MOMENTUM_BURST"
    EMA_CROSSOVER = "EMA_CROSSOVER"
    ORDER_FLOW = "ORDER_FLOW"
    SUPERTREND = "SUPERTREND"


class MarketRegime(Enum):
    """Market regime classification."""
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING = "RANGING"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"


class TimeFrame(Enum):
    """Supported timeframes for data and strategies."""
    TICK = "TICK"
    _1M = "1m"
    _5M = "5m"
    _15M = "15m"
    _1H = "1h"


class BrokerType(Enum):
    """Supported broker integrations."""
    ZERODHA = "ZERODHA"
    ALPACA = "ALPACA"
    INTERACTIVE_BROKERS = "INTERACTIVE_BROKERS"
    PAPER = "PAPER"


class TradeStatus(Enum):
    """Trade lifecycle statuses."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    STOPPED_OUT = "STOPPED_OUT"
    TIMED_OUT = "TIMED_OUT"


class NotificationLevel(Enum):
    """Notification priority levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    TRADE = "TRADE"


class LogLevel(Enum):
    """Logging levels for the trading system."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Type aliases
Symbol = str
Quantity = float
Price = float
Timestamp = datetime
OrderId = str
TradeId = str
StrategyWeight = float
ConfidenceScore = float


# =============================================================================
# TICK DATA MODELS
# =============================================================================

@dataclass
class Tick:
    """Single tick data point from market feed."""
    symbol: Symbol
    timestamp: Timestamp
    last_price: Price
    last_quantity: Quantity
    bid_price: Optional[Price] = None
    bid_quantity: Optional[Quantity] = None
    ask_price: Optional[Price] = None
    ask_quantity: Optional[Quantity] = None
    volume: Optional[Quantity] = None
    open_interest: Optional[Quantity] = None
    
    def spread(self) -> Optional[Price]:
        """Calculate bid-ask spread."""
        if self.bid_price and self.ask_price:
            return self.ask_price - self.bid_price
        return None
    
    def spread_pct(self) -> Optional[Price]:
        """Calculate spread as percentage of mid price."""
        spread = self.spread()
        if spread and self.bid_price and self.ask_price:
            mid = (self.bid_price + self.ask_price) / 2
            return (spread / mid) * 100 if mid != 0 else 0
        return None


@dataclass
class OHLCV:
    """OHLCV candle data."""
    symbol: Symbol
    timestamp: Timestamp
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Quantity
    timeframe: TimeFrame = TimeFrame._1M
    
    @property
    def range(self) -> Price:
        """Price range of the candle."""
        return self.high - self.low
    
    @property
    def body(self) -> Price:
        """Body size of the candle."""
        return abs(self.close - self.open)
    
    def is_bullish(self) -> bool:
        """Check if candle is bullish."""
        return self.close > self.open
    
    def is_bearish(self) -> bool:
        """Check if candle is bearish."""
        return self.close < self.open


# =============================================================================
# SIGNAL MODELS
# =============================================================================

@dataclass
class StrategySignal:
    """Signal generated by a single strategy."""
    strategy: StrategyName
    signal: SignalType
    confidence: ConfidenceScore  # 0.0 to 1.0
    reason: str
    symbol: Symbol
    timestamp: Timestamp
    suggested_sl: Optional[Price] = None
    suggested_tp: Optional[Price] = None
    timeframe: TimeFrame = TimeFrame._1M
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary for serialization."""
        return {
            "strategy": self.strategy.value,
            "signal": self.signal.value,
            "confidence": self.confidence,
            "reason": self.reason,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "suggested_sl": self.suggested_sl,
            "suggested_tp": self.suggested_tp,
            "timeframe": self.timeframe.value,
            "metadata": self.metadata
        }


@dataclass
class AggregatedSignal:
    """Aggregated signal from multiple strategies."""
    symbol: Symbol
    timestamp: Timestamp
    net_score: float  # -1.0 to +1.0
    net_confidence: float  # 0.0 to 1.0
    final_signal: SignalType
    contributing_strategies: List[StrategySignal]
    regime_filter_passed: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert aggregated signal to dictionary."""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "net_score": self.net_score,
            "net_confidence": self.net_confidence,
            "final_signal": self.final_signal.value,
            "regime_filter_passed": self.regime_filter_passed,
            "contributing_strategies": [s.to_dict() for s in self.contributing_strategies]
        }


# =============================================================================
# ORDER MODELS
# =============================================================================

@dataclass
class Order:
    """Order placed with the broker."""
    order_id: OrderId
    symbol: Symbol
    order_side: OrderSide
    order_type: OrderType
    quantity: Quantity
    price: Optional[Price] = None  # For LIMIT orders
    trigger_price: Optional[Price] = None  # For SL orders
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Quantity = 0.0
    average_fill_price: Optional[Price] = None
    timestamp: Timestamp = field(default_factory=datetime.utcnow)
    broker_order_id: Optional[str] = None
    validity: OrderValidity = OrderValidity.MIS
    strategy: Optional[StrategyName] = None
    parent_order_id: Optional[OrderId] = None  # For bracket orders
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    @property
    def remaining_quantity(self) -> Quantity:
        """Calculate remaining quantity to fill."""
        return self.quantity - self.filled_quantity
    
    @property
    def is_completed(self) -> bool:
        """Check if order is fully filled."""
        return self.filled_quantity >= self.quantity - 0.001  # Allow for rounding
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary."""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.order_side.value,
            "type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "status": self.status.value,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "timestamp": self.timestamp.isoformat(),
            "broker_order_id": self.broker_order_id,
            "validity": self.validity.value,
            "strategy": self.strategy.value if self.strategy else None,
            "parent_order_id": self.parent_order_id,
            "idempotency_key": self.idempotency_key
        }


@dataclass
class BracketOrder:
    """Bracket order with entry, stop loss, and take profit."""
    entry_order: Order
    sl_order: Order
    tp_order: Optional[Order] = None
    
    @property
    def order_id(self) -> OrderId:
        """Get the entry order ID."""
        return self.entry_order.order_id
    
    @property
    def is_active(self) -> bool:
        """Check if any order in the bracket is still active."""
        return (
            self.entry_order.status not in [OrderStatus.COMPLETED, OrderStatus.CANCELLED, OrderStatus.REJECTED] or
            self.sl_order.status not in [OrderStatus.COMPLETED, OrderStatus.CANCELLED, OrderStatus.REJECTED] or
            (self.tp_order and self.tp_order.status not in [OrderStatus.COMPLETED, OrderStatus.CANCELLED, OrderStatus.REJECTED])
        )


# =============================================================================
# POSITION AND TRADE MODELS
# =============================================================================

@dataclass
class Position:
    """Open position in a symbol."""
    symbol: Symbol
    side: PositionSide
    quantity: Quantity
    entry_price: Price
    entry_timestamp: Timestamp
    strategy: Optional[StrategyName] = None
    stop_loss: Optional[Price] = None
    take_profit: Optional[Price] = None
    trailing_stop: Optional[Price] = None
    order_id: Optional[OrderId] = None
    
    @property
    def current_value(self, current_price: Price) -> Price:
        """Calculate current value of the position."""
        if self.side == PositionSide.LONG:
            return current_price * self.quantity
        elif self.side == PositionSide.SHORT:
            return self.entry_price * self.quantity - (current_price - self.entry_price) * self.quantity
        return 0.0
    
    @property
    def pnl(self, current_price: Price) -> Price:
        """Calculate current PnL."""
        if self.side == PositionSide.LONG:
            return (current_price - self.entry_price) * self.quantity
        elif self.side == PositionSide.SHORT:
            return (self.entry_price - current_price) * self.quantity
        return 0.0
    
    @property
    def pnl_pct(self, current_price: Price) -> float:
        """Calculate PnL as percentage of entry value."""
        entry_value = self.entry_price * self.quantity
        if entry_value == 0:
            return 0.0
        return (self.pnl(current_price) / entry_value) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary."""
        return {
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "entry_timestamp": self.entry_timestamp.isoformat(),
            "strategy": self.strategy.value if self.strategy else None,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "trailing_stop": self.trailing_stop,
            "order_id": self.order_id
        }


@dataclass
class Trade:
    """Completed trade record."""
    trade_id: TradeId
    symbol: Symbol
    side: OrderSide
    entry_price: Price
    exit_price: Price
    quantity: Quantity
    entry_timestamp: Timestamp
    exit_timestamp: Timestamp
    stop_loss: Optional[Price] = None
    take_profit: Optional[Price] = None
    strategy: Optional[StrategyName] = None
    confidence: Optional[float] = None
    pnl: Optional[Price] = None
    brokerage: float = 0.0
    exchange_fees: float = 0.0
    stt: float = 0.0
    gst: float = 0.0
    stamp_duty: float = 0.0
    slippage: float = 0.0
    status: TradeStatus = TradeStatus.CLOSED
    tags: List[str] = field(default_factory=list)
    
    @property
    def net_pnl(self) -> float:
        """Calculate net PnL after all costs."""
        if self.pnl is None:
            return 0.0
        return self.pnl - (self.brokerage + self.exchange_fees + self.stt + self.gst + self.stamp_duty + self.slippage)
    
    @property
    def duration_seconds(self) -> float:
        """Calculate trade duration in seconds."""
        return (self.exit_timestamp - self.entry_timestamp).total_seconds()
    
    @property
    def return_pct(self) -> float:
        """Calculate return percentage."""
        if self.entry_price == 0:
            return 0.0
        if self.side == OrderSide.BUY:
            return ((self.exit_price - self.entry_price) / self.entry_price) * 100
        else:
            return ((self.entry_price - self.exit_price) / self.entry_price) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trade to dictionary for CSV export."""
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "quantity": self.quantity,
            "entry_timestamp": self.entry_timestamp.isoformat(),
            "exit_timestamp": self.exit_timestamp.isoformat(),
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "strategy": self.strategy.value if self.strategy else None,
            "confidence": self.confidence,
            "pnl": self.pnl,
            "net_pnl": self.net_pnl,
            "brokerage": self.brokerage,
            "exchange_fees": self.exchange_fees,
            "stt": self.stt,
            "gst": self.gst,
            "stamp_duty": self.stamp_duty,
            "slippage": self.slippage,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "return_pct": self.return_pct,
            "tags": ",".join(self.tags)
        }


# =============================================================================
# PORTFOLIO MODELS
# =============================================================================

@dataclass
class PortfolioState:
    """Current state of the portfolio."""
    equity: float
    cash: float
    total_value: float
    positions: List[Position]
    open_trades: List[Trade]
    closed_trades: List[Trade]
    timestamp: Timestamp = field(default_factory=datetime.utcnow)
    
    @property
    def num_open_positions(self) -> int:
        """Count of open positions."""
        return len([p for p in self.positions if p.side != PositionSide.FLAT])
    
    @property
    def unrealized_pnl(self, price_map: Dict[Symbol, Price]) -> float:
        """Calculate total unrealized PnL."""
        return sum(
            p.pnl(price_map.get(p.symbol, 0)) 
            for p in self.positions 
            if p.side != PositionSide.FLAT
        )
    
    @property
    def realized_pnl(self) -> float:
        """Calculate total realized PnL."""
        return sum(t.net_pnl for t in self.closed_trades)
    
    @property
    def total_pnl(self, price_map: Dict[Symbol, Price]) -> float:
        """Calculate total PnL (realized + unrealized)."""
        return self.realized_pnl + self.unrealized_pnl(price_map)
    
    @property
    def drawdown(self, peak_equity: float) -> float:
        """Calculate current drawdown from peak equity."""
        if peak_equity == 0:
            return 0.0
        return ((peak_equity - self.equity) / peak_equity) * 100


# =============================================================================
# RISK MODELS
# =============================================================================

@dataclass
class RiskParameters:
    """Risk management parameters."""
    max_per_trade_risk_pct: float = 0.5  # 0.5%
    max_concurrent_positions: int = 5
    max_daily_loss_pct: float = 2.0  # 2%
    max_daily_profit_pct: float = 4.0  # 4%
    trailing_stop_atr_multiplier: float = 1.5
    hard_stop_loss_pct: float = 1.0  # 1% from entry
    time_stop_time: time = field(default_factory=lambda: time(15, 15))  # 15:15 IST
    max_spread_pct: float = 0.15  # 0.15%
    slippage_buffer_pct: float = 0.05  # 0.05%
    cooldown_seconds: int = 60
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "max_per_trade_risk_pct": self.max_per_trade_risk_pct,
            "max_concurrent_positions": self.max_concurrent_positions,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_daily_profit_pct": self.max_daily_profit_pct,
            "trailing_stop_atr_multiplier": self.trailing_stop_atr_multiplier,
            "hard_stop_loss_pct": self.hard_stop_loss_pct,
            "time_stop_time": self.time_stop_time.isoformat(),
            "max_spread_pct": self.max_spread_pct,
            "slippage_buffer_pct": self.slippage_buffer_pct,
            "cooldown_seconds": self.cooldown_seconds
        }


@dataclass
class RiskCheckResult:
    """Result of a risk check."""
    passed: bool
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskState:
    """Current risk state."""
    daily_pnl: float
    daily_pnl_pct: float
    current_exposure: float
    current_exposure_pct: float
    open_positions_count: int
    cooldown_symbols: Dict[Symbol, Timestamp]
    consecutive_failures: int
    kill_switch_activated: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "daily_pnl": self.daily_pnl,
            "daily_pnl_pct": self.daily_pnl_pct,
            "current_exposure": self.current_exposure,
            "current_exposure_pct": self.current_exposure_pct,
            "open_positions_count": self.open_positions_count,
            "cooldown_symbols": {k: v.isoformat() for k, v in self.cooldown_symbols.items()},
            "consecutive_failures": self.consecutive_failures,
            "kill_switch_activated": self.kill_switch_activated
        }


# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================

@dataclass
class StrategyConfig:
    """Configuration for a single strategy."""
    name: StrategyName
    enabled: bool = True
    weight: float = 1.0
    timeframe: TimeFrame = TimeFrame._1M
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name.value,
            "enabled": self.enabled,
            "weight": self.weight,
            "timeframe": self.timeframe.value,
            "parameters": self.parameters
        }


@dataclass
class AggregationConfig:
    """Configuration for signal aggregation."""
    threshold: float = 0.65
    min_agreeing_strategies: int = 2
    use_regime_filter: bool = True
    regime_volatility_threshold: float = 0.75  # ATR percentile threshold
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "threshold": self.threshold,
            "min_agreeing_strategies": self.min_agreeing_strategies,
            "use_regime_filter": self.use_regime_filter,
            "regime_volatility_threshold": self.regime_volatility_threshold
        }


# =============================================================================
# BROKER MODELS
# =============================================================================

@dataclass
class BrokerConfig:
    """Configuration for broker connection."""
    broker_type: BrokerType
    api_key: str
    api_secret: str
    access_token: Optional[str] = None
    account_id: Optional[str] = None
    base_url: Optional[str] = None
    websocket_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (with sensitive data masked)."""
        return {
            "broker_type": self.broker_type.value,
            "api_key": "***MASKED***" if self.api_key else None,
            "api_secret": "***MASKED***" if self.api_secret else None,
            "access_token": "***MASKED***" if self.access_token else None,
            "account_id": self.account_id,
            "base_url": self.base_url,
            "websocket_url": self.websocket_url
        }


@dataclass
class OrderResponse:
    """Response from broker after placing an order."""
    success: bool
    order_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "order_id": self.order_id,
            "broker_order_id": self.broker_order_id,
            "error_code": self.error_code,
            "error_message": self.error_message
        }


# =============================================================================
# MARKET DATA MODELS
# =============================================================================

@dataclass
class MarketDataRequest:
    """Request for market data."""
    symbol: Symbol
    timeframe: Optional[TimeFrame] = None
    from_date: Optional[Timestamp] = None
    to_date: Optional[Timestamp] = None
    limit: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe.value if self.timeframe else None,
            "from_date": self.from_date.isoformat() if self.from_date else None,
            "to_date": self.to_date.isoformat() if self.to_date else None,
            "limit": self.limit
        }


@dataclass
class InstrumentInfo:
    """Information about a trading instrument."""
    symbol: Symbol
    exchange: str
    instrument_type: str  # EQUITY, FUTURE, OPTION, etc.
    lot_size: int = 1
    tick_size: float = 0.05
    min_quantity: float = 1.0
    max_quantity: float = 100000.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "instrument_type": self.instrument_type,
            "lot_size": self.lot_size,
            "tick_size": self.tick_size,
            "min_quantity": self.min_quantity,
            "max_quantity": self.max_quantity
        }


# =============================================================================
# SYSTEM STATE
# =============================================================================

@dataclass
class SystemState:
    """Global system state."""
    is_running: bool = False
    is_paper_trading: bool = True
    current_mode: str = "PAPER"
    start_time: Optional[Timestamp] = None
    last_tick_time: Dict[Symbol, Timestamp] = field(default_factory=dict)
    last_signal_time: Dict[Symbol, Timestamp] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_running": self.is_running,
            "is_paper_trading": self.is_paper_trading,
            "current_mode": self.current_mode,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "last_tick_time": {k: v.isoformat() for k, v in self.last_tick_time.items()},
            "last_signal_time": {k: v.isoformat() for k, v in self.last_signal_time.items()}
        }


# =============================================================================
# PERFORMANCE METRICS
# =============================================================================

@dataclass
class PerformanceMetrics:
    """Trading performance metrics."""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_net_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown: float = 0.0
    current_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    profit_factor: float = 0.0
    cagr: float = 0.0
    mar_ratio: float = 0.0
    expectancy: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "total_pnl": self.total_pnl,
            "total_net_pnl": self.total_net_pnl,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "max_drawdown": self.max_drawdown,
            "current_drawdown": self.current_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "profit_factor": self.profit_factor,
            "cagr": self.cagr,
            "mar_ratio": self.mar_ratio,
            "expectancy": self.expectancy
        }
