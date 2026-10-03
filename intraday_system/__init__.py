"""
Rapid Fire Intraday Trading System

This package contains the complete intraday trading system with:
- Multi-broker support (Zerodha, Alpaca, Interactive Brokers)
- 6 trading strategies
- Real-time data feeds
- Signal aggregation and risk management
- Order execution and portfolio tracking
- Event-driven backtesting
- Streamlit dashboard
"""

from .models import (
    SignalType, OrderType, OrderStatus, OrderSide, OrderValidity,
    PositionSide, StrategyName, MarketRegime, TimeFrame, BrokerType,
    TradeStatus, NotificationLevel, Tick, OHLCV, StrategySignal,
    AggregatedSignal, Order, BracketOrder, Position, Trade, PortfolioState,
    RiskParameters, RiskState, PerformanceMetrics, SystemState,
    RiskCheckResult, StrategyConfig, AggregationConfig, BrokerConfig,
    OrderResponse, MarketDataRequest, InstrumentInfo
)

from .config_loader import ConfigLoader, load_config
from .logger import get_logger, strategy_logger, execution_logger, data_feed_logger

# Version
__version__ = "1.0.0"
__author__ = "Rapid Fire Trading System"
__description__ = "Production-ready intraday trading system with sub-second latency"