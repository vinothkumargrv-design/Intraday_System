"""
Unit tests for models.py - Core data structures and enums.
"""

import pytest
from datetime import datetime, time
from decimal import Decimal
from typing import Optional

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from intraday_system.models import (
    # Enums
    SignalType,
    OrderType,
    OrderStatus,
    OrderSide,
    OrderValidity,
    PositionSide,
    StrategyName,
    MarketRegime,
    TimeFrame,
    BrokerType,
    TradeStatus,
    NotificationLevel,
    
    # Data models
    Tick,
    OHLCV,
    
    # Signal models
    StrategySignal,
    AggregatedSignal,
    
    # Order models
    Order,
    BracketOrder,
    
    # Position and trade models
    Position,
    Trade,
    
    # Portfolio models
    PortfolioState,
    
    # Risk models
    RiskParameters,
    RiskCheckResult,
    RiskState,
    
    # Strategy configuration
    StrategyConfig,
    AggregationConfig,
    
    # Broker models
    BrokerConfig,
    OrderResponse,
    
    # Market data models
    MarketDataRequest,
    InstrumentInfo,
    
    # System state
    SystemState,
    
    # Performance metrics
    PerformanceMetrics
)


class TestEnums:
    """Test enum classes."""
    
    def test_signal_type_values(self):
        """Test SignalType enum values."""
        assert SignalType.BUY.value == "BUY"
        assert SignalType.SELL.value == "SELL"
        assert SignalType.HOLD.value == "HOLD"
    
    def test_order_type_values(self):
        """Test OrderType enum values."""
        assert OrderType.MARKET.value == "MARKET"
        assert OrderType.LIMIT.value == "LIMIT"
        assert OrderType.SL.value == "SL"
        assert OrderType.SL_M.value == "SL-M"
        assert OrderType.OCO.value == "OCO"
        assert OrderType.BRACKET.value == "BRACKET"
    
    def test_order_status_values(self):
        """Test OrderStatus enum values."""
        assert OrderStatus.PENDING.value == "PENDING"
        assert OrderStatus.PLACED.value == "PLACED"
        assert OrderStatus.EXECUTED.value == "EXECUTED"
        assert OrderStatus.PARTIALLY_FILLED.value == "PARTIALLY_FILLED"
        assert OrderStatus.CANCELLED.value == "CANCELLED"
        assert OrderStatus.REJECTED.value == "REJECTED"
        assert OrderStatus.COMPLETED.value == "COMPLETED"
    
    def test_order_side_values(self):
        """Test OrderSide enum values."""
        assert OrderSide.BUY.value == "BUY"
        assert OrderSide.SELL.value == "SELL"
    
    def test_position_side_values(self):
        """Test PositionSide enum values."""
        assert PositionSide.LONG.value == "LONG"
        assert PositionSide.SHORT.value == "SHORT"
        assert PositionSide.FLAT.value == "FLAT"
    
    def test_strategy_name_values(self):
        """Test StrategyName enum values."""
        assert StrategyName.ORB.value == "ORB"
        assert StrategyName.VWAP_MR.value == "VWAP_MR"
        assert StrategyName.MOMENTUM_BURST.value == "MOMENTUM_BURST"
        assert StrategyName.EMA_CROSSOVER.value == "EMA_CROSSOVER"
        assert StrategyName.ORDER_FLOW.value == "ORDER_FLOW"
        assert StrategyName.SUPERTREND.value == "SUPERTREND"
    
    def test_time_frame_values(self):
        """Test TimeFrame enum values."""
        assert TimeFrame.TICK.value == "TICK"
        assert TimeFrame._1M.value == "1m"
        assert TimeFrame._5M.value == "5m"
        assert TimeFrame._15M.value == "15m"
        assert TimeFrame._1H.value == "1h"
    
    def test_broker_type_values(self):
        """Test BrokerType enum values."""
        assert BrokerType.ZERODHA.value == "ZERODHA"
        assert BrokerType.ALPACA.value == "ALPACA"
        assert BrokerType.INTERACTIVE_BROKERS.value == "INTERACTIVE_BROKERS"
        assert BrokerType.PAPER.value == "PAPER"
    
    def test_market_regime_values(self):
        """Test MarketRegime enum values."""
        assert MarketRegime.TRENDING_UP.value == "TRENDING_UP"
        assert MarketRegime.TRENDING_DOWN.value == "TRENDING_DOWN"
        assert MarketRegime.RANGING.value == "RANGING"
        assert MarketRegime.HIGH_VOLATILITY.value == "HIGH_VOLATILITY"
        assert MarketRegime.LOW_VOLATILITY.value == "LOW_VOLATILITY"


class TestTick:
    """Test Tick model."""
    
    def test_tick_creation(self):
        """Test creating a Tick instance."""
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime(2024, 1, 1, 10, 0, 0),
            last_price=18000.0,
            last_quantity=100.0,
            bid_price=17999.5,
            bid_quantity=50.0,
            ask_price=18000.5,
            ask_quantity=75.0,
            volume=1000.0,
            open_interest=5000.0
        )
        
        assert tick.symbol == "NIFTY"
        assert tick.last_price == 18000.0
        assert tick.bid_price == 17999.5
        assert tick.ask_price == 18000.5
    
    def test_tick_spread(self):
        """Test tick spread calculation."""
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17999.5,
            ask_price=18000.5
        )
        
        assert tick.spread() == 1.0
    
    def test_tick_spread_none(self):
        """Test tick spread when bid/ask not available."""
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0
        )
        
        assert tick.spread() is None
    
    def test_tick_spread_pct(self):
        """Test tick spread percentage calculation."""
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17999.5,
            ask_price=18000.5
        )
        
        # Spread is 1.0, mid price is 18000.0
        # Spread percentage = (1.0 / 18000.0) * 100 = 0.005555...
        spread_pct = tick.spread_pct()
        assert abs(spread_pct - 0.005555555555555556) < 0.000001


class TestOHLCV:
    """Test OHLCV model."""
    
    def test_ohlcv_creation(self):
        """Test creating an OHLCV instance."""
        ohlcv = OHLCV(
            symbol="NIFTY",
            timestamp=datetime(2024, 1, 1, 10, 0, 0),
            open=18000.0,
            high=18050.0,
            low=17950.0,
            close=18025.0,
            volume=10000.0,
            timeframe=TimeFrame._1M
        )
        
        assert ohlcv.symbol == "NIFTY"
        assert ohlcv.open == 18000.0
        assert ohlcv.high == 18050.0
        assert ohlcv.low == 17950.0
        assert ohlcv.close == 18025.0
    
    def test_ohlcv_range(self):
        """Test OHLCV range calculation."""
        ohlcv = OHLCV(
            symbol="NIFTY",
            timestamp=datetime.now(),
            open=18000.0,
            high=18050.0,
            low=17950.0,
            close=18025.0,
            volume=10000.0
        )
        
        assert ohlcv.range == 100.0  # 18050 - 17950
    
    def test_ohlcv_body(self):
        """Test OHLCV body calculation."""
        ohlcv = OHLCV(
            symbol="NIFTY",
            timestamp=datetime.now(),
            open=18000.0,
            high=18050.0,
            low=17950.0,
            close=18025.0,
            volume=10000.0
        )
        
        assert ohlcv.body == 25.0  # |18025 - 18000|
    
    def test_ohlcv_is_bullish(self):
        """Test bullish candle detection."""
        bullish = OHLCV(
            symbol="NIFTY",
            timestamp=datetime.now(),
            open=18000.0,
            high=18050.0,
            low=17950.0,
            close=18025.0,
            volume=10000.0
        )
        
        bearish = OHLCV(
            symbol="NIFTY",
            timestamp=datetime.now(),
            open=18025.0,
            high=18050.0,
            low=17950.0,
            close=18000.0,
            volume=10000.0
        )
        
        assert bullish.is_bullish() is True
        assert bearish.is_bullish() is False
        assert bullish.is_bearish() is False
        assert bearish.is_bearish() is True


class TestStrategySignal:
    """Test StrategySignal model."""
    
    def test_strategy_signal_creation(self):
        """Test creating a StrategySignal instance."""
        signal = StrategySignal(
            strategy=StrategyName.ORB,
            signal=SignalType.BUY,
            confidence=0.85,
            reason="Breakout above opening range",
            symbol="NIFTY",
            timestamp=datetime.now(),
            suggested_sl=17950.0,
            suggested_tp=18100.0,
            timeframe=TimeFrame._5M
        )
        
        assert signal.strategy == StrategyName.ORB
        assert signal.signal == SignalType.BUY
        assert signal.confidence == 0.85
        assert signal.reason == "Breakout above opening range"
    
    def test_strategy_signal_to_dict(self):
        """Test StrategySignal to_dict method."""
        signal = StrategySignal(
            strategy=StrategyName.EMA_CROSSOVER,
            signal=SignalType.SELL,
            confidence=0.75,
            reason="EMA 9 crossed below EMA 21",
            symbol="BANKNIFTY",
            timestamp=datetime(2024, 1, 1, 10, 0, 0),
            timeframe=TimeFrame._1M
        )
        
        result = signal.to_dict()
        
        assert result["strategy"] == "EMA_CROSSOVER"
        assert result["signal"] == "SELL"
        assert result["confidence"] == 0.75
        assert result["symbol"] == "BANKNIFTY"


class TestAggregatedSignal:
    """Test AggregatedSignal model."""
    
    def test_aggregated_signal_creation(self):
        """Test creating an AggregatedSignal instance."""
        signals = [
            StrategySignal(
                strategy=StrategyName.ORB,
                signal=SignalType.BUY,
                confidence=0.8,
                reason="ORB breakout",
                symbol="NIFTY",
                timestamp=datetime.now()
            ),
            StrategySignal(
                strategy=StrategyName.VWAP_MR,
                signal=SignalType.BUY,
                confidence=0.7,
                reason="VWAP mean reversion",
                symbol="NIFTY",
                timestamp=datetime.now()
            )
        ]
        
        aggregated = AggregatedSignal(
            symbol="NIFTY",
            timestamp=datetime.now(),
            net_score=0.75,
            net_confidence=0.75,
            final_signal=SignalType.BUY,
            contributing_strategies=signals,
            regime_filter_passed=True
        )
        
        assert aggregated.symbol == "NIFTY"
        assert aggregated.net_score == 0.75
        assert aggregated.final_signal == SignalType.BUY
        assert len(aggregated.contributing_strategies) == 2


class TestOrder:
    """Test Order model."""
    
    def test_order_creation(self):
        """Test creating an Order instance."""
        order = Order(
            order_id="ORD001",
            symbol="NIFTY",
            order_side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=50.0,
            price=18000.0,
            status=OrderStatus.PLACED
        )
        
        assert order.order_id == "ORD001"
        assert order.symbol == "NIFTY"
        assert order.order_side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.quantity == 50.0
        assert order.price == 18000.0
    
    def test_order_remaining_quantity(self):
        """Test order remaining quantity calculation."""
        order = Order(
            order_id="ORD001",
            symbol="NIFTY",
            order_side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=100.0,
            filled_quantity=30.0
        )
        
        assert order.remaining_quantity == 70.0
    
    def test_order_is_completed(self):
        """Test order completion status."""
        completed = Order(
            order_id="ORD001",
            symbol="NIFTY",
            order_side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=50.0,
            filled_quantity=50.0
        )
        
        partial = Order(
            order_id="ORD002",
            symbol="NIFTY",
            order_side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=100.0,
            filled_quantity=50.0
        )
        
        assert completed.is_completed is True
        assert partial.is_completed is False
    
    def test_order_to_dict(self):
        """Test Order to_dict method."""
        order = Order(
            order_id="ORD001",
            symbol="NIFTY",
            order_side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=50.0,
            price=18000.0,
            status=OrderStatus.PLACED,
            validity=OrderValidity.MIS
        )
        
        result = order.to_dict()
        
        assert result["order_id"] == "ORD001"
        assert result["symbol"] == "NIFTY"
        assert result["side"] == "BUY"
        assert result["type"] == "LIMIT"
        assert result["quantity"] == 50.0


class TestPosition:
    """Test Position model."""
    
    def test_position_creation(self):
        """Test creating a Position instance."""
        position = Position(
            symbol="NIFTY",
            side=PositionSide.LONG,
            quantity=50.0,
            entry_price=18000.0,
            entry_timestamp=datetime.now(),
            strategy=StrategyName.ORB,
            stop_loss=17950.0,
            take_profit=18100.0
        )
        
        assert position.symbol == "NIFTY"
        assert position.side == PositionSide.LONG
        assert position.quantity == 50.0
        assert position.entry_price == 18000.0
    
    def test_position_pnl_long(self):
        """Test PnL calculation for long position."""
        position = Position(
            symbol="NIFTY",
            side=PositionSide.LONG,
            quantity=50.0,
            entry_price=18000.0,
            entry_timestamp=datetime.now()
        )
        
        # Current price higher than entry
        assert position.pnl(18050.0) == 250.0  # (18050 - 18000) * 50
        
        # Current price lower than entry
        assert position.pnl(17950.0) == -250.0  # (17950 - 18000) * 50
    
    def test_position_pnl_short(self):
        """Test PnL calculation for short position."""
        position = Position(
            symbol="NIFTY",
            side=PositionSide.SHORT,
            quantity=50.0,
            entry_price=18000.0,
            entry_timestamp=datetime.now()
        )
        
        # Current price lower than entry
        assert position.pnl(17950.0) == 250.0  # (18000 - 17950) * 50
        
        # Current price higher than entry
        assert position.pnl(18050.0) == -250.0  # (18000 - 18050) * 50
    
    def test_position_pnl_pct(self):
        """Test PnL percentage calculation."""
        position = Position(
            symbol="NIFTY",
            side=PositionSide.LONG,
            quantity=50.0,
            entry_price=18000.0,
            entry_timestamp=datetime.now()
        )
        
        # PnL of 250 on entry value of 900000 (18000 * 50)
        pnl_pct = position.pnl_pct(18050.0)
        expected_pct = (250 / 900000) * 100  # 0.027777...
        assert abs(pnl_pct - expected_pct) < 0.000001


class TestTrade:
    """Test Trade model."""
    
    def test_trade_creation(self):
        """Test creating a Trade instance."""
        trade = Trade(
            trade_id="TRADE001",
            symbol="NIFTY",
            side=OrderSide.BUY,
            entry_price=18000.0,
            exit_price=18050.0,
            quantity=50.0,
            entry_timestamp=datetime(2024, 1, 1, 10, 0, 0),
            exit_timestamp=datetime(2024, 1, 1, 10, 5, 0),
            strategy=StrategyName.ORB,
            confidence=0.85
        )
        
        assert trade.trade_id == "TRADE001"
        assert trade.symbol == "NIFTY"
        assert trade.entry_price == 18000.0
        assert trade.exit_price == 18050.0
    
    def test_trade_net_pnl(self):
        """Test net PnL calculation."""
        trade = Trade(
            trade_id="TRADE001",
            symbol="NIFTY",
            side=OrderSide.BUY,
            entry_price=18000.0,
            exit_price=18050.0,
            quantity=50.0,
            entry_timestamp=datetime.now(),
            exit_timestamp=datetime.now() + timedelta(minutes=5),
            pnl=250.0,  # Gross PnL
            brokerage=10.0,
            exchange_fees=5.0,
            stt=2.0,
            gst=1.0,
            stamp_duty=0.5,
            slippage=2.0
        )
        
        # Net PnL = 250 - (10 + 5 + 2 + 1 + 0.5 + 2) = 250 - 20.5 = 229.5
        assert trade.net_pnl == 229.5
    
    def test_trade_duration(self):
        """Test trade duration calculation."""
        entry_time = datetime(2024, 1, 1, 10, 0, 0)
        exit_time = datetime(2024, 1, 1, 10, 5, 30)
        
        trade = Trade(
            trade_id="TRADE001",
            symbol="NIFTY",
            side=OrderSide.BUY,
            entry_price=18000.0,
            exit_price=18050.0,
            quantity=50.0,
            entry_timestamp=entry_time,
            exit_timestamp=exit_time
        )
        
        assert trade.duration_seconds == 330.0  # 5 minutes 30 seconds
    
    def test_trade_return_pct(self):
        """Test trade return percentage calculation."""
        trade = Trade(
            trade_id="TRADE001",
            symbol="NIFTY",
            side=OrderSide.BUY,
            entry_price=18000.0,
            exit_price=18050.0,
            quantity=50.0,
            entry_timestamp=datetime.now(),
            exit_timestamp=datetime.now() + timedelta(minutes=5)
        )
        
        # Return % = ((18050 - 18000) / 18000) * 100 = (50 / 18000) * 100 = 0.277777...
        assert abs(trade.return_pct - 0.2777777777777778) < 0.000001


class TestRiskParameters:
    """Test RiskParameters model."""
    
    def test_risk_parameters_defaults(self):
        """Test RiskParameters default values."""
        risk = RiskParameters()
        
        assert risk.max_per_trade_risk_pct == 0.5
        assert risk.max_concurrent_positions == 5
        assert risk.max_daily_loss_pct == 2.0
        assert risk.max_daily_profit_pct == 4.0
        assert risk.trailing_stop_atr_multiplier == 1.5
        assert risk.hard_stop_loss_pct == 1.0
        assert risk.max_spread_pct == 0.15
        assert risk.slippage_buffer_pct == 0.05
        assert risk.cooldown_seconds == 60


class TestPerformanceMetrics:
    """Test PerformanceMetrics model."""
    
    def test_performance_metrics_defaults(self):
        """Test PerformanceMetrics default values."""
        metrics = PerformanceMetrics()
        
        assert metrics.total_trades == 0
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.total_pnl == 0.0
        assert metrics.total_net_pnl == 0.0


class TestPortfolioState:
    """Test PortfolioState model."""
    
    def test_portfolio_state_creation(self):
        """Test creating a PortfolioState instance."""
        positions = [
            Position(
                symbol="NIFTY",
                side=PositionSide.LONG,
                quantity=50.0,
                entry_price=18000.0,
                entry_timestamp=datetime.now()
            )
        ]
        
        state = PortfolioState(
            equity=105000.0,
            cash=5000.0,
            total_value=110000.0,
            positions=positions,
            open_trades=[],
            closed_trades=[]
        )
        
        assert state.equity == 105000.0
        assert state.cash == 5000.0
        assert state.num_open_positions == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])