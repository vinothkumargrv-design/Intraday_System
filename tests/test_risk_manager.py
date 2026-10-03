"""
Unit tests for risk_manager.py - Risk management module.
"""

import pytest
import os
from datetime import datetime, time
from unittest.mock import Mock, MagicMock

import sys

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from intraday_system.models import (
    Position,
    PositionSide,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    StrategyName,
    RiskParameters,
    RiskState,
    PortfolioState,
    Tick,
    SignalType
)
from intraday_system.risk_manager import RiskManager
from intraday_system.config_loader import Config, RiskConfig, TradingConfig


class MockConfig:
    """Mock configuration for testing."""
    
    def __init__(self):
        self.trading = TradingConfig(
            paper_trading=True,
            initial_capital=100000.0,
            symbols=["NIFTY", "BANKNIFTY"]
        )
        
        self.risk = RiskConfig(
            max_per_trade_risk_pct=0.5,
            max_concurrent_positions=5,
            max_daily_loss_pct=2.0,
            max_daily_profit_pct=4.0,
            trailing_stop_atr_multiplier=1.5,
            hard_stop_loss_pct=1.0,
            time_stop_time=time(15, 15),
            max_spread_pct=0.15,
            slippage_buffer_pct=0.05,
            cooldown_seconds=60
        )


class TestRiskManagerInitialization:
    """Test RiskManager initialization."""
    
    def test_risk_manager_init(self):
        """Test RiskManager initialization."""
        config = MockConfig()
        portfolio_tracker = Mock()
        system_state = Mock()
        
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        assert risk_manager is not None
        assert risk_manager.config == config
        assert risk_manager.portfolio_tracker == portfolio_tracker
        assert risk_manager.system_state == system_state
        assert risk_manager._daily_pnl == 0.0
        assert risk_manager._daily_pnl_pct == 0.0


class TestRiskChecks:
    """Test risk check methods."""
    
    def test_check_max_positions_pass(self):
        """Test max positions check when under limit."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.get_num_open_positions.return_value = 2
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        result = risk_manager.check_max_positions("NIFTY")
        
        assert result.passed is True
    
    def test_check_max_positions_fail(self):
        """Test max positions check when at limit."""
        config = MockConfig()
        config.risk.max_concurrent_positions = 2
        
        portfolio_tracker = Mock()
        portfolio_tracker.get_num_open_positions.return_value = 2
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        result = risk_manager.check_max_positions("NIFTY")
        
        assert result.passed is False
    
    def test_check_position_in_symbol(self):
        """Test existing position check."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.has_position.return_value = True
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        result = risk_manager.check_no_existing_position("NIFTY")
        
        assert result.passed is False
        assert "already has" in result.reason.lower()
    
    def test_check_cooldown(self):
        """Test cooldown check."""
        config = MockConfig()
        portfolio_tracker = Mock()
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # Add symbol to cooldown
        risk_manager._cooldown_symbols["NIFTY"] = datetime.now()
        
        result = risk_manager.check_cooldown("NIFTY")
        
        assert result.passed is False
        assert "cooldown" in result.reason.lower()


class TestPositionSizing:
    """Test position sizing calculations."""
    
    def test_calculate_position_size(self):
        """Test position size calculation."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.get_current_equity.return_value = 100000.0
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # Position size = (equity * risk_pct) / |entry - SL|
        # = (100000 * 0.005) / |18000 - 17900| = 500 / 100 = 5
        size = risk_manager.calculate_position_size("NIFTY", 18000.0, 17900.0)
        
        assert size == 5.0
    
    def test_calculate_position_size_sell(self):
        """Test position size calculation for sell orders."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.get_current_equity.return_value = 100000.0
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # For sell orders, SL is above entry
        # Size = (100000 * 0.005) / |18000 - 18100| = 500 / 100 = 5
        size = risk_manager.calculate_position_size("NIFTY", 18000.0, 18100.0, OrderSide.SELL)
        
        assert size == 5.0


class TestRiskState:
    """Test risk state management."""
    
    def test_get_state(self):
        """Test getting risk state."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.get_current_equity.return_value = 100000.0
        portfolio_tracker.get_num_open_positions.return_value = 2
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # Add some cooldown symbols
        risk_manager._cooldown_symbols["NIFTY"] = datetime.now()
        
        state = risk_manager.get_state()
        
        assert isinstance(state, RiskState)
        assert state.daily_pnl == 0.0
        assert state.open_positions_count == 2
        assert len(state.cooldown_symbols) == 1


class TestKillSwitch:
    """Test kill switch functionality."""
    
    def test_check_kill_switch_daily_loss(self):
        """Test kill switch on daily loss."""
        config = MockConfig()
        config.risk.max_daily_loss_pct = 2.0
        
        portfolio_tracker = Mock()
        portfolio_tracker.get_initial_capital.return_value = 100000.0
        portfolio_tracker.get_daily_pnl.return_value = -3000.0  # 3% loss
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # Manually set daily PnL to trigger kill switch
        risk_manager._daily_pnl = -3000.0
        risk_manager._daily_pnl_pct = -3.0
        
        risk_manager.check_daily_limits()
        
        assert risk_manager._kill_switch_activated is True
    
    def test_check_kill_switch_daily_profit(self):
        """Test kill switch on daily profit limit."""
        config = MockConfig()
        config.risk.max_daily_profit_pct = 4.0
        
        portfolio_tracker = Mock()
        portfolio_tracker.get_initial_capital.return_value = 100000.0
        portfolio_tracker.get_daily_pnl.return_value = 5000.0  # 5% profit
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        # Manually set daily PnL to trigger profit limit
        risk_manager._daily_pnl = 5000.0
        risk_manager._daily_pnl_pct = 5.0
        
        risk_manager.check_daily_limits()
        
        # Should activate kill switch on profit limit
        assert risk_manager._daily_profit_limit_reached is True


class TestSpreadCheck:
    """Test spread validation."""
    
    def test_check_spread_pass(self):
        """Test spread check with acceptable spread."""
        config = MockConfig()
        portfolio_tracker = Mock()
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17999.5,
            ask_price=18000.5
        )
        
        result = risk_manager.check_spread(tick, OrderSide.BUY)
        
        # Spread is 1.0, which is (1.0/18000)*100 = 0.00555% < 0.15%
        assert result.passed is True
    
    def test_check_spread_fail(self):
        """Test spread check with excessive spread."""
        config = MockConfig()
        config.risk.max_spread_pct = 0.1  # 0.1% max spread
        
        portfolio_tracker = Mock()
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17900.0,  # Spread = 100
            ask_price=18100.0
        )
        
        result = risk_manager.check_spread(tick, OrderSide.BUY)
        
        # Spread is 200, which is (200/18000)*100 = 1.111% > 0.1%
        assert result.passed is False


class TestMultipleRiskChecks:
    """Test combined risk checks."""
    
    def test_check_all_risk_pass(self):
        """Test all risk checks passing."""
        config = MockConfig()
        portfolio_tracker = Mock()
        portfolio_tracker.has_position.return_value = False
        portfolio_tracker.get_num_open_positions.return_value = 0
        portfolio_tracker.get_current_equity.return_value = 100000.0
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17999.5,
            ask_price=18000.5
        )
        
        result = risk_manager.check_all_risk(
            symbol="NIFTY",
            side=OrderSide.BUY,
            price=18000.0,
            stop_loss=17900.0,
            tick=tick
        )
        
        assert result.passed is True
    
    def test_check_all_risk_fail_max_positions(self):
        """Test all risk checks failing on max positions."""
        config = MockConfig()
        config.risk.max_concurrent_positions = 1
        
        portfolio_tracker = Mock()
        portfolio_tracker.has_position.return_value = False
        portfolio_tracker.get_num_open_positions.return_value = 1
        portfolio_tracker.get_current_equity.return_value = 100000.0
        
        system_state = Mock()
        risk_manager = RiskManager(config, portfolio_tracker, system_state)
        
        tick = Tick(
            symbol="NIFTY",
            timestamp=datetime.now(),
            last_price=18000.0,
            bid_price=17999.5,
            ask_price=18000.5
        )
        
        result = risk_manager.check_all_risk(
            symbol="NIFTY",
            side=OrderSide.BUY,
            price=18000.0,
            stop_loss=17900.0,
            tick=tick
        )
        
        assert result.passed is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])