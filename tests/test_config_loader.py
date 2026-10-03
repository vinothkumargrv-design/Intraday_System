"""
Unit tests for config_loader.py - Configuration management.
"""

import pytest
import os
import tempfile
from datetime import time

import sys

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from intraday_system.config_loader import ConfigLoader, load_config
from intraday_system.models import BrokerType, TimeFrame, StrategyName


class TestConfigLoader:
    """Test ConfigLoader class."""
    
    def test_load_config_valid(self):
        """Test loading a valid configuration file."""
        # Create a temporary config file
        config_content = """
broker:
  broker_type: ZERODHA
  api_key: "test_key"
  api_secret: "test_secret"

trading:
  paper_trading: true
  initial_capital: 100000
  symbols:
    - NIFTY
    - BANKNIFTY

risk:
  max_per_trade_risk_pct: 0.5
  max_concurrent_positions: 5

strategies:
  ORB:
    enabled: true
    weight: 1.2
  VWAP_MR:
    enabled: true
    weight: 1.0

logging:
  level: INFO
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            
            assert config.broker.broker_type == BrokerType.ZERODHA
            assert config.broker.api_key == "test_key"
            assert config.trading.paper_trading is True
            assert config.trading.initial_capital == 100000
            assert config.trading.symbols == ["NIFTY", "BANKNIFTY"]
            assert config.risk.max_per_trade_risk_pct == 0.5
        finally:
            os.unlink(config_path)
    
    def test_load_config_missing_file(self):
        """Test loading a non-existent configuration file."""
        loader = ConfigLoader("nonexistent.yaml")
        
        with pytest.raises(FileNotFoundError):
            loader.load_config()
    
    def test_load_config_invalid_yaml(self):
        """Test loading a configuration file with invalid YAML."""
        config_content = """
broker:
  broker_type: ZERODHA
  api_key: "test_key"
  invalid yaml here
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            
            with pytest.raises(Exception):  # YAML parsing error
                loader.load_config()
        finally:
            os.unlink(config_path)
    
    def test_config_validation(self):
        """Test configuration validation."""
        config_content = """
broker:
  broker_type: ZERODHA
  api_key: "test_key"
  api_secret: "test_secret"

trading:
  paper_trading: true
  initial_capital: 100000
  symbols:
    - NIFTY

risk:
  max_per_trade_risk_pct: 0.5
  max_concurrent_positions: 5
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            
            # Should not raise validation errors for valid config
            assert config is not None
        finally:
            os.unlink(config_path)


class TestConfigValidation:
    """Test configuration validation."""
    
    def test_required_fields(self):
        """Test that required fields are present."""
        config_content = """
broker:
  broker_type: ZERODHA

trading:
  paper_trading: true
  initial_capital: 100000
  symbols:
    - NIFTY
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            
            # Should have default values for missing optional fields
            assert hasattr(config, 'risk')
        finally:
            os.unlink(config_path)
    
    def test_time_conversion(self):
        """Test time string to time object conversion."""
        config_content = """
trading:
  paper_trading: true
  initial_capital: 100000

risk:
  time_stop_time: "15:15:00"
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            
            assert config.risk.time_stop_time == time(15, 15)
        finally:
            os.unlink(config_path)


class TestConfigLoaderMethods:
    """Test ConfigLoader methods."""
    
    def test_get_config_value(self):
        """Test getting specific configuration values."""
        config_content = """
broker:
  broker_type: ZERODHA

trading:
  paper_trading: true
  initial_capital: 100000
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            
            assert loader.get_config_value('broker.broker_type') == BrokerType.ZERODHA
            assert loader.get_config_value('trading.paper_trading') is True
            assert loader.get_config_value('trading.initial_capital') == 100000
        finally:
            os.unlink(config_path)
    
    def test_validate_config(self):
        """Test config validation method."""
        config_content = """
broker:
  broker_type: ZERODHA
  api_key: "test_key"
  api_secret: "test_secret"

trading:
  paper_trading: true
  initial_capital: 100000
  symbols:
    - NIFTY

risk:
  max_per_trade_risk_pct: 0.5
  max_concurrent_positions: 5
  max_daily_loss_pct: 2.0
  max_daily_profit_pct: 4.0
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            is_valid = loader.validate_config(config)
            
            assert is_valid is True
        finally:
            os.unlink(config_path)


class TestLoadConfigFunction:
    """Test the load_config function."""
    
    def test_load_config_function(self):
        """Test the global load_config function."""
        config_content = """
trading:
  paper_trading: true
  initial_capital: 100000
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            config = load_config(config_path)
            
            assert config.trading.paper_trading is True
            assert config.trading.initial_capital == 100000
        finally:
            os.unlink(config_path)
    
    def test_load_config_default_path(self):
        """Test loading config with default path."""
        # This test assumes config.yaml exists in the current directory
        # We'll skip it if the file doesn't exist
        if os.path.exists("config.yaml"):
            config = load_config()
            assert config is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])