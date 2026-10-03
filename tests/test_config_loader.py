"""
Unit tests for config_loader.py - configuration loading, defaults,
environment overrides and typed accessors.
"""

import os
import sys
from datetime import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from intraday_system.config_loader import (
    ConfigLoader, apply_env_overrides, load_config, parse_bool, parse_time, parse_timeframe
)
from intraday_system.models import BrokerType, StrategyName, TimeFrame

ROOT = os.path.join(os.path.dirname(__file__), '..')

SAMPLE = """
broker:
  active: ZERODHA
  zerodha:
    api_key: "test_key"
    api_secret: "test_secret"

market_data:
  websocket:
    symbols: [NIFTY, BANKNIFTY]

risk_management:
  per_trade:
    max_risk_pct: 0.75
  limits:
    max_concurrent_positions: 3
  time_stop:
    square_off_time: "15:10"

strategies:
  weights:
    ORB: 0.5
    EMA_CROSSOVER: 0.5
  parameters:
    ORB:
      timeframe: "5m"
    EMA_CROSSOVER:
      enabled: false
"""


@pytest.fixture
def sample_path(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(SAMPLE)
    # Run from tmp_path so the loader's fallback search paths don't find the repo config
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith("RAPIDFIRE_") or key.startswith("ZERODHA_"):
            monkeypatch.delenv(key, raising=False)
    return path


class TestLoading:
    def test_load_values(self, sample_path):
        loader = ConfigLoader(str(sample_path))
        config = loader.load()
        assert config['broker']['active'] == 'ZERODHA'
        assert loader.get('broker.zerodha.api_key') == 'test_key'
        assert loader.get_symbols() == ['NIFTY', 'BANKNIFTY']

    def test_defaults_are_filled(self, sample_path):
        config = ConfigLoader(str(sample_path)).load()
        assert config['risk_management']['daily']['max_loss_pct'] == 2.0
        assert config['signal_aggregation']['threshold'] == 0.65
        assert config['portfolio']['initial_equity'] == 100000.0

    def test_missing_file_gives_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        loader = ConfigLoader(str(tmp_path / "nope.yaml"))
        config = loader.load()
        assert config['broker']['active'] == 'PAPER'
        assert loader.is_paper()

    def test_get_with_default(self, sample_path):
        loader = ConfigLoader(str(sample_path))
        assert loader.get('does.not.exist', 42) == 42

    def test_from_dict(self):
        loader = ConfigLoader.from_dict({'broker': {'active': 'PAPER'}})
        assert loader.get('broker.active') == 'PAPER'
        assert loader.get('risk_management.limits.max_concurrent_positions') == 5

    def test_repository_config_loads(self):
        loader = ConfigLoader(os.path.join(ROOT, 'config.yaml'))
        config = loader.load()
        assert config['broker']['active'] == 'PAPER'
        assert len(loader.get_strategy_configs()) == 6


class TestTypedAccessors:
    def test_risk_parameters(self, sample_path):
        params = ConfigLoader(str(sample_path)).get_risk_parameters()
        assert params.max_per_trade_risk_pct == 0.75
        assert params.max_concurrent_positions == 3
        assert params.time_stop_time == time(15, 10)

    def test_strategy_configs(self, sample_path):
        configs = {c.name: c for c in ConfigLoader(str(sample_path)).get_strategy_configs()}
        assert configs[StrategyName.ORB].timeframe == TimeFrame._5M
        assert configs[StrategyName.ORB].enabled
        assert not configs[StrategyName.EMA_CROSSOVER].enabled  # Per-strategy flag respected

    def test_broker_config(self, sample_path):
        broker = ConfigLoader(str(sample_path)).get_broker_config()
        assert broker.broker_type == BrokerType.ZERODHA
        assert broker.api_key == 'test_key'
        assert broker.to_dict()['api_key'] == '***MASKED***'


class TestEnvironmentOverrides:
    def test_secret_from_environment(self, sample_path, monkeypatch):
        monkeypatch.setenv('ZERODHA_ACCESS_TOKEN', 'from-env')
        config = ConfigLoader(str(sample_path)).load()
        assert config['broker']['zerodha']['access_token'] == 'from-env'

    def test_prefixed_override_keeps_type(self, monkeypatch):
        monkeypatch.setenv('RAPIDFIRE_PORTFOLIO__INITIAL_EQUITY', '250000')
        monkeypatch.setenv('RAPIDFIRE_RISK_MANAGEMENT__DAILY__MAX_LOSS_PCT', '1.5')
        result = apply_env_overrides({'portfolio': {'initial_equity': 100000.0},
                                      'risk_management': {'daily': {'max_loss_pct': 2.0}}})
        assert result['portfolio']['initial_equity'] == 250000.0
        assert result['risk_management']['daily']['max_loss_pct'] == 1.5


class TestParsers:
    @pytest.mark.parametrize("value,expected", [("true", True), ("0", False), ("yes", True), (False, False)])
    def test_parse_bool(self, value, expected):
        assert parse_bool(value) is expected

    def test_parse_time(self):
        assert parse_time("09:15") == time(9, 15)
        assert parse_time("garbage") == time(15, 15)

    def test_parse_timeframe(self):
        assert parse_timeframe("5m") == TimeFrame._5M
        assert parse_timeframe("TICK") == TimeFrame.TICK
        assert parse_timeframe("weird") == TimeFrame._1M


def test_load_config_function_sets_global(sample_path):
    from intraday_system import config_loader
    config = load_config(str(sample_path))
    assert config_loader.get_config() is config
