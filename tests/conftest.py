"""
Shared pytest fixtures.

``paper_stack`` builds an isolated paper-trading system (data feed, portfolio,
risk manager, order manager with PaperBroker, execution engine) with no
background threads, temporary storage paths and market-hours enforcement
disabled, so tests are deterministic and can run at any time of day.
"""

import os
import sys
from datetime import datetime
from types import SimpleNamespace

import pytest

ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, ROOT)

CONFIG_PATH = os.path.join(ROOT, 'config.yaml')


def reset_singletons() -> None:
    """Tear down every process-wide component singleton."""
    from intraday_system import (data_feed, execution_engine, order_manager, portfolio_tracker,
                                 risk_manager, signal_aggregator, strategy_engine)
    from intraday_system.brokers import broker_factory
    if order_manager._order_manager is not None:
        order_manager._order_manager.shutdown()
    if risk_manager._risk_manager is not None:
        risk_manager._risk_manager.stop()
    for module, attr in ((data_feed, '_data_feed'), (execution_engine, '_execution_engine'),
                         (order_manager, '_order_manager'), (portfolio_tracker, '_portfolio_tracker'),
                         (risk_manager, '_risk_manager'), (signal_aggregator, '_signal_aggregator'),
                         (strategy_engine, '_strategy_engine')):
        setattr(module, attr, None)
    broker_factory.clear_broker_cache()


def make_test_config(tmp_path, **overrides):
    """Load config.yaml as the global config, redirected to tmp_path."""
    from intraday_system import config_loader
    config = config_loader.load_config(CONFIG_PATH)
    config['broker']['active'] = 'PAPER'
    config['broker']['paper'] = {'simulated_slippage': 0.0}
    config['system']['candle_db_path'] = str(tmp_path / 'candles.sqlite')
    config['system']['trade_journal_path'] = str(tmp_path / 'journal.csv')
    config['logging']['file']['enabled'] = False
    config['logging']['console']['enabled'] = False
    config['risk_management']['time_stop']['enforce_market_hours'] = False
    config['risk_management']['circuit_breakers']['manual_kill_file'] = str(tmp_path / 'KILL')
    config['execution']['entry']['order_type'] = 'MARKET'
    config['execution']['exits']['use_bracket_orders'] = True
    config['market_data']['websocket']['symbols'] = ['TCS', 'INFY']
    for path, value in overrides.items():
        node = config
        keys = path.split('.')
        for key in keys[:-1]:
            node = node[key]
        node[keys[-1]] = value
    from intraday_system.logger import setup_logging
    setup_logging(config)
    return config


@pytest.fixture
def paper_stack(tmp_path):
    reset_singletons()
    config = make_test_config(tmp_path)

    from intraday_system.data_feed import get_data_feed
    from intraday_system.execution_engine import get_execution_engine
    from intraday_system.models import Tick
    from intraday_system.order_manager import get_order_manager
    from intraday_system.portfolio_tracker import get_portfolio_tracker
    from intraday_system.risk_manager import get_risk_manager

    feed = get_data_feed()
    portfolio = get_portfolio_tracker()
    portfolio.start()
    risk = get_risk_manager()
    om = get_order_manager()
    engine = get_execution_engine()
    engine._running = True  # Accept signals without starting the monitor thread
    om.subscribe(engine._on_order_update)

    def tick(symbol, price, spread=0.1, volume=1_000_000):
        feed.handle_tick(Tick(symbol=symbol, timestamp=datetime.now(), last_price=price,
                              last_quantity=100, bid_price=price - spread / 2,
                              ask_price=price + spread / 2, volume=volume))
        om.poll_orders()
        engine._sync_all()
        engine._manage_open_positions()

    yield SimpleNamespace(config=config, feed=feed, portfolio=portfolio, risk=risk,
                          om=om, engine=engine, tick=tick)
    reset_singletons()
