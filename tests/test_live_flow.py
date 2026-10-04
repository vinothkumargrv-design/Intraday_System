"""
Integration tests for the live/paper order flow:

    AggregatedSignal -> ExecutionEngine -> OrderManager -> PaperBroker
                     -> fills -> brackets (OCO) -> exits -> PortfolioTracker

Market data is injected through DataFeed.handle_tick so no network or
background feed is needed.
"""

from datetime import datetime

import pytest

from intraday_system.models import (
    AggregatedSignal, OrderStatus, OrderType, SignalType, StrategyName, StrategySignal
)


@pytest.fixture
def system(paper_stack):
    return paper_stack


def _signal(symbol, direction, price, sl, tp):
    now = datetime.now()
    contributors = [
        StrategySignal(strategy=name, signal=direction, confidence=0.8, reason="test",
                       symbol=symbol, timestamp=now, suggested_sl=sl, suggested_tp=tp)
        for name in (StrategyName.EMA_CROSSOVER, StrategyName.SUPERTREND)
    ]
    return AggregatedSignal(symbol=symbol, timestamp=now, net_score=1.0 if direction == SignalType.BUY else -1.0,
                            net_confidence=0.8, final_signal=direction,
                            contributing_strategies=contributors, regime_filter_passed=True)


def _open(system, symbol, direction=SignalType.BUY, price=1000.0, sl=990.0, tp=1020.0):
    system.tick(symbol, price)
    system.engine.on_aggregated_signal(_signal(symbol, direction, price, sl, tp))
    positions = [mp for mp in system.engine.get_active_trades() if mp.symbol == symbol]
    assert len(positions) == 1
    return positions[0]


def test_entry_places_protective_brackets(system):
    mp = _open(system, 'TCS')
    assert mp.state == 'OPEN'
    assert mp.entry_price == pytest.approx(1000.0)
    sl = system.om.get_order(mp.sl_order_id)
    tp = system.om.get_order(mp.tp_order_id)
    assert sl.order_type == OrderType.SL_M and sl.trigger_price == pytest.approx(990.0)
    assert tp.order_type == OrderType.LIMIT and tp.price == pytest.approx(1020.0)
    assert sl.status == OrderStatus.PLACED and tp.status == OrderStatus.PLACED
    assert system.portfolio.has_position('TCS')
    # Risk-based size: 0.5% of 100k / 10 stop = 50 shares
    assert mp.quantity == 50


def test_take_profit_fill_cancels_stop(system):
    mp = _open(system, 'TCS')
    sl_id = mp.sl_order_id
    system.tick('TCS', 1021.0)

    assert not system.engine.get_active_trades()
    assert system.om.get_order(sl_id).status == OrderStatus.CANCELLED
    trades = system.portfolio.get_trade_history()
    assert len(trades) == 1
    trade = trades[0]
    assert trade.tags[0] == 'TAKE_PROFIT'
    assert trade.pnl == pytest.approx((1020.0 - 1000.0) * 50)
    assert trade.net_pnl < trade.pnl  # Charges applied
    assert system.om.broker.net_quantity('TCS') == 0


def test_stop_loss_on_short(system):
    mp = _open(system, 'INFY', SignalType.SELL, price=1500.0, sl=1510.0, tp=1480.0)
    assert mp.stop_loss == pytest.approx(1510.0)
    system.tick('INFY', 1512.0)

    trades = system.portfolio.get_trade_history()
    assert len(trades) == 1 and trades[0].tags[0] == 'STOP_LOSS'
    assert trades[0].pnl < 0
    assert system.om.broker.net_quantity('INFY') == 0


def test_square_off_sends_real_exit_and_cancels_brackets(system):
    mp = _open(system, 'TCS')
    sl_id, tp_id = mp.sl_order_id, mp.tp_order_id
    system.tick('TCS', 1005.0)
    system.engine.square_off_all_positions("TIME_STOP")

    assert not system.engine.get_active_trades()
    assert system.om.get_order(sl_id).status == OrderStatus.CANCELLED
    assert system.om.get_order(tp_id).status == OrderStatus.CANCELLED
    assert system.om.broker.net_quantity('TCS') == 0
    trade = system.portfolio.get_trade_history()[0]
    assert trade.tags[0] == 'TIME_STOP'
    assert trade.exit_price == pytest.approx(1005.0)


def test_one_position_per_symbol(system):
    _open(system, 'TCS')
    system.engine.on_aggregated_signal(_signal('TCS', SignalType.BUY, 1000.0, 990.0, 1020.0))
    assert len(system.engine.get_active_trades()) == 1
    assert system.om.broker.net_quantity('TCS') == 50


def test_trailing_stop_moves_broker_stop(system):
    mp = _open(system, 'TCS')
    # Build some 1m bars so ATR is available
    from intraday_system.models import OHLCV, TimeFrame
    for i in range(16):
        system.feed.cache.add_ohlcv(OHLCV(symbol='TCS', timestamp=datetime(2024, 1, 1, 10, i),
                                          open=1000, high=1001, low=999, close=1000, volume=1000,
                                          timeframe=TimeFrame._1M))
    system.tick('TCS', 1015.0)  # 1.5R in profit -> trailing armed
    sl = system.om.get_order(mp.sl_order_id)
    assert mp.stop_loss > 990.0
    assert sl.trigger_price == pytest.approx(mp.stop_loss)


def test_daily_loss_limit_trips_kill_switch_and_flattens(system):
    # 2% of 100k = 2000 loss limit; a 50-share stop-out costs ~500, so use a gap
    mp = _open(system, 'TCS')
    _open(system, 'INFY', price=1000.0, sl=990.0, tp=1020.0)
    system.tick('TCS', 940.0)   # Gap through the stop: ~ -3000 realized

    assert system.risk.get_risk_state().kill_switch_activated
    assert not system.engine.get_active_trades()
    assert system.om.broker.net_quantity('INFY') == 0
    # New entries are refused
    system.engine.on_aggregated_signal(_signal('TCS', SignalType.BUY, 940.0, 930.0, 960.0))
    assert not system.engine.get_active_trades()


def test_limit_entry_falls_back_to_market(system):
    """An unfilled LIMIT entry is replaced by a MARKET order and the position still opens."""
    system.engine.entry_order_type = 'LIMIT'
    system.om.fallback_timeout = 0
    system.tick('TCS', 1000.0)
    # The engine prices the limit below the market, so it rests unfilled
    system.engine._get_current_price = lambda symbol: 995.0
    system.engine.on_aggregated_signal(_signal('TCS', SignalType.BUY, 995.0, 985.0, 1015.0))
    mp = system.engine.get_active_trades()[0]
    assert mp.state == 'PENDING_ENTRY'
    limit = system.om.get_order(mp.entry_order_ids[0])
    assert limit.order_type == OrderType.LIMIT and limit.status == OrderStatus.PLACED
    
    system.om.check_order_timeouts()
    del system.engine._get_current_price
    system.tick('TCS', 1000.0)
    
    assert system.om.get_order(limit.order_id).status == OrderStatus.CANCELLED
    assert len(mp.entry_order_ids) == 2
    assert mp.state == 'OPEN' and mp.entry_price == pytest.approx(1000.0)
    assert system.om.broker.net_quantity('TCS') == mp.quantity
