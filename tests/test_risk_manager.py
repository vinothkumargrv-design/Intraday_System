"""
Unit tests for risk_manager.py - position sizing, pre-trade checks,
cooldowns, daily limits and the kill switch.
"""

from datetime import datetime, timedelta

import pytest

from intraday_system.models import OrderSide, Position, PositionSide


def _position(symbol='TCS', side=PositionSide.LONG, qty=10, price=1000.0):
    return Position(symbol=symbol, side=side, quantity=qty, entry_price=price,
                    entry_timestamp=datetime.now())


class TestPositionSizing:
    def test_risk_based_size(self, paper_stack):
        # 0.5% of 100k = 500 risk; 10 stop distance -> 50 shares
        assert paper_stack.risk.calculate_position_size('TCS', 1000.0, 990.0) == 50

    def test_short_side_uses_absolute_distance(self, paper_stack):
        assert paper_stack.risk.calculate_position_size('TCS', 1000.0, 1010.0) == 50

    def test_leverage_caps_notional(self, paper_stack):
        # Tiny stop would mean 5000 shares; cap = 100k * 5x / 5 positions = 100k notional
        assert paper_stack.risk.calculate_position_size('TCS', 1000.0, 999.9) == 100

    def test_zero_stop_distance(self, paper_stack):
        assert paper_stack.risk.calculate_position_size('TCS', 1000.0, 1000.0) == 0


class TestPreTradeChecks:
    def test_all_checks_pass(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 50, 1000.0, 990.0)
        assert result.passed, result.reason

    def test_requires_market_data(self, paper_stack):
        result = paper_stack.risk.check_trade_risk('INFY', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'No tick data' in result.reason

    def test_existing_position_blocks(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        paper_stack.portfolio.on_position_opened(_position('TCS'))
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'Existing position' in result.reason

    def test_max_concurrent_positions(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        for i in range(5):
            paper_stack.portfolio.on_position_opened(_position(f'SYM{i}', qty=1))
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'Max concurrent positions' in result.reason

    def test_exposure_limit(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        # 500% of 100k equity = 500k max gross notional
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 600, 1000.0, 990.0)
        assert not result.passed and 'Exposure limit' in result.reason

    def test_wide_spread_blocks(self, paper_stack):
        paper_stack.tick('TCS', 1000.0, spread=5.0)  # 0.5% > 0.15% max
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'Spread too wide' in result.reason

    def test_low_volume_blocks(self, paper_stack):
        paper_stack.tick('TCS', 1000.0, volume=10)
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'Insufficient volume' in result.reason

    def test_cooldown_after_exit(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        paper_stack.risk._state.cooldown_symbols['TCS'] = datetime.utcnow()
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'cooldown' in result.reason

        paper_stack.risk._state.cooldown_symbols['TCS'] = datetime.utcnow() - timedelta(minutes=5)
        assert paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0).passed


class TestDailyLimitsAndKillSwitch:
    def _book_realized(self, stack, pnl):
        from intraday_system.models import Trade
        now = datetime.now()
        trade = Trade(trade_id=f'T{pnl}', symbol='X', side=OrderSide.BUY, entry_price=100,
                      exit_price=100 + pnl, quantity=1, entry_timestamp=now, exit_timestamp=now,
                      pnl=pnl)
        stack.portfolio.on_trade_closed(trade)
        stack.risk.on_trade_closed(trade, trade.exit_price)

    def test_daily_profit_target_stops_new_trades(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        self._book_realized(paper_stack, 4500)  # 4.5% > 4% target
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed and 'profit target' in result.reason
        assert not paper_stack.risk.get_risk_state().kill_switch_activated

    def test_daily_loss_limit_activates_kill_switch(self, paper_stack):
        self._book_realized(paper_stack, -2500)  # 2.5% loss > 2% limit
        state = paper_stack.risk.get_risk_state()
        assert state.kill_switch_activated
        assert state.daily_pnl == pytest.approx(-2500)

    def test_kill_file_activates_kill_switch(self, paper_stack, tmp_path):
        paper_stack.tick('TCS', 1000.0)
        (tmp_path / 'KILL').write_text('stop')
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed
        assert paper_stack.risk.get_risk_state().kill_switch_activated

    def test_consecutive_failures_activate_kill_switch(self, paper_stack):
        paper_stack.tick('TCS', 1000.0)
        for i in range(3):
            paper_stack.risk.on_order_failed(f'O{i}', 'rejected')
        result = paper_stack.risk.check_trade_risk('TCS', OrderSide.BUY, 10, 1000.0, 990.0)
        assert not result.passed
        assert paper_stack.risk.get_risk_state().kill_switch_activated

    def test_fill_resets_failure_counter(self, paper_stack):
        paper_stack.risk.on_order_failed('O1', 'rejected')
        paper_stack.risk.on_order_filled('O2', 10, 1000.0)
        assert paper_stack.risk.get_risk_state().consecutive_failures == 0


class TestStopHelpers:
    def test_stop_and_target_hits(self, paper_stack):
        long_pos = _position(side=PositionSide.LONG)
        long_pos.stop_loss, long_pos.take_profit = 990.0, 1020.0
        assert paper_stack.risk.check_stop_loss_hit(long_pos, 989.0)
        assert not paper_stack.risk.check_stop_loss_hit(long_pos, 995.0)
        assert paper_stack.risk.check_take_profit_hit(long_pos, 1021.0)

        short_pos = _position(side=PositionSide.SHORT)
        short_pos.stop_loss, short_pos.take_profit = 1010.0, 980.0
        assert paper_stack.risk.check_stop_loss_hit(short_pos, 1011.0)
        assert paper_stack.risk.check_take_profit_hit(short_pos, 979.0)

    def test_default_stop_without_atr(self, paper_stack):
        sl, tp = paper_stack.risk.calculate_stop_loss('TCS', 1000.0, OrderSide.BUY)
        assert sl == pytest.approx(990.0)   # 1% hard stop
        assert tp == pytest.approx(1020.0)  # 2R
