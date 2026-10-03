"""
Tests for the notifier's event routing and the dashboard's data helpers.
"""

import json
from datetime import datetime, timedelta

import pandas as pd
import pytest

from intraday_system.dashboard import (
    equity_from_trades, latest_backtest, load_status, load_trades, status_age_seconds, trade_stats
)
from intraday_system.models import NotificationLevel
from intraday_system.notifier import Notifier


def make_notifier(telegram_events=("TRADE_EXIT", "KILL_SWITCH_ACTIVATED"), rate=10):
    config = {'notifications': {'enabled': True, 'telegram': {
        'enabled': True, 'bot_token': 'TOKEN', 'chat_id': '42',
        'notify_on': list(telegram_events), 'rate_limit': {'max_messages_per_minute': rate}}}}
    notifier = Notifier(config)
    sent = []
    notifier._send_telegram = lambda n, _orig=notifier._send_telegram: sent.append(n)
    return notifier, sent


class TestNotifier:
    def test_disabled_without_credentials(self):
        notifier = Notifier({'notifications': {'telegram': {'enabled': True, 'bot_token': ''}}})
        assert not notifier.telegram_enabled and not notifier.email_enabled

    def test_only_configured_events_are_sent(self):
        notifier, sent = make_notifier()
        notifier.notify("TRADE_ENTRY", "t", "m")
        notifier.notify("TRADE_EXIT", "t", "m")
        assert [n.event for n in sent] == ["TRADE_EXIT"]
        assert len(notifier.history) == 2

    def test_execution_events_are_mapped(self):
        notifier, sent = make_notifier(telegram_events=("STOP_LOSS_HIT", "TAKE_PROFIT_HIT", "TRADE_EXIT"))
        base = {'symbol': 'TCS', 'side': 'BUY', 'quantity': 10, 'entry_price': 100, 'exit_price': 99,
                'net_pnl': -12.5}
        notifier.on_execution_event('position_closed', {**base, 'tags': 'STOP_LOSS,EMA_CROSSOVER'})
        notifier.on_execution_event('position_closed', {**base, 'tags': 'TAKE_PROFIT,ORB'})
        notifier.on_execution_event('position_closed', {**base, 'tags': 'TIME_STOP,ORB'})
        assert [n.event for n in sent] == ["STOP_LOSS_HIT", "TAKE_PROFIT_HIT", "TRADE_EXIT"]
        assert "-12.50" in sent[0].message

    def test_kill_switch_event(self):
        notifier, sent = make_notifier(telegram_events=("KILL_SWITCH_ACTIVATED", "DAILY_LIMIT_HIT"))
        notifier.on_risk_event('kill_switch_activated', {'reason': 'Daily loss limit (2.0%) breached'})
        assert [n.event for n in sent] == ["KILL_SWITCH_ACTIVATED", "DAILY_LIMIT_HIT"]
        assert sent[0].level == NotificationLevel.CRITICAL

    def test_rate_limit_spares_critical(self):
        notifier = Notifier({'notifications': {'telegram': {
            'enabled': True, 'bot_token': 'T', 'chat_id': '1', 'notify_on': ['TRADE_EXIT', 'ERROR'],
            'rate_limit': {'max_messages_per_minute': 2}}}})
        posted = []
        import intraday_system.notifier as mod

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return b'{"ok": true}'

        original = mod.urllib.request.urlopen
        mod.urllib.request.urlopen = lambda req, timeout=10: posted.append(json.loads(req.data)) or FakeResponse()
        try:
            for _ in range(4):
                notifier.notify("TRADE_EXIT", "exit", "m")
            notifier.notify("ERROR", "boom", "m", level=NotificationLevel.ERROR)
        finally:
            mod.urllib.request.urlopen = original
        assert len(posted) == 3  # 2 trade exits within limit + the error
        assert posted[0]['chat_id'] == '1'

    def test_background_worker_flushes_on_stop(self):
        notifier, sent = make_notifier()
        notifier.start()
        for _ in range(3):
            notifier.notify("TRADE_EXIT", "t", "m")
        notifier.stop()
        assert len(sent) == 3


def trades_frame():
    t0 = datetime(2024, 1, 2, 10, 0)
    return pd.DataFrame({
        'exit_timestamp': [t0, t0 + timedelta(minutes=5), t0 + timedelta(minutes=10)],
        'net_pnl': [100.0, -50.0, 25.0],
        'brokerage': [10.0, 10.0, 10.0],
        'stt': [1.0, 1.0, 1.0],
    })


class TestDashboardData:
    def test_trade_stats(self):
        stats = trade_stats(trades_frame())
        assert stats['trades'] == 3
        assert stats['net_pnl'] == pytest.approx(75.0)
        assert stats['win_rate'] == pytest.approx(2 / 3)
        assert stats['profit_factor'] == pytest.approx(125 / 50)
        assert stats['charges'] == pytest.approx(33.0)

    def test_empty_stats(self):
        assert trade_stats(pd.DataFrame())['trades'] == 0

    def test_equity_and_drawdown(self):
        curve = equity_from_trades(trades_frame(), 1000.0)
        assert curve['equity'].tolist() == [1100.0, 1050.0, 1075.0]
        assert curve['drawdown_pct'].iloc[1] == pytest.approx(50 / 1100 * 100)

    def test_status_file(self, tmp_path):
        path = tmp_path / 'status.json'
        assert load_status(path) is None
        path.write_text(json.dumps({'timestamp': datetime.now().isoformat(), 'equity': 1}))
        status = load_status(path)
        assert status['equity'] == 1 and status_age_seconds(status) < 5
        path.write_text("{broken")
        assert load_status(path) is None

    def test_load_trades_and_latest_backtest(self, tmp_path):
        assert latest_backtest(tmp_path) is None
        (tmp_path / 'summary_20240101_000000.txt').write_text('old')
        (tmp_path / 'summary_20240102_000000.txt').write_text('new')
        csv = tmp_path / 'trades_20240102_000000.csv'
        csv.write_text("entry_timestamp,exit_timestamp,net_pnl,tags\n"
                       "2024-01-02 10:00,2024-01-02 10:05,10,\"TAKE_PROFIT,ORB\"\n")
        run = latest_backtest(tmp_path)
        assert run['stamp'] == '20240102_000000' and run['trades'] == csv
        trades = load_trades(csv)
        assert trades['exit_reason'].tolist() == ['TAKE_PROFIT']
