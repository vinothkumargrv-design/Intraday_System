"""
Tests for the backtester: data loading, determinism, ledger consistency,
no look-ahead, risk sizing and the cost model.
"""

import asyncio
import os
from datetime import date, datetime

import pytest

from intraday_system.backtester import (
    Backtester, bar_to_ticks, generate_synthetic_data, load_csv_bars, resample_bars
)
from intraday_system.config_loader import ConfigLoader
from intraday_system.costs import CostModel
from intraday_system.models import OrderSide, TimeFrame

ROOT_CONFIG = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')


def base_config(tmp_path):
    config = ConfigLoader(ROOT_CONFIG).load()
    config['logging']['file']['enabled'] = False
    config['logging']['console']['enabled'] = False
    return config


def run_synthetic(tmp_path, symbols=('RELIANCE', 'TCS', 'INFY'), days=8):
    bt = Backtester(base_config(tmp_path), start_date='2024-01-01', end_date='2100-01-01',
                    symbols=list(symbols), synthetic=True, synthetic_days=days,
                    results_dir=str(tmp_path / 'results'))
    asyncio.run(bt.setup())
    assert asyncio.run(bt.load_historical_data())
    asyncio.run(bt.run(save=True))
    return bt


class TestData:
    def test_synthetic_data_is_reproducible_and_in_session(self):
        a = generate_synthetic_data('TCS', date(2024, 1, 1), 2)
        b = generate_synthetic_data('TCS', date(2024, 1, 1), 2)
        assert [x.close for x in a] == [x.close for x in b]
        assert len(a) == 2 * 375
        assert all(x.low <= min(x.open, x.close) and x.high >= max(x.open, x.close) for x in a)
        assert {x.timestamp.weekday() for x in a} <= {0, 1, 2, 3, 4}
        assert a[0].timestamp.time().hour == 9 and a[0].timestamp.time().minute == 15

    def test_csv_loading(self, tmp_path):
        path = tmp_path / 'TCS.csv'
        path.write_text("Timestamp,Open,High,Low,Close,Volume\n"
                        "2024-01-02 09:16:00,101,102,100,101.5,500\n"
                        "2024-01-02 09:15:00,100,101,99,100.5,400\n")
        bars = load_csv_bars(path, 'TCS')
        assert [b.close for b in bars] == [100.5, 101.5]  # Sorted by time
        assert bars[0].timeframe == TimeFrame._1M

    def test_resample_emits_on_last_minute(self):
        bars = generate_synthetic_data('TCS', date(2024, 1, 1), 1)[:10]
        five = resample_bars(bars, 5, TimeFrame._5M)
        first = five[bars[4].timestamp]
        assert first.timestamp == bars[0].timestamp
        assert first.open == bars[0].open and first.close == bars[4].close
        assert first.high == max(b.high for b in bars[:5])
        assert first.volume == sum(b.volume for b in bars[:5])

    def test_ticks_follow_bar_path(self):
        b = generate_synthetic_data('TCS', date(2024, 1, 1), 1)[0]
        ticks = bar_to_ticks(b, None)
        assert ticks[0].last_price == b.open and ticks[-1].last_price == b.close
        assert {t.last_price for t in ticks} >= {b.high, b.low}


class TestBacktestRun:
    def test_runs_and_ledger_is_consistent(self, tmp_path):
        bt = run_synthetic(tmp_path)
        assert bt.trades, "Expected some trades on 8 days x 3 symbols"
        final_equity = bt.equity_curve[-1]['equity']
        assert final_equity == pytest.approx(bt.initial_capital + sum(t.net_pnl for t in bt.trades))
        # Results written
        files = {p.name.split('_')[0] for p in (tmp_path / 'results').iterdir()}
        assert {'trades', 'summary', 'equity'} <= files

    def test_deterministic(self, tmp_path):
        a = run_synthetic(tmp_path / 'a')
        b = run_synthetic(tmp_path / 'b')
        assert [t.to_dict() for t in a.trades] == [t.to_dict() for t in b.trades]

    def test_trade_invariants(self, tmp_path):
        bt = run_synthetic(tmp_path)
        bars = {s: {b.timestamp: b for b in v} for s, v in bt.historical_data.items()}
        for t in bt.trades:
            # Entry happens at the open of an existing bar (decided on the previous bar)
            assert t.entry_timestamp in bars[t.symbol]
            assert t.entry_price == bars[t.symbol][t.entry_timestamp].open
            assert t.exit_timestamp >= t.entry_timestamp
            # Intraday: never held overnight
            assert t.entry_timestamp.date() == t.exit_timestamp.date()
            direction = 1 if t.side == OrderSide.BUY else -1
            assert t.pnl == pytest.approx((t.exit_price - t.entry_price) * t.quantity * direction)
            assert t.net_pnl < t.pnl

    def test_no_entries_after_cutoff(self, tmp_path):
        bt = run_synthetic(tmp_path)
        assert all(t.entry_timestamp.time() < bt.no_entry_after for t in bt.trades)


class TestCostModel:
    def test_charges(self):
        costs = CostModel(brokerage_rate=0.0003, brokerage_cap=20, exchange_fee_rate=0.0000297,
                          stt_rate=0.00025, gst_rate=0.18, stamp_duty_rate=0.00003)
        c = costs.charges(buy_value=100_000, sell_value=101_000)
        assert c['brokerage'] == pytest.approx(40.0)  # Capped at 20 per order
        assert c['stt'] == pytest.approx(101_000 * 0.00025)
        assert c['stamp_duty'] == pytest.approx(100_000 * 0.00003)
        assert c['gst'] == pytest.approx((c['brokerage'] + c['exchange_fees']) * 0.18)

    def test_slippage_is_adverse(self):
        costs = CostModel(slippage_rate=0.001)
        assert costs.fill_price(100, OrderSide.BUY) == pytest.approx(100.1)
        assert costs.fill_price(100, OrderSide.SELL) == pytest.approx(99.9)
