"""
Tests for strategy signal logic and the shared aggregation rules.
"""

from datetime import datetime, timedelta

import pytest

from intraday_system.models import (
    OHLCV, SignalType, StrategyConfig, StrategyName, StrategySignal, TimeFrame
)
from intraday_system.signal_aggregator import (
    atr_percentile, decide, latest_per_strategy, regime_allows_trading, score_signals
)
from intraday_system.strategy_engine import (
    EMACrossoverScalperStrategy, OpeningRangeBreakoutStrategy, SupertrendStrategy
)

T0 = datetime(2024, 1, 2, 9, 15)


def bar(i, close, open_=None, high=None, low=None, volume=10_000, tf=TimeFrame._1M, minutes=1, start=T0):
    open_ = close if open_ is None else open_
    return OHLCV(symbol='TCS', timestamp=start + timedelta(minutes=i * minutes), open=open_,
                 high=high if high is not None else max(open_, close) + 0.5,
                 low=low if low is not None else min(open_, close) - 0.5,
                 close=close, volume=volume, timeframe=tf)


def run(strategy, bars):
    signals = []
    for b in bars:
        strategy.clock = b.timestamp
        s = strategy.on_bar(b)
        if s:
            signals.append(s)
    return signals


class TestEMACrossover:
    def make(self):
        return EMACrossoverScalperStrategy(StrategyConfig(
            name=StrategyName.EMA_CROSSOVER, parameters={'rsi_filter_enabled': False}))

    def test_fires_once_per_cross(self):
        prices = [100.0] * 25 + [100 + i for i in range(1, 15)]  # Flat, then trend up
        signals = run(self.make(), [bar(i, p) for i, p in enumerate(prices)])
        assert [s.signal for s in signals] == [SignalType.BUY]

    def test_cross_down_after_up(self):
        prices = [100.0] * 25 + [100 + i for i in range(1, 15)] + [114 - 2 * i for i in range(1, 20)]
        signals = run(self.make(), [bar(i, p) for i, p in enumerate(prices)])
        assert [s.signal for s in signals] == [SignalType.BUY, SignalType.SELL]

    def test_signal_carries_market_time(self):
        prices = [100.0] * 25 + [100 + i for i in range(1, 15)]
        bars = [bar(i, p) for i, p in enumerate(prices)]
        signal = run(self.make(), bars)[0]
        assert T0 <= signal.timestamp <= bars[-1].timestamp
        assert signal.suggested_sl < bars[0].close + 15 < signal.suggested_tp + 15


class TestSupertrend:
    def test_flips_on_trend_reversal(self):
        strategy = SupertrendStrategy(StrategyConfig(
            name=StrategyName.SUPERTREND, parameters={'trending_regime_only': False}))
        up = [100 + i * 0.8 for i in range(40)]
        down = [up[-1] - i * 1.2 for i in range(1, 40)]
        signals = run(strategy, [bar(i, p) for i, p in enumerate(up + down)])
        assert signals, "Supertrend never flipped"
        assert signals[-1].signal == SignalType.SELL
        # Never two consecutive signals in the same direction
        assert all(a.signal != b.signal for a, b in zip(signals, signals[1:]))


class TestORB:
    def make(self):
        return OpeningRangeBreakoutStrategy(StrategyConfig(
            name=StrategyName.ORB, timeframe=TimeFrame._5M, parameters={'orb_multiplier': 0.1}))

    def test_breakout_once_per_direction(self):
        strategy = self.make()
        five = dict(tf=TimeFrame._5M, minutes=5)
        bars = [bar(0, 100, high=101, low=99, **five), bar(1, 100, high=101, low=99, **five),
                bar(2, 100, high=101, low=99, **five), bar(3, 100, **five),     # range set
                bar(4, 102, **five), bar(5, 103, **five)]                        # breakout twice
        signals = run(strategy, bars)
        assert [s.signal for s in signals] == [SignalType.BUY]
        assert signals[0].suggested_sl == pytest.approx(99)  # Opening range low

    def test_reset_allows_next_day(self):
        strategy = self.make()
        five = dict(tf=TimeFrame._5M, minutes=5)
        day = [bar(0, 100, high=101, low=99, **five), bar(1, 100, high=101, low=99, **five),
               bar(2, 100, high=101, low=99, **five), bar(3, 100, **five), bar(4, 102, **five)]
        assert len(run(strategy, day)) == 1
        strategy.reset('TCS')
        next_day = [bar(i, b.close, high=b.high, low=b.low, start=T0 + timedelta(days=1), **five)
                    for i, b in enumerate(day)]
        assert len(run(strategy, next_day)) == 1


def sig(strategy, direction, confidence=0.8, at=T0):
    return StrategySignal(strategy=strategy, signal=direction, confidence=confidence,
                          reason='', symbol='TCS', timestamp=at)


WEIGHTS = {name: 1 / 6 for name in StrategyName}


class TestAggregation:
    def test_agreement_required(self):
        one = [sig(StrategyName.ORB, SignalType.BUY)]
        score, buys, sells = score_signals(one, WEIGHTS)
        assert score == 1.0
        assert decide(score, buys, sells, 0.65, 2) == SignalType.HOLD  # Only one strategy

    def test_two_agreeing(self):
        two = [sig(StrategyName.ORB, SignalType.BUY), sig(StrategyName.SUPERTREND, SignalType.BUY)]
        assert decide(*score_signals(two, WEIGHTS), 0.65, 2) == SignalType.BUY

    def test_disagreement_does_not_count_as_agreement(self):
        mixed = [sig(StrategyName.ORB, SignalType.BUY), sig(StrategyName.SUPERTREND, SignalType.BUY),
                 sig(StrategyName.VWAP_MR, SignalType.SELL), sig(StrategyName.EMA_CROSSOVER, SignalType.SELL)]
        score, buys, sells = score_signals(mixed, WEIGHTS)
        assert score == pytest.approx(0.0)
        assert decide(score, buys, sells, 0.65, 2) == SignalType.HOLD

    def test_sell_side(self):
        two = [sig(StrategyName.ORB, SignalType.SELL), sig(StrategyName.MOMENTUM_BURST, SignalType.SELL)]
        assert decide(*score_signals(two, WEIGHTS), 0.65, 2) == SignalType.SELL

    def test_latest_per_strategy_and_ttl(self):
        signals = [sig(StrategyName.ORB, SignalType.SELL, at=T0),
                   sig(StrategyName.ORB, SignalType.BUY, at=T0 + timedelta(minutes=1)),
                   sig(StrategyName.EMA_CROSSOVER, SignalType.BUY, at=T0 - timedelta(minutes=30))]
        latest = latest_per_strategy(signals, now=T0 + timedelta(minutes=2), max_age_seconds=300)
        assert [(s.strategy, s.signal) for s in latest] == [(StrategyName.ORB, SignalType.BUY)]


class TestRegimeFilter:
    def test_percentile_of_calm_vs_volatile(self):
        calm = [bar(i, 100, high=100.2, low=99.8) for i in range(60)]
        spike = calm + [bar(60 + i, 100, high=103, low=97) for i in range(5)]
        assert atr_percentile(spike) > 95
        assert atr_percentile(calm) == pytest.approx(50.0)
        assert not regime_allows_trading(spike, 0.75)
        assert regime_allows_trading(calm, 0.75)

    def test_insufficient_history_allows(self):
        assert atr_percentile([bar(i, 100) for i in range(10)]) is None
        assert regime_allows_trading([bar(i, 100) for i in range(10)], 0.75)
