"""
Event-driven backtester for the Rapid Fire Intraday Trading System.

This module provides:
- Bar-by-bar event-driven simulation over 1-minute OHLCV data
- The same strategy classes and signal-aggregation rules used live
- Risk-based position sizing, stop loss / take profit / trailing stop,
  time-based square-off, per-symbol cooldown and daily loss/profit limits
- Indian intraday cost model: brokerage, exchange fees, STT, GST, stamp duty,
  slippage
- Performance metrics (Sharpe, Sortino, CAGR, max drawdown, profit factor, ...)
- Trade-by-trade CSV, summary, equity curve CSV and plot
- Walk-forward / grid optimisation helper

Key design principle: signals come from the live strategy implementations and
the live aggregation functions; only order execution is simulated.

Historical data is read from CSV files named ``<SYMBOL>.csv`` or
``<SYMBOL>_1m.csv`` in ``backtester.data.data_dir`` with the columns
``timestamp, open, high, low, close, volume``. If no data is found and
``backtester.data.source`` is ``synthetic`` (or ``synthetic=True`` is passed),
a reproducible random-walk dataset is generated instead.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config_loader import Config, ConfigLoader, parse_time
from .costs import CostModel
from .logger import get_logger
from .models import (
    OHLCV,
    OrderSide,
    PerformanceMetrics,
    SignalType,
    StrategyName,
    StrategySignal,
    Tick,
    TimeFrame,
    Trade,
    TradeStatus,
)
from .signal_aggregator import (
    decide, latest_per_strategy, net_confidence, regime_allows_trading, score_signals
)
from .strategy_engine import (
    BaseStrategy,
    EMACrossoverScalperStrategy,
    MomentumBurstStrategy,
    OpeningRangeBreakoutStrategy,
    OrderFlowImbalanceStrategy,
    SupertrendStrategy,
    VWAPMeanReversionStrategy,
)


STRATEGY_CLASSES = {
    StrategyName.ORB: OpeningRangeBreakoutStrategy,
    StrategyName.VWAP_MR: VWAPMeanReversionStrategy,
    StrategyName.MOMENTUM_BURST: MomentumBurstStrategy,
    StrategyName.EMA_CROSSOVER: EMACrossoverScalperStrategy,
    StrategyName.ORDER_FLOW: OrderFlowImbalanceStrategy,
    StrategyName.SUPERTREND: SupertrendStrategy,
}

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)


# =============================================================================
# SUPPORT TYPES
# =============================================================================

@dataclass
class SimPosition:
    """An open simulated position."""
    symbol: str
    side: OrderSide
    quantity: int
    entry_price: float          # Ideal (pre-slippage) entry price
    entry_fill: float           # Actual fill price
    entry_time: datetime
    stop_loss: float
    take_profit: float
    initial_risk: float         # |entry - stop| per share
    strategy: Optional[StrategyName]
    confidence: float
    contributors: List[str] = field(default_factory=list)
    best_price: float = 0.0     # Most favourable price seen (for trailing)

    def unrealized(self, price: float) -> float:
        direction = 1 if self.side == OrderSide.BUY else -1
        return (price - self.entry_fill) * self.quantity * direction


@dataclass
class PendingEntry:
    """An entry decided on a bar close, filled at the next bar's open."""
    side: OrderSide
    stop_loss: Optional[float]
    take_profit: Optional[float]
    strategy: Optional[StrategyName]
    confidence: float
    contributors: List[str]


# =============================================================================
# DATA HELPERS
# =============================================================================

def _stable_seed(text: str) -> int:
    return int(hashlib.sha256(text.encode()).hexdigest()[:8], 16)


def generate_synthetic_data(symbol: str, start: date, days: int,
                            base_price: Optional[float] = None,
                            seed: Optional[int] = None) -> List[OHLCV]:
    """
    Generate reproducible synthetic 1-minute bars for NSE market hours.

    Each day has its own drift and volatility regime so that trending and
    ranging sessions both occur. Volume follows the usual U-shaped intraday
    profile with occasional bursts.
    """
    rng = np.random.default_rng(seed if seed is not None else _stable_seed(symbol))
    price = base_price or float(100 + _stable_seed(symbol) % 2900)
    bars: List[OHLCV] = []
    minutes_per_day = 375  # 09:15 - 15:30

    current = start
    produced = 0
    while produced < days:
        if current.weekday() >= 5:  # Skip weekends
            current += timedelta(days=1)
            continue

        # Overnight gap and daily regime
        price *= 1 + rng.normal(0, 0.006)
        trending = rng.random() < 0.4
        drift = rng.normal(0, 0.00015) * (4 if trending else 1)
        vol = rng.uniform(0.0006, 0.0016)
        base_volume = rng.uniform(5_000, 20_000)
        session_start = datetime.combine(current, MARKET_OPEN)

        for i in range(minutes_per_day):
            ts = session_start + timedelta(minutes=i)
            open_ = price
            ret = drift + rng.normal(0, vol)
            close = max(0.01, open_ * (1 + ret))
            wick = abs(rng.normal(0, vol / 2))
            high = max(open_, close) * (1 + wick)
            low = min(open_, close) * (1 - wick)

            # U-shaped volume curve plus random bursts
            u_shape = 1.0 + 2.0 * ((i - minutes_per_day / 2) / (minutes_per_day / 2)) ** 2
            burst = 3.0 if rng.random() < 0.02 else 1.0
            volume = float(int(base_volume * u_shape * burst * rng.uniform(0.6, 1.4)))

            bars.append(OHLCV(symbol=symbol, timestamp=ts, open=round(open_, 2),
                              high=round(high, 2), low=round(low, 2),
                              close=round(close, 2), volume=volume,
                              timeframe=TimeFrame._1M))
            price = close

        produced += 1
        current += timedelta(days=1)

    return bars


def load_csv_bars(path: Path, symbol: str) -> List[OHLCV]:
    """Load 1-minute bars from a CSV file."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    ts_col = next((c for c in ('timestamp', 'datetime', 'date', 'time') if c in df.columns), None)
    if ts_col is None:
        raise ValueError(f"{path}: no timestamp column")
    missing = {'open', 'high', 'low', 'close'} - set(df.columns)
    if missing:
        raise ValueError(f"{path}: missing columns {sorted(missing)}")
    if 'volume' not in df.columns:
        df['volume'] = 0.0

    df[ts_col] = pd.to_datetime(df[ts_col])
    if getattr(df[ts_col].dt, 'tz', None) is not None:
        df[ts_col] = df[ts_col].dt.tz_localize(None)
    df = df.sort_values(ts_col).drop_duplicates(ts_col)

    return [
        OHLCV(symbol=symbol, timestamp=row[0].to_pydatetime(), open=float(row[1]),
              high=float(row[2]), low=float(row[3]), close=float(row[4]),
              volume=float(row[5]), timeframe=TimeFrame._1M)
        for row in df[[ts_col, 'open', 'high', 'low', 'close', 'volume']].itertuples(index=False)
    ]


def resample_bars(bars: List[OHLCV], minutes: int, timeframe: TimeFrame) -> Dict[datetime, OHLCV]:
    """
    Resample 1-minute bars into N-minute bars.

    Returns a map from the timestamp of the *last* 1-minute bar in each bucket
    to the completed N-minute bar, so the bar can be emitted exactly when it
    closes.
    """
    result: Dict[datetime, OHLCV] = {}
    bucket: List[OHLCV] = []
    bucket_key = None

    def flush():
        if bucket:
            result[bucket[-1].timestamp] = OHLCV(
                symbol=bucket[0].symbol, timestamp=bucket_key,
                open=bucket[0].open, high=max(b.high for b in bucket),
                low=min(b.low for b in bucket), close=bucket[-1].close,
                volume=sum(b.volume for b in bucket), timeframe=timeframe)

    for bar in bars:
        ts = bar.timestamp
        key = ts.replace(minute=ts.minute - ts.minute % minutes, second=0, microsecond=0)
        if key != bucket_key:
            flush()
            bucket = []
            bucket_key = key
        bucket.append(bar)
    flush()
    return result


def bar_to_ticks(bar: OHLCV, prev_close: Optional[float], spread_pct: float = 0.0005) -> List[Tick]:
    """
    Decompose a bar into four synthetic ticks (O, H/L, L/H, C).

    Upticks trade at the ask and downticks at the bid (tick rule) so that
    order-flow style strategies see a plausible aggressor side.
    """
    path = [bar.open, bar.low, bar.high, bar.close] if bar.is_bullish() else \
           [bar.open, bar.high, bar.low, bar.close]
    ticks = []
    last = prev_close if prev_close is not None else bar.open
    qty = bar.volume / 4 if bar.volume else 0.0
    for i, price in enumerate(path):
        half_spread = price * spread_pct / 2
        if price >= last:  # Buyer-initiated: trade at the ask
            bid, ask = price - 2 * half_spread, price
        else:              # Seller-initiated: trade at the bid
            bid, ask = price, price + 2 * half_spread
        ticks.append(Tick(symbol=bar.symbol, timestamp=bar.timestamp + timedelta(seconds=15 * i),
                          last_price=price, last_quantity=qty, bid_price=bid,
                          bid_quantity=qty / 2, ask_price=ask, ask_quantity=qty / 2,
                          volume=qty * (i + 1)))
        last = price
    return ticks


# =============================================================================
# BACKTESTER
# =============================================================================

class Backtester:
    """
    Event-driven backtester that mirrors live trading logic.

    Each 1-minute bar is processed in timestamp order across all symbols:
    1. Pending entries fill at the bar open (no look-ahead).
    2. Open positions are checked against the bar's high/low for stop loss /
       take profit, then trailing stops and time stops are applied.
    3. The bar (and synthetic ticks) are fed to the strategies; signals are
       aggregated with the live voting rules and may queue a new entry.
    4. Equity is marked to market.
    """

    def __init__(self, config: Config, start_date: Optional[str] = None,
                 end_date: Optional[str] = None, symbols: Optional[List[str]] = None,
                 synthetic: Optional[bool] = None, synthetic_days: int = 20,
                 results_dir: Optional[str] = None):
        self.logger = get_logger("backtester")
        self.config_loader = ConfigLoader.from_dict(config)
        self.config = self.config_loader.load()

        bt = self.config.get('backtester', {}) or {}
        data_cfg = bt.get('data', {}) or {}
        risk = self.config.get('risk_management', {}) or {}

        # Data settings
        self.data_path = data_cfg.get('data_dir', 'data/historical')
        self.start_date = self._parse_date(start_date or bt.get('start_date'))
        self.end_date = self._parse_date(end_date or bt.get('end_date'))
        self.synthetic = (str(data_cfg.get('source', 'local')).lower() == 'synthetic'
                          if synthetic is None else synthetic)
        self.synthetic_days = synthetic_days
        self.results_dir = Path(results_dir or (bt.get('metrics', {}) or {}).get('output_dir', 'results'))

        configured_symbols = (self.config.get('market_data', {}).get('websocket', {}) or {}).get('symbols', [])
        self.symbols: List[str] = list(symbols or configured_symbols or ["RELIANCE", "TCS", "INFY"])

        # Capital and risk
        self.initial_capital = float(bt.get('initial_capital',
                                            self.config.get('portfolio', {}).get('initial_equity', 100000.0)))
        risk_params = self.config_loader.get_risk_parameters()
        self.risk_per_trade_pct = risk_params.max_per_trade_risk_pct
        self.max_positions = risk_params.max_concurrent_positions
        self.max_daily_loss_pct = risk_params.max_daily_loss_pct
        self.max_daily_profit_pct = risk_params.max_daily_profit_pct
        self.hard_stop_pct = risk_params.hard_stop_loss_pct
        self.min_stop_pct = risk_params.min_stop_loss_pct
        self.trailing_enabled = bool((risk.get('stop_loss', {}) or {}).get('trailing_stop_enabled', True))
        self.trailing_atr_mult = risk_params.trailing_stop_atr_multiplier
        # Trailing only starts once the trade is this many R in profit
        self.trailing_activation_r = float((risk.get('stop_loss', {}) or {}).get('trailing_activation_r', 1.0))
        time_stop = risk.get('time_stop', {}) or {}
        self.time_stop_enabled = bool(time_stop.get('enabled', True))
        self.square_off_time: time = parse_time(time_stop.get('square_off_time', '15:15'))
        self.no_entry_after: time = (datetime.combine(date.today(), self.square_off_time) -
                                     timedelta(minutes=int(time_stop.get('pre_square_off_minutes', 5)))).time()
        self.max_leverage = float((risk.get('leverage', {}) or {}).get('max_leverage', 5.0))

        agg = self.config_loader.get_aggregation_config()
        self.threshold = agg.threshold
        self.min_agreeing = agg.min_agreeing_strategies
        sig_cfg = self.config.get('signal_aggregation', {}) or {}
        self.cooldown_seconds = float((sig_cfg.get('cooldown', {}) or {}).get('seconds_per_symbol', 60))
        self.signal_ttl_seconds = float(sig_cfg.get('signal_ttl_seconds', 300))
        self.use_regime_filter = agg.use_regime_filter
        self.regime_threshold = agg.regime_volatility_threshold
        self.regime_lookback = int((sig_cfg.get('regime', {}) or {}).get('lookback_bars', 120))

        self.costs = CostModel.from_config(self.config)

        # State (populated by setup / run)
        self.strategies: Dict[str, Dict[StrategyName, BaseStrategy]] = {}
        self.strategy_weights: Dict[StrategyName, float] = {}
        self.historical_data: Dict[str, List[OHLCV]] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.performance_metrics: Optional[PerformanceMetrics] = None
        self.signal_counts: Dict[str, int] = {}
        self._reset_state()

        self.logger.info("Backtester initialized")

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        if value in (None, ''):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    def _reset_state(self) -> None:
        self.cash = self.initial_capital
        self.positions: Dict[str, SimPosition] = {}
        self.pending: Dict[str, PendingEntry] = {}
        self.signal_buffer: Dict[str, List[StrategySignal]] = {}
        self.cooldown_until: Dict[str, datetime] = {}
        self.last_price: Dict[str, float] = {}
        self.prev_close: Dict[str, float] = {}
        self.atr: Dict[str, float] = {}
        self.recent_bars: Dict[str, List[OHLCV]] = {}
        self.current_day: Optional[date] = None
        self.day_start_equity = self.initial_capital
        self.trading_halted_today = False
        self.peak_equity = self.initial_capital
        self.trades = []
        self.equity_curve = []
        self.signal_counts = {}
        self._trade_seq = 0

    async def setup(self) -> None:
        """Instantiate one set of strategies per symbol."""
        configs = [c for c in self.config_loader.get_strategy_configs() if c.enabled]
        total = sum(c.weight for c in configs) or 1.0
        self.strategy_weights = {c.name: c.weight / total for c in configs}

        self.strategies = {}
        for symbol in self.symbols:
            self.strategies[symbol] = {}
            for cfg in configs:
                cls = STRATEGY_CLASSES.get(cfg.name)
                if cls:
                    self.strategies[symbol][cfg.name] = cls(copy.deepcopy(cfg))

        self.logger.info(f"Backtester set up with {len(configs)} strategies for {len(self.symbols)} symbols")

    async def load_historical_data(self) -> bool:
        """Load 1-minute bars for every symbol (CSV, or synthetic fallback)."""
        data_dir = Path(self.data_path)
        self.historical_data = {}

        for symbol in self.symbols:
            bars: List[OHLCV] = []
            for name in (f"{symbol}.csv", f"{symbol}_1m.csv", f"{symbol.lower()}.csv"):
                path = data_dir / name
                if path.exists():
                    try:
                        bars = load_csv_bars(path, symbol)
                        self.logger.info(f"Loaded {len(bars)} bars for {symbol} from {path}")
                    except Exception as e:
                        self.logger.error(f"Failed to load {path}: {e}")
                    break

            if not bars and self.synthetic:
                start = self.start_date or date(2024, 1, 1)
                bars = generate_synthetic_data(symbol, start, self.synthetic_days)
                self.logger.info(f"Generated {len(bars)} synthetic bars for {symbol}")

            if self.start_date:
                bars = [b for b in bars if b.timestamp.date() >= self.start_date]
            if self.end_date:
                bars = [b for b in bars if b.timestamp.date() <= self.end_date]

            if bars:
                self.historical_data[symbol] = bars
            else:
                self.logger.warning(f"No data found for {symbol}")

        if not self.historical_data:
            self.logger.error(f"No historical data loaded from {data_dir}. Cannot backtest.")
            return False
        return True

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, save: bool = True) -> Dict[str, Any]:
        """Run the complete backtest and return the results dictionary."""
        self.logger.info("Starting backtest...")
        if not self.strategies:
            await self.setup()
        if not self.historical_data and not await self.load_historical_data():
            return self.get_results()

        self._reset_state()
        five_minute = {s: resample_bars(bars, 5, TimeFrame._5M) for s, bars in self.historical_data.items()}

        # Build a global timeline of (timestamp, symbol, bar)
        events: List[Tuple[datetime, str, OHLCV]] = [
            (bar.timestamp, symbol, bar)
            for symbol, bars in self.historical_data.items() for bar in bars
        ]
        events.sort(key=lambda e: (e[0], e[1]))

        last_ts: Optional[datetime] = None
        for ts, symbol, bar in events:
            if last_ts is not None and ts != last_ts:
                self._record_equity(last_ts)
            if self.current_day != ts.date():
                self._start_new_day(ts.date())
            last_ts = ts

            self._process_bar(symbol, bar, five_minute[symbol].get(ts))

        # Close anything still open at the end of the data
        if last_ts is not None:
            for symbol in list(self.positions):
                self._close_position(symbol, self.last_price[symbol], last_ts, "END_OF_DATA")
            self._record_equity(last_ts)

        self._calculate_performance_metrics()
        if save:
            self._save_results()

        self.logger.info(f"Backtest completed: {len(self.trades)} trades")
        return self.get_results()

    def _start_new_day(self, day: date) -> None:
        # Flatten anything carried over (intraday system)
        for symbol in list(self.positions):
            self._close_position(symbol, self.last_price[symbol],
                                 datetime.combine(self.current_day or day, MARKET_CLOSE), "TIME_STOP")
        self.pending.clear()
        self.signal_buffer.clear()
        self.current_day = day
        self.day_start_equity = self._equity()
        self.trading_halted_today = False
        for symbol, per_symbol in self.strategies.items():
            for strategy in per_symbol.values():
                strategy.reset(symbol)

    def _process_bar(self, symbol: str, bar: OHLCV, bar_5m: Optional[OHLCV]) -> None:
        bar_close_time = bar.timestamp + timedelta(minutes=1)

        # 1. Fill pending entry at this bar's open
        pending = self.pending.pop(symbol, None)
        if pending and symbol not in self.positions and not self.trading_halted_today:
            self._open_position(symbol, pending, bar.open, bar.timestamp)

        # 2. Manage open position against this bar
        if symbol in self.positions:
            self._manage_position(symbol, bar, bar_close_time)

        self._update_atr(symbol, bar)
        self.last_price[symbol] = bar.close
        history = self.recent_bars.setdefault(symbol, [])
        history.append(bar)
        del history[:-self.regime_lookback]

        # 3. Strategies
        new_signals = self._run_strategies(symbol, bar, bar_5m, bar_close_time)
        self.prev_close[symbol] = bar.close

        if new_signals:
            buffer = self.signal_buffer.setdefault(symbol, [])
            buffer.extend(new_signals)
            del buffer[:-20]
            self._maybe_queue_entry(symbol, bar_close_time)

        # 4. Daily risk limits
        self._check_daily_limits(bar_close_time)

    def _run_strategies(self, symbol: str, bar: OHLCV, bar_5m: Optional[OHLCV],
                        clock: datetime) -> List[StrategySignal]:
        signals: List[StrategySignal] = []
        strategies = self.strategies.get(symbol, {})
        ticks = bar_to_ticks(bar, self.prev_close.get(symbol))

        for name, strategy in strategies.items():
            try:
                for tick in ticks:
                    strategy.clock = tick.timestamp
                    signal = strategy.on_tick(tick)
                    if signal:
                        signals.append(signal)

                strategy.clock = clock
                if strategy.timeframe == TimeFrame._1M:
                    signal = strategy.on_bar(bar)
                    if signal:
                        signals.append(signal)
                elif strategy.timeframe == TimeFrame._5M and bar_5m is not None:
                    signal = strategy.on_bar(bar_5m)
                    if signal:
                        signals.append(signal)
            except Exception as e:
                self.logger.error(f"Strategy {name.value} failed on {symbol}: {e}")

        for s in signals:
            self.signal_counts[s.strategy.value] = self.signal_counts.get(s.strategy.value, 0) + 1
        return signals

    # ------------------------------------------------------------------
    # Entry logic
    # ------------------------------------------------------------------

    def _maybe_queue_entry(self, symbol: str, now: datetime) -> None:
        if (self.trading_halted_today or symbol in self.positions or symbol in self.pending
                or len(self.positions) + len(self.pending) >= self.max_positions):
            return
        if now.time() >= self.no_entry_after or now.time() < MARKET_OPEN:
            return
        if symbol in self.cooldown_until and now < self.cooldown_until[symbol]:
            return
        if self.use_regime_filter and not regime_allows_trading(
                self.recent_bars.get(symbol, []), self.regime_threshold):
            return

        signals = latest_per_strategy(self.signal_buffer.get(symbol, []), now, self.signal_ttl_seconds)
        if not signals:
            return
        net_score, buy_count, sell_count = score_signals(signals, self.strategy_weights)
        decision = decide(net_score, buy_count, sell_count, self.threshold, self.min_agreeing)
        if decision == SignalType.HOLD:
            return

        agreeing = [s for s in signals if s.signal == decision]
        side = OrderSide.BUY if decision == SignalType.BUY else OrderSide.SELL
        stops = [s.suggested_sl for s in agreeing if s.suggested_sl]
        targets = [s.suggested_tp for s in agreeing if s.suggested_tp]
        lead = max(agreeing, key=lambda s: s.confidence * self.strategy_weights.get(s.strategy, 0))

        self.pending[symbol] = PendingEntry(
            side=side,
            stop_loss=float(np.median(stops)) if stops else None,
            take_profit=float(np.median(targets)) if targets else None,
            strategy=lead.strategy,
            confidence=net_confidence(signals, decision, net_score),
            contributors=sorted(s.strategy.value for s in agreeing),
        )
        # Consume the signals that produced this entry
        self.signal_buffer[symbol] = []

    def _open_position(self, symbol: str, entry: PendingEntry, price: float, ts: datetime) -> None:
        side = entry.side
        direction = 1 if side == OrderSide.BUY else -1

        # Stop loss: strategy suggestion if on the correct side, capped at the hard stop
        hard_distance = price * self.hard_stop_pct / 100
        stop = entry.stop_loss
        if stop is None or (stop - price) * direction >= 0:
            stop_distance = hard_distance
        else:
            stop_distance = min(abs(price - stop), hard_distance)
        stop_distance = max(stop_distance, price * self.min_stop_pct / 100)
        stop_price = price - direction * stop_distance
        
        target = entry.take_profit
        if target is None or (target - price) * direction <= 0:
            target = price + direction * 2 * stop_distance
        elif abs(target - price) < stop_distance:  # Never accept less than 1R
            target = price + direction * stop_distance

        # Risk-based position sizing, capped by available leverage
        equity = self._equity()
        risk_amount = equity * self.risk_per_trade_pct / 100
        quantity = int(risk_amount / stop_distance)
        max_notional = equity * self.max_leverage / max(self.max_positions, 1)
        quantity = min(quantity, int(max_notional / price))
        if quantity <= 0:
            return

        fill = self.costs.fill_price(price, side)
        self.positions[symbol] = SimPosition(
            symbol=symbol, side=side, quantity=quantity, entry_price=price,
            entry_fill=fill, entry_time=ts, stop_loss=stop_price, take_profit=target,
            initial_risk=stop_distance, strategy=entry.strategy,
            confidence=entry.confidence, contributors=entry.contributors, best_price=price)
        self.logger.debug(f"OPEN {side.value} {quantity} {symbol} @ {fill:.2f} SL {stop_price:.2f} TP {target:.2f}")

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def _manage_position(self, symbol: str, bar: OHLCV, bar_close_time: datetime) -> None:
        pos = self.positions[symbol]
        long = pos.side == OrderSide.BUY

        # Gaps through the stop/target fill at the open
        if long:
            if bar.open <= pos.stop_loss:
                return self._close_position(symbol, bar.open, bar.timestamp, "STOP_LOSS")
            if bar.open >= pos.take_profit:
                return self._close_position(symbol, bar.open, bar.timestamp, "TAKE_PROFIT")
            stop_hit, target_hit = bar.low <= pos.stop_loss, bar.high >= pos.take_profit
        else:
            if bar.open >= pos.stop_loss:
                return self._close_position(symbol, bar.open, bar.timestamp, "STOP_LOSS")
            if bar.open <= pos.take_profit:
                return self._close_position(symbol, bar.open, bar.timestamp, "TAKE_PROFIT")
            stop_hit, target_hit = bar.high >= pos.stop_loss, bar.low <= pos.take_profit

        # If both are inside the bar, assume the stop was hit first (conservative)
        if stop_hit:
            return self._close_position(symbol, pos.stop_loss, bar_close_time, "STOP_LOSS")
        if target_hit:
            return self._close_position(symbol, pos.take_profit, bar_close_time, "TAKE_PROFIT")

        # Time-based square-off
        if self.time_stop_enabled and bar_close_time.time() >= self.square_off_time:
            return self._close_position(symbol, bar.close, bar_close_time, "TIME_STOP")

        # Trailing stop (ATR based), armed after the trade is in profit by
        # `trailing_activation_r` x initial risk; it only ever tightens
        pos.best_price = max(pos.best_price, bar.high) if long else min(pos.best_price, bar.low)
        favourable = (pos.best_price - pos.entry_price) if long else (pos.entry_price - pos.best_price)
        if (self.trailing_enabled and symbol in self.atr
                and favourable >= self.trailing_activation_r * pos.initial_risk):
            trail = self.atr[symbol] * self.trailing_atr_mult
            if long:
                pos.stop_loss = max(pos.stop_loss, pos.best_price - trail)
            else:
                pos.stop_loss = min(pos.stop_loss, pos.best_price + trail)

    def _close_position(self, symbol: str, price: float, ts: datetime, reason: str) -> None:
        pos = self.positions.pop(symbol)
        exit_side = OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY
        exit_fill = self.costs.fill_price(price, exit_side)
        direction = 1 if pos.side == OrderSide.BUY else -1

        gross = (price - pos.entry_price) * pos.quantity * direction
        slippage = (abs(pos.entry_fill - pos.entry_price) + abs(exit_fill - price)) * pos.quantity
        if pos.side == OrderSide.BUY:
            buy_value, sell_value = pos.entry_fill * pos.quantity, exit_fill * pos.quantity
        else:
            buy_value, sell_value = exit_fill * pos.quantity, pos.entry_fill * pos.quantity
        charges = self.costs.charges(buy_value, sell_value)

        self._trade_seq += 1
        status = {"STOP_LOSS": TradeStatus.STOPPED_OUT, "TIME_STOP": TradeStatus.TIMED_OUT}.get(
            reason, TradeStatus.CLOSED)
        trade = Trade(
            trade_id=f"BT{self._trade_seq:06d}", symbol=symbol, side=pos.side,
            entry_price=pos.entry_price, exit_price=price, quantity=pos.quantity,
            entry_timestamp=pos.entry_time, exit_timestamp=ts, stop_loss=pos.stop_loss,
            take_profit=pos.take_profit, strategy=pos.strategy, confidence=pos.confidence,
            pnl=gross, slippage=slippage, status=status,
            tags=[reason] + pos.contributors, **charges)
        self.trades.append(trade)
        self.cash += trade.net_pnl
        self.cooldown_until[symbol] = ts + timedelta(seconds=self.cooldown_seconds)
        self.logger.debug(f"CLOSE {symbol} {reason} @ {price:.2f} net {trade.net_pnl:.2f}")

    def _update_atr(self, symbol: str, bar: OHLCV, period: int = 14) -> None:
        prev = self.prev_close.get(symbol, bar.open)
        tr = max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
        if symbol not in self.atr:
            self.atr[symbol] = tr
        else:
            self.atr[symbol] = (self.atr[symbol] * (period - 1) + tr) / period

    def _check_daily_limits(self, now: datetime) -> None:
        if self.trading_halted_today or self.day_start_equity <= 0:
            return
        day_pnl_pct = (self._equity() - self.day_start_equity) / self.day_start_equity * 100
        reason = None
        if day_pnl_pct <= -self.max_daily_loss_pct:
            reason = "DAILY_LOSS_LIMIT"
        elif day_pnl_pct >= self.max_daily_profit_pct:
            reason = "DAILY_PROFIT_TARGET"
        if reason:
            self.logger.info(f"{reason} hit ({day_pnl_pct:.2f}%) on {now.date()} - flattening")
            self.trading_halted_today = True
            self.pending.clear()
            for symbol in list(self.positions):
                self._close_position(symbol, self.last_price[symbol], now, reason)

    def _equity(self) -> float:
        return self.cash + sum(p.unrealized(self.last_price.get(s, p.entry_price))
                               for s, p in self.positions.items())

    def _record_equity(self, ts: datetime) -> None:
        equity = self._equity()
        self.peak_equity = max(self.peak_equity, equity)
        drawdown = (self.peak_equity - equity) / self.peak_equity * 100 if self.peak_equity else 0.0
        self.equity_curve.append({
            "timestamp": ts.isoformat(),
            "equity": equity,
            "cash": self.cash,
            "pnl": equity - self.initial_capital,
            "drawdown": drawdown,
        })

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _calculate_performance_metrics(self) -> None:
        if not self.trades:
            self.logger.warning("No trades to calculate metrics")
            self.performance_metrics = PerformanceMetrics()
            return

        net = [t.net_pnl for t in self.trades]
        wins = [p for p in net if p > 0]
        losses = [p for p in net if p < 0]
        total_trades = len(net)
        win_rate = len(wins) / total_trades
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        gross_loss = abs(sum(losses))
        profit_factor = sum(wins) / gross_loss if gross_loss > 0 else (float('inf') if wins else 0.0)
        expectancy = sum(net) / total_trades

        # Daily returns from end-of-day equity
        sharpe = sortino = cagr = max_dd = current_dd = 0.0
        if self.equity_curve:
            df = pd.DataFrame(self.equity_curve)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            daily = df.groupby(df['timestamp'].dt.date)['equity'].last()
            daily = pd.concat([pd.Series([self.initial_capital]), daily.reset_index(drop=True)])
            returns = daily.pct_change().dropna()
            if len(returns) > 1 and returns.std() > 0:
                sharpe = float(returns.mean() / returns.std() * math.sqrt(252))
            downside = returns[returns < 0]
            if len(downside) > 1 and downside.std() > 0:
                sortino = float(returns.mean() / downside.std() * math.sqrt(252))

            max_dd = float(df['drawdown'].max())
            current_dd = float(df['drawdown'].iloc[-1])

            days = max((df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).days, 1)
            end_equity = float(df['equity'].iloc[-1])
            if end_equity > 0:
                cagr = ((end_equity / self.initial_capital) ** (365.25 / days) - 1) * 100

        self.performance_metrics = PerformanceMetrics(
            total_trades=total_trades,
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=win_rate,
            total_pnl=sum(t.pnl or 0.0 for t in self.trades),
            total_net_pnl=sum(net),
            avg_win=avg_win,
            avg_loss=avg_loss,
            max_drawdown=max_dd,
            current_drawdown=current_dd,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            profit_factor=profit_factor,
            cagr=cagr,
            mar_ratio=cagr / max_dd if max_dd else 0.0,
            expectancy=expectancy,
        )
        self.logger.info(f"Performance metrics calculated: {total_trades} trades, net PnL {sum(net):.2f}")

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def _save_results(self) -> None:
        self.results_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._save_trades_csv(self.results_dir / f"trades_{stamp}.csv")
        self._save_summary(self.results_dir / f"summary_{stamp}.txt")
        self._save_equity_curve(self.results_dir / f"equity_curve_{stamp}.csv")
        if (self.config.get('backtester', {}).get('metrics', {}) or {}).get('generate_plots', True):
            self._generate_equity_plot(self.results_dir / f"equity_curve_{stamp}.png")
        self.logger.info(f"Results saved to {self.results_dir}")

    def _save_trades_csv(self, path: Path) -> None:
        if not self.trades:
            headers = ["trade_id"]
            rows: List[Dict[str, Any]] = []
        else:
            rows = [t.to_dict() for t in self.trades]
            headers = list(rows[0].keys())
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        self.logger.info(f"Trades saved to {path}")

    def format_summary(self) -> str:
        m = self.performance_metrics or PerformanceMetrics()
        final_equity = self.equity_curve[-1]['equity'] if self.equity_curve else self.initial_capital
        days = sorted({b.timestamp.date() for bars in self.historical_data.values() for b in bars})
        lines = [
            "=" * 60,
            "RAPID FIRE INTRADAY TRADING SYSTEM - BACKTEST SUMMARY",
            "=" * 60,
            "",
            "CONFIGURATION:",
            "-" * 30,
            f"Initial Capital: {self.initial_capital:,.2f}",
            f"Symbols: {', '.join(self.historical_data) or 'none'}",
            f"Period: {days[0] if days else 'N/A'} to {days[-1] if days else 'N/A'} ({len(days)} sessions)",
            f"Data: {'synthetic' if self.synthetic else self.data_path}",
            f"Risk/Trade: {self.risk_per_trade_pct}%  Max Positions: {self.max_positions}",
            f"Aggregation: threshold {self.threshold}, min agreeing {self.min_agreeing}",
            "",
            "PERFORMANCE METRICS:",
            "-" * 30,
            f"Total Trades: {m.total_trades}",
            f"Winning / Losing: {m.winning_trades} / {m.losing_trades}",
            f"Win Rate: {m.win_rate:.2%}",
            f"Gross PnL: {m.total_pnl:,.2f}",
            f"Net PnL: {m.total_net_pnl:,.2f}",
            f"Final Equity: {final_equity:,.2f}",
            f"Average Win: {m.avg_win:,.2f}",
            f"Average Loss: {m.avg_loss:,.2f}",
            f"Profit Factor: {m.profit_factor:.2f}",
            f"Expectancy/Trade: {m.expectancy:,.2f}",
            f"Sharpe Ratio: {m.sharpe_ratio:.2f}",
            f"Sortino Ratio: {m.sortino_ratio:.2f}",
            f"CAGR: {m.cagr:.2f}%",
            f"Max Drawdown: {m.max_drawdown:.2f}%",
            f"MAR Ratio: {m.mar_ratio:.2f}",
            "",
            "SIGNALS BY STRATEGY:",
            "-" * 30,
        ]
        for name in sorted(self.signal_counts):
            lines.append(f"{name}: {self.signal_counts[name]}")
        if self.trades:
            lines += ["", "EXIT REASONS:", "-" * 30]
            reasons: Dict[str, int] = {}
            for t in self.trades:
                reasons[t.tags[0]] = reasons.get(t.tags[0], 0) + 1
            lines += [f"{k}: {v}" for k, v in sorted(reasons.items())]
        lines += ["", "=" * 60]
        return "\n".join(lines)

    def _save_summary(self, path: Path) -> None:
        path.write_text(self.format_summary() + "\n")
        self.logger.info(f"Summary saved to {path}")

    def _save_equity_curve(self, path: Path) -> None:
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "equity", "cash", "pnl", "drawdown"])
            writer.writeheader()
            writer.writerows(self.equity_curve)
        self.logger.info(f"Equity curve saved to {path}")

    def _generate_equity_plot(self, path: Path) -> None:
        if not self.equity_curve:
            return
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            self.logger.warning("Matplotlib not available, skipping equity curve plot")
            return

        equity = [s["equity"] for s in self.equity_curve]
        drawdown = [-s["drawdown"] for s in self.equity_curve]
        x = range(len(equity))  # Bar index avoids overnight gaps

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                       gridspec_kw={"height_ratios": [3, 1]})
        ax1.plot(x, equity, color="#2563eb", linewidth=1)
        ax1.axhline(self.initial_capital, color="#9ca3af", linewidth=0.8, linestyle="--")
        ax1.set_title("Equity Curve - Rapid Fire Intraday Trading System")
        ax1.set_ylabel("Equity")
        ax1.grid(True, alpha=0.3)
        ax2.fill_between(x, drawdown, 0, color="#dc2626", alpha=0.4)
        ax2.set_ylabel("Drawdown %")
        ax2.set_xlabel("Bar")
        ax2.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)
        self.logger.info(f"Equity curve plot saved to {path}")

    def get_results(self) -> Dict[str, Any]:
        """Get complete backtest results."""
        return {
            "trades": [trade.to_dict() for trade in self.trades],
            "performance_metrics": self.performance_metrics.to_dict() if self.performance_metrics else None,
            "equity_curve": self.equity_curve,
            "signal_counts": dict(self.signal_counts),
            "summary": self._get_summary_dict(),
        }

    def _get_summary_dict(self) -> Dict[str, Any]:
        if not self.performance_metrics:
            return {}
        m = self.performance_metrics
        return {
            "total_trades": m.total_trades,
            "winning_trades": m.winning_trades,
            "losing_trades": m.losing_trades,
            "win_rate": m.win_rate,
            "total_net_pnl": m.total_net_pnl,
            "profit_factor": m.profit_factor,
            "sharpe_ratio": m.sharpe_ratio,
            "sortino_ratio": m.sortino_ratio,
            "cagr": m.cagr,
            "max_drawdown": m.max_drawdown,
            "mar_ratio": m.mar_ratio,
        }


# =============================================================================
# WALK-FORWARD OPTIMISATION
# =============================================================================

class WalkForwardOptimizer:
    """
    Grid search over parameters, evaluated with walk-forward windows.

    Parameter keys are dotted config paths, e.g.
    ``"signal_aggregation.threshold"`` or
    ``"strategies.parameters.EMA_CROSSOVER.fast_ema_period"``.
    """

    def __init__(self, config: Config, **backtester_kwargs: Any):
        self.logger = get_logger("walk_forward")
        self.config = config
        self.backtester_kwargs = backtester_kwargs

    async def optimize(
        self,
        parameter_grid: Dict[str, List[Any]],
        optimization_metric: str = "sharpe_ratio",
        in_sample_periods: Optional[List[Tuple[str, str]]] = None,
        out_of_sample_periods: Optional[List[Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        For every parameter combination, average the metric over the
        in-sample periods; the best combination is then evaluated on the
        out-of-sample periods.
        """
        in_sample_periods = in_sample_periods or [(None, None)]
        combos = self._generate_parameter_combinations(parameter_grid)
        self.logger.info(f"Testing {len(combos)} parameter combinations")

        all_results = []
        best_parameters: Dict[str, Any] = {}
        best_result = -float('inf')

        for params in combos:
            scores = [await self._evaluate(params, start, end, optimization_metric)
                      for start, end in in_sample_periods]
            avg = float(np.mean(scores)) if scores else 0.0
            all_results.append({"parameters": params, "result": avg})
            if avg > best_result:
                best_result, best_parameters = avg, params

        out_of_sample = [
            {"period": (start, end),
             "result": await self._evaluate(best_parameters, start, end, optimization_metric)}
            for start, end in (out_of_sample_periods or [])
        ]

        self.logger.info(f"Optimization completed. Best parameters: {best_parameters}")
        return {
            "best_parameters": best_parameters,
            "best_result": best_result,
            "out_of_sample": out_of_sample,
            "all_results": all_results,
        }

    async def _evaluate(self, params: Dict[str, Any], start: Optional[str],
                        end: Optional[str], metric: str) -> float:
        config = self._apply_parameters(params)
        backtester = Backtester(config, start_date=start, end_date=end, **self.backtester_kwargs)
        results = await backtester.run(save=False)
        value = (results.get("performance_metrics") or {}).get(metric, 0.0)
        return float(value) if value is not None and math.isfinite(value) else 0.0

    def _generate_parameter_combinations(self, parameter_grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        from itertools import product
        keys = list(parameter_grid.keys())
        return [dict(zip(keys, combo)) for combo in product(*parameter_grid.values())]

    def _apply_parameters(self, params: Dict[str, Any]) -> Config:
        """Return a deep copy of the config with dotted-path overrides applied."""
        config = copy.deepcopy(self.config)
        for path, value in params.items():
            node = config
            keys = path.split('.')
            for key in keys[:-1]:
                node = node.setdefault(key, {})
            node[keys[-1]] = value
        return config
