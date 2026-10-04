"""
Signal Aggregator for the Rapid Fire Intraday Trading System.

This module aggregates signals from multiple strategies using weighted voting
and confidence scoring. It implements the aggregation rules specified in the
requirements:
- Weighted vote (weights from config.yaml)
- Fire trade ONLY if:
    * net_score >= threshold (default 0.65)
    * at least 2 strategies agree on direction
    * market regime filter passes (VIX / ATR percentile)
    * no existing position in that symbol
    * daily loss limit not breached
- Cooldown: 60 s per symbol after any exit
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import numpy as np

from intraday_system.config_loader import get_config_loader
from intraday_system.data_feed import get_data_feed
from intraday_system.logger import signal_logger
from intraday_system.models import (
    AggregatedSignal, AggregationConfig, MarketRegime, OHLCV, OrderSide,
    PortfolioState, Position, PositionSide, RiskState, SignalType, StrategyName,
    StrategySignal, Symbol, TimeFrame, Timestamp
)
from intraday_system.portfolio_tracker import get_portfolio_tracker
from intraday_system.risk_manager import get_risk_manager


# =============================================================================
# PURE AGGREGATION LOGIC (shared by live trading and the backtester)
# =============================================================================

def latest_per_strategy(signals: List[StrategySignal],
                        now: Optional[Timestamp] = None,
                        max_age_seconds: Optional[float] = None) -> List[StrategySignal]:
    """Keep the most recent non-HOLD signal from each strategy, dropping stale ones."""
    latest: Dict[StrategyName, StrategySignal] = {}
    for signal in signals:
        if signal.signal == SignalType.HOLD:
            continue
        if now is not None and max_age_seconds is not None:
            if (now - signal.timestamp).total_seconds() > max_age_seconds:
                continue
        current = latest.get(signal.strategy)
        if current is None or signal.timestamp >= current.timestamp:
            latest[signal.strategy] = signal
    return list(latest.values())


def score_signals(signals: List[StrategySignal],
                  weights: Dict[StrategyName, float]) -> Tuple[float, int, int]:
    """
    Weighted vote across strategies.
    
    Returns (net_score, buy_count, sell_count) where net_score is in [-1, +1]:
    +1 means all weighted conviction is on the BUY side, -1 all on SELL.
    """
    buy_score = 0.0
    sell_score = 0.0
    buy_count = 0
    sell_count = 0
    
    for signal in signals:
        weight = weights.get(signal.strategy, 0.0)
        if signal.signal == SignalType.BUY:
            buy_score += weight * signal.confidence
            buy_count += 1
        elif signal.signal == SignalType.SELL:
            sell_score += weight * signal.confidence
            sell_count += 1
    
    total_score = buy_score + sell_score
    if total_score <= 0:
        return 0.0, buy_count, sell_count
    
    return (buy_score - sell_score) / total_score, buy_count, sell_count


def decide(net_score: float, buy_count: int, sell_count: int,
           threshold: float, min_agreeing: int) -> SignalType:
    """
    Turn a weighted vote into a trading decision.
    
    Requires |net_score| >= threshold AND at least `min_agreeing` strategies
    voting in the winning direction.
    """
    if net_score >= threshold and buy_count >= min_agreeing:
        return SignalType.BUY
    if net_score <= -threshold and sell_count >= min_agreeing:
        return SignalType.SELL
    return SignalType.HOLD


def atr_percentile(bars: List[OHLCV], period: int = 14) -> Optional[float]:
    """
    Percentile rank (0-100) of the latest ATR(period) among the rolling
    ATR(period) values over ``bars``. None if there is not enough history.
    """
    if len(bars) < period * 2:
        return None
    true_ranges = [
        max(cur.high - cur.low, abs(cur.high - prev.close), abs(cur.low - prev.close))
        for prev, cur in zip(bars, bars[1:])
    ]
    atrs = [sum(true_ranges[i - period:i]) / period for i in range(period, len(true_ranges) + 1)]
    current = atrs[-1]
    # Mid-rank: ties count half, so constant volatility ranks at the 50th percentile
    below = sum(1 for a in atrs if a < current)
    equal = sum(1 for a in atrs if a == current)
    return 100.0 * (below + 0.5 * equal) / len(atrs)


def regime_allows_trading(bars: List[OHLCV], volatility_threshold: float,
                          period: int = 14) -> bool:
    """
    Volatility regime filter: block entries when the current ATR is in the
    top (1 - threshold) of its recent range, e.g. threshold 0.75 blocks the
    most volatile 25% of conditions. Allows trading when history is short.
    """
    percentile = atr_percentile(bars, period)
    return percentile is None or percentile <= volatility_threshold * 100


def net_confidence(signals: List[StrategySignal], final_signal: SignalType,
                   net_score: float) -> float:
    """Confidence of the aggregated decision (0..1)."""
    agreeing = [s for s in signals if s.signal == final_signal]
    if not agreeing:
        return 0.0
    avg_confidence = sum(s.confidence for s in agreeing) / len(agreeing)
    agreement_ratio = len(agreeing) / max(len(signals), 1)
    return min(1.0, avg_confidence * 0.7 + abs(net_score) * 0.2 + agreement_ratio * 0.1)


# =============================================================================
# SIGNAL AGGREGATOR
# =============================================================================

class SignalAggregator:
    """
    Aggregates signals from multiple strategies and generates trading decisions.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the signal aggregator."""
        self.config = config or get_config_loader().load()
        self.agg_config: AggregationConfig = get_config_loader().get_aggregation_config()
        self.logger = signal_logger()
        
        # Strategy weights
        strategy_configs = get_config_loader().get_strategy_configs()
        self.strategy_weights: Dict[StrategyName, float] = {
            config.name: config.weight for config in strategy_configs if config.enabled
        }
        
        # Normalize weights to sum to 1.0
        total_weight = sum(self.strategy_weights.values())
        if total_weight > 0:
            self.strategy_weights = {k: v / total_weight for k, v in self.strategy_weights.items()}
        
        # Bars of history used by the volatility regime filter
        self.regime_lookback: int = int(
            (self.config.get('signal_aggregation', {}).get('regime', {}) or {}).get('lookback_bars', 120)
        )
        
        # Signals older than this are ignored when aggregating
        self.signal_ttl_seconds: float = float(
            self.config.get('signal_aggregation', {}).get('signal_ttl_seconds', 300)
        )
        
        # Signal buffers
        self._signal_buffer: Dict[Symbol, List[StrategySignal]] = defaultdict(list)
        self._buffer_size = 10  # Keep last 10 signals per symbol
        self._buffer_lock = threading.Lock()
        
        # Cooldown tracking
        self._cooldown_symbols: Dict[Symbol, Timestamp] = {}
        self._cooldown_lock = threading.Lock()
        
        # Last aggregation results
        self._last_aggregation: Dict[Symbol, AggregatedSignal] = {}
        
        # Subscribers
        self.subscribers: List[Callable] = []
        
        # Market regime tracking
        self._market_regime: Dict[Symbol, MarketRegime] = {}
        
        # Data feed reference
        self.data_feed = get_data_feed()
        
        self.logger.info("SignalAggregator initialized")
    
    def start(self) -> None:
        """Start the signal aggregator."""
        # Subscribe to strategy engine signals
        from intraday_system.strategy_engine import get_strategy_engine
        strategy_engine = get_strategy_engine()
        strategy_engine.subscribe(self.on_strategy_signal)
        
        self.logger.info("SignalAggregator started and subscribed to strategy signals")
    
    def stop(self) -> None:
        """Stop the signal aggregator."""
        # Unsubscribe from strategy engine
        from intraday_system.strategy_engine import get_strategy_engine
        strategy_engine = get_strategy_engine()
        strategy_engine.unsubscribe(self.on_strategy_signal)
        
        self.logger.info("SignalAggregator stopped")
    
    def on_strategy_signal(self, signal: StrategySignal) -> None:
        """Handle incoming signal from a strategy."""
        # Apply cooldown check
        if self._is_in_cooldown(signal.symbol):
            self.logger.debug(f"Signal from {signal.strategy.value} for {signal.symbol} ignored - in cooldown")
            return
        
        # Add signal to buffer
        with self._buffer_lock:
            self._signal_buffer[signal.symbol].append(signal)
            
            # Limit buffer size
            if len(self._signal_buffer[signal.symbol]) > self._buffer_size:
                self._signal_buffer[signal.symbol] = self._signal_buffer[signal.symbol][-self._buffer_size:]
        
        # Log the signal
        self.logger.log_signal('signal_aggregator', signal.to_dict())
        
        # Trigger aggregation for this symbol
        self.aggregate_signals(signal.symbol)
    
    def aggregate_signals(self, symbol: Symbol) -> Optional[AggregatedSignal]:
        """
        Aggregate signals for a symbol and return the aggregated result.
        
        Returns AggregatedSignal if conditions are met, None otherwise.
        """
        with self._buffer_lock:
            signals = self._signal_buffer.get(symbol, [])
        
        if not signals:
            return None
        
        # Check cooldown
        if self._is_in_cooldown(symbol):
            return None
        
        # Check market regime
        if not self._check_market_regime(symbol):
            self.logger.debug(f"Market regime check failed for {symbol}")
            return None
        
        # Check position limit
        if not self._check_position_limit(symbol):
            self.logger.debug(f"Position limit reached for {symbol}")
            return None
        
        # Check daily limits
        if not self._check_daily_limits():
            self.logger.debug("Daily limits reached")
            return None
        
        # Aggregate signals
        aggregated = self._perform_aggregation(symbol, signals)
        
        if aggregated:
            # Store last aggregation
            self._last_aggregation[symbol] = aggregated
            
            # Publish to subscribers
            self._publish_aggregated_signal(aggregated)
        
        return aggregated
    
    def _perform_aggregation(self, symbol: Symbol, signals: List[StrategySignal]) -> Optional[AggregatedSignal]:
        """
        Perform the actual aggregation logic.
        
        Returns AggregatedSignal if conditions are met, None otherwise.
        """
        now = max(s.timestamp for s in signals) if signals else datetime.utcnow()
        unique_signals = latest_per_strategy(signals, now, self.signal_ttl_seconds)
        
        if not unique_signals:
            return None
        
        # Calculate net score
        net_score, buy_count, sell_count = self._calculate_net_score(unique_signals)
        
        # Determine final signal
        final_signal = self._determine_final_signal(unique_signals, net_score)
        
        # Calculate net confidence
        confidence = net_confidence(unique_signals, final_signal, net_score)
        
        # Check conditions
        if not self._conditions_met(symbol, net_score, buy_count, sell_count, final_signal):
            return None
        
        # Create aggregated signal
        aggregated = AggregatedSignal(
            symbol=symbol,
            timestamp=now,
            net_score=net_score,
            net_confidence=confidence,
            final_signal=final_signal,
            contributing_strategies=unique_signals,
            regime_filter_passed=True  # Will be updated by _check_market_regime
        )
        
        return aggregated
    
    def _calculate_net_score(self, signals: List[StrategySignal]) -> Tuple[float, int, int]:
        """
        Calculate net score from strategy signals.
        
        Returns: (net_score, buy_count, sell_count)
        """
        return score_signals(signals, self.strategy_weights)
    
    def _calculate_net_confidence(self, signals: List[StrategySignal], net_score: float) -> float:
        """
        Calculate net confidence score.
        
        Returns: confidence score from 0.0 to 1.0
        """
        if not signals:
            return 0.0
        
        # Average confidence of all signals
        avg_confidence = sum(s.confidence for s in signals) / len(signals)
        
        # Add bonus for net score magnitude
        score_bonus = abs(net_score) * 0.2
        
        # Add bonus for agreement (more strategies agreeing = higher confidence)
        active_signals = [s for s in signals if s.signal != SignalType.HOLD]
        if active_signals:
            buy_count = sum(1 for s in active_signals if s.signal == SignalType.BUY)
            sell_count = sum(1 for s in active_signals if s.signal == SignalType.SELL)
            max_count = max(buy_count, sell_count)
            agreement_bonus = (max_count / len(active_signals)) * 0.1
        else:
            agreement_bonus = 0.0
        
        # Add regime bonus if applicable
        regime_bonus = 0.1 if self.agg_config.use_regime_filter else 0.0
        
        # Calculate final confidence
        net_confidence = min(1.0, avg_confidence + score_bonus + agreement_bonus + regime_bonus)
        
        return net_confidence
    
    def _determine_final_signal(self, signals: List[StrategySignal], net_score: float) -> SignalType:
        """
        Determine the final signal based on net score and individual signals.
        """
        buy_count = sum(1 for s in signals if s.signal == SignalType.BUY)
        sell_count = sum(1 for s in signals if s.signal == SignalType.SELL)
        return decide(net_score, buy_count, sell_count,
                      self.agg_config.threshold, self.agg_config.min_agreeing_strategies)
    
    def _conditions_met(self, symbol: Symbol, net_score: float, 
                        buy_count: int, sell_count: int, final_signal: SignalType) -> bool:
        """Check if all conditions for triggering a trade are met."""
        conditions = [
            # Net score meets threshold and enough strategies agree on direction
            decide(net_score, buy_count, sell_count, self.agg_config.threshold,
                   self.agg_config.min_agreeing_strategies) == final_signal,
            
            # Not HOLD
            final_signal != SignalType.HOLD,
            
            # No position in symbol
            self._check_position_limit(symbol),
            
            # Daily limits not breached
            self._check_daily_limits(),
            
            # Not in cooldown
            not self._is_in_cooldown(symbol)
        ]
        
        return all(conditions)
    
    def _check_market_regime(self, symbol: Symbol) -> bool:
        """Check if market regime allows trading."""
        if not self.agg_config.use_regime_filter:
            return True
        
        try:
            bars = self.data_feed.cache.get_ohlcv(symbol, TimeFrame._1M, limit=self.regime_lookback)
            if not regime_allows_trading(bars, self.agg_config.regime_volatility_threshold):
                self.logger.debug(f"High volatility regime for {symbol}: ATR percentile "
                                  f"{atr_percentile(bars):.0f} > {self.agg_config.regime_volatility_threshold * 100:.0f}")
                return False
            
            regime_cfg = self.config.get('signal_aggregation', {}).get('regime', {}) or {}
            if regime_cfg.get('use_vix_filter', False):
                vix = self._get_vix_value()
                vix_max = regime_cfg.get('vix_max', 25.0)
                if vix > vix_max:
                    self.logger.debug(f"High VIX regime: {vix:.2f} > {vix_max}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking market regime: {e}")
            # Fail-safe: allow trading if we can't determine regime
            return True
    
    def _calculate_atr_percentile(self, symbol: Symbol) -> float:
        """ATR percentile (0-100) for a symbol; 50 when history is insufficient."""
        bars = self.data_feed.cache.get_ohlcv(symbol, TimeFrame._1M, limit=self.regime_lookback)
        value = atr_percentile(bars)
        return 50.0 if value is None else value
    
    def _get_vix_value(self) -> float:
        """Get VIX value (mock implementation)."""
        # In a real implementation, this would fetch from a data source
        # For now, return a reasonable default
        return 15.0
    
    def _check_position_limit(self, symbol: Symbol) -> bool:
        """Check if we can open a position in this symbol."""
        try:
            portfolio_tracker = get_portfolio_tracker()
            portfolio = portfolio_tracker.get_portfolio_state()
            
            # Check if we already have a position in this symbol
            for position in portfolio.positions:
                if position.symbol == symbol and position.side != PositionSide.FLAT:
                    self.logger.debug(f"Existing position found in {symbol}")
                    return False
            
            # Check concurrent position limit
            risk_manager = get_risk_manager()
            risk_state = risk_manager.get_risk_state()
            
            if risk_state.open_positions_count >= risk_manager.config.max_concurrent_positions:
                self.logger.debug("Max concurrent positions reached")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking position limit: {e}")
            # Fail-safe: allow trading
            return True
    
    def _check_daily_limits(self) -> bool:
        """Check if daily limits are not breached."""
        try:
            risk_manager = get_risk_manager()
            risk_state = risk_manager.get_risk_state()
            
            # Check daily loss limit
            if risk_state.daily_pnl_pct <= -risk_manager.config.max_daily_loss_pct:
                self.logger.warning("Daily loss limit breached")
                return False
            
            # Check daily profit target
            if risk_state.daily_pnl_pct >= risk_manager.config.max_daily_profit_pct:
                self.logger.info("Daily profit target reached")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking daily limits: {e}")
            # Fail-safe: allow trading
            return True
    
    def _is_in_cooldown(self, symbol: Symbol) -> bool:
        """Check if a symbol is in cooldown period."""
        with self._cooldown_lock:
            cooldown_end = self._cooldown_symbols.get(symbol)
            if cooldown_end:
                cooldown_seconds = self.config.get('risk_management', {}).get('circuit_breakers', {}).get('cooldown_seconds', 60)
                if (datetime.utcnow() - cooldown_end).total_seconds() < cooldown_seconds:
                    return True
            return False
    
    def start_cooldown(self, symbol: Symbol) -> None:
        """Start cooldown period for a symbol."""
        with self._cooldown_lock:
            self._cooldown_symbols[symbol] = datetime.utcnow()
        self.logger.info(f"Cooldown started for {symbol}")
    
    def clear_cooldown(self, symbol: Symbol) -> None:
        """Clear cooldown for a symbol."""
        with self._cooldown_lock:
            if symbol in self._cooldown_symbols:
                del self._cooldown_symbols[symbol]
        self.logger.debug(f"Cooldown cleared for {symbol}")
    
    def _publish_aggregated_signal(self, signal: AggregatedSignal) -> None:
        """Publish aggregated signal to subscribers."""
        self.logger.info(f"AGGREGATED SIGNAL: {signal.final_signal.value} {signal.symbol} "
                        f"score={signal.net_score:.2f} confidence={signal.net_confidence:.2f}")
        self.logger.log_signal('signal_aggregator', signal.to_dict())
        
        for subscriber in self.subscribers:
            try:
                subscriber(signal)
            except Exception as e:
                self.logger.error(f"Error in aggregated signal subscriber {subscriber.__name__}: {e}")
    
    def subscribe(self, callback: Callable) -> None:
        """Subscribe to aggregated signals."""
        self.subscribers.append(callback)
        self.logger.debug(f"New subscriber: {callback.__name__}")
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from aggregated signals."""
        if callback in self.subscribers:
            self.subscribers.remove(callback)
    
    def get_last_aggregation(self, symbol: Symbol) -> Optional[AggregatedSignal]:
        """Get the last aggregated signal for a symbol."""
        return self._last_aggregation.get(symbol)
    
    def get_active_signals(self) -> Dict[Symbol, AggregatedSignal]:
        """Get all active aggregated signals."""
        return self._last_aggregation.copy()
    
    def reset(self, symbol: Optional[Symbol] = None) -> None:
        """Reset signal aggregator state."""
        if symbol:
            with self._buffer_lock:
                if symbol in self._signal_buffer:
                    del self._signal_buffer[symbol]
            with self._cooldown_lock:
                if symbol in self._cooldown_symbols:
                    del self._cooldown_symbols[symbol]
            if symbol in self._last_aggregation:
                del self._last_aggregation[symbol]
        else:
            with self._buffer_lock:
                self._signal_buffer.clear()
            with self._cooldown_lock:
                self._cooldown_symbols.clear()
            self._last_aggregation.clear()
    
    def get_aggregation_stats(self) -> Dict[str, Any]:
        """Get statistics about signal aggregation."""
        with self._buffer_lock:
            total_signals = sum(len(signals) for signals in self._signal_buffer.values())
        
        return {
            'total_signals': total_signals,
            'active_symbols': len(self._signal_buffer),
            'cooldown_symbols': len(self._cooldown_symbols),
            'aggregated_signals': len(self._last_aggregation),
            'strategy_weights': self.strategy_weights,
            'aggregation_config': self.agg_config.to_dict()
        }


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_signal_aggregator: Optional[SignalAggregator] = None


def get_signal_aggregator() -> SignalAggregator:
    """Get the singleton SignalAggregator instance."""
    global _signal_aggregator
    if _signal_aggregator is None:
        _signal_aggregator = SignalAggregator()
    return _signal_aggregator


def start_signal_aggregator() -> SignalAggregator:
    """Start the signal aggregator and return the instance."""
    global _signal_aggregator
    _signal_aggregator = SignalAggregator()
    _signal_aggregator.start()
    return _signal_aggregator


def stop_signal_aggregator() -> None:
    """Stop the signal aggregator."""
    global _signal_aggregator
    if _signal_aggregator:
        _signal_aggregator.stop()
        _signal_aggregator = None
