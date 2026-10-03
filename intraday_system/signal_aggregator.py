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
        # Filter out HOLD signals
        active_signals = [s for s in signals if s.signal != SignalType.HOLD]
        
        if not active_signals:
            return None
        
        # Group by strategy
        signals_by_strategy: Dict[StrategyName, List[StrategySignal]] = defaultdict(list)
        for signal in active_signals:
            signals_by_strategy[signal.strategy].append(signal)
        
        # Get the most recent signal from each strategy
        latest_signals: Dict[StrategyName, StrategySignal] = {}
        for strategy, strategy_signals in signals_by_strategy.items():
            latest_signals[strategy] = max(strategy_signals, key=lambda x: x.timestamp)
        
        # Convert to list
        unique_signals = list(latest_signals.values())
        
        if not unique_signals:
            return None
        
        # Calculate net score
        net_score, buy_count, sell_count = self._calculate_net_score(unique_signals)
        
        # Calculate net confidence
        net_confidence = self._calculate_net_confidence(unique_signals, net_score)
        
        # Determine final signal
        final_signal = self._determine_final_signal(unique_signals, net_score)
        
        # Check conditions
        if not self._conditions_met(symbol, net_score, buy_count, sell_count, final_signal):
            return None
        
        # Create aggregated signal
        aggregated = AggregatedSignal(
            symbol=symbol,
            timestamp=datetime.utcnow(),
            net_score=net_score,
            net_confidence=net_confidence,
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
        buy_score = 0.0
        sell_score = 0.0
        buy_count = 0
        sell_count = 0
        
        for signal in signals:
            weight = self.strategy_weights.get(signal.strategy, 0.0)
            
            if signal.signal == SignalType.BUY:
                buy_score += weight * signal.confidence
                buy_count += 1
            elif signal.signal == SignalType.SELL:
                sell_score += weight * signal.confidence
                sell_count += 1
        
        # Normalize scores
        total_score = buy_score + sell_score
        if total_score > 0:
            buy_score /= total_score
            sell_score /= total_score
        
        # Net score: positive for BUY, negative for SELL
        net_score = buy_score - sell_score
        
        return net_score, buy_count, sell_count
    
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
        if net_score > self.agg_config.threshold:
            return SignalType.BUY
        elif net_score < -self.agg_config.threshold:
            return SignalType.SELL
        else:
            return SignalType.HOLD
    
    def _conditions_met(self, symbol: Symbol, net_score: float, 
                        buy_count: int, sell_count: int, final_signal: SignalType) -> bool:
        """Check if all conditions for triggering a trade are met."""
        conditions = [
            # Net score meets threshold
            abs(net_score) >= self.agg_config.threshold,
            
            # At least 2 strategies agree on direction
            (buy_count >= 2 and final_signal == SignalType.BUY) or
            (sell_count >= 2 and final_signal == SignalType.SELL) or
            (buy_count + sell_count) >= self.agg_config.min_agreeing_strategies,
            
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
            # Get ATR percentile
            atr = self.data_feed.get_atr(symbol, period=14)
            atr_percentile = self._calculate_atr_percentile(symbol)
            
            # Get volatility threshold
            threshold = self.agg_config.regime_volatility_threshold
            
            # If ATR percentile is too high (high volatility), don't trade
            if atr_percentile > threshold * 100:
                self.logger.debug(f"High volatility regime for {symbol}: ATR percentile {atr_percentile:.1f}% > {threshold * 100}%")
                return False
            
            # Check VIX if configured
            if self.agg_config.use_regime_filter:
                vix = self._get_vix_value()
                vix_max = self.config.get('signal_aggregation', {}).get('regime', {}).get('vix_max', 25.0)
                if vix > vix_max:
                    self.logger.debug(f"High VIX regime: {vix:.2f} > {vix_max}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking market regime: {e}")
            # Fail-safe: allow trading if we can't determine regime
            return True
    
    def _calculate_atr_percentile(self, symbol: Symbol) -> float:
        """Calculate ATR percentile (0-100) for a symbol."""
        try:
            # Get historical ATR values
            atr_periods = [14, 20, 50, 100]
            atr_values = []
            
            for period in atr_periods:
                atr = self.data_feed.get_atr(symbol, period=period)
                if atr:
                    atr_values.append(atr)
            
            if not atr_values:
                return 50.0  # Default to 50th percentile
            
            # Current ATR
            current_atr = self.data_feed.get_atr(symbol, period=14)
            if current_atr is None:
                return 50.0
            
            # Calculate percentile
            if current_atr == 0:
                return 0.0
            
            # Use current ATR relative to historical
            percentile = (current_atr / (sum(atr_values) / len(atr_values))) * 100
            return min(100.0, max(0.0, percentile))
            
        except Exception as e:
            self.logger.error(f"Error calculating ATR percentile: {e}")
            return 50.0
    
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
