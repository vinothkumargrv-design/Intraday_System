"""
Strategy Engine for the Rapid Fire Intraday Trading System.

This module implements all six trading strategies with pluggable architecture.
Each strategy analyzes market data independently and returns signals with
confidence scores. The strategies are designed to work with multiple symbols
and timeframes simultaneously.

Strategies implemented:
A. Opening Range Breakout (ORB) - 15-min range on 5-min candles
B. VWAP Mean Reversion - fade extremes beyond 2σ from VWAP
C. Momentum Burst - 3 consecutive candles + volume > 2x avg
D. EMA Crossover Scalper - 9/21 EMA on 1-min with RSI filter
E. Order Flow Imbalance - bid/ask delta from tick data
F. Supertrend + ATR trailing - trending regime only
"""

import asyncio
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd

from intraday_system.config_loader import get_config_loader
# Use dependency injection for data_feed to avoid circular imports
# from intraday_system.data_feed import get_data_feed
from intraday_system.logger import strategy_logger
from intraday_system.models import (
    AggregationConfig, OHLCV, SignalType, StrategyConfig, StrategyName,
    StrategySignal, Symbol, Tick, TimeFrame, Timestamp
)


# =============================================================================
# BASE STRATEGY CLASS
# =============================================================================

class BaseStrategy:
    """Base class for all trading strategies."""
    
    def __init__(self, config: StrategyConfig):
        """Initialize base strategy."""
        self.config = config
        self.name = config.name
        self.enabled = config.enabled
        self.weight = config.weight
        self.timeframe = config.timeframe
        self.parameters = config.parameters
        self.logger = strategy_logger()
        
        # Strategy-specific data caches
        self._cache: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # Performance tracking
        self.signals_generated = 0
        self.signals_triggered = 0
        self.last_signal_time: Dict[Symbol, Timestamp] = {}
    
    def initialize(self) -> None:
        """Initialize strategy-specific resources."""
        pass
    
    def cleanup(self) -> None:
        """Clean up strategy resources."""
        pass
    
    def calculate(self, symbol: Symbol, data: Any) -> Optional[StrategySignal]:
        """Calculate signal for a symbol. This must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement calculate()")
    
    def on_tick(self, tick: Tick) -> Optional[StrategySignal]:
        """Process a new tick. Returns signal if conditions are met."""
        return None
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process a new OHLCV bar. Returns signal if conditions are met."""
        return None
    
    def reset(self, symbol: Symbol) -> None:
        """Reset strategy state for a symbol."""
        pass
    
    def get_info(self) -> Dict[str, Any]:
        """Get strategy information."""
        return {
            'name': self.name.value,
            'enabled': self.enabled,
            'weight': self.weight,
            'timeframe': self.timeframe.value,
            'signals_generated': self.signals_generated,
            'signals_triggered': self.signals_triggered
        }
    
    def _create_signal(self, symbol: Symbol, signal: SignalType, 
                       confidence: float, reason: str,
                       suggested_sl: Optional[float] = None,
                       suggested_tp: Optional[float] = None) -> Optional[StrategySignal]:
        """Create a strategy signal."""
        if not self.enabled:
            return None
        
        self.signals_generated += 1
        self.last_signal_time[symbol] = datetime.utcnow()
        
        return StrategySignal(
            strategy=self.name,
            signal=signal,
            confidence=min(1.0, max(0.0, confidence)),
            reason=reason,
            symbol=symbol,
            timestamp=datetime.utcnow(),
            suggested_sl=suggested_sl,
            suggested_tp=suggested_tp,
            timeframe=self.timeframe,
            metadata={
                'strategy_params': self.parameters,
                'generation_count': self.signals_generated
            }
        )


# =============================================================================
# STRATEGY IMPLEMENTATIONS
# =============================================================================

class OpeningRangeBreakoutStrategy(BaseStrategy):
    """
    Opening Range Breakout (ORB) Strategy
    
    - Identifies the opening range (first 15 minutes of trading)
    - Trades breakouts above/below the range
    - Uses 5-min candles for range calculation
    - Exits with ATR-based stop loss
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize ORB strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.orb_minutes = self.parameters.get('orb_minutes', 15)
        self.orb_multiplier = self.parameters.get('orb_multiplier', 1.0)
        self.atr_period = self.parameters.get('atr_period', 14)
        self.atr_multiplier = self.parameters.get('atr_multiplier', 0.5)
        self.min_volume_filter = self.parameters.get('min_volume_filter', 1000)
        
        # Track opening ranges per symbol
        self._opening_ranges: Dict[Symbol, Tuple[float, float]] = {}  # (high, low)
        self._range_established: Dict[Symbol, bool] = {}
        self._trading_start_time: Dict[Symbol, time] = {}
        
        # Market open time (IST)
        self.market_open = time(9, 15)  # 9:15 AM IST
        self.market_close = time(15, 30)  # 3:30 PM IST
    
    def initialize(self) -> None:
        """Initialize opening range tracking."""
        data_feed = get_data_feed()
        symbols = data_feed.symbols
        
        for symbol in symbols:
            self._opening_ranges[symbol] = (0, float('inf'))
            self._range_established[symbol] = False
            self._trading_start_time[symbol] = self.market_open
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process new OHLCV bar for ORB."""
        if ohlcv.timeframe != TimeFrame._5M:
            return None
        
        symbol = ohlcv.symbol
        current_time = ohlcv.timestamp.time()
        
        # Check if we're within the opening range period
        if not self._range_established.get(symbol, False):
            self._update_opening_range(ohlcv)
            return None
        
        # Check if we're after the opening range period
        if not self._is_after_opening_range(current_time):
            return None
        
        # Check for breakout
        if self._opening_ranges.get(symbol):
            range_high, range_low = self._opening_ranges[symbol]
            range_size = range_high - range_low
            
            # Check for breakout above range
            if ohlcv.close > range_high * (1 + self.orb_multiplier * 0.01):
                # Calculate confidence based on breakout strength
                breakout_strength = (ohlcv.close - range_high) / range_size
                confidence = min(0.8, 0.5 + breakout_strength * 0.3)
                
                # Check volume filter
                if ohlcv.volume < self.min_volume_filter:
                    confidence *= 0.5
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.BUY,
                    confidence=confidence,
                    reason=f"ORB Breakout: Price {ohlcv.close:.2f} broke above range high {range_high:.2f}",
                    suggested_sl=range_low,
                    suggested_tp=range_high + (range_size * 2)  # 2:1 reward
                )
            
            # Check for breakout below range
            elif ohlcv.close < range_low * (1 - self.orb_multiplier * 0.01):
                # Calculate confidence based on breakout strength
                breakout_strength = (range_low - ohlcv.close) / range_size
                confidence = min(0.8, 0.5 + breakout_strength * 0.3)
                
                # Check volume filter
                if ohlcv.volume < self.min_volume_filter:
                    confidence *= 0.5
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.SELL,
                    confidence=confidence,
                    reason=f"ORB Breakout: Price {ohlcv.close:.2f} broke below range low {range_low:.2f}",
                    suggested_sl=range_high,
                    suggested_tp=range_low - (range_size * 2)  # 2:1 reward
                )
        
        return None
    
    def _update_opening_range(self, ohlcv: OHLCV) -> None:
        """Update opening range for a symbol."""
        symbol = ohlcv.symbol
        current_time = ohlcv.timestamp.time()
        
        # Only update during opening range period
        if not self._is_within_opening_range(current_time):
            self._range_established[symbol] = True
            return
        
        # Initialize if not exists
        if symbol not in self._opening_ranges:
            self._opening_ranges[symbol] = (ohlcv.low, ohlcv.high)
        
        # Update range
        current_high, current_low = self._opening_ranges[symbol]
        self._opening_ranges[symbol] = (
            max(current_high, ohlcv.high),
            min(current_low, ohlcv.low)
        )
    
    def _is_within_opening_range(self, current_time: time) -> bool:
        """Check if current time is within the opening range period."""
        # Convert to seconds for comparison
        open_seconds = self.market_open.hour * 3600 + self.market_open.minute * 60
        current_seconds = current_time.hour * 3600 + current_time.minute * 60
        end_seconds = open_seconds + self.orb_minutes * 60
        
        return open_seconds <= current_seconds <= end_seconds
    
    def _is_after_opening_range(self, current_time: time) -> bool:
        """Check if current time is after the opening range period."""
        open_seconds = self.market_open.hour * 3600 + self.market_open.minute * 60
        current_seconds = current_time.hour * 3600 + current_time.minute * 60
        end_seconds = open_seconds + self.orb_minutes * 60
        
        return current_seconds > end_seconds
    
    def reset(self, symbol: Symbol) -> None:
        """Reset strategy for a symbol (e.g., at market close)."""
        self._range_established[symbol] = False
        self._opening_ranges[symbol] = (0, float('inf'))


class VWAPMeanReversionStrategy(BaseStrategy):
    """
    VWAP Mean Reversion Strategy
    
    - Calculates Volume Weighted Average Price (VWAP)
    - Identifies price extremes beyond 2 standard deviations from VWAP
    - Fades extremes (sell when price is high, buy when price is low)
    - Uses RSI as additional filter
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize VWAP Mean Reversion strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.vwap_period = self.parameters.get('vwap_period', 50)
        self.std_multiplier = self.parameters.get('std_deviation_multiplier', 2.0)
        self.rsi_period = self.parameters.get('rsi_period', 14)
        self.rsi_overbought = self.parameters.get('rsi_overbought', 70)
        self.rsi_oversold = self.parameters.get('rsi_oversold', 30)
        self.min_deviation_pct = self.parameters.get('min_deviation_pct', 0.5)
        
        # Track VWAP calculations per symbol
        self._vwap_cache: Dict[Symbol, float] = {}
        self._vwap_std_cache: Dict[Symbol, float] = {}
        self._price_history: Dict[Symbol, List[float]] = {}
        self._volume_history: Dict[Symbol, List[float]] = {}
        self._rsi_cache: Dict[Symbol, float] = {}
    
    def on_tick(self, tick: Tick) -> Optional[StrategySignal]:
        """Process new tick for VWAP calculation."""
        symbol = tick.symbol
        
        # Update price and volume history
        if symbol not in self._price_history:
            self._price_history[symbol] = []
            self._volume_history[symbol] = []
        
        self._price_history[symbol].append(tick.last_price)
        self._volume_history[symbol].append(tick.last_quantity or 0)
        
        # Keep only the most recent data within the period
        max_history = self.vwap_period * 10  # 10 ticks per minute approximation
        if len(self._price_history[symbol]) > max_history:
            self._price_history[symbol] = self._price_history[symbol][-max_history:]
            self._volume_history[symbol] = self._volume_history[symbol][-max_history:]
        
        # Recalculate VWAP and std
        self._calculate_vwap(symbol)
        
        # Check for signal
        return self._check_signal(symbol, tick.last_price)
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process new OHLCV bar for VWAP signal check."""
        return self._check_signal(ohlcv.symbol, ohlcv.close)
    
    def _calculate_vwap(self, symbol: Symbol) -> None:
        """Calculate VWAP and standard deviation for a symbol."""
        prices = self._price_history.get(symbol, [])
        volumes = self._volume_history.get(symbol, [])
        
        if not prices or len(prices) != len(volumes):
            return
        
        # Calculate VWAP
        total_volume = sum(volumes)
        if total_volume == 0:
            self._vwap_cache[symbol] = prices[-1]
            self._vwap_std_cache[symbol] = 0
            return
        
        weighted_sum = sum(p * v for p, v in zip(prices, volumes))
        vwap = weighted_sum / total_volume
        self._vwap_cache[symbol] = vwap
        
        # Calculate standard deviation
        squared_diffs = [(p - vwap) ** 2 for p in prices]
        variance = sum(squared_diffs) / len(prices)
        std_dev = variance ** 0.5
        self._vwap_std_cache[symbol] = std_dev
        
        # Calculate RSI
        self._rsi_cache[symbol] = self._calculate_rsi(prices, self.rsi_period)
    
    def _calculate_rsi(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate RSI for price series."""
        if len(prices) < period + 1:
            return None
        
        deltas = np.diff(prices)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = sum(gains[-period:]) / period if len(gains) >= period else None
        avg_loss = sum(losses[-period:]) / period if len(losses) >= period else None
        
        if avg_loss is None or avg_loss == 0:
            return 100.0
        if avg_gain is None:
            return None
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    def _check_signal(self, symbol: Symbol, current_price: float) -> Optional[StrategySignal]:
        """Check if signal conditions are met."""
        vwap = self._vwap_cache.get(symbol)
        std_dev = self._vwap_std_cache.get(symbol, 0)
        rsi = self._rsi_cache.get(symbol)
        
        if vwap is None:
            return None
        
        # Calculate deviation from VWAP
        deviation = current_price - vwap
        deviation_pct = (deviation / vwap) * 100 if vwap != 0 else 0
        std_multiplier = self.std_multiplier * std_dev
        
        # Check if price is significantly above VWAP
        if deviation > std_multiplier and deviation_pct > self.min_deviation_pct:
            # Check RSI filter - only sell if RSI is overbought
            if rsi is not None and rsi > self.rsi_overbought:
                confidence = min(0.9, 0.6 + (deviation_pct / 100) * 1.5)
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.SELL,
                    confidence=confidence,
                    reason=f"VWAP MR: Price {current_price:.2f} is {deviation_pct:.2f}% above VWAP {vwap:.2f}",
                    suggested_sl=vwap + (std_dev * 3),  # 3 std dev SL
                    suggested_tp=vwap  # Target VWAP
                )
        
        # Check if price is significantly below VWAP
        elif deviation < -std_multiplier and deviation_pct < -self.min_deviation_pct:
            # Check RSI filter - only buy if RSI is oversold
            if rsi is not None and rsi < self.rsi_oversold:
                confidence = min(0.9, 0.6 + (abs(deviation_pct) / 100) * 1.5)
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.BUY,
                    confidence=confidence,
                    reason=f"VWAP MR: Price {current_price:.2f} is {abs(deviation_pct):.2f}% below VWAP {vwap:.2f}",
                    suggested_sl=vwap - (std_dev * 3),  # 3 std dev SL
                    suggested_tp=vwap  # Target VWAP
                )
        
        return None


class MomentumBurstStrategy(BaseStrategy):
    """
    Momentum Burst Strategy
    
    - Looks for 3 consecutive bullish/bearish candles
    - Requires volume > 2x average volume
    - Confirms with increasing volume pattern
    - Works on 1-minute timeframe
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize Momentum Burst strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.lookback_period = self.parameters.get('lookback_period', 3)
        self.volume_multiplier = self.parameters.get('volume_multiplier', 2.0)
        self.avg_volume_period = self.parameters.get('avg_volume_period', 20)
        self.price_change_pct = self.parameters.get('price_change_pct', 0.5)
        self.confirm_with_volume = self.parameters.get('confirm_with_volume', True)
        
        # Track candle history per symbol
        self._candle_history: Dict[Symbol, List[OHLCV]] = {}
        self._avg_volume: Dict[Symbol, float] = {}
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process new OHLCV bar for momentum burst detection."""
        symbol = ohlcv.symbol
        
        # Initialize history if not exists
        if symbol not in self._candle_history:
            self._candle_history[symbol] = []
        
        # Add current candle
        self._candle_history[symbol].append(ohlcv)
        
        # Keep only the necessary history
        max_history = max(self.lookback_period * 2, self.avg_volume_period) + 10
        if len(self._candle_history[symbol]) > max_history:
            self._candle_history[symbol] = self._candle_history[symbol][-max_history:]
        
        # Calculate average volume
        self._calculate_avg_volume(symbol)
        
        # Check for momentum burst
        return self._check_momentum_burst(symbol)
    
    def _calculate_avg_volume(self, symbol: Symbol) -> None:
        """Calculate average volume for a symbol."""
        history = self._candle_history.get(symbol, [])
        if len(history) < self.avg_volume_period:
            return
        
        volumes = [c.volume for c in history[-self.avg_volume_period:]]
        self._avg_volume[symbol] = sum(volumes) / len(volumes)
    
    def _check_momentum_burst(self, symbol: Symbol) -> Optional[StrategySignal]:
        """Check for momentum burst pattern."""
        history = self._candle_history.get(symbol, [])
        
        if len(history) < self.lookback_period:
            return None
        
        # Get the last N candles
        recent_candles = history[-self.lookback_period:]
        
        # Check for consecutive bullish candles with volume confirmation
        if all(c.is_bullish() for c in recent_candles):
            # Check volume condition
            current_volume = recent_candles[-1].volume
            avg_volume = self._avg_volume.get(symbol, 0)
            
            volume_condition = (current_volume > avg_volume * self.volume_multiplier) if avg_volume > 0 else True
            
            # Check price change
            price_change = ((recent_candles[-1].close - recent_candles[0].open) / recent_candles[0].open) * 100
            
            if volume_condition and price_change > self.price_change_pct:
                # Increasing volume confirmation
                volume_increasing = all(
                    recent_candles[i].volume > recent_candles[i-1].volume 
                    for i in range(1, len(recent_candles))
                )
                
                confidence = 0.6
                if volume_increasing:
                    confidence += 0.15
                if self.confirm_with_volume and volume_condition:
                    confidence += 0.15
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.BUY,
                    confidence=confidence,
                    reason=f"Momentum Burst: {self.lookback_period} consecutive bullish candles with volume {current_volume:.0f} > {avg_volume:.0f} avg",
                    suggested_sl=recent_candles[-1].low - ((recent_candles[-1].high - recent_candles[-1].low) * 0.5),
                    suggested_tp=recent_candles[-1].close + ((recent_candles[-1].high - recent_candles[-1].low) * 2)
                )
        
        # Check for consecutive bearish candles
        elif all(c.is_bearish() for c in recent_candles):
            # Check volume condition
            current_volume = recent_candles[-1].volume
            avg_volume = self._avg_volume.get(symbol, 0)
            
            volume_condition = (current_volume > avg_volume * self.volume_multiplier) if avg_volume > 0 else True
            
            # Check price change
            price_change = ((recent_candles[0].open - recent_candles[-1].close) / recent_candles[0].open) * 100
            
            if volume_condition and price_change > self.price_change_pct:
                # Increasing volume confirmation
                volume_increasing = all(
                    recent_candles[i].volume > recent_candles[i-1].volume 
                    for i in range(1, len(recent_candles))
                )
                
                confidence = 0.6
                if volume_increasing:
                    confidence += 0.15
                if self.confirm_with_volume and volume_condition:
                    confidence += 0.15
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.SELL,
                    confidence=confidence,
                    reason=f"Momentum Burst: {self.lookback_period} consecutive bearish candles with volume {current_volume:.0f} > {avg_volume:.0f} avg",
                    suggested_sl=recent_candles[-1].high + ((recent_candles[-1].high - recent_candles[-1].low) * 0.5),
                    suggested_tp=recent_candles[-1].close - ((recent_candles[-1].high - recent_candles[-1].low) * 2)
                )
        
        return None


class EMACrossoverScalperStrategy(BaseStrategy):
    """
    EMA Crossover Scalper Strategy
    
    - Uses 9/21 EMA crossover on 1-minute charts
    - Includes RSI filter to avoid overbought/oversold conditions
    - Implements ATR-based stop loss
    - Scalping with tight risk management
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize EMA Crossover Scalper strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.fast_ema_period = self.parameters.get('fast_ema_period', 9)
        self.slow_ema_period = self.parameters.get('slow_ema_period', 21)
        self.rsi_period = self.parameters.get('rsi_period', 14)
        self.rsi_filter_enabled = self.parameters.get('rsi_filter_enabled', True)
        self.rsi_min = self.parameters.get('rsi_min', 40)
        self.rsi_max = self.parameters.get('rsi_max', 60)
        self.atr_period = self.parameters.get('atr_period', 14)
        self.sl_atr_multiplier = self.parameters.get('sl_atr_multiplier', 1.5)
        self.tp_atr_multiplier = self.parameters.get('tp_atr_multiplier', 2.0)
        
        # Track EMA and RSI values per symbol
        self._ema_fast: Dict[Symbol, float] = {}
        self._ema_slow: Dict[Symbol, float] = {}
        self._rsi: Dict[Symbol, float] = {}
        self._atr: Dict[Symbol, float] = {}
        self._price_history: Dict[Symbol, List[float]] = {}
        self._prev_crossover: Dict[Symbol, bool] = {}  # True for bullish crossover, False for bearish
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process new OHLCV bar for EMA crossover detection."""
        symbol = ohlcv.symbol
        
        # Initialize history if not exists
        if symbol not in self._price_history:
            self._price_history[symbol] = []
        
        # Add current close price
        self._price_history[symbol].append(ohlcv.close)
        
        # Keep only necessary history
        max_history = max(self.fast_ema_period, self.slow_ema_period, 
                          self.rsi_period, self.atr_period) * 2
        if len(self._price_history[symbol]) > max_history:
            self._price_history[symbol] = self._price_history[symbol][-max_history:]
        
        # Calculate indicators
        self._calculate_indicators(symbol)
        
        # Check for signal
        return self._check_crossover(symbol, ohlcv)
    
    def _calculate_indicators(self, symbol: Symbol) -> None:
        """Calculate EMA, RSI, and ATR for a symbol."""
        prices = self._price_history.get(symbol, [])
        
        if not prices:
            return
        
        # Calculate EMAs
        self._ema_fast[symbol] = self._calculate_ema(prices, self.fast_ema_period)
        self._ema_slow[symbol] = self._calculate_ema(prices, self.slow_ema_period)
        
        # Calculate RSI
        self._rsi[symbol] = self._calculate_rsi(prices, self.rsi_period)
        
        # Calculate ATR (using close prices as approximation)
        self._atr[symbol] = self._calculate_atr(prices, self.atr_period)
    
    def _calculate_ema(self, data: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average."""
        if len(data) < period:
            return None
        
        weights = np.exp(np.linspace(-1., 0., period))
        weights /= weights.sum()
        
        return float(np.convolve(data[-period:], weights, mode='valid')[0])
    
    def _calculate_rsi(self, data: List[float], period: int) -> Optional[float]:
        """Calculate Relative Strength Index."""
        if len(data) < period + 1:
            return None
        
        deltas = np.diff(data)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = sum(gains[-period:]) / period if len(gains) >= period else None
        avg_loss = sum(losses[-period:]) / period if len(losses) >= period else None
        
        if avg_loss is None or avg_loss == 0:
            return 100.0
        if avg_gain is None:
            return None
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    def _calculate_atr(self, data: List[float], period: int) -> Optional[float]:
        """Calculate Average True Range (simplified version)."""
        if len(data) < period + 1:
            return None
        
        true_ranges = []
        for i in range(1, len(data)):
            tr = max(
                abs(data[i] - data[i-1]),
                abs(data[i] - data[i-1])  # Simplified - normally needs HLC
            )
            true_ranges.append(tr)
        
        return sum(true_ranges[-period:]) / period if true_ranges else None
    
    def _check_crossover(self, symbol: Symbol, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Check for EMA crossover signal."""
        ema_fast = self._ema_fast.get(symbol)
        ema_slow = self._ema_slow.get(symbol)
        rsi = self._rsi.get(symbol)
        atr = self._atr.get(symbol)
        
        if ema_fast is None or ema_slow is None:
            return None
        
        current_price = ohlcv.close
        
        # Check for bullish crossover (fast EMA crosses above slow EMA)
        if ema_fast > ema_slow:
            # Check if this is a new crossover
            was_bearish = self._prev_crossover.get(symbol, None)
            if was_bearish is False or was_bearish is None:
                # Check RSI filter
                if self.rsi_filter_enabled:
                    if rsi is not None and rsi < self.rsi_min:
                        # Don't buy if RSI is too low (oversold, wait for recovery)
                        return None
                
                confidence = 0.7
                if rsi is not None:
                    confidence += (rsi / 100) * 0.1  # Higher RSI = slightly higher confidence
                
                sl_distance = atr * self.sl_atr_multiplier if atr else ohlcv.range * 0.5
                tp_distance = atr * self.tp_atr_multiplier if atr else ohlcv.range * 1.0
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.BUY,
                    confidence=confidence,
                    reason=f"EMA Crossover: Fast EMA ({ema_fast:.2f}) crossed above Slow EMA ({ema_slow:.2f})",
                    suggested_sl=current_price - sl_distance,
                    suggested_tp=current_price + tp_distance
                )
        
        # Check for bearish crossover (fast EMA crosses below slow EMA)
        elif ema_fast < ema_slow:
            # Check if this is a new crossover
            was_bullish = self._prev_crossover.get(symbol, None)
            if was_bullish is True or was_bullish is None:
                # Check RSI filter
                if self.rsi_filter_enabled:
                    if rsi is not None and rsi > self.rsi_max:
                        # Don't sell if RSI is too high (overbought, wait for pullback)
                        return None
                
                confidence = 0.7
                if rsi is not None:
                    confidence += ((100 - rsi) / 100) * 0.1  # Lower RSI = slightly higher confidence
                
                sl_distance = atr * self.sl_atr_multiplier if atr else ohlcv.range * 0.5
                tp_distance = atr * self.tp_atr_multiplier if atr else ohlcv.range * 1.0
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.SELL,
                    confidence=confidence,
                    reason=f"EMA Crossover: Fast EMA ({ema_fast:.2f}) crossed below Slow EMA ({ema_slow:.2f})",
                    suggested_sl=current_price + sl_distance,
                    suggested_tp=current_price - tp_distance
                )
        
        # Update crossover state
        if ema_fast > ema_slow:
            self._prev_crossover[symbol] = True
        else:
            self._prev_crossover[symbol] = False
        
        return None


class OrderFlowImbalanceStrategy(BaseStrategy):
    """
    Order Flow Imbalance Strategy
    
    - Analyzes bid/ask tick data to detect order flow imbalances
    - Identifies aggressive buying/selling through bid/ask changes
    - Uses volume-weighted aggression detection
    - Works on tick data for maximum responsiveness
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize Order Flow Imbalance strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.tick_window_seconds = self.parameters.get('tick_window_seconds', 5)
        self.imbalance_threshold = self.parameters.get('imbalance_threshold', 0.6)
        self.min_tick_count = self.parameters.get('min_tick_count', 10)
        self.volume_filter_pct = self.parameters.get('volume_filter_pct', 0.1)
        self.aggression_weight = self.parameters.get('aggression_weight', 0.7)
        
        # Track tick data per symbol
        self._tick_windows: Dict[Symbol, List[Tick]] = {}
        self._last_window_time: Dict[Symbol, Timestamp] = {}
        self._aggressive_buys: Dict[Symbol, int] = {}
        self._aggressive_sells: Dict[Symbol, int] = {}
    
    def on_tick(self, tick: Tick) -> Optional[StrategySignal]:
        """Process new tick for order flow analysis."""
        symbol = tick.symbol
        
        # Skip ticks without bid/ask data
        if tick.bid_price is None or tick.ask_price is None:
            return None
        
        # Initialize window if not exists
        if symbol not in self._tick_windows:
            self._tick_windows[symbol] = []
            self._last_window_time[symbol] = tick.timestamp
            self._aggressive_buys[symbol] = 0
            self._aggressive_sells[symbol] = 0
        
        # Add tick to window
        self._tick_windows[symbol].append(tick)
        
        # Check if we need to process the window
        current_time = tick.timestamp
        last_time = self._last_window_time[symbol]
        
        # Process window if time has elapsed or we have enough ticks
        if (current_time - last_time).total_seconds() >= self.tick_window_seconds:
            # Process the window
            signal = self._process_window(symbol)
            
            # Reset window for next period
            self._tick_windows[symbol] = []
            self._last_window_time[symbol] = current_time
            
            return signal
        
        return None
    
    def _process_window(self, symbol: Symbol) -> Optional[StrategySignal]:
        """Process a window of ticks to detect imbalance."""
        ticks = self._tick_windows.get(symbol, [])
        
        if len(ticks) < self.min_tick_count:
            return None
        
        # Analyze order flow
        total_aggressive_buys = 0
        total_aggressive_sells = 0
        total_volume = 0
        total_ticks = 0
        
        # Track previous bid/ask for aggression detection
        prev_bid = ticks[0].bid_price
        prev_ask = ticks[0].ask_price
        
        for tick in ticks[1:]:
            if tick.bid_price is None or tick.ask_price is None:
                continue
            
            # Check for aggressive buying (ask is hit)
            if tick.last_price >= tick.ask_price and tick.ask_price >= prev_ask:
                aggressive_buys = int(tick.last_quantity or 0)
                total_aggressive_buys += aggressive_buys
            
            # Check for aggressive selling (bid is hit)
            elif tick.last_price <= tick.bid_price and tick.bid_price <= prev_bid:
                aggressive_sells = int(tick.last_quantity or 0)
                total_aggressive_sells += aggressive_sells
            
            # Update previous prices
            prev_bid = tick.bid_price
            prev_ask = tick.ask_price
            
            total_volume += tick.last_quantity or 0
            total_ticks += 1
        
        # Calculate imbalance ratio
        if total_aggressive_buys + total_aggressive_sells == 0:
            return None
        
        # Weight aggression by volume
        aggression_score = (
            self.aggression_weight * (total_aggressive_buys - total_aggressive_sells) / 
            (total_aggressive_buys + total_aggressive_sells) +
            (1 - self.aggression_weight) * (total_aggressive_buys - total_aggressive_sells) / 
            max(total_aggressive_buys, total_aggressive_sells, 1)
        )
        
        # Normalize to -1 to +1 range
        imbalance = (total_aggressive_buys - total_aggressive_sells) / max(
            total_aggressive_buys + total_aggressive_sells, 1
        )
        
        # Check for significant imbalance
        if imbalance > self.imbalance_threshold:
            # Strong buying pressure
            confidence = 0.5 + (imbalance * 0.3)
            
            # Volume filter
            avg_volume = total_volume / max(total_ticks, 1)
            if avg_volume < (self._get_average_volume(symbol) * self.volume_filter_pct):
                confidence *= 0.5
            
            current_price = ticks[-1].last_price
            
            return self._create_signal(
                symbol=symbol,
                signal=SignalType.BUY,
                confidence=confidence,
                reason=f"Order Flow: Buying imbalance {imbalance:.2%} with aggression score {aggression_score:.2f}",
                suggested_sl=current_price - (current_price * 0.005),  # 0.5% SL
                suggested_tp=current_price + (current_price * 0.01)   # 1% TP
            )
        
        elif imbalance < -self.imbalance_threshold:
            # Strong selling pressure
            confidence = 0.5 + (abs(imbalance) * 0.3)
            
            # Volume filter
            avg_volume = total_volume / max(total_ticks, 1)
            if avg_volume < (self._get_average_volume(symbol) * self.volume_filter_pct):
                confidence *= 0.5
            
            current_price = ticks[-1].last_price
            
            return self._create_signal(
                symbol=symbol,
                signal=SignalType.SELL,
                confidence=confidence,
                reason=f"Order Flow: Selling imbalance {abs(imbalance):.2%} with aggression score {abs(aggression_score):.2f}",
                suggested_sl=current_price + (current_price * 0.005),  # 0.5% SL
                suggested_tp=current_price - (current_price * 0.01)   # 1% TP
            )
        
        return None
    
    def _get_average_volume(self, symbol: Symbol) -> float:
        """Get average volume for a symbol."""
        data_feed = get_data_feed()
        ohlcv_data = data_feed.cache.get_ohlcv(symbol, TimeFrame._1M, limit=20)
        
        if not ohlcv_data:
            return 1000  # Default
        
        volumes = [c.volume for c in ohlcv_data]
        return sum(volumes) / len(volumes)


class SupertrendStrategy(BaseStrategy):
    """
    Supertrend Strategy with ATR Trailing Stop
    
    - Uses Supertrend indicator to identify trends
    - Implements ATR-based trailing stops
    - Only trades in strong trending regimes
    - Works on 1-minute timeframe
    """
    
    def __init__(self, config: StrategyConfig):
        """Initialize Supertrend strategy."""
        super().__init__(config)
        
        # Strategy parameters with defaults
        self.atr_period = self.parameters.get('atr_period', 10)
        self.multiplier = self.parameters.get('multiplier', 3.0)
        self.trending_regime_only = self.parameters.get('trending_regime_only', True)
        self.use_trailing_stop = self.parameters.get('use_trailing_stop', True)
        self.trailing_atr_multiplier = self.parameters.get('trailing_atr_multiplier', 1.5)
        
        # Track Supertrend values per symbol
        self._supertrend: Dict[Symbol, float] = {}
        self._supertrend_direction: Dict[Symbol, int] = {}  # 1 for up, -1 for down
        self._atr: Dict[Symbol, float] = {}
        self._close_prices: Dict[Symbol, List[float]] = {}
        self._highs: Dict[Symbol, List[float]] = {}
        self._lows: Dict[Symbol, List[float]] = {}
        
        # Track trend strength
        self._trend_strength: Dict[Symbol, float] = {}
    
    def on_bar(self, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Process new OHLCV bar for Supertrend calculation."""
        symbol = ohlcv.symbol
        
        # Initialize history if not exists
        if symbol not in self._close_prices:
            self._close_prices[symbol] = []
            self._highs[symbol] = []
            self._lows[symbol] = []
        
        # Add current values
        self._close_prices[symbol].append(ohlcv.close)
        self._highs[symbol].append(ohlcv.high)
        self._lows[symbol].append(ohlcv.low)
        
        # Keep only necessary history
        max_history = self.atr_period * 2
        if len(self._close_prices[symbol]) > max_history:
            self._close_prices[symbol] = self._close_prices[symbol][-max_history:]
            self._highs[symbol] = self._highs[symbol][-max_history:]
            self._lows[symbol] = self._lows[symbol][-max_history:]
        
        # Calculate Supertrend and ATR
        self._calculate_supertrend(symbol)
        self._calculate_trend_strength(symbol)
        
        # Check for signal
        return self._check_signal(symbol, ohlcv)
    
    def _calculate_supertrend(self, symbol: Symbol) -> None:
        """Calculate Supertrend for a symbol."""
        closes = self._close_prices.get(symbol, [])
        highs = self._highs.get(symbol, [])
        lows = self._lows.get(symbol, [])
        
        if len(closes) < self.atr_period + 1:
            return
        
        # Calculate ATR
        true_ranges = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            true_ranges.append(tr)
        
        atr = sum(true_ranges[-self.atr_period:]) / self.atr_period
        self._atr[symbol] = atr
        
        # Calculate Supertrend
        hl2 = [(highs[i] + lows[i]) / 2 for i in range(len(highs))]
        
        upper_band = [hl2[i] + (self.multiplier * atr) for i in range(len(hl2))]
        lower_band = [hl2[i] - (self.multiplier * atr) for i in range(len(hl2))]
        
        # Initialize Supertrend
        st = []
        direction = []
        
        for i in range(len(closes)):
            if i == 0:
                st.append(upper_band[i] if closes[i] > upper_band[i] else lower_band[i])
                direction.append(1 if closes[i] > upper_band[i] else -1)
            else:
                if direction[i-1] == 1:
                    new_st = max(lower_band[i], st[i-1])
                    if closes[i] > upper_band[i]:
                        new_st = lower_band[i]
                        new_direction = 1
                    else:
                        new_direction = 1
                else:
                    new_st = min(upper_band[i], st[i-1])
                    if closes[i] < lower_band[i]:
                        new_st = upper_band[i]
                        new_direction = -1
                    else:
                        new_direction = -1
                
                st.append(new_st)
                direction.append(new_direction)
        
        if st:
            self._supertrend[symbol] = st[-1]
            self._supertrend_direction[symbol] = direction[-1]
    
    def _calculate_trend_strength(self, symbol: Symbol) -> None:
        """Calculate trend strength for a symbol."""
        closes = self._close_prices.get(symbol, [])
        
        if len(closes) < 20:
            self._trend_strength[symbol] = 0.0
            return
        
        # Simple trend strength calculation using price deviation from SMA
        sma = sum(closes[-20:]) / 20
        price_deviation = abs(closes[-1] - sma) / sma * 100
        
        # Normalize to 0-1 range
        self._trend_strength[symbol] = min(1.0, price_deviation / 5.0)
    
    def _check_signal(self, symbol: Symbol, ohlcv: OHLCV) -> Optional[StrategySignal]:
        """Check for Supertrend signal."""
        supertrend = self._supertrend.get(symbol)
        direction = self._supertrend_direction.get(symbol)
        atr = self._atr.get(symbol)
        trend_strength = self._trend_strength.get(symbol, 0)
        
        if supertrend is None or direction is None:
            return None
        
        current_price = ohlcv.close
        
        # Check if we're in a trending regime (if required)
        if self.trending_regime_only and trend_strength < 0.5:
            return None
        
        # Check for buy signal (price crosses above Supertrend)
        if direction == 1 and current_price > supertrend:
            # Only trigger if direction has changed
            if self._prev_direction(symbol) != 1:
                confidence = 0.6 + (trend_strength * 0.2)
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.BUY,
                    confidence=confidence,
                    reason=f"Supertrend: Price {current_price:.2f} crossed above ST {supertrend:.2f}, Direction=UP",
                    suggested_sl=current_price - (atr * self.trailing_atr_multiplier) if self.use_trailing_stop else supertrend,
                    suggested_tp=current_price + (atr * 2)  # 2x ATR
                )
        
        # Check for sell signal (price crosses below Supertrend)
        elif direction == -1 and current_price < supertrend:
            # Only trigger if direction has changed
            if self._prev_direction(symbol) != -1:
                confidence = 0.6 + (trend_strength * 0.2)
                
                return self._create_signal(
                    symbol=symbol,
                    signal=SignalType.SELL,
                    confidence=confidence,
                    reason=f"Supertrend: Price {current_price:.2f} crossed below ST {supertrend:.2f}, Direction=DOWN",
                    suggested_sl=current_price + (atr * self.trailing_atr_multiplier) if self.use_trailing_stop else supertrend,
                    suggested_tp=current_price - (atr * 2)  # 2x ATR
                )
        
        return None
    
    def _prev_direction(self, symbol: Symbol) -> Optional[int]:
        """Get previous Supertrend direction."""
        # Store previous direction in a temporary cache
        if not hasattr(self, '_prev_direction_cache'):
            self._prev_direction_cache = {}
        
        current = self._supertrend_direction.get(symbol)
        prev = self._prev_direction_cache.get(symbol)
        
        # Update cache
        self._prev_direction_cache[symbol] = current
        
        return prev


# =============================================================================
# STRATEGY ENGINE
# =============================================================================

class StrategyEngine:
    """Main engine that manages all trading strategies."""
    
    def __init__(self):
        """Initialize the strategy engine."""
        self.config = get_config_loader().load()
        self.logger = strategy_logger()
        
        # Load strategy configurations
        self.strategy_configs: List[StrategyConfig] = get_config_loader().get_strategy_configs()
        
        # Initialize strategies
        self.strategies: Dict[StrategyName, BaseStrategy] = {}
        self._init_strategies()
        
        # Subscribers
        self.subscribers: List[Callable] = []
        
        # Data feed
        self.data_feed = get_data_feed()
        
        # Register with data feed
        self._register_with_data_feed()
        
        self.logger.info(f"StrategyEngine initialized with {len(self.strategies)} strategies")
    
    def _init_strategies(self) -> None:
        """Initialize all strategies from configuration."""
        strategy_classes = {
            StrategyName.ORB: OpeningRangeBreakoutStrategy,
            StrategyName.VWAP_MR: VWAPMeanReversionStrategy,
            StrategyName.MOMENTUM_BURST: MomentumBurstStrategy,
            StrategyName.EMA_CROSSOVER: EMACrossoverScalperStrategy,
            StrategyName.ORDER_FLOW: OrderFlowImbalanceStrategy,
            StrategyName.SUPERTREND: SupertrendStrategy
        }
        
        for strategy_config in self.strategy_configs:
            if not strategy_config.enabled:
                continue
            
            strategy_class = strategy_classes.get(strategy_config.name)
            if strategy_class:
                strategy = strategy_class(strategy_config)
                strategy.initialize()
                self.strategies[strategy_config.name] = strategy
                
                self.logger.info(f"Initialized strategy: {strategy_config.name.value}")
    
    def _register_with_data_feed(self) -> None:
        """Register strategies with the data feed."""
        def on_tick_callback(symbol: Symbol, data: Tick) -> None:
            self.on_tick(symbol, data)
        
        def on_bar_callback(symbol: Symbol, data: OHLCV) -> None:
            self.on_bar(symbol, data)
        
        # Subscribe to tick data
        self.data_feed.subscribe(on_tick_callback, data_type="tick")
        
        # Subscribe to OHLCV data
        self.data_feed.subscribe(on_bar_callback, data_type="ohlcv")
    
    def start(self) -> None:
        """Start all strategies."""
        for strategy in self.strategies.values():
            try:
                strategy.initialize()
            except Exception as e:
                self.logger.error(f"Failed to start strategy {strategy.name}: {e}")
        
        self.logger.info("All strategies started")
    
    def stop(self) -> None:
        """Stop all strategies."""
        for strategy in self.strategies.values():
            try:
                strategy.cleanup()
            except Exception as e:
                self.logger.error(f"Failed to stop strategy {strategy.name}: {e}")
        
        self.logger.info("All strategies stopped")
    
    def on_tick(self, symbol: Symbol, tick: Tick) -> None:
        """Process a new tick through all strategies."""
        # Skip if data quality is poor
        data_feed = get_data_feed()
        quality_score = data_feed.get_data_quality_score(symbol, TimeFrame._1M)
        
        if quality_score < 0.7:
            self.logger.debug(f"Skipping tick processing for {symbol} due to low data quality: {quality_score:.2f}")
            return
        
        # Process tick through each strategy
        for strategy in self.strategies.values():
            if not strategy.enabled:
                continue
            
            try:
                signal = strategy.on_tick(tick)
                if signal:
                    self._publish_signal(signal)
            except Exception as e:
                self.logger.error(f"Error in strategy {strategy.name} on_tick: {e}")
    
    def on_bar(self, symbol: Symbol, ohlcv: OHLCV) -> None:
        """Process a new OHLCV bar through all strategies."""
        # Skip if data quality is poor
        data_feed = get_data_feed()
        quality_score = data_feed.get_data_quality_score(symbol, ohlcv.timeframe)
        
        if quality_score < 0.7:
            self.logger.debug(f"Skipping bar processing for {symbol} {ohlcv.timeframe.value} due to low data quality: {quality_score:.2f}")
            return
        
        # Process bar through each strategy
        for strategy in self.strategies.values():
            if not strategy.enabled:
                continue
            
            # Only process bars that match the strategy's timeframe
            if strategy.timeframe != ohlcv.timeframe:
                continue
            
            try:
                signal = strategy.on_bar(ohlcv)
                if signal:
                    self._publish_signal(signal)
            except Exception as e:
                self.logger.error(f"Error in strategy {strategy.name} on_bar: {e}")
    
    def calculate_all(self, symbol: Symbol) -> List[StrategySignal]:
        """Calculate signals for all strategies for a symbol."""
        signals = []
        
        for strategy in self.strategies.values():
            if not strategy.enabled:
                continue
            
            try:
                # Get current data
                current_tick = self.data_feed.cache.get_current_tick(symbol)
                current_ohlcv = self.data_feed.cache.get_current_ohlcv(symbol, strategy.timeframe)
                
                # Calculate signal
                if current_ohlcv:
                    signal = strategy.on_bar(current_ohlcv)
                    if signal:
                        signals.append(signal)
                elif current_tick:
                    signal = strategy.on_tick(current_tick)
                    if signal:
                        signals.append(signal)
                        
            except Exception as e:
                self.logger.error(f"Error calculating signal for {strategy.name}: {e}")
        
        return signals
    
    def _publish_signal(self, signal: StrategySignal) -> None:
        """Publish a signal to all subscribers."""
        self.logger.log_signal('strategy_engine', signal.to_dict())
        
        # Notify subscribers
        for subscriber in self.subscribers:
            try:
                subscriber(signal)
            except Exception as e:
                self.logger.error(f"Error in signal subscriber {subscriber.__name__}: {e}")
    
    def subscribe(self, callback: Callable) -> None:
        """Subscribe to strategy signals."""
        self.subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from strategy signals."""
        if callback in self.subscribers:
            self.subscribers.remove(callback)
    
    def get_strategy(self, name: StrategyName) -> Optional[BaseStrategy]:
        """Get a specific strategy by name."""
        return self.strategies.get(name)
    
    def get_enabled_strategies(self) -> List[BaseStrategy]:
        """Get all enabled strategies."""
        return [s for s in self.strategies.values() if s.enabled]
    
    def get_strategy_performance(self) -> Dict[str, Dict[str, Any]]:
        """Get performance metrics for all strategies."""
        return {name.value: strategy.get_info() 
                for name, strategy in self.strategies.items()}
    
    def reset_strategy(self, symbol: Symbol, strategy_name: Optional[StrategyName] = None) -> None:
        """Reset strategies for a symbol."""
        if strategy_name:
            strategy = self.strategies.get(strategy_name)
            if strategy:
                strategy.reset(symbol)
        else:
            for strategy in self.strategies.values():
                strategy.reset(symbol)


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_strategy_engine: Optional[StrategyEngine] = None


def get_strategy_engine() -> StrategyEngine:
    """Get the singleton StrategyEngine instance."""
    global _strategy_engine
    if _strategy_engine is None:
        _strategy_engine = StrategyEngine()
    return _strategy_engine


def start_strategy_engine() -> StrategyEngine:
    """Start the strategy engine and return the instance."""
    global _strategy_engine
    _strategy_engine = StrategyEngine()
    _strategy_engine.start()
    return _strategy_engine


def stop_strategy_engine() -> None:
    """Stop the strategy engine."""
    global _strategy_engine
    if _strategy_engine:
        _strategy_engine.stop()
        _strategy_engine = None
