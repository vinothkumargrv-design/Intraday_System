"""
Structured logging system for the Rapid Fire Intraday Trading System.

This module provides JSON and text-based structured logging with timestamps,
log levels, and component identification. All logs are written to both file
and console (configurable) with proper rotation and formatting.
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union
import threading

from intraday_system.models import NotificationLevel


# =============================================================================
# LOG FORMATTERS
# =============================================================================

class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs log records as JSON."""
    
    def __init__(self, include_timestamps: bool = True, 
                 include_level: bool = True,
                 include_module: bool = True,
                 include_function: bool = True):
        super().__init__()
        self.include_timestamps = include_timestamps
        self.include_level = include_level
        self.include_module = include_module
        self.include_function = include_function
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "timestamp_local": datetime.now().astimezone().isoformat(),
            "level": record.levelname,
            "message": record.getMessage()
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add stack info if present
        if record.stack_info:
            log_data["stack_info"] = self.formatStack(record.stack_info)
        
        # Add module and function
        if self.include_module:
            log_data["module"] = record.module
        if self.include_function:
            log_data["function"] = record.funcName
        if self.include_level:
            log_data["level_num"] = record.levelno
        
        # Add thread info
        log_data["thread"] = record.threadName
        log_data["thread_id"] = record.thread
        log_data["process"] = record.processName
        log_data["process_id"] = record.process
        
        # Add custom fields from extra
        if hasattr(record, 'custom_fields'):
            log_data.update(record.custom_fields)
        
        # Add standard attributes
        standard_attrs = ['name', 'pathname', 'filename', 'lineno']
        for attr in standard_attrs:
            if hasattr(record, attr):
                log_data[attr] = getattr(record, attr)
        
        return json.dumps(log_data, default=str)


class TextFormatter(logging.Formatter):
    """Custom formatter for human-readable text output."""
    
    def __init__(self, include_timestamps: bool = True,
                 include_level: bool = True,
                 include_module: bool = True,
                 include_function: bool = True):
        super().__init__()
        self.include_timestamps = include_timestamps
        self.include_level = include_level
        self.include_module = include_module
        self.include_function = include_function
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as human-readable text."""
        parts = []
        
        # Timestamp
        if self.include_timestamps:
            parts.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]")
        
        # Level
        if self.include_level:
            parts.append(f"[{record.levelname}]")
        
        # Module and function
        if self.include_module:
            parts.append(f"[{record.module}]")
        if self.include_function:
            parts.append(f"[{record.funcName}]")
        
        # Message
        parts.append(record.getMessage())
        
        # Custom fields
        if hasattr(record, 'custom_fields') and record.custom_fields:
            parts.append(f"  {record.custom_fields}")
        
        # Exception
        if record.exc_info:
            parts.append(f"\nException: {self.formatException(record.exc_info)}")
        
        return " ".join(parts)


# =============================================================================
# CUSTOM LOG RECORD
# =============================================================================

class TradingLogRecord(logging.LogRecord):
    """Extended log record with custom fields."""
    
    def __init__(self, name: str, level: int, pathname: str, lineno: int,
                 msg: str, args: tuple, exc_info: Optional[tuple] = None,
                 func: Optional[str] = None, sinfo: Optional[str] = None,
                 custom_fields: Optional[Dict[str, Any]] = None):
        super().__init__(name, level, pathname, lineno, msg, args, exc_info, func, sinfo)
        self.custom_fields = custom_fields or {}


# =============================================================================
# CUSTOM LOGGER CLASS
# =============================================================================

class TradingLogger(logging.Logger):
    """Custom logger class for the trading system."""
    
    def __init__(self, name: str, level: int = logging.INFO):
        super().__init__(name, level)
        self.component = name
        self.lock = threading.Lock()
    
    def makeRecord(self, name: str, level: int, fn: str, lineno: int,
                   msg: str, args: tuple, exc_info: Optional[tuple] = None,
                   func: Optional[str] = None, extra: Optional[Dict[str, Any]] = None,
                   sinfo: Optional[str] = None) -> TradingLogRecord:
        """Create a log record with custom fields."""
        rv = TradingLogRecord(
            name, level, fn, lineno, msg, args, exc_info, func, sinfo,
            custom_fields=extra or {}
        )
        rv.custom_fields.update(extra or {})
        return rv
    
    def log(self, level: int, msg: str, *args: Any, 
            custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Log a message with optional custom fields."""
        if custom_fields:
            extra = kwargs.get('extra', {})
            extra.update(custom_fields)
            kwargs['extra'] = extra
        # Report the real call site rather than these wrapper methods
        kwargs.setdefault('stacklevel', 2)
        super().log(level, msg, *args, **kwargs)
    
    def debug(self, msg: str, *args: Any, custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Debug level log."""
        kwargs.setdefault('stacklevel', 3)
        self.log(logging.DEBUG, msg, *args, custom_fields=custom_fields, **kwargs)
    
    def info(self, msg: str, *args: Any, custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Info level log."""
        kwargs.setdefault('stacklevel', 3)
        self.log(logging.INFO, msg, *args, custom_fields=custom_fields, **kwargs)
    
    def warning(self, msg: str, *args: Any, custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Warning level log."""
        kwargs.setdefault('stacklevel', 3)
        self.log(logging.WARNING, msg, *args, custom_fields=custom_fields, **kwargs)
    
    def error(self, msg: str, *args: Any, custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Error level log."""
        kwargs.setdefault('stacklevel', 3)
        self.log(logging.ERROR, msg, *args, custom_fields=custom_fields, **kwargs)
    
    def critical(self, msg: str, *args: Any, custom_fields: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """Critical level log."""
        kwargs.setdefault('stacklevel', 3)
        self.log(logging.CRITICAL, msg, *args, custom_fields=custom_fields, **kwargs)
    
    # Structured event helpers (component argument kept for call-site clarity)
    
    def log_signal(self, component: str, signal_data: Dict[str, Any]) -> None:
        """Log a strategy/aggregated signal event (DEBUG: signals are high-volume)."""
        self.debug(
            f"SIGNAL: {signal_data.get('strategy', signal_data.get('final_signal', ''))} -> "
            f"{signal_data.get('signal', signal_data.get('final_signal', ''))} {signal_data.get('symbol', '')}",
            custom_fields={"signal": signal_data, "event_type": "SIGNAL", "source": component},
            stacklevel=4
        )
    
    def log_order(self, component: str, order_data: Dict[str, Any]) -> None:
        """Log an order event."""
        self.info(
            f"ORDER: {order_data.get('order_id', '')} {order_data.get('side', '')} "
            f"{order_data.get('quantity', 0)} {order_data.get('symbol', '')} {order_data.get('type', '')}",
            custom_fields={"order": order_data, "event_type": "ORDER", "source": component},
            stacklevel=4
        )
    
    def log_trade(self, component: str, trade_data: Dict[str, Any]) -> None:
        """Log a completed trade."""
        self.info(
            f"TRADE: {trade_data.get('side', '')} {trade_data.get('quantity', 0)} "
            f"{trade_data.get('symbol', '')} net {trade_data.get('net_pnl', 0)}",
            custom_fields={"trade": trade_data, "event_type": "TRADE", "source": component},
            stacklevel=4
        )


# =============================================================================
# LOG MANAGER
# =============================================================================

class LogManager:
    """Manager for configuring and accessing loggers throughout the system."""
    
    _instance: Optional['LogManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls, config: Optional[Dict[str, Any]] = None):
        """Singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the log manager."""
        if self._initialized:
            return
        
        self._initialized = True
        self.config = config or {}
        self.loggers: Dict[str, TradingLogger] = {}
        self._handlers: list = []
        self._setup_logging()
    
    def configure(self, config: Optional[Dict[str, Any]]) -> None:
        """(Re)apply logging configuration, replacing handlers installed earlier."""
        root = logging.getLogger()
        for handler in self._handlers:
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        self._handlers = []
        self.config = config or {}
        self._setup_logging()
    
    def _new_logger(self, component: str, level: int = logging.INFO) -> 'TradingLogger':
        logger = TradingLogger(f"intraday_system.{component}", level)
        # Created outside the logging manager, so attach it to the root logger
        # explicitly; otherwise records never reach any handler.
        logger.parent = logging.getLogger()
        return logger
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        # Get configuration
        log_config = self.config.get('logging', {})
        file_config = log_config.get('file', {})
        console_config = log_config.get('console', {})
        structured_config = log_config.get('structured', {})
        component_levels = log_config.get('component_levels', {})
        
        # Determine log format
        use_json = structured_config.get('enabled', False) and structured_config.get('format', 'json') == 'json'
        include_timestamps = structured_config.get('include_timestamps', True)
        include_level = structured_config.get('include_level', True)
        include_module = structured_config.get('include_module', True)
        include_function = structured_config.get('include_function', True)
        
        # Create formatter
        if use_json:
            formatter = JSONFormatter(
                include_timestamps=include_timestamps,
                include_level=include_level,
                include_module=include_module,
                include_function=include_function
            )
        else:
            formatter = TextFormatter(
                include_timestamps=include_timestamps,
                include_level=include_level,
                include_module=include_module,
                include_function=include_function
            )
        
        # Setup file handler
        file_enabled = file_config.get('enabled', True)
        if file_enabled:
            log_path = Path(file_config.get('path', 'logs/trading_system.log'))
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(log_path),
                maxBytes=file_config.get('max_size_mb', 10) * 1024 * 1024,
                backupCount=file_config.get('max_backups', 5)
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(getattr(logging, file_config.get('level', 'INFO').upper(), logging.INFO))
            
            # Add to root logger
            logging.getLogger().addHandler(file_handler)
            self._handlers.append(file_handler)
        
        # Setup console handler
        console_enabled = console_config.get('enabled', True)
        if console_enabled:
            console_handler = logging.StreamHandler(sys.stdout)
            # Console is for humans: plain text unless explicitly asked for JSON
            if console_config.get('format', 'text') == 'json':
                console_handler.setFormatter(formatter)
            else:
                console_handler.setFormatter(TextFormatter(include_function=False))
            console_handler.setLevel(getattr(logging, console_config.get('level', 'INFO').upper(), logging.INFO))
            logging.getLogger().addHandler(console_handler)
            self._handlers.append(console_handler)
        
        # Set root logger level
        root_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
        logging.getLogger().setLevel(root_level)
        
        # Prevent propagation to root for our loggers
        logging.getLogger('intraday_system').propagate = False
        
        # Setup component-specific loggers
        self._setup_component_loggers(component_levels)
    
    def _setup_component_loggers(self, component_levels: Dict[str, str]) -> None:
        """Setup loggers for specific components."""
        components = [
            'data_feed', 'strategy_engine', 'signal_aggregator', 'risk_manager',
            'order_manager', 'execution_engine', 'portfolio_tracker', 'broker',
            'backtester', 'notifier', 'dashboard', 'main'
        ]
        
        for component in components:
            level = component_levels.get(component, 'INFO')
            level_num = getattr(logging, level.upper(), logging.INFO)
            
            if component in self.loggers:
                self.loggers[component].setLevel(level_num)
            else:
                self.loggers[component] = self._new_logger(component, level_num)
    
    def get_logger(self, component: str) -> TradingLogger:
        """Get a logger for a specific component."""
        if component not in self.loggers:
            self.loggers[component] = self._new_logger(component)
        
        return self.loggers[component]
    
    def log_trade(self, component: str, trade_data: Dict[str, Any]) -> None:
        """Log a trade event with special formatting."""
        logger = self.get_logger(component)
        logger.info(
            f"TRADE: {trade_data.get('side', 'UNKNOWN')} {trade_data.get('quantity', 0)} "
            f"{trade_data.get('symbol', 'UNKNOWN')} @ {trade_data.get('price', 0)}",
            custom_fields={
                **trade_data,
                "event_type": "TRADE"
            }
        )
    
    def log_signal(self, component: str, signal_data: Dict[str, Any]) -> None:
        """Log a signal event with special formatting."""
        logger = self.get_logger(component)
        logger.info(
            f"SIGNAL: {signal_data.get('strategy', 'UNKNOWN')} -> "
            f"{signal_data.get('signal', 'UNKNOWN')} {signal_data.get('symbol', 'UNKNOWN')}",
            custom_fields={
                **signal_data,
                "event_type": "SIGNAL"
            }
        )
    
    def log_order(self, component: str, order_data: Dict[str, Any]) -> None:
        """Log an order event with special formatting."""
        logger = self.get_logger(component)
        logger.info(
            f"ORDER: {order_data.get('order_id', 'UNKNOWN')} "
            f"{order_data.get('side', 'UNKNOWN')} {order_data.get('quantity', 0)} "
            f"{order_data.get('symbol', 'UNKNOWN')}",
            custom_fields={
                **order_data,
                "event_type": "ORDER"
            }
        )
    
    def log_error(self, component: str, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        """Log an error with exception details."""
        logger = self.get_logger(component)
        context = context or {}
        logger.error(
            f"ERROR: {str(error)}",
            exc_info=True,
            custom_fields={
                **context,
                "error_type": type(error).__name__,
                "event_type": "ERROR"
            }
        )
    
    def log_performance(self, component: str, metrics: Dict[str, Any]) -> None:
        """Log performance metrics."""
        logger = self.get_logger(component)
        logger.info(
            f"PERFORMANCE: Win Rate={metrics.get('win_rate', 0):.2f}% "
            f"PnL={metrics.get('total_net_pnl', 0):.2f}",
            custom_fields={
                **metrics,
                "event_type": "PERFORMANCE"
            }
        )
    
    def log_alert(self, level: NotificationLevel, message: str, 
                  context: Optional[Dict[str, Any]] = None) -> None:
        """Log an alert with notification level."""
        level_map = {
            NotificationLevel.INFO: logging.INFO,
            NotificationLevel.WARNING: logging.WARNING,
            NotificationLevel.ERROR: logging.ERROR,
            NotificationLevel.CRITICAL: logging.CRITICAL,
            NotificationLevel.TRADE: logging.INFO
        }
        
        logger = self.get_logger('notifier')
        context = context or {}
        
        logger.log(
            level_map.get(level, logging.INFO),
            f"ALERT [{level.value}]: {message}",
            custom_fields={
                **context,
                "alert_level": level.value,
                "event_type": "ALERT"
            }
        )


# =============================================================================
# GLOBAL ACCESS
# =============================================================================

_log_manager: Optional[LogManager] = None


def setup_logging(config: Optional[Dict[str, Any]] = None) -> None:
    """Setup the logging system with the given configuration."""
    global _log_manager
    if _log_manager is None:
        _log_manager = LogManager(config)
    else:
        _log_manager.configure(config)


def get_logger(component: str) -> TradingLogger:
    """Get a logger for a specific component."""
    global _log_manager
    if _log_manager is None:
        setup_logging()
    return _log_manager.get_logger(component)


def log_trade(component: str, trade_data: Dict[str, Any]) -> None:
    """Log a trade event."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_trade(component, trade_data)


def log_signal(component: str, signal_data: Dict[str, Any]) -> None:
    """Log a signal event."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_signal(component, signal_data)


def log_order(component: str, order_data: Dict[str, Any]) -> None:
    """Log an order event."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_order(component, order_data)


def log_error(component: str, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
    """Log an error."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_error(component, error, context)


def log_performance(component: str, metrics: Dict[str, Any]) -> None:
    """Log performance metrics."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_performance(component, metrics)


def log_alert(level: NotificationLevel, message: str, 
              context: Optional[Dict[str, Any]] = None) -> None:
    """Log an alert."""
    global _log_manager
    if _log_manager is not None:
        _log_manager.log_alert(level, message, context)


# =============================================================================
# CONVENIENCE FUNCTIONS FOR COMMON COMPONENTS
# =============================================================================

def data_feed_logger() -> TradingLogger:
    """Get logger for data feed."""
    return get_logger('data_feed')


def strategy_logger() -> TradingLogger:
    """Get logger for strategy engine."""
    return get_logger('strategy_engine')


def signal_logger() -> TradingLogger:
    """Get logger for signal aggregator."""
    return get_logger('signal_aggregator')


def risk_logger() -> TradingLogger:
    """Get logger for risk manager."""
    return get_logger('risk_manager')


def order_logger() -> TradingLogger:
    """Get logger for order manager."""
    return get_logger('order_manager')


def execution_logger() -> TradingLogger:
    """Get logger for execution engine."""
    return get_logger('execution_engine')


def portfolio_logger() -> TradingLogger:
    """Get logger for portfolio tracker."""
    return get_logger('portfolio_tracker')


def broker_logger() -> TradingLogger:
    """Get logger for broker."""
    return get_logger('broker')


def backtester_logger() -> TradingLogger:
    """Get logger for backtester."""
    return get_logger('backtester')


def notifier_logger() -> TradingLogger:
    """Get logger for notifier."""
    return get_logger('notifier')


def dashboard_logger() -> TradingLogger:
    """Get logger for dashboard."""
    return get_logger('dashboard')


def main_logger() -> TradingLogger:
    """Get logger for main module."""
    return get_logger('main')


def set_log_level(level: 'LogLevel') -> None:
    """Set the log level for all loggers."""
    from intraday_system.models import LogLevel
    
    global _log_manager
    if _log_manager is not None:
        # Map LogLevel to Python logging levels
        level_map = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }
        
        root_level = level_map.get(level, logging.INFO)
        logging.getLogger().setLevel(root_level)
        
        # Update all component loggers
        for component in _log_manager.loggers.values():
            component.setLevel(root_level)
