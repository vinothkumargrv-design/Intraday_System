"""
Configuration loader and manager for the Rapid Fire Intraday Trading System.

This module handles loading, parsing, and validating configuration from YAML files
and environment variables. It provides type-safe access to all system parameters
with proper defaults and validation.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, TypeAlias, Union
import yaml

from .models import (
    AggregationConfig, BrokerConfig, BrokerType, OrderType, OrderValidity,
    RiskParameters, StrategyConfig, StrategyName, TimeFrame
)

# Type alias for configuration dictionary
Config: TypeAlias = Dict[str, Any]


# =============================================================================
# ENVIRONMENT VARIABLE PARSING
# =============================================================================

def parse_bool(value: Union[str, bool]) -> bool:
    """Parse boolean from string or boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 't', 'y', 'yes', 'on')
    return bool(value)


def parse_float(value: Union[str, float, int]) -> float:
    """Parse float from string or number."""
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def parse_int(value: Union[str, int, float]) -> int:
    """Parse int from string or number."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            try:
                return int(float(value))
            except ValueError:
                return 0
    return 0


def parse_time(value: Union[str, Any]) -> Any:
    """Parse time from string in format HH:MM."""
    from datetime import time
    if isinstance(value, str):
        try:
            hour, minute = value.split(':')
            return time(int(hour), int(minute))
        except (ValueError, AttributeError):
            return time(15, 15)  # Default to 15:15
    return value


def parse_list(value: Union[str, List[Any]]) -> List[Any]:
    """Parse list from string (comma-separated) or list."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [item.strip() for item in value.split(',') if item.strip()]
    return []


def parse_timeframe(value: Union[str, TimeFrame]) -> TimeFrame:
    """Parse TimeFrame from string."""
    if isinstance(value, TimeFrame):
        return value
    if isinstance(value, str):
        mapping = {
            'tick': TimeFrame.TICK,
            '1m': TimeFrame._1M,
            '5m': TimeFrame._5M,
            '15m': TimeFrame._15M,
            '1h': TimeFrame._1H
        }
        return mapping.get(value.lower(), TimeFrame._1M)
    return TimeFrame._1M


def parse_strategy_name(value: Union[str, StrategyName]) -> StrategyName:
    """Parse StrategyName from string."""
    if isinstance(value, StrategyName):
        return value
    if isinstance(value, str):
        mapping = {
            'ORB': StrategyName.ORB,
            'VWAP_MR': StrategyName.VWAP_MR,
            'MOMENTUM_BURST': StrategyName.MOMENTUM_BURST,
            'EMA_CROSSOVER': StrategyName.EMA_CROSSOVER,
            'ORDER_FLOW': StrategyName.ORDER_FLOW,
            'SUPERTREND': StrategyName.SUPERTREND
        }
        return mapping.get(value.upper(), StrategyName.ORB)
    return StrategyName.ORB


def parse_broker_type(value: Union[str, BrokerType]) -> BrokerType:
    """Parse BrokerType from string."""
    if isinstance(value, BrokerType):
        return value
    if isinstance(value, str):
        mapping = {
            'ZERODHA': BrokerType.ZERODHA,
            'ALPACA': BrokerType.ALPACA,
            'INTERACTIVE_BROKERS': BrokerType.INTERACTIVE_BROKERS,
            'PAPER': BrokerType.PAPER
        }
        return mapping.get(value.upper(), BrokerType.PAPER)
    return BrokerType.PAPER


def parse_order_type(value: Union[str, OrderType]) -> OrderType:
    """Parse OrderType from string."""
    if isinstance(value, OrderType):
        return value
    if isinstance(value, str):
        mapping = {
            'MARKET': OrderType.MARKET,
            'LIMIT': OrderType.LIMIT,
            'SL': OrderType.SL,
            'SL-M': OrderType.SL_M,
            'OCO': OrderType.OCO,
            'BRACKET': OrderType.BRACKET
        }
        return mapping.get(value.upper(), OrderType.MARKET)
    return OrderType.MARKET


def parse_order_validity(value: Union[str, OrderValidity]) -> OrderValidity:
    """Parse OrderValidity from string."""
    if isinstance(value, OrderValidity):
        return value
    if isinstance(value, str):
        mapping = {
            'DAY': OrderValidity.DAY,
            'IOC': OrderValidity.IOC,
            'MIS': OrderValidity.MIS,
            'NRML': OrderValidity.NRML,
            'CNS': OrderValidity.CNS
        }
        return mapping.get(value.upper(), OrderValidity.MIS)
    return OrderValidity.MIS


# =============================================================================
# ENVIRONMENT VARIABLE OVERRIDE
# =============================================================================

def get_env_value(key: str, default: Any = None) -> Optional[str]:
    """Get environment variable value."""
    return os.environ.get(key, default)


def apply_env_overrides(config: Dict[str, Any], prefix: str = "RAPIDFIRE") -> Dict[str, Any]:
    """Apply environment variable overrides to configuration.
    
    Variables are ``{PREFIX}_{PATH}`` where path segments are separated by a
    double underscore, so keys may contain single underscores:
    
        RAPIDFIRE_RISK_MANAGEMENT__DAILY__MAX_LOSS_PCT=1.5
        -> config['risk_management']['daily']['max_loss_pct'] = 1.5
    
    Values are converted to the type of the existing setting when there is one.
    """
    result = config.copy()
    
    for key, value in os.environ.items():
        if not key.startswith(prefix + "_"):
            continue
        
        key_path = [part.lower() for part in key[len(prefix) + 1:].split('__') if part]
        if not key_path:
            continue
        
        # Navigate through the config structure
        current = result
        for part in key_path[:-1]:
            if not isinstance(current.get(part), dict):
                current[part] = {}
            current = current[part]
        
        final_key = key_path[-1]
        env_value = value
        
        # Convert based on the existing value type
        if final_key in current:
            existing_value = current[final_key]
            if isinstance(existing_value, bool):
                current[final_key] = parse_bool(env_value)
            elif isinstance(existing_value, float):
                current[final_key] = parse_float(env_value)
            elif isinstance(existing_value, int):
                current[final_key] = parse_int(env_value)
            elif isinstance(existing_value, list):
                current[final_key] = parse_list(env_value)
            else:
                current[final_key] = env_value
        else:
            # Guess the type from the value
            if env_value.lower() in ('true', 'false'):
                current[final_key] = parse_bool(env_value)
            else:
                try:
                    current[final_key] = int(env_value)
                except ValueError:
                    try:
                        current[final_key] = float(env_value)
                    except ValueError:
                        current[final_key] = env_value
    
    return result


# =============================================================================
# CONFIGURATION LOADER
# =============================================================================

class ConfigLoader:
    """Loads and manages configuration for the trading system."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize config loader."""
        self.config_path = config_path or "config.yaml"
        self._config: Optional[Dict[str, Any]] = None
        self._loaded = False
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "ConfigLoader":
        """Create a loader around an already-loaded configuration dictionary."""
        loader = cls()
        loader._config = loader._validate(config)
        loader._loaded = True
        return loader
    
    def load(self) -> Dict[str, Any]:
        """Load configuration from file and apply environment overrides."""
        if self._loaded:
            return self._config or {}
        
        # Load from YAML file
        config = self._load_yaml()
        
        # Apply environment overrides
        config = apply_env_overrides(config)
        
        # Apply sensitive data from environment (for API keys)
        config = self._apply_sensitive_overrides(config)
        
        # Validate configuration
        config = self._validate(config)
        
        self._config = config
        self._loaded = True
        
        return config
    
    def _load_yaml(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = Path(self.config_path)
        
        # Look for config file in multiple locations
        search_paths = [
            config_path,
            Path("config.yaml"),
            Path(__file__).resolve().parent.parent / "config.yaml",
            Path.home() / ".rapidfire" / "config.yaml",
            Path("/etc/rapidfire/config.yaml")
        ]
        
        for path in search_paths:
            if path.exists():
                try:
                    with open(path, 'r') as f:
                        return yaml.safe_load(f) or {}
                except (yaml.YAMLError, OSError) as e:
                    print(f"Warning: Could not load config from {path}: {e}")
        
        # Return empty config if no file found
        return {}
    
    def _apply_sensitive_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply sensitive data overrides from environment."""
        result = config.copy()
        
        # Broker API keys
        broker_config = result.get('broker', {})
        
        # Zerodha
        if 'zerodha' in broker_config:
            zerodha = broker_config['zerodha']
            zerodha['api_key'] = get_env_value('ZERODHA_API_KEY', zerodha.get('api_key'))
            zerodha['api_secret'] = get_env_value('ZERODHA_API_SECRET', zerodha.get('api_secret'))
            zerodha['access_token'] = get_env_value('ZERODHA_ACCESS_TOKEN', zerodha.get('access_token'))
            zerodha['request_token'] = get_env_value('ZERODHA_REQUEST_TOKEN', zerodha.get('request_token'))
            zerodha['user_id'] = get_env_value('ZERODHA_USER_ID', zerodha.get('user_id'))
            zerodha['password'] = get_env_value('ZERODHA_PASSWORD', zerodha.get('password'))
            zerodha['pin'] = get_env_value('ZERODHA_PIN', zerodha.get('pin'))
        
        # Alpaca
        if 'alpaca' in broker_config:
            alpaca = broker_config['alpaca']
            alpaca['api_key'] = get_env_value('ALPACA_API_KEY', alpaca.get('api_key'))
            alpaca['api_secret'] = get_env_value('ALPACA_API_SECRET', alpaca.get('api_secret'))
        
        # Interactive Brokers
        if 'interactive_brokers' in broker_config:
            ib = broker_config['interactive_brokers']
            ib['account_id'] = get_env_value('IB_ACCOUNT_ID', ib.get('account_id'))
        
        # Telegram
        if 'notifications' in result and 'telegram' in result['notifications']:
            telegram = result['notifications']['telegram']
            telegram['bot_token'] = get_env_value('TELEGRAM_BOT_TOKEN', telegram.get('bot_token'))
            telegram['chat_id'] = get_env_value('TELEGRAM_CHAT_ID', telegram.get('chat_id'))
        
        # Email
        if 'notifications' in result and 'email' in result['notifications']:
            email = result['notifications']['email']
            email['smtp_host'] = get_env_value('SMTP_HOST', email.get('smtp_host'))
            email['smtp_port'] = parse_int(get_env_value('SMTP_PORT', str(email.get('smtp_port', 587))))
            email['username'] = get_env_value('SMTP_USERNAME', email.get('username'))
            email['password'] = get_env_value('SMTP_PASSWORD', email.get('password'))
            email['from_email'] = get_env_value('SMTP_FROM_EMAIL', email.get('from_email'))
        
        return result
    
    def _validate(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration and set defaults."""
        result = config.copy()
        
        # Ensure system section exists
        if 'system' not in result:
            result['system'] = {}
        
        system = result['system']
        system.setdefault('name', 'RapidFire_Intraday')
        system.setdefault('version', '1.0.0')
        system.setdefault('environment', 'development')
        system.setdefault('timezone', 'Asia/Kolkata')
        system.setdefault('log_level', 'INFO')
        system.setdefault('data_dir', 'data')
        system.setdefault('candle_db_path', 'data/candles.sqlite')
        system.setdefault('trade_journal_path', 'data/trade_journal.csv')
        system.setdefault('log_file_path', 'logs/trading_system.log')
        
        # Ensure broker section exists
        if 'broker' not in result:
            result['broker'] = {}
        
        broker = result['broker']
        broker.setdefault('active', 'PAPER')
        broker.setdefault('reconnect_attempts', 3)
        broker.setdefault('reconnect_delay_seconds', 5)
        broker.setdefault('api_timeout_seconds', 10)
        broker.setdefault('max_api_retries', 3)
        broker.setdefault('retry_backoff_multiplier', 2.0)
        
        # Ensure strategies section exists
        if 'strategies' not in result:
            result['strategies'] = {}
        
        strategies = result['strategies']
        strategies.setdefault('enabled', True)
        strategies.setdefault('max_strategies_per_symbol', 6)
        
        if 'weights' not in strategies:
            strategies['weights'] = {
                'ORB': 0.15,
                'VWAP_MR': 0.20,
                'MOMENTUM_BURST': 0.20,
                'EMA_CROSSOVER': 0.15,
                'ORDER_FLOW': 0.15,
                'SUPERTREND': 0.15
            }
        
        if 'parameters' not in strategies:
            strategies['parameters'] = {}
        
        # Ensure signal aggregation section exists
        if 'signal_aggregation' not in result:
            result['signal_aggregation'] = {}
        
        signal_agg = result['signal_aggregation']
        signal_agg.setdefault('threshold', 0.65)
        signal_agg.setdefault('min_agreeing_strategies', 2)
        signal_agg.setdefault('use_regime_filter', True)
        
        # Ensure risk management section exists
        if 'risk_management' not in result:
            result['risk_management'] = {}
        
        risk = result['risk_management']
        
        if 'per_trade' not in risk:
            risk['per_trade'] = {}
        risk['per_trade'].setdefault('max_risk_pct', 0.5)
        risk['per_trade'].setdefault('min_risk_pct', 0.1)
        
        if 'limits' not in risk:
            risk['limits'] = {}
        risk['limits'].setdefault('max_concurrent_positions', 5)
        risk['limits'].setdefault('max_positions_per_symbol', 1)
        risk['limits'].setdefault('max_positions_per_strategy', 10)
        risk['limits'].setdefault('max_exposure_pct', 500.0)
        
        if 'daily' not in risk:
            risk['daily'] = {}
        risk['daily'].setdefault('max_loss_pct', 2.0)
        risk['daily'].setdefault('max_profit_pct', 4.0)
        risk['daily'].setdefault('reset_time', '00:00')
        
        if 'stop_loss' not in risk:
            risk['stop_loss'] = {}
        risk['stop_loss'].setdefault('hard_stop_enabled', True)
        risk['stop_loss'].setdefault('hard_stop_pct', 1.0)
        risk['stop_loss'].setdefault('trailing_stop_enabled', True)
        risk['stop_loss'].setdefault('trailing_stop_atr_multiplier', 1.5)
        
        if 'time_stop' not in risk:
            risk['time_stop'] = {}
        risk['time_stop'].setdefault('enabled', True)
        risk['time_stop'].setdefault('square_off_time', '15:15')
        risk['time_stop'].setdefault('pre_square_off_minutes', 5)
        
        if 'protection' not in risk:
            risk['protection'] = {}
        risk['protection'].setdefault('max_spread_pct', 0.15)
        risk['protection'].setdefault('slippage_buffer_pct', 0.05)
        risk['protection'].setdefault('min_volume_for_entry', 1000)
        
        if 'circuit_breakers' not in risk:
            risk['circuit_breakers'] = {}
        risk['circuit_breakers'].setdefault('daily_loss_kill_switch', True)
        risk['circuit_breakers'].setdefault('consecutive_failures_limit', 3)
        risk['circuit_breakers'].setdefault('websocket_disconnect_timeout_seconds', 10)
        risk['circuit_breakers'].setdefault('manual_kill_file', '/tmp/KILL')
        
        # Ensure portfolio section exists
        if 'portfolio' not in result:
            result['portfolio'] = {}
        
        portfolio = result['portfolio']
        portfolio.setdefault('initial_equity', 100000.0)
        portfolio.setdefault('initial_cash', 100000.0)
        portfolio.setdefault('currency', 'INR')
        
        if 'fees' not in portfolio:
            portfolio['fees'] = {}
        
        # Ensure notifications section exists
        if 'notifications' not in result:
            result['notifications'] = {}
        
        notifications = result['notifications']
        notifications.setdefault('enabled', True)
        
        if 'telegram' not in notifications:
            notifications['telegram'] = {}
        notifications['telegram'].setdefault('enabled', False)
        notifications['telegram'].setdefault('notify_on', [])
        
        if 'email' not in notifications:
            notifications['email'] = {}
        notifications['email'].setdefault('enabled', False)
        notifications['email'].setdefault('notify_on', [])
        
        # Ensure backtester section exists
        if 'backtester' not in result:
            result['backtester'] = {}
        
        backtester = result['backtester']
        backtester.setdefault('enabled', True)
        backtester.setdefault('initial_capital', 100000.0)
        
        # Ensure logging section exists
        if 'logging' not in result:
            result['logging'] = {}
        
        logging_config = result['logging']
        logging_config.setdefault('level', 'INFO')
        logging_config.setdefault('file', {})
        logging_config['file'].setdefault('enabled', True)
        logging_config['file'].setdefault('path', 'logs/trading_system.log')
        logging_config['file'].setdefault('max_size_mb', 10)
        logging_config['file'].setdefault('max_backups', 5)
        
        logging_config.setdefault('console', {})
        logging_config['console'].setdefault('enabled', True)
        logging_config['console'].setdefault('level', 'INFO')
        
        logging_config.setdefault('structured', {})
        logging_config['structured'].setdefault('enabled', True)
        logging_config['structured'].setdefault('format', 'json')
        logging_config['structured'].setdefault('include_timestamps', True)
        logging_config['structured'].setdefault('include_level', True)
        logging_config['structured'].setdefault('include_module', True)
        logging_config['structured'].setdefault('include_function', True)
        
        return result
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        if not self._loaded:
            self.load()
        
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return default
        
        return value
    
    def get_risk_parameters(self) -> RiskParameters:
        """Get risk parameters as a dataclass."""
        risk_config = self.get('risk_management', {})
        per_trade_config = risk_config.get('per_trade', {})
        limits_config = risk_config.get('limits', {})
        daily_config = risk_config.get('daily', {})
        stop_loss_config = risk_config.get('stop_loss', {})
        time_stop_config = risk_config.get('time_stop', {})
        protection_config = risk_config.get('protection', {})
        circuit_breakers_config = risk_config.get('circuit_breakers', {})
        
        return RiskParameters(
            max_per_trade_risk_pct=per_trade_config.get('max_risk_pct', 0.5),
            max_concurrent_positions=limits_config.get('max_concurrent_positions', 5),
            max_daily_loss_pct=daily_config.get('max_loss_pct', 2.0),
            max_daily_profit_pct=daily_config.get('max_profit_pct', 4.0),
            trailing_stop_atr_multiplier=stop_loss_config.get('trailing_stop_atr_multiplier', 1.5),
            hard_stop_loss_pct=stop_loss_config.get('hard_stop_pct', 1.0),
            time_stop_time=parse_time(time_stop_config.get('square_off_time', '15:15')),
            max_spread_pct=protection_config.get('max_spread_pct', 0.15),
            slippage_buffer_pct=protection_config.get('slippage_buffer_pct', 0.05),
            cooldown_seconds=circuit_breakers_config.get(
                'cooldown_seconds',
                (self.get('signal_aggregation.cooldown', {}) or {}).get('seconds_per_symbol', 60)),
            max_exposure_pct=float(limits_config.get('max_exposure_pct', 500.0)),
            max_leverage=float((risk_config.get('leverage', {}) or {}).get('max_leverage', 5.0)),
            min_volume_for_entry=float(protection_config.get('min_volume_for_entry', 1000)),
            max_consecutive_failures=int(circuit_breakers_config.get('consecutive_failures_limit', 3)),
            use_trailing_stop=parse_bool(stop_loss_config.get('trailing_stop_enabled', True)),
            trailing_activation_r=float(stop_loss_config.get('trailing_activation_r', 1.0)),
            min_stop_loss_pct=float(stop_loss_config.get('min_stop_pct', 0.5))
        )
    
    def get_strategy_configs(self) -> List[StrategyConfig]:
        """Get strategy configurations as dataclasses."""
        strategies_config = self.get('strategies', {})
        weights = strategies_config.get('weights', {})
        parameters = strategies_config.get('parameters', {})
        
        configs = []
        for name, weight in weights.items():
            try:
                strategy_name = parse_strategy_name(name)
                strategy_params = parameters.get(name, {}) or {}
                enabled = (parse_bool(strategies_config.get('enabled', True)) and
                           parse_bool(strategy_params.get('enabled', True)))
                timeframe = parse_timeframe(strategy_params.get('timeframe', '1m'))
                
                configs.append(StrategyConfig(
                    name=strategy_name,
                    enabled=enabled,
                    weight=float(weight),
                    timeframe=timeframe,
                    parameters=strategy_params
                ))
            except Exception as e:
                print(f"Warning: Could not parse strategy config for {name}: {e}")
        
        return configs
    
    def get_aggregation_config(self) -> AggregationConfig:
        """Get signal aggregation configuration."""
        config = self.get('signal_aggregation', {})
        regime_config = config.get('regime', {})
        
        return AggregationConfig(
            threshold=config.get('threshold', 0.65),
            min_agreeing_strategies=config.get('min_agreeing_strategies', 2),
            use_regime_filter=config.get('use_regime_filter', True),
            regime_volatility_threshold=regime_config.get('volatility_threshold', 0.75)
        )
    
    def get_broker_config(self) -> BrokerConfig:
        """Get broker configuration."""
        broker_config = self.get('broker', {})
        active_broker = broker_config.get('active', 'PAPER')
        broker_type = parse_broker_type(active_broker)
        
        # Get specific broker config
        specific_config = broker_config.get(active_broker.lower(), {})
        
        return BrokerConfig(
            broker_type=broker_type,
            api_key=specific_config.get('api_key', ''),
            api_secret=specific_config.get('api_secret', ''),
            access_token=specific_config.get('access_token'),
            account_id=specific_config.get('account_id'),
            base_url=specific_config.get('base_url'),
            websocket_url=specific_config.get('websocket_url')
        )
    
    def get_symbols(self) -> List[str]:
        """Get list of symbols to trade."""
        symbols = self.get('market_data.websocket.symbols', [])
        if isinstance(symbols, str):
            symbols = parse_list(symbols)
        return symbols
    
    def get_environment(self) -> str:
        """Get the current environment."""
        return self.get('system.environment', 'development')
    
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.get_environment() == 'production'
    
    def is_live(self) -> bool:
        """Check if running in live mode."""
        return self.get('system.environment') == 'live'
    
    def is_paper(self) -> bool:
        """Check if running in paper trading mode."""
        return self.get('broker.active') == 'PAPER' or self.get_environment() == 'paper'


# =============================================================================
# GLOBAL CONFIGURATION ACCESS
# =============================================================================

_config_loader: Optional[ConfigLoader] = None


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration globally."""
    global _config_loader
    _config_loader = ConfigLoader(config_path)
    return _config_loader.load()


def get_config() -> Dict[str, Any]:
    """Get the loaded configuration."""
    global _config_loader
    if _config_loader is None:
        load_config()
    return _config_loader.load()


def get_config_loader() -> ConfigLoader:
    """Get the config loader instance."""
    global _config_loader
    if _config_loader is None:
        load_config()
    return _config_loader
