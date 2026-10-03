"""
Broker Factory for the Rapid Fire Intraday Trading System.

This module provides a factory for creating broker instances based on 
configuration. It supports dynamic loading of broker implementations and
manages the lifecycle of broker connections.
"""

from typing import Any, Dict, Optional, Type
import importlib

from intraday_system.models import BrokerType
from .base_broker import BaseBroker, BrokerError, BrokerType as BaseBrokerType


# Mapping of broker type strings to implementation modules
BROKER_IMPLEMENTATIONS: Dict[str, str] = {
    'zerodha': 'intraday_system.brokers.zerodha_broker.ZerodhaBroker',
    'alpaca': 'intraday_system.brokers.alpaca_broker.AlpacaBroker',
    'interactive_brokers': 'intraday_system.brokers.ib_broker.InteractiveBrokersBroker',
    'paper': 'intraday_system.brokers.paper_broker.PaperBroker',
    'simulation': 'intraday_system.brokers.simulation_broker.SimulationBroker',
}

# Cache for broker instances
_broker_instances: Dict[str, BaseBroker] = {}


def get_broker(broker_type: str, config: Dict[str, Any]) -> BaseBroker:
    """
    Create and return a broker instance of the specified type.
    
    Args:
        broker_type: String identifier for the broker type
        config: Configuration dictionary for the broker
        
    Returns:
        BaseBroker instance
        
    Raises:
        BrokerError: If broker type is not supported or instantiation fails
    """
    global _broker_instances
    
    # Check if we already have an instance for this config
    # Convert config to a hashable representation
    config_str = str(sorted(config.items())) if config else ""
    cache_key = f"{broker_type}:{hash(config_str)}"
    if cache_key in _broker_instances:
        return _broker_instances[cache_key]
    
    # Convert string to lowercase for enum matching
    broker_type_lower = broker_type.lower()
    
    # Convert string to BrokerType enum
    try:
        broker_type_enum = BaseBrokerType(broker_type_lower)
    except ValueError:
        raise BrokerError(f"Unsupported broker type: {broker_type}")
    
    # Get implementation class
    module_path = BROKER_IMPLEMENTATIONS.get(broker_type_lower)
    if not module_path:
        raise BrokerError(f"No implementation found for broker type: {broker_type}")
    
    try:
        # Dynamically import the module
        module_name, class_name = module_path.rsplit('.', 1)
        module = importlib.import_module(module_name)
        broker_class = getattr(module, class_name)
        
        # Create instance
        broker = broker_class(broker_type_enum, config)
        
        # Cache the instance
        _broker_instances[cache_key] = broker
        
        return broker
        
    except ImportError as e:
        raise BrokerError(f"Failed to import broker module {module_path}: {e}")
    except AttributeError as e:
        raise BrokerError(f"Broker class {class_name} not found in {module_name}: {e}")
    except Exception as e:
        raise BrokerError(f"Failed to create broker instance: {e}")


def get_broker_by_enum(broker_type: BaseBrokerType, config: Dict[str, Any]) -> BaseBroker:
    """
    Create and return a broker instance using BrokerType enum.
    
    Args:
        broker_type: BrokerType enum
        config: Configuration dictionary for the broker
        
    Returns:
        BaseBroker instance
    """
    return get_broker(broker_type.value, config)


def list_available_brokers() -> list:
    """Return list of available broker types."""
    return list(BROKER_IMPLEMENTATIONS.keys())


def destroy_broker(broker: BaseBroker) -> None:
    """
    Destroy a broker instance and remove from cache.
    
    Args:
        broker: The broker instance to destroy
    """
    global _broker_instances
    
    # Find and remove from cache
    keys_to_remove = []
    for key, cached_broker in _broker_instances.items():
        if cached_broker is broker:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del _broker_instances[key]


def clear_broker_cache() -> None:
    """Clear all cached broker instances."""
    global _broker_instances
    _broker_instances.clear()


def reconfigure_broker(broker: BaseBroker, new_config: Dict[str, Any]) -> bool:
    """
    Reconfigure an existing broker with new configuration.
    
    Args:
        broker: The broker instance to reconfigure
        new_config: New configuration dictionary
        
    Returns:
        True if reconfiguration successful, False otherwise
    """
    try:
        # Disconnect current broker
        import asyncio
        asyncio.run_coroutine_threadsafe(broker.disconnect(), asyncio.get_event_loop())
        
        # Update configuration
        broker.config.update(new_config)
        
        # Reconnect with new config
        asyncio.run_coroutine_threadsafe(broker.connect(), asyncio.get_event_loop())
        
        return True
    except Exception:
        return False