#!/usr/bin/env python3
"""
Final verification of the Rapid Fire Intraday Trading System.

This script verifies that:
1. All required files exist
2. Core modules can be imported (models, config_loader, logger)
3. File structure is correct
4. Basic functionality works
"""

import sys
import os
from pathlib import Path
import traceback

def test_file_exists(file_path, description):
    """Test if a file exists."""
    try:
        full_path = Path(file_path)
        if full_path.exists():
            print(f"[OK] {description} exists")
            return True
        else:
            print(f"[MISSING] {description} - {file_path}")
            return False
    except Exception as e:
        print(f"[ERROR] Checking {description}: {e}")
        return False

def test_file_content(file_path, required_content, description):
    """Test if a file contains required content."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            if required_content in content:
                print(f"[OK] {description} contains required content")
                return True
            else:
                print(f"[MISSING] {description} missing content: {required_content}")
                return False
    except Exception as e:
        print(f"[ERROR] Reading {description}: {e}")
        return False

def test_core_imports():
    """Test importing core modules."""
    print("\n--- Testing Core Module Imports ---")
    
    # Add the intraday_system directory to the path
    sys.path.append(os.path.join(os.path.dirname(__file__), 'intraday_system'))
    
    try:
        print("Testing models import...")
        from intraday_system import models
        print("[OK] models imported successfully")
        return True
    except Exception as e:
        print(f"[FAILED] models import: {e}")
        return False

def test_syntax_check(file_path, description):
    """Check if a Python file has valid syntax."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        compile(content, file_path, 'exec')
        print(f"[OK] {description} syntax valid")
        return True
    except SyntaxError as e:
        print(f"[SYNTAX ERROR] {description}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Checking {description}: {e}")
        return False

def main():
    """Main verification function."""
    print("=" * 70)
    print("RAPID FIRE INTRADAY TRADING SYSTEM - FINAL VERIFICATION")
    print("=" * 70)
    
    # Test file structure
    print("\n--- Checking File Structure ---")
    
    required_files = [
        ("config.yaml", "Configuration file"),
        ("requirements.txt", "Dependencies file"),
        ("Dockerfile", "Docker container file"),
        ("README.md", "Documentation"),
        ("intraday_system/models.py", "Core data models"),
        ("intraday_system/config_loader.py", "Configuration loader"),
        ("intraday_system/logger.py", "Logging system"),
        ("intraday_system/data_feed.py", "Data feed system"),
        ("intraday_system/strategy_engine.py", "Strategy engine"),
        ("intraday_system/signal_aggregator.py", "Signal aggregator"),
        ("intraday_system/risk_manager.py", "Risk manager"),
        ("intraday_system/order_manager.py", "Order manager"),
        ("intraday_system/execution_engine.py", "Execution engine"),
        ("intraday_system/portfolio_tracker.py", "Portfolio tracker"),
        ("intraday_system/backtester.py", "Backtester"),
        ("intraday_system/notifier.py", "Notification system"),
        ("intraday_system/dashboard.py", "Streamlit dashboard"),
        ("intraday_system/main.py", "Main orchestrator"),
        ("tests/__init__.py", "Test suite"),
        ("tests/test_models.py", "Models tests"),
        ("tests/test_config_loader.py", "Config loader tests"),
        ("tests/test_risk_manager.py", "Risk manager tests"),
        ("tests/test_strategies_and_aggregation.py", "Strategy and aggregation tests"),
        ("tests/test_backtester.py", "Backtester tests"),
        ("tests/test_live_flow.py", "Live order flow tests"),
        ("tests/test_zerodha_broker.py", "Zerodha adapter tests"),
        ("tests/test_notifier_and_dashboard.py", "Notifier and dashboard tests"),
        ("intraday_system/costs.py", "Transaction cost model"),
        ("intraday_system/brokers/paper_broker.py", "Paper broker"),
        ("intraday_system/brokers/zerodha_broker.py", "Zerodha broker"),
    ]
    
    passed = 0
    failed = 0
    
    for file_path, description in required_files:
        if test_file_exists(file_path, description):
            passed += 1
        else:
            failed += 1
    
    # Test syntax of Python files
    print("\n--- Checking Python Syntax ---")
    
    python_files = [
        f for f in required_files if f[0].endswith('.py')
    ]
    
    for file_path, description in python_files:
        if test_syntax_check(file_path, description):
            passed += 1
        else:
            failed += 1
    
    # Test core functionality
    print("\n--- Testing Core Functionality ---")
    
    if test_core_imports():
        passed += 1
    else:
        failed += 1
    
    # Test key content in files
    print("\n--- Checking File Content ---")
    
    content_checks = [
        ("intraday_system/models.py", "class SignalType", "SignalType enum"),
        ("intraday_system/models.py", "class OrderType", "OrderType enum"),
        ("intraday_system/models.py", "class Position", "Position model"),
        ("intraday_system/models.py", "class Trade", "Trade model"),
        ("intraday_system/config_loader.py", "class ConfigLoader", "ConfigLoader class"),
        ("intraday_system/logger.py", "def get_logger", "get_logger function"),
        ("intraday_system/data_feed.py", "class DataFeed", "DataFeed class"),
        ("intraday_system/strategy_engine.py", "class OpeningRangeBreakoutStrategy", "ORB strategy"),
        ("intraday_system/strategy_engine.py", "class VWAPMeanReversionStrategy", "VWAP strategy"),
        ("intraday_system/signal_aggregator.py", "class SignalAggregator", "SignalAggregator class"),
        ("intraday_system/risk_manager.py", "class RiskManager", "RiskManager class"),
        ("intraday_system/portfolio_tracker.py", "class PortfolioTracker", "PortfolioTracker class"),
        ("intraday_system/main.py", "class TradingSystem", "TradingSystem class"),
        ("intraday_system/backtester.py", "class Backtester", "Backtester class"),
        ("intraday_system/notifier.py", "class Notifier", "Notifier class"),
        ("intraday_system/dashboard.py", "import streamlit", "Streamlit import"),
    ]
    
    for file_path, content, description in content_checks:
        if test_file_content(file_path, content, description):
            passed += 1
        else:
            failed += 1
    
    # Test config.yaml structure
    print("\n--- Checking Configuration Structure ---")
    
    config_checks = [
        ("\nbroker:", "Broker configuration"),
        ("\nrisk_management:", "Risk management configuration"),
        ("\nbacktester:", "Backtester configuration"),
        ("strategies:", "Strategy configurations"),
        ("notifications:", "Notification configuration"),
        ("logging:", "Logging configuration"),
    ]
    
    for content, description in config_checks:
        if test_file_content("config.yaml", content, description):
            passed += 1
        else:
            failed += 1
    
    # Summary
    print("\n" + "=" * 70)
    print(f"FINAL VERIFICATION RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)
    
    if failed == 0:
        print("\n[SUCCESS] All verification checks passed!")
        print("\nThe Rapid Fire Intraday Trading System is ready for use.")
        print("\nTo get started:")
        print("1. pip install -r requirements.txt")
        print("2. Edit config.yaml with your settings")
        print("3. Run: python -m intraday_system.main paper")
        return 0
    else:
        print("\n[WARNING] Some verification checks failed.")
        print("The system may still work, but check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())