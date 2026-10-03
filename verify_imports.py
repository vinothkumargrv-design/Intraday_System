#!/usr/bin/env python3
"""
Verify that all imports work correctly for the Rapid Fire Intraday Trading System.
"""

import sys
import os
import traceback

# Add the intraday_system directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'intraday_system'))

def test_import(module_name, description):
    """Test importing a module."""
    try:
        print(f"Testing {description}...", end=" ")
        __import__(f"intraday_system.{module_name}")
        print("[OK]")
        return True
    except Exception as e:
        print("[FAILED]")
        print(f"  Error: {e}")
        traceback.print_exc()
        return False

def main():
    """Main verification function."""
    print("=" * 60)
    print("RAPID FIRE INTRADAY TRADING SYSTEM - IMPORT VERIFICATION")
    print("=" * 60)
    
    modules_to_test = [
        ("models", "Core data models"),
        ("config_loader", "Configuration loader"),
        ("logger", "Logging system"),
        ("data_feed", "Data feed system"),
        ("strategy_engine", "Strategy engine"),
        ("signal_aggregator", "Signal aggregator"),
        ("risk_manager", "Risk manager"),
        ("order_manager", "Order manager"),
        ("execution_engine", "Execution engine"),
        ("portfolio_tracker", "Portfolio tracker"),
        ("backtester", "Backtester"),
        ("notifier", "Notification system"),
        ("dashboard", "Streamlit dashboard"),
    ]
    
    passed = 0
    failed = 0
    
    for module_name, description in modules_to_test:
        if test_import(module_name, description):
            passed += 1
        else:
            failed += 1
    
    print("=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("[SUCCESS] All imports successful!")
        return 0
    else:
        print("[ERROR] Some imports failed. Check errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())