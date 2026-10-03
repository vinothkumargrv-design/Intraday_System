#!/usr/bin/env python3
"""
Simple backtest runner that avoids circular import issues.
"""

import sys
import os
import asyncio

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

# Set up the intraday_system path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'intraday_system'))

async def main():
    """Run a simple backtest demonstration."""
    print("Starting Rapid Fire Intraday Trading System Backtest...")
    print("=" * 60)
    
    try:
        # Try to import core modules
        print("Importing core modules...")
        from intraday_system.models import (
            SignalType, OrderType, OrderSide, StrategyName, 
            Position, Trade, OHLCV, Tick
        )
        print("[OK] Models imported successfully")
        
        from intraday_system.logger import get_logger
        print("[OK] Logger imported successfully")
        
        from intraday_system.config_loader import ConfigLoader
        print("[OK] Config loader imported successfully")
        
        # Load configuration
        print("\nLoading configuration...")
        config_loader = ConfigLoader("config.yaml")
        config = config_loader.load()
        print("[OK] Configuration loaded")
        print(f"  Trading: {config.get('trading', {}).get('paper_trading', True)}")
        print(f"  Symbols: {config.get('trading', {}).get('symbols', [])}")
        print(f"  Initial Capital: {config.get('trading', {}).get('initial_capital', 100000)}")
        
        # Try to import backtester
        print("\nImporting backtester...")
        from intraday_system.backtester import Backtester
        print("[OK] Backtester imported successfully")
        
        # Create and run backtester
        print("\nCreating backtester...")
        backtester = Backtester(config)
        print("[OK] Backtester created")
        
        print("\nSetting up backtester...")
        await backtester.setup()
        print("[OK] Backtester setup complete")
        
        print("\nLoading historical data...")
        data_loaded = await backtester.load_historical_data()
        if data_loaded:
            print("[OK] Historical data loaded")
            
            print("\nRunning backtest...")
            await backtester.run()
            print("[OK] Backtest completed")
            
            # Print results
            print("\n" + "=" * 60)
            print("BACKTEST RESULTS")
            print("=" * 60)
            
            results = backtester.get_results()
            if results:
                trades = results.get('trades', [])
                metrics = results.get('performance_metrics', {})
                
                print(f"Total Trades: {len(trades)}")
                print(f"Total PnL: {metrics.get('total_net_pnl', 0):.2f}")
                print(f"Win Rate: {metrics.get('win_rate', 0):.2%}")
                print(f"Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}")
                print(f"Max Drawdown: {metrics.get('max_drawdown', 0):.2f}%")
                
                if trades:
                    print(f"\nRecent Trades:")
                    for trade in trades[-5:]:  # Show last 5 trades
                        print(f"  {trade.get('symbol')}: {trade.get('side')} @ {trade.get('entry_price')} -> {trade.get('exit_price')} | PnL: {trade.get('net_pnl', 0):.2f}")
        else:
            print("[WARNING] No historical data found. Cannot run backtest.")
            print("  Make sure you have historical data in the 'data/historical' directory.")
            
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n" + "=" * 60)
    print("Backtest completed successfully!")
    return 0

if __name__ == "__main__":
    # For Windows asyncio compatibility
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    exit_code = asyncio.run(main())
    sys.exit(exit_code)