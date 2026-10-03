#!/usr/bin/env python3
"""
Backtest runner for the Rapid Fire Intraday Trading System.

Examples:
    # Backtest on CSV files in data/historical (<SYMBOL>.csv, 1-minute bars)
    python run_backtest.py --data-dir data/historical --symbols RELIANCE TCS

    # Quick demo on reproducible synthetic data
    python run_backtest.py --synthetic --days 20 --symbols RELIANCE TCS INFY
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from intraday_system.backtester import Backtester
from intraday_system.config_loader import load_config
from intraday_system.logger import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Rapid Fire intraday backtest")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--symbols", nargs="+", help="Symbols to test (default: from config)")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: backtester.start_date)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: backtester.end_date)")
    parser.add_argument("--data-dir", help="Directory with <SYMBOL>.csv 1-minute bars")
    parser.add_argument("--synthetic", action="store_true",
                        help="Generate synthetic data for symbols without a CSV file")
    parser.add_argument("--days", type=int, default=20, help="Trading days of synthetic data")
    parser.add_argument("--capital", type=float, help="Initial capital override")
    parser.add_argument("--results-dir", default=None, help="Output directory (default: results)")
    parser.add_argument("--no-save", action="store_true", help="Do not write result files")
    return parser.parse_args()


async def main() -> int:
    args = parse_args()

    config = load_config(args.config)
    # Keep the console for the summary; details still go to the log file
    config.setdefault('logging', {}).setdefault('console', {})['level'] = 'WARNING'
    setup_logging(config)
    if args.data_dir:
        config.setdefault('backtester', {}).setdefault('data', {})['data_dir'] = args.data_dir
    if args.capital:
        config.setdefault('backtester', {})['initial_capital'] = args.capital

    # With synthetic data and no explicit start date, don't filter by the
    # configured calendar-year window
    start, end = args.start, args.end
    if args.synthetic and not args.start:
        start = "2024-01-01"
        end = end or "2100-01-01"

    backtester = Backtester(
        config,
        start_date=start,
        end_date=end,
        symbols=args.symbols,
        synthetic=args.synthetic or None,
        synthetic_days=args.days,
        results_dir=args.results_dir,
    )
    await backtester.setup()

    if not await backtester.load_historical_data():
        print(f"[ERROR] No historical data found in '{backtester.data_path}'.")
        print("        Provide <SYMBOL>.csv files with columns "
              "timestamp,open,high,low,close,volume or use --synthetic.")
        return 1

    await backtester.run(save=not args.no_save)
    print(backtester.format_summary())

    if backtester.trades:
        print("\nLast trades:")
        for t in backtester.trades[-5:]:
            print(f"  {t.entry_timestamp:%Y-%m-%d %H:%M} {t.symbol:<10} {t.side.value:<4} "
                  f"{t.entry_price:>10.2f} -> {t.exit_price:>10.2f}  qty {t.quantity:<6} "
                  f"net {t.net_pnl:>10.2f}  [{t.tags[0]}]")
    if not args.no_save:
        print(f"\nResults written to {backtester.results_dir}/")
    return 0


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(main()))
