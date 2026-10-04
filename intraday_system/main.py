"""
Main orchestrator for the Rapid Fire Intraday Trading System.

Wires the live components together and runs them until shutdown:

    DataFeed --ticks/bars--> StrategyEngine --signals--> SignalAggregator
        --aggregated signal--> ExecutionEngine --orders--> OrderManager --> Broker
                                    |
                                    +--> RiskManager / PortfolioTracker

Each component is a process-wide singleton (``get_<component>()``) that
subscribes to the one upstream of it, so this module only has to create them
in dependency order, start them, supervise, and stop them.

Usage:
    python -m intraday_system.main paper  [--duration SECONDS]
    python -m intraday_system.main live   --confirm-live
    python -m intraday_system.main backtest [--synthetic --days 20]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .config_loader import get_config_loader, load_config
from .logger import get_logger, setup_logging


class TradingSystem:
    """Main trading system orchestrator for paper and live trading."""

    def __init__(self, config_path: str = "config.yaml", mode: str = "paper",
                 ignore_market_hours: bool = False):
        self.mode = mode.upper()
        self.config: Dict[str, Any] = load_config(config_path)
        setup_logging(self.config)
        self.logger = get_logger("main")

        broker = self.config.setdefault('broker', {})
        if self.mode == "PAPER":
            # Paper trading always uses the simulated broker and mock/live feed
            broker['active'] = 'PAPER'
        elif str(broker.get('active', 'PAPER')).upper() == 'PAPER':
            raise ValueError("Live mode requires broker.active to be a real broker (e.g. ZERODHA)")

        if ignore_market_hours:
            if self.mode != "PAPER":
                raise ValueError("--ignore-market-hours is only allowed in paper mode")
            time_stop = self.config.setdefault('risk_management', {}).setdefault('time_stop', {})
            time_stop['enforce_market_hours'] = False
        
        self.components: Dict[str, Any] = {}
        self.notifier = None
        self.start_time: Optional[datetime] = None
        self._shutdown_event: Optional[asyncio.Event] = None

        self.logger.info(f"TradingSystem initialized in {self.mode} mode "
                         f"with broker {broker.get('active')}")

    def setup(self) -> None:
        """Create all components in dependency order."""
        # Imported lazily so that importing this module stays cheap
        from .data_feed import get_data_feed
        from .execution_engine import get_execution_engine
        from .notifier import get_notifier
        from .order_manager import get_order_manager
        from .portfolio_tracker import get_portfolio_tracker
        from .risk_manager import get_risk_manager
        from .signal_aggregator import get_signal_aggregator
        from .strategy_engine import get_strategy_engine
        
        self.components = {
            'data_feed': get_data_feed(),
            'portfolio_tracker': get_portfolio_tracker(),
            'risk_manager': get_risk_manager(),
            'order_manager': get_order_manager(),
            'strategy_engine': get_strategy_engine(),
            'signal_aggregator': get_signal_aggregator(),
            'execution_engine': get_execution_engine(),
        }
        
        notifier = get_notifier(self.config)
        notifier.report_provider = self._daily_report
        self.components['execution_engine'].subscribe(notifier.on_execution_event)
        self.components['risk_manager'].subscribe(notifier.on_risk_event)
        self.notifier = notifier
        self.logger.info("All components created")
    
    async def _start_component(self, name: str) -> None:
        component = self.components[name]
        result = component.start()
        if asyncio.iscoroutine(result):
            await result
        self.logger.info(f"Started {name}")
    
    async def _stop_component(self, name: str) -> None:
        component = self.components.get(name)
        if component is None:
            return
        try:
            result = component.stop()
            if asyncio.iscoroutine(result):
                await result
            self.logger.info(f"Stopped {name}")
        except Exception as e:
            self.logger.error(f"Error stopping {name}: {e}")
    
    async def start(self, duration: Optional[float] = None) -> None:
        """Start everything and run until stopped (or for `duration` seconds)."""
        self._shutdown_event = asyncio.Event()
        self.start_time = datetime.now()
        self.setup()
        self.notifier.start()
        
        # Consumers first, so no data/signals are dropped, then the data feed
        start_order = ['portfolio_tracker', 'risk_manager', 'order_manager',
                       'execution_engine', 'signal_aggregator', 'strategy_engine', 'data_feed']
        for name in start_order:
            await self._start_component(name)
        
        self.logger.info("Trading system running")
        self.notifier.notify("SYSTEM_START", f"RapidFire started ({self.mode})",
                             f"Broker {self.config['broker'].get('active')}, "
                             f"{len(self.components['data_feed'].symbols)} symbols")
        health_task = asyncio.create_task(self._monitor_health())
        try:
            if duration:
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=duration)
                except asyncio.TimeoutError:
                    self.logger.info(f"Run duration of {duration}s reached")
            else:
                await self._shutdown_event.wait()
        finally:
            health_task.cancel()
            await asyncio.gather(health_task, return_exceptions=True)
            for name in reversed(start_order):
                await self._stop_component(name)
            self._write_status()
            final = self.status()
            self.notifier.notify("SYSTEM_STOP", f"RapidFire stopped ({self.mode})",
                                 f"Equity {final.get('equity')}, trades {final.get('closed_trades')}")
            self.notifier.stop()
            self.logger.info("Trading system stopped gracefully")
    
    def stop(self) -> None:
        """Request shutdown (safe to call from a signal handler)."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()
    
    def status(self) -> Dict[str, Any]:
        """Compact snapshot of the system for logging."""
        data_feed = self.components.get('data_feed')
        portfolio = self.components.get('portfolio_tracker')
        risk = self.components.get('risk_manager')
        
        status: Dict[str, Any] = {
            'mode': self.mode,
            'uptime_seconds': round((datetime.now() - self.start_time).total_seconds()) if self.start_time else 0,
        }
        if data_feed is not None:
            status['feed_connected'] = data_feed.ws_connected
            status['symbols_with_ticks'] = len(data_feed.cache.latest_ticks)
        if portfolio is not None:
            status['equity'] = round(portfolio.get_current_equity(), 2)
            status['open_positions'] = portfolio.get_num_open_positions()
            status['closed_trades'] = len(portfolio.closed_trades)
        if risk is not None:
            state = risk.get_risk_state()
            status['daily_pnl'] = round(state.daily_pnl, 2)
            status['kill_switch'] = state.kill_switch_activated
        return status
    
    def snapshot(self) -> Dict[str, Any]:
        """Detailed state written to the status file for the dashboard."""
        snap = self.status()
        snap['timestamp'] = datetime.now().isoformat()
        portfolio = self.components.get('portfolio_tracker')
        engine = self.components.get('execution_engine')
        om = self.components.get('order_manager')
        risk = self.components.get('risk_manager')
        feed = self.components.get('data_feed')
        if portfolio is not None:
            snap['initial_capital'] = portfolio.initial_capital
            snap['realized_pnl'] = round(portfolio.get_realized_pnl(), 2)
            snap['unrealized_pnl'] = round(portfolio.get_unrealized_pnl(), 2)
            snap['drawdown_pct'] = round(portfolio.get_drawdown(), 3)
            snap['equity_history'] = portfolio.get_equity_history()[-500:]
        if engine is not None:
            prices = feed.get_current_prices() if feed is not None else {}
            snap['positions'] = [
                {'symbol': mp.symbol, 'state': mp.state, 'side': mp.side.value, 'quantity': mp.quantity,
                 'entry_price': round(mp.entry_price, 2), 'stop_loss': mp.stop_loss,
                 'take_profit': mp.take_profit, 'last_price': prices.get(mp.symbol),
                 'strategies': mp.contributors}
                for mp in engine.get_active_trades()]
        if om is not None:
            snap['open_orders'] = [o.to_dict() for o in om.get_pending_orders()]
            snap['order_stats'] = om.get_order_stats()
        if risk is not None:
            snap['risk'] = risk.get_risk_state().to_dict()
        snap['notifications'] = [n.to_dict() for n in list(self.notifier.history)[-20:]]
        return snap
    
    def _write_status(self) -> None:
        path = Path(self.config.get('system', {}).get('status_file', 'data/status.json'))
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix('.tmp')
            tmp.write_text(json.dumps(self.snapshot(), default=str))
            tmp.replace(path)  # Atomic: the dashboard never reads a partial file
        except Exception as e:
            self.logger.error(f"Could not write status file: {e}")
    
    def _daily_report(self) -> str:
        snap = self.status()
        portfolio = self.components.get('portfolio_tracker')
        lines = [f"RapidFire daily report {datetime.now():%Y-%m-%d}", ""]
        lines += [f"{k}: {v}" for k, v in snap.items()]
        if portfolio is not None:
            today = datetime.now().date()
            trades = [t for t in portfolio.get_trade_history() if t.exit_timestamp.date() == today]
            lines += ["", f"Trades today: {len(trades)}"]
            lines += [f"  {t.symbol} {t.side.value} {t.quantity:g} {t.entry_price:.2f}->{t.exit_price:.2f} "
                      f"net {t.net_pnl:.2f} [{t.tags[0] if t.tags else ''}]" for t in trades]
        return "\n".join(lines)
    
    async def _monitor_health(self, status_interval: float = 5.0, log_interval: float = 60.0) -> None:
        last_log = 0.0
        while True:
            await asyncio.sleep(status_interval)
            try:
                self._write_status()
                now = datetime.now().timestamp()
                if now - last_log >= log_interval:
                    self.logger.info(f"Status: {self.status()}")
                    last_log = now
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")


# =============================================================================
# ENTRY POINTS
# =============================================================================

def _install_signal_handlers(system: TradingSystem) -> None:
    loop = asyncio.get_running_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, signame, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, system.stop)
        except (NotImplementedError, RuntimeError):
            # Windows: fall back to KeyboardInterrupt handling in main()
            pass


async def run_trading(mode: str, config_path: str, duration: Optional[float],
                      ignore_market_hours: bool = False) -> None:
    system = TradingSystem(config_path, mode=mode, ignore_market_hours=ignore_market_hours)
    _install_signal_handlers(system)
    await system.start(duration=duration)
    print(f"Final status: {system.status()}")


async def run_backtest(args: argparse.Namespace) -> None:
    from .backtester import Backtester

    config = load_config(args.config)
    config.setdefault('logging', {}).setdefault('console', {})['level'] = 'WARNING'
    setup_logging(config)
    backtester = Backtester(config, start_date=args.start, end_date=args.end,
                            symbols=args.symbols, synthetic=args.synthetic or None,
                            synthetic_days=args.days)
    await backtester.setup()
    if not await backtester.load_historical_data():
        print(f"No historical data in '{backtester.data_path}'. Add <SYMBOL>.csv files or use --synthetic.")
        return
    await backtester.run()
    print(backtester.format_summary())


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Rapid Fire Intraday Trading System")
    parser.add_argument("mode", choices=["backtest", "paper", "live"], help="Run mode")
    parser.add_argument("--config", default="config.yaml", help="Path to configuration file")
    parser.add_argument("--duration", type=float, default=None,
                        help="Stop automatically after N seconds (paper/live)")
    parser.add_argument("--confirm-live", action="store_true",
                        help="Required to start live trading with real money")
    parser.add_argument("--ignore-market-hours", action="store_true",
                        help="Paper mode only: trade outside NSE hours (e.g. on mock data)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    # Backtest options
    parser.add_argument("--symbols", nargs="+", help="Backtest symbols")
    parser.add_argument("--start", help="Backtest start date YYYY-MM-DD")
    parser.add_argument("--end", help="Backtest end date YYYY-MM-DD")
    parser.add_argument("--synthetic", action="store_true", help="Backtest on synthetic data")
    parser.add_argument("--days", type=int, default=20, help="Synthetic trading days")
    args = parser.parse_args(argv)

    if args.debug:
        from .logger import set_log_level
        from .models import LogLevel
        set_log_level(LogLevel.DEBUG)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        if args.mode == "backtest":
            if args.synthetic and not args.start:
                args.start, args.end = "2024-01-01", args.end or "2100-01-01"
            asyncio.run(run_backtest(args))
        elif args.mode == "paper":
            asyncio.run(run_trading("paper", args.config, args.duration, args.ignore_market_hours))
        else:
            if not args.confirm_live:
                print("Refusing to start LIVE trading without --confirm-live (real money at risk).")
                return 2
            asyncio.run(run_trading("live", args.config, args.duration))
    except KeyboardInterrupt:
        print("Interrupted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
