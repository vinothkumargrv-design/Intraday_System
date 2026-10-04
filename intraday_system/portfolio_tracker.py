"""
Portfolio Tracker for the Rapid Fire Intraday Trading System.

The tracker is the system's ledger:
- The execution engine reports positions it opens and trades it closes
  (``on_position_opened`` / ``on_trade_closed``).
- Open positions are marked to market from the data feed's last prices.
- Equity = initial capital + realized net PnL + unrealized PnL.
- Closed trades are appended to a CSV trade journal (one row per trade).
- Performance metrics, drawdown and an equity history are maintained.
"""

from __future__ import annotations

import csv
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .logger import get_logger
from .models import (
    OrderSide, PerformanceMetrics, PortfolioState, Position, PositionSide,
    StrategyName, Trade, TradeStatus,
)

Config = Dict[str, Any]

JOURNAL_HEADERS = [
    "trade_id", "symbol", "side", "entry_price", "exit_price", "quantity",
    "entry_timestamp", "exit_timestamp", "stop_loss", "take_profit",
    "strategy", "confidence", "pnl", "net_pnl", "brokerage",
    "exchange_fees", "stt", "gst", "stamp_duty", "slippage",
    "status", "duration_seconds", "return_pct", "tags",
]


class PortfolioTracker:
    """Tracks positions, realized/unrealized PnL, equity and performance."""

    def __init__(self, config: Optional[Config] = None, trade_queue: Any = None,
                 price_provider: Optional[Callable[[str], Optional[float]]] = None):
        self.logger = get_logger("portfolio_tracker")
        self.config = config or {}
        self.trade_queue = trade_queue

        portfolio_cfg = self.config.get('portfolio', {}) or {}
        self.initial_capital = float(portfolio_cfg.get('initial_equity',
                                                       portfolio_cfg.get('initial_cash', 100000.0)))
        self.trade_journal_path = Path(
            (self.config.get('system', {}) or {}).get('trade_journal_path', 'data/trade_journal.csv'))
        self.load_history = bool(portfolio_cfg.get('load_trade_history', False))

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Trade] = []
        self.open_trades: Dict[str, Trade] = {}
        self.realized_pnl = 0.0
        self.peak_equity = self.initial_capital
        self.max_drawdown = 0.0
        self.equity_history: List[Dict[str, Any]] = []
        self.equity_history_max_len = 10000
        self.performance_metrics = PerformanceMetrics()

        self._prices: Dict[str, float] = {}
        self._price_provider = price_provider
        self._lock = threading.RLock()
        self._subscribed = False

        if self.load_history:
            self._load_trade_history()
        self._record_equity_snapshot()
        self.logger.info(f"Portfolio tracker initialized with capital: {self.initial_capital}")

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> None:
        """Subscribe to tick prices for mark-to-market."""
        if self._subscribed:
            return
        try:
            from .data_feed import get_data_feed
            get_data_feed().subscribe(self._on_tick, data_type="tick")
            self._subscribed = True
        except Exception as e:
            self.logger.error(f"Could not subscribe to data feed: {e}")
        self.logger.info("Portfolio tracker started")

    def stop(self) -> None:
        if self._subscribed:
            try:
                from .data_feed import get_data_feed
                get_data_feed().unsubscribe(self._on_tick)
            except Exception:
                pass
            self._subscribed = False
        self.logger.info(f"Portfolio tracker stopped. Equity {self.get_current_equity():.2f}, "
                         f"{len(self.closed_trades)} closed trades")

    def _on_tick(self, symbol: str, tick: Any) -> None:
        self.update_price(symbol, tick.last_price)

    # =========================================================================
    # EVENTS
    # =========================================================================

    def update_price(self, symbol: str, price: float) -> None:
        with self._lock:
            self._prices[symbol] = price
            if symbol in self.positions:
                self._update_drawdown()

    def on_position_opened(self, position: Position) -> None:
        with self._lock:
            self.positions[position.symbol] = position
            self._record_equity_snapshot()
        self.logger.info(f"Position opened: {position.side.value} {position.quantity:g} "
                         f"{position.symbol} @ {position.entry_price:.2f}")

    def on_trade_closed(self, trade: Trade) -> None:
        with self._lock:
            self.positions.pop(trade.symbol, None)
            self.open_trades.pop(trade.trade_id, None)
            self.closed_trades.append(trade)
            self.realized_pnl += trade.net_pnl
            self._update_drawdown()
            self._update_performance_metrics()
            self._record_equity_snapshot()
        self._append_to_journal(trade)
        self.logger.info(f"Trade closed: {trade.trade_id} {trade.symbol} net PnL {trade.net_pnl:.2f} "
                         f"({trade.return_pct:.2f}%)")

    # =========================================================================
    # VALUATION
    # =========================================================================

    def _price(self, symbol: str, fallback: float) -> float:
        price = self._prices.get(symbol)
        if price is None and self._price_provider is not None:
            try:
                price = self._price_provider(symbol)
            except Exception:
                price = None
        return price if price is not None else fallback

    def get_unrealized_pnl(self) -> float:
        with self._lock:
            return sum(p.pnl(self._price(s, p.entry_price)) for s, p in self.positions.items())

    def get_realized_pnl(self) -> float:
        with self._lock:
            return self.realized_pnl

    def get_current_equity(self) -> float:
        with self._lock:
            return self.initial_capital + self.realized_pnl + self.get_unrealized_pnl()

    def get_available_cash(self) -> float:
        with self._lock:
            return self.initial_capital + self.realized_pnl

    def get_gross_exposure(self) -> float:
        with self._lock:
            return sum(abs(p.quantity) * self._price(s, p.entry_price) for s, p in self.positions.items())

    def _update_drawdown(self) -> None:
        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        if self.peak_equity > 0:
            self.max_drawdown = max(self.max_drawdown, (self.peak_equity - equity) / self.peak_equity * 100)

    def get_drawdown(self) -> float:
        with self._lock:
            equity = self.get_current_equity()
            return (self.peak_equity - equity) / self.peak_equity * 100 if self.peak_equity else 0.0

    def get_max_drawdown(self) -> float:
        with self._lock:
            return self.max_drawdown

    # =========================================================================
    # STATE QUERIES
    # =========================================================================

    def get_state(self) -> PortfolioState:
        with self._lock:
            equity = self.get_current_equity()
            return PortfolioState(
                equity=equity,
                cash=self.get_available_cash(),
                total_value=equity,
                positions=list(self.positions.values()),
                open_trades=list(self.open_trades.values()),
                closed_trades=list(self.closed_trades),
            )

    # Name used by the risk manager and signal aggregator
    get_portfolio_state = get_state

    def get_position(self, symbol: str) -> Optional[Position]:
        with self._lock:
            return self.positions.get(symbol)

    def get_position_size(self, symbol: str) -> float:
        with self._lock:
            position = self.positions.get(symbol)
            if position is None:
                return 0.0
            return -position.quantity if position.side == PositionSide.SHORT else position.quantity

    def has_position(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self.positions

    def get_num_open_positions(self) -> int:
        with self._lock:
            return len(self.positions)

    def get_current_pnl(self, symbol: str, current_price: float) -> float:
        with self._lock:
            position = self.positions.get(symbol)
            return position.pnl(current_price) if position else 0.0

    def get_trade_history(self) -> List[Trade]:
        with self._lock:
            return list(self.closed_trades)

    def get_equity_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.equity_history)

    def get_daily_pnl(self) -> float:
        """Today's realized net PnL plus current unrealized PnL."""
        with self._lock:
            today = datetime.now().date()
            realized_today = sum(t.net_pnl for t in self.closed_trades
                                 if t.exit_timestamp.date() == today)
            return realized_today + self.get_unrealized_pnl()

    def get_daily_pnl_pct(self) -> float:
        return self.get_daily_pnl() / self.initial_capital * 100 if self.initial_capital else 0.0

    def get_performance_metrics(self) -> PerformanceMetrics:
        with self._lock:
            return PerformanceMetrics(**self.performance_metrics.to_dict())

    # =========================================================================
    # METRICS / PERSISTENCE
    # =========================================================================

    def _update_performance_metrics(self) -> None:
        net = [t.net_pnl for t in self.closed_trades]
        wins = [p for p in net if p > 0]
        losses = [p for p in net if p < 0]
        m = self.performance_metrics
        m.total_trades = len(net)
        m.winning_trades = len(wins)
        m.losing_trades = len(losses)
        m.win_rate = len(wins) / len(net) if net else 0.0
        m.total_pnl = sum(t.pnl or 0.0 for t in self.closed_trades)
        m.total_net_pnl = sum(net)
        m.avg_win = sum(wins) / len(wins) if wins else 0.0
        m.avg_loss = sum(losses) / len(losses) if losses else 0.0
        m.profit_factor = sum(wins) / abs(sum(losses)) if losses else 0.0
        m.expectancy = sum(net) / len(net) if net else 0.0
        m.max_drawdown = self.max_drawdown
        m.current_drawdown = self.get_drawdown()

    def _record_equity_snapshot(self) -> None:
        self.equity_history.append({
            "timestamp": datetime.now().isoformat(),
            "equity": self.get_current_equity(),
            "cash": self.get_available_cash(),
            "open_positions": len(self.positions),
            "total_trades": len(self.closed_trades),
        })
        if len(self.equity_history) > self.equity_history_max_len:
            del self.equity_history[:-self.equity_history_max_len]

    def _append_to_journal(self, trade: Trade) -> None:
        try:
            self.trade_journal_path.parent.mkdir(parents=True, exist_ok=True)
            new_file = not self.trade_journal_path.exists()
            with open(self.trade_journal_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=JOURNAL_HEADERS, extrasaction='ignore')
                if new_file:
                    writer.writeheader()
                row = trade.to_dict()
                writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in JOURNAL_HEADERS})
        except Exception as e:
            self.logger.error(f"Error writing trade journal: {e}")

    def _load_trade_history(self) -> None:
        if not self.trade_journal_path.exists():
            return
        try:
            with open(self.trade_journal_path, newline='') as f:
                for row in csv.DictReader(f):
                    def num(key: str) -> Optional[float]:
                        return float(row[key]) if row.get(key) not in (None, "") else None
                    trade = Trade(
                        trade_id=row["trade_id"], symbol=row["symbol"], side=OrderSide(row["side"]),
                        entry_price=float(row["entry_price"]), exit_price=float(row["exit_price"]),
                        quantity=float(row["quantity"]),
                        entry_timestamp=datetime.fromisoformat(row["entry_timestamp"]),
                        exit_timestamp=datetime.fromisoformat(row["exit_timestamp"]),
                        stop_loss=num("stop_loss"), take_profit=num("take_profit"),
                        strategy=StrategyName(row["strategy"]) if row.get("strategy") else None,
                        confidence=num("confidence"), pnl=num("pnl"),
                        brokerage=num("brokerage") or 0.0, exchange_fees=num("exchange_fees") or 0.0,
                        stt=num("stt") or 0.0, gst=num("gst") or 0.0,
                        stamp_duty=num("stamp_duty") or 0.0, slippage=num("slippage") or 0.0,
                        status=TradeStatus(row["status"]),
                        tags=row["tags"].split(",") if row.get("tags") else [],
                    )
                    self.closed_trades.append(trade)
                    self.realized_pnl += trade.net_pnl
            self._update_performance_metrics()
            self.logger.info(f"Loaded {len(self.closed_trades)} trades from {self.trade_journal_path}")
        except Exception as e:
            self.logger.error(f"Error loading trade history: {e}")


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_portfolio_tracker: Optional[PortfolioTracker] = None


def get_portfolio_tracker() -> PortfolioTracker:
    """Get the singleton PortfolioTracker instance."""
    global _portfolio_tracker
    if _portfolio_tracker is None:
        from .config_loader import get_config_loader
        _portfolio_tracker = PortfolioTracker(get_config_loader().load())
    return _portfolio_tracker


def start_portfolio_tracker(config: Optional[Dict[str, Any]] = None) -> PortfolioTracker:
    """Create (if needed), start and return the portfolio tracker."""
    global _portfolio_tracker
    if _portfolio_tracker is None:
        _portfolio_tracker = PortfolioTracker(config) if config else get_portfolio_tracker()
    _portfolio_tracker.start()
    return _portfolio_tracker


def stop_portfolio_tracker() -> None:
    """Stop the portfolio tracker."""
    global _portfolio_tracker
    if _portfolio_tracker:
        _portfolio_tracker.stop()
        _portfolio_tracker = None
