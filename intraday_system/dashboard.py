"""
Streamlit dashboard for the Rapid Fire Intraday Trading System.

The dashboard is a separate process from the trading system. It reads what
the trading process writes to disk:

- ``system.status_file`` (default ``data/status.json``): live snapshot of
  equity, positions, working orders, risk state and recent alerts, refreshed
  every few seconds by ``main.py``.
- ``system.trade_journal_path`` (default ``data/trade_journal.csv``): every
  closed trade.
- ``results/``: backtest outputs (trades, equity curve, summary).

Run with:
    streamlit run intraday_system/dashboard.py -- --config config.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

try:  # Allow `streamlit run intraday_system/dashboard.py` without installing the package
    from intraday_system.config_loader import ConfigLoader
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from intraday_system.config_loader import ConfigLoader


# =============================================================================
# DATA ACCESS (pure functions, unit-testable without Streamlit)
# =============================================================================

def load_status(path: Path) -> Optional[Dict[str, Any]]:
    """Read the live status snapshot, or None if missing/unreadable."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def status_age_seconds(status: Dict[str, Any]) -> Optional[float]:
    try:
        return (datetime.now() - datetime.fromisoformat(status['timestamp'])).total_seconds()
    except (KeyError, ValueError, TypeError):
        return None


def load_trades(path: Path) -> pd.DataFrame:
    """Load a trade journal / backtest trades CSV."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ('entry_timestamp', 'exit_timestamp'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    if 'tags' in df.columns:
        df['exit_reason'] = df['tags'].fillna('').astype(str).str.split(',').str[0]
    return df


def trade_stats(df: pd.DataFrame) -> Dict[str, float]:
    """Headline statistics for a trades table."""
    if df.empty or 'net_pnl' not in df.columns:
        return {'trades': 0, 'net_pnl': 0.0, 'win_rate': 0.0, 'profit_factor': 0.0,
                'avg_win': 0.0, 'avg_loss': 0.0, 'charges': 0.0}
    wins = df.loc[df['net_pnl'] > 0, 'net_pnl']
    losses = df.loc[df['net_pnl'] < 0, 'net_pnl']
    charge_cols = [c for c in ('brokerage', 'exchange_fees', 'stt', 'gst', 'stamp_duty', 'slippage')
                   if c in df.columns]
    return {
        'trades': int(len(df)),
        'net_pnl': float(df['net_pnl'].sum()),
        'win_rate': float(len(wins) / len(df)),
        'profit_factor': float(wins.sum() / abs(losses.sum())) if len(losses) else float('inf') if len(wins) else 0.0,
        'avg_win': float(wins.mean()) if len(wins) else 0.0,
        'avg_loss': float(losses.mean()) if len(losses) else 0.0,
        'charges': float(df[charge_cols].sum().sum()) if charge_cols else 0.0,
    }


def equity_from_trades(df: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    """Equity and drawdown after each closed trade."""
    if df.empty:
        return pd.DataFrame(columns=['timestamp', 'equity', 'drawdown_pct'])
    out = df.sort_values('exit_timestamp')[['exit_timestamp', 'net_pnl']].copy()
    out['equity'] = initial_capital + out['net_pnl'].cumsum()
    peak = out['equity'].cummax().clip(lower=initial_capital)
    out['drawdown_pct'] = (peak - out['equity']) / peak * 100
    return out.rename(columns={'exit_timestamp': 'timestamp'})[['timestamp', 'equity', 'drawdown_pct']]


def latest_backtest(results_dir: Path) -> Optional[Dict[str, Path]]:
    """Paths of the most recent backtest run's files."""
    summaries = sorted(results_dir.glob('summary_*.txt'))
    if not summaries:
        return None
    stamp = summaries[-1].stem.replace('summary_', '')
    return {
        'stamp': stamp,
        'summary': summaries[-1],
        'trades': results_dir / f'trades_{stamp}.csv',
        'equity': results_dir / f'equity_curve_{stamp}.csv',
    }


# =============================================================================
# STREAMLIT UI
# =============================================================================

def _money(value: Any) -> str:
    try:
        return f"₹{float(value):,.2f}"
    except (TypeError, ValueError):
        return "—"


def render(config_path: str = "config.yaml") -> None:  # pragma: no cover - exercised via AppTest
    import plotly.express as px
    import streamlit as st

    st.set_page_config(page_title="RapidFire Intraday", layout="wide")
    config = ConfigLoader(config_path).load()
    system = config.get('system', {})
    status_path = Path(system.get('status_file', 'data/status.json'))
    journal_path = Path(system.get('trade_journal_path', 'data/trade_journal.csv'))
    results_dir = Path((config.get('backtester', {}).get('metrics', {}) or {}).get('output_dir', 'results'))
    initial_capital = float(config.get('portfolio', {}).get('initial_equity', 100000.0))

    with st.sidebar:
        st.title("RapidFire")
        auto = st.toggle("Auto refresh", value=True, key="auto_refresh")
        interval = st.slider("Refresh every (s)", 2, 60, 5)
        st.caption(f"Status: `{status_path}`")
        st.caption(f"Journal: `{journal_path}`")
        st.caption(f"Backtests: `{results_dir}`")

    live_tab, journal_tab, backtest_tab = st.tabs(["Live", "Trade journal", "Backtest"])

    # ------------------------------------------------------------------ Live
    with live_tab:
        status = load_status(status_path)
        if status is None:
            st.info("No live status yet. Start the system with "
                    "`python -m intraday_system.main paper` and this page will update.")
        else:
            age = status_age_seconds(status)
            stale = age is None or age > 30
            header = f"{status.get('mode', '?')} mode"
            if stale:
                st.warning(f"{header} — last update {age:.0f}s ago; the trading process may be stopped."
                           if age is not None else f"{header} — status time unknown")
            else:
                st.success(f"{header} — running, updated {age:.0f}s ago")
            if status.get('kill_switch'):
                st.error("KILL SWITCH ACTIVE — trading halted")

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Equity", _money(status.get('equity')),
                      _money(float(status.get('equity', 0)) - float(status.get('initial_capital', initial_capital))))
            c2.metric("Day PnL", _money(status.get('daily_pnl')))
            c3.metric("Unrealized", _money(status.get('unrealized_pnl')))
            c4.metric("Open positions", status.get('open_positions', 0))
            c5.metric("Drawdown", f"{status.get('drawdown_pct', 0):.2f}%")

            st.subheader("Positions")
            positions = pd.DataFrame(status.get('positions', []))
            if positions.empty:
                st.caption("Flat")
            else:
                if {'last_price', 'entry_price', 'quantity', 'side'} <= set(positions.columns):
                    direction = positions['side'].map({'BUY': 1, 'SELL': -1})
                    positions['unrealized'] = ((positions['last_price'] - positions['entry_price'])
                                               * positions['quantity'] * direction).round(2)
                st.dataframe(positions, width='stretch', hide_index=True)

            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Working orders")
                orders = pd.DataFrame(status.get('open_orders', []))
                if orders.empty:
                    st.caption("None")
                else:
                    cols = [c for c in ('symbol', 'side', 'type', 'quantity', 'price', 'trigger_price',
                                        'status', 'timestamp') if c in orders.columns]
                    st.dataframe(orders[cols], width='stretch', hide_index=True)
            with col_b:
                st.subheader("Risk")
                st.json(status.get('risk', {}), expanded=False)

            history = pd.DataFrame(status.get('equity_history', []))
            if len(history) > 1:
                history['timestamp'] = pd.to_datetime(history['timestamp'])
                st.plotly_chart(px.line(history, x='timestamp', y='equity', title='Equity (session)'),
                                width='stretch')

            st.subheader("Recent alerts")
            alerts = pd.DataFrame(status.get('notifications', []))
            if alerts.empty:
                st.caption("None")
            else:
                st.dataframe(alerts[['timestamp', 'level', 'title', 'message']].iloc[::-1],
                             width='stretch', hide_index=True)

    # --------------------------------------------------------------- Journal
    with journal_tab:
        trades = load_trades(journal_path)
        _render_trades(st, px, trades, initial_capital, "No closed trades in the journal yet.")

    # -------------------------------------------------------------- Backtest
    with backtest_tab:
        run = latest_backtest(results_dir)
        if run is None:
            st.info("No backtest results found. Run `python run_backtest.py --synthetic`.")
        else:
            st.caption(f"Latest run: {run['stamp']}")
            if run['equity'].exists():
                curve = pd.read_csv(run['equity'])
                if not curve.empty:
                    curve['timestamp'] = pd.to_datetime(curve['timestamp'])
                    st.plotly_chart(px.line(curve, y='equity', title='Backtest equity (by bar)'),
                                    width='stretch')
            _render_trades(st, px, load_trades(run['trades']), initial_capital, "No trades in this run.")
            with st.expander("Summary"):
                st.code(run['summary'].read_text())

    if auto:
        time.sleep(interval)
        st.rerun()


def _render_trades(st, px, trades: pd.DataFrame, initial_capital: float,
                   empty_message: str) -> None:  # pragma: no cover - UI
    if trades.empty:
        st.info(empty_message)
        return
    stats = trade_stats(trades)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Trades", stats['trades'])
    c2.metric("Net PnL", _money(stats['net_pnl']))
    c3.metric("Win rate", f"{stats['win_rate']:.1%}")
    c4.metric("Profit factor", f"{stats['profit_factor']:.2f}")
    c5.metric("Charges paid", _money(stats['charges']))

    curve = equity_from_trades(trades, initial_capital)
    st.plotly_chart(px.line(curve, x='timestamp', y='equity', title='Equity after each trade'),
                    width='stretch')

    col_a, col_b = st.columns(2)
    if 'strategy' in trades.columns:
        by_strategy = trades.groupby('strategy', dropna=False)['net_pnl'].agg(['count', 'sum']).reset_index()
        col_a.plotly_chart(px.bar(by_strategy, x='strategy', y='sum', title='Net PnL by lead strategy',
                                  hover_data=['count']), width='stretch')
    if 'exit_reason' in trades.columns:
        by_exit = trades.groupby('exit_reason')['net_pnl'].agg(['count', 'sum']).reset_index()
        col_b.plotly_chart(px.bar(by_exit, x='exit_reason', y='count', title='Exits by reason',
                                  hover_data=['sum']), width='stretch')

    cols = [c for c in ('entry_timestamp', 'exit_timestamp', 'symbol', 'side', 'quantity', 'entry_price',
                        'exit_price', 'net_pnl', 'exit_reason', 'strategy') if c in trades.columns]
    st.dataframe(trades[cols].sort_values('exit_timestamp', ascending=False),
                 width='stretch', hide_index=True)


def _config_from_argv() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args, _ = parser.parse_known_args()
    return args.config


if __name__ == "__main__":  # pragma: no cover
    render(_config_from_argv())
