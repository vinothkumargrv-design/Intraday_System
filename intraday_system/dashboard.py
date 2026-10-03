"""
Streamlit live dashboard for the Rapid Fire Intraday Trading System.

This module provides a comprehensive live dashboard with:
- Real-time portfolio overview
- Live PnL and drawdown charts
- Open positions and recent trades
- Strategy performance heatmap
- Order book and execution status
- System health monitoring
- Performance metrics

Run with: streamlit run dashboard.py
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
import webbrowser

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .models import (
    AggregatedSignal,
    BrokerType,
    OHLCV,
    Order,
    OrderSide,
    OrderStatus,
    PerformanceMetrics,
    Position,
    PositionSide,
    PortfolioState,
    RiskState,
    SignalType,
    StrategyName,
    SystemState,
    Tick,
    TimeFrame,
    Trade,
    TradeStatus,
)

from .logger import get_logger
from .config_loader import Config, load_config
from .portfolio_tracker import PortfolioTracker
from .risk_manager import RiskManager
from .signal_aggregator import SignalAggregator
from .order_manager import OrderManager
from .execution_engine import ExecutionEngine
from .data_feed import DataFeed


class Dashboard:
    """
    Streamlit live dashboard for the trading system.
    
    Features:
    - Real-time portfolio monitoring
    - Interactive charts and tables
    - System status and health
    - Performance analytics
    - Trade journal and history
    """
    
    def __init__(self, config: Config):
        """
        Initialize the dashboard.
        
        Args:
            config: System configuration
        """
        self.logger = get_logger("dashboard")
        self.config = config
        
        # Component references (set externally)
        self.portfolio_tracker: Optional[PortfolioTracker] = None
        self.risk_manager: Optional[RiskManager] = None
        self.signal_aggregator: Optional[SignalAggregator] = None
        self.order_manager: Optional[OrderManager] = None
        self.execution_engine: Optional[ExecutionEngine] = None
        self.data_feed: Optional[DataFeed] = None
        self.system_state: Optional[SystemState] = None
        
        # Dashboard state
        self._running = False
        self._update_thread: Optional[threading.Thread] = None
        self._last_update_time = datetime.now()
        
        # Cache for performance
        self._portfolio_state_cache: Optional[PortfolioState] = None
        self._risk_state_cache: Optional[RiskState] = None
        self._recent_trades_cache: List[Trade] = []
        self._open_orders_cache: List[Order] = []
        self._signals_cache: List[AggregatedSignal] = []
        
        self.logger.info("Dashboard initialized")
    
    def start(self) -> None:
        """
        Start the dashboard.
        """
        if self._running:
            return
        
        self._running = True
        
        # Start update thread
        self._update_thread = threading.Thread(target=self._update_cache, daemon=True)
        self._update_thread.start()
        
        # Launch Streamlit
        self._run_streamlit()
    
    def stop(self) -> None:
        """
        Stop the dashboard.
        """
        self._running = False
        if self._update_thread:
            self._update_thread.join(timeout=5)
    
    def _update_cache(self) -> None:
        """
        Background thread to update cached data.
        """
        while self._running:
            try:
                # Update portfolio state
                if self.portfolio_tracker:
                    self._portfolio_state_cache = self.portfolio_tracker.get_state()
                    self._recent_trades_cache = self.portfolio_tracker.get_trade_history()[-10:]  # Last 10 trades
                
                # Update risk state
                if self.risk_manager:
                    self._risk_state_cache = self.risk_manager.get_state()
                
                # Update signals
                if self.signal_aggregator:
                    # This would need to be implemented in the signal aggregator
                    pass
                
                # Update orders
                if self.order_manager:
                    self._open_orders_cache = self.order_manager.get_open_orders()
                
                # Update time
                self._last_update_time = datetime.now()
                
                # Sleep for 1 second
                threading.Event().wait(1.0)
                
            except Exception as e:
                self.logger.error(f"Error in dashboard update thread: {e}")
    
    def _run_streamlit(self) -> None:
        """
        Run the Streamlit application.
        """
        st.set_page_config(
            page_title="Rapid Fire Intraday Trading System",
            page_icon="📈",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Custom CSS
        st.markdown("""
        <style>
        .main-header {
            font-size: 2rem;
            font-weight: bold;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .positive {
            color: green;
        }
        .negative {
            color: red;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Title
        st.markdown('<p class="main-header">📈 Rapid Fire Intraday Trading System</p>', unsafe_allow_html=True)
        
        # Sidebar
        self._render_sidebar()
        
        # Main content
        self._render_main_content()
    
    def _render_sidebar(self) -> None:
        """
        Render the sidebar with system controls and info.
        """
        with st.sidebar:
            st.header("System Controls")
            
            # System status
            if self.system_state:
                status_color = "🟢" if self.system_state.is_running else "🔴"
                st.markdown(f"**Status:** {status_color} {self.system_state.current_mode}")
                
                if self.system_state.start_time:
                    st.markdown(f"**Uptime:** {datetime.now() - self.system_state.start_time}")
            
            st.divider()
            
            # System info
            st.header("System Info")
            st.markdown(f"**Initial Capital:** ₹{self.config.trading.initial_capital:,.2f}")
            st.markdown(f"**Broker:** {self.config.broker.broker_type.value}")
            st.markdown(f"**Symbols:** {', '.join(self.config.trading.symbols)}")
            
            st.divider()
            
            # Risk settings
            st.header("Risk Settings")
            st.markdown(f"**Max Risk/Trade:** {self.config.risk.max_per_trade_risk_pct}%")
            st.markdown(f"**Max Positions:** {self.config.risk.max_concurrent_positions}")
            st.markdown(f"**Daily Loss Limit:** {self.config.risk.max_daily_loss_pct}%")
            st.markdown(f"**Daily Profit Limit:** {self.config.risk.max_daily_profit_pct}%")
            
            st.divider()
            
            # Last update
            st.header("Dashboard")
            st.markdown(f"**Last Update:** {self._last_update_time.strftime('%H:%M:%S')}")
    
    def _render_main_content(self) -> None:
        """
        Render the main dashboard content.
        """
        # Create tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Portfolio", 
            "🎯 Signals & Orders", 
            "📈 Performance", 
            "🔍 Analytics",
            "⚙️ System"
        ])
        
        with tab1:
            self._render_portfolio_tab()
        
        with tab2:
            self._render_signals_orders_tab()
        
        with tab3:
            self._render_performance_tab()
        
        with tab4:
            self._render_analytics_tab()
        
        with tab5:
            self._render_system_tab()
    
    def _render_portfolio_tab(self) -> None:
        """
        Render portfolio overview tab.
        """
        st.header("Portfolio Overview")
        
        if not self._portfolio_state_cache:
            st.warning("No portfolio data available")
            return
        
        portfolio = self._portfolio_state_cache
        
        # Top metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            pnl_color = "positive" if portfolio.total_pnl({}) >= 0 else "negative"
            pnl_text = f"₹{portfolio.total_pnl({}):,.2f}"
            st.markdown(f"""
            <div class="metric-card">
                <h3>Total PnL</h3>
                <p class="{pnl_color}">{pnl_text}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            equity_change = portfolio.equity - self.config.trading.initial_capital
            equity_color = "positive" if equity_change >= 0 else "negative"
            equity_pct = (equity_change / self.config.trading.initial_capital * 100) if self.config.trading.initial_capital > 0 else 0
            st.markdown(f"""
            <div class="metric-card">
                <h3>Equity</h3>
                <p>₹{portfolio.equity:,.2f}</p>
                <p class="{equity_color}">({equity_pct:+.2f}%)</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            cash_color = "positive" if portfolio.cash >= 0 else "negative"
            st.markdown(f"""
            <div class="metric-card">
                <h3>Cash</h3>
                <p class="{cash_color}">₹{portfolio.cash:,.2f}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Positions</h3>
                <p>{portfolio.num_open_positions}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            drawdown = portfolio.drawdown(self.peak_equity if hasattr(self, 'peak_equity') else portfolio.equity)
            drawdown_color = "positive" if drawdown <= 5 else "negative"
            st.markdown(f"""
            <div class="metric-card">
                <h3>Drawdown</h3>
                <p class="{drawdown_color}">{drawdown:.2f}%</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.divider()
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_equity_chart()
        
        with col2:
            self._render_drawdown_chart()
        
        st.divider()
        
        # Open positions
        st.subheader("Open Positions")
        self._render_positions_table()
    
    def _render_equity_chart(self) -> None:
        """
        Render equity curve chart.
        """
        if not self.portfolio_tracker:
            st.warning("No portfolio tracker available")
            return
        
        equity_history = self.portfolio_tracker.get_equity_history()
        
        if not equity_history or len(equity_history) < 2:
            st.warning("Insufficient equity history data")
            return
        
        # Extract data
        timestamps = [datetime.fromisoformat(s["timestamp"]) for s in equity_history]
        equity = [s["equity"] for s in equity_history]
        
        # Create figure
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=equity,
            mode='lines',
            name='Equity',
            line=dict(color='#1f77b4', width=2)
        ))
        
        # Add current equity marker
        fig.add_trace(go.Scatter(
            x=[timestamps[-1]],
            y=[equity[-1]],
            mode='markers',
            name='Current',
            marker=dict(color='red', size=10, symbol='star')
        ))
        
        fig.update_layout(
            title="Equity Curve",
            xaxis_title="Time",
            yaxis_title="Equity (₹)",
            hovermode='x unified',
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_drawdown_chart(self) -> None:
        """
        Render drawdown chart.
        """
        if not self.portfolio_tracker:
            st.warning("No portfolio tracker available")
            return
        
        equity_history = self.portfolio_tracker.get_equity_history()
        
        if not equity_history or len(equity_history) < 2:
            st.warning("Insufficient equity history data")
            return
        
        # Calculate drawdown
        peak_equity = max(s["equity"] for s in equity_history)
        timestamps = [datetime.fromisoformat(s["timestamp"]) for s in equity_history]
        drawdowns = [((peak_equity - s["equity"]) / peak_equity * 100) for s in equity_history]
        
        # Create figure
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=drawdowns,
            mode='lines',
            name='Drawdown',
            line=dict(color='#d62728', width=2),
            fill='tozeroy',
            fillcolor='rgba(214, 39, 40, 0.2)'
        ))
        
        fig.update_layout(
            title="Drawdown",
            xaxis_title="Time",
            yaxis_title="Drawdown (%)",
            hovermode='x unified',
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_positions_table(self) -> None:
        """
        Render open positions table.
        """
        if not self._portfolio_state_cache:
            st.warning("No position data available")
            return
        
        positions = self._portfolio_state_cache.positions
        
        if not positions:
            st.info("No open positions")
            return
        
        # Create DataFrame
        data = []
        for pos in positions:
            # Calculate current PnL (using entry price as fallback)
            current_pnl = pos.pnl(pos.entry_price)  # Would use current price in real-time
            
            data.append({
                "Symbol": pos.symbol,
                "Side": pos.side.value,
                "Quantity": pos.quantity,
                "Entry Price": pos.entry_price,
                "Entry Time": pos.entry_timestamp.strftime('%H:%M:%S'),
                "Stop Loss": pos.stop_loss,
                "Take Profit": pos.take_profit,
                "Current PnL": current_pnl,
                "Strategy": pos.strategy.value if pos.strategy else "N/A"
            })
        
        df = pd.DataFrame(data)
        
        # Format numbers
        if not df.empty:
            df["Entry Price"] = df["Entry Price"].apply(lambda x: f"₹{x:.2f}")
            df["Stop Loss"] = df["Stop Loss"].apply(lambda x: f"₹{x:.2f}" if x else "N/A")
            df["Take Profit"] = df["Take Profit"].apply(lambda x: f"₹{x:.2f}" if x else "N/A")
            df["Current PnL"] = df["Current PnL"].apply(lambda x: f"₹{x:.2f}")
        
        # Display table
        st.dataframe(df, use_container_width=True)
    
    def _render_signals_orders_tab(self) -> None:
        """
        Render signals and orders tab.
        """
        st.header("Signals & Orders")
        
        # Recent signals
        st.subheader("Recent Signals")
        self._render_signals_table()
        
        st.divider()
        
        # Open orders
        st.subheader("Open Orders")
        self._render_orders_table()
        
        st.divider()
        
        # Recent trades
        st.subheader("Recent Trades")
        self._render_trades_table()
    
    def _render_signals_table(self) -> None:
        """
        Render recent signals table.
        """
        # This would need to be implemented with access to signal data
        # For now, show placeholder
        
        if not self._signals_cache:
            st.warning("No recent signals")
            return
        
        data = []
        for signal in self._signals_cache[-10:]:  # Last 10 signals
            data.append({
                "Time": signal.timestamp.strftime('%H:%M:%S'),
                "Symbol": signal.symbol,
                "Signal": signal.final_signal.value,
                "Score": f"{signal.net_score:.2f}",
                "Confidence": f"{signal.net_confidence:.2f}",
                "Regime OK": "✅" if signal.regime_filter_passed else "❌",
                "Strategies": len(signal.contributing_strategies)
            })
        
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True)
    
    def _render_orders_table(self) -> None:
        """
        Render open orders table.
        """
        if not self._open_orders_cache:
            st.warning("No open orders")
            return
        
        orders = self._open_orders_cache
        
        if not orders:
            st.info("No open orders")
            return
        
        data = []
        for order in orders:
            data.append({
                "Order ID": order.order_id,
                "Symbol": order.symbol,
                "Side": order.order_side.value,
                "Type": order.order_type.value,
                "Quantity": order.quantity,
                "Price": order.price or "MARKET",
                "Filled": f"{order.filled_quantity}/{order.quantity}",
                "Status": order.status.value,
                "Time": order.timestamp.strftime('%H:%M:%S')
            })
        
        df = pd.DataFrame(data)
        
        # Add progress bar for partially filled orders
        if not df.empty:
            df["Progress"] = df.apply(
                lambda row: st.progress(row["Filled"].split('/')[0] / row["Filled"].split('/')[1]) if '/' in row["Filled"] else None,
                axis=1
            )
        
        st.dataframe(df, use_container_width=True)
    
    def _render_trades_table(self) -> None:
        """
        Render recent trades table.
        """
        if not self._recent_trades_cache:
            st.warning("No recent trades")
            return
        
        trades = self._recent_trades_cache
        
        if not trades:
            st.info("No recent trades")
            return
        
        data = []
        for trade in trades[-10:]:  # Last 10 trades
            pnl_color = "positive" if trade.net_pnl >= 0 else "negative"
            
            data.append({
                "Trade ID": trade.trade_id,
                "Time": trade.exit_timestamp.strftime('%H:%M:%S'),
                "Symbol": trade.symbol,
                "Side": trade.side.value,
                "Entry": trade.entry_price,
                "Exit": trade.exit_price,
                "Qty": trade.quantity,
                "PnL": trade.net_pnl,
                "Return": f"{trade.return_pct:.2f}%",
                "Duration": f"{trade.duration_seconds:.0f}s",
                "Strategy": trade.strategy.value if trade.strategy else "N/A"
            })
        
        df = pd.DataFrame(data)
        
        # Format numbers
        if not df.empty:
            df["Entry"] = df["Entry"].apply(lambda x: f"₹{x:.2f}")
            df["Exit"] = df["Exit"].apply(lambda x: f"₹{x:.2f}")
            df["PnL"] = df["PnL"].apply(lambda x: f"₹{x:.2f}")
        
        # Add conditional formatting for PnL
        st.dataframe(df, use_container_width=True)
    
    def _render_performance_tab(self) -> None:
        """
        Render performance metrics tab.
        """
        st.header("Performance Metrics")
        
        if not self._portfolio_state_cache:
            st.warning("No performance data available")
            return
        
        portfolio = self._portfolio_state_cache
        
        # Performance metrics cards
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = portfolio_tracker.get_performance_metrics() if hasattr(self, 'portfolio_tracker') and self.portfolio_tracker else None
        
        if metrics:
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Total Trades</h3>
                    <p>{metrics.total_trades}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                win_rate_color = "positive" if metrics.win_rate >= 0.6 else "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Win Rate</h3>
                    <p class="{win_rate_color}">{metrics.win_rate:.1%}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                profit_factor_color = "positive" if metrics.profit_factor > 1 else "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Profit Factor</h3>
                    <p class="{profit_factor_color}">{metrics.profit_factor:.2f}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                sharpe_color = "positive" if metrics.sharpe_ratio > 1 else "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Sharpe Ratio</h3>
                    <p class="{sharpe_color}">{metrics.sharpe_ratio:.2f}</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.divider()
        
        # Additional metrics
        if metrics:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>Winning Trades</h4>
                    <p>{metrics.winning_trades}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>Losing Trades</h4>
                    <p>{metrics.losing_trades}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                avg_win_color = "positive"
                st.markdown(f"""
                <div class="metric-card">
                    <h4>Avg Win</h4>
                    <p class="{avg_win_color}">₹{metrics.avg_win:.2f}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                avg_loss_color = "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <h4>Avg Loss</h4>
                    <p class="{avg_loss_color}">₹{metrics.avg_loss:.2f}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col5:
                max_dd_color = "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <h4>Max Drawdown</h4>
                    <p class="{max_dd_color}">{metrics.max_drawdown:.2f}%</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.divider()
        
        # Performance charts
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_trade_distribution_chart()
        
        with col2:
            self._render_monthly_performance_chart()
    
    def _render_trade_distribution_chart(self) -> None:
        """
        Render trade distribution chart (winning vs losing).
        """
        if not self._recent_trades_cache:
            st.warning("No trade data available")
            return
        
        trades = self._recent_trades_cache
        
        winning = len([t for t in trades if t.net_pnl > 0])
        losing = len([t for t in trades if t.net_pnl < 0])
        breakeven = len([t for t in trades if t.net_pnl == 0])
        
        fig = go.Figure(data=[go.Pie(
            labels=['Winning', 'Losing', 'Breakeven'],
            values=[winning, losing, breakeven],
            hole=0.4,
            marker_colors=['#4CAF50', '#F44336', '#9E9E9E']
        )])
        
        fig.update_layout(
            title="Trade Distribution",
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_monthly_performance_chart(self) -> None:
        """
        Render monthly performance chart.
        """
        if not self._recent_trades_cache:
            st.warning("No trade data available")
            return
        
        trades = self._recent_trades_cache
        
        # Group by month
        monthly_pnl = {}
        for trade in trades:
            month = trade.exit_timestamp.strftime('%Y-%m')
            if month not in monthly_pnl:
                monthly_pnl[month] = 0
            monthly_pnl[month] += trade.net_pnl
        
        if not monthly_pnl:
            st.warning("No monthly data available")
            return
        
        # Create figure
        months = list(monthly_pnl.keys())
        pnl_values = list(monthly_pnl.values())
        
        colors = ['#4CAF50' if p >= 0 else '#F44336' for p in pnl_values]
        
        fig = go.Figure(data=[go.Bar(
            x=months,
            y=pnl_values,
            marker_color=colors
        )])
        
        fig.update_layout(
            title="Monthly Performance",
            xaxis_title="Month",
            yaxis_title="PnL (₹)",
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_analytics_tab(self) -> None:
        """
        Render analytics tab with strategy performance and heatmaps.
        """
        st.header("Analytics")
        
        # Strategy performance heatmap
        st.subheader("Strategy Performance Heatmap")
        self._render_strategy_heatmap()
        
        st.divider()
        
        # Symbol performance
        st.subheader("Symbol Performance")
        self._render_symbol_performance()
    
    def _render_strategy_heatmap(self) -> None:
        """
        Render strategy performance heatmap.
        """
        # This would need to be implemented with access to strategy performance data
        # For now, show a placeholder
        
        # Create sample data
        strategies = [s.value for s in StrategyName]
        metrics = ['Win Rate', 'Profit Factor', 'Sharpe Ratio', 'Total PnL']
        
        # Random data for demonstration
        import random
        data = []
        for strat in strategies:
            row = []
            for metric in metrics:
                if metric == 'Win Rate':
                    row.append(random.uniform(0.5, 0.9))
                elif metric == 'Profit Factor':
                    row.append(random.uniform(1.0, 3.0))
                elif metric == 'Sharpe Ratio':
                    row.append(random.uniform(0.5, 2.0))
                else:
                    row.append(random.uniform(1000, 10000))
            data.append(row)
        
        df = pd.DataFrame(data, index=strategies, columns=metrics)
        
        # Create heatmap
        fig = px.imshow(
            df,
            labels=dict(x="Metric", y="Strategy", color="Value"),
            color_continuous_scale='RdYlGn',
            aspect="auto"
        )
        
        fig.update_layout(
            title="Strategy Performance Heatmap",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_symbol_performance(self) -> None:
        """
        Render symbol performance chart.
        """
        if not self._recent_trades_cache:
            st.warning("No trade data available")
            return
        
        trades = self._recent_trades_cache
        
        # Calculate PnL by symbol
        symbol_pnl = {}
        symbol_trades = {}
        
        for trade in trades:
            symbol = trade.symbol
            if symbol not in symbol_pnl:
                symbol_pnl[symbol] = 0
                symbol_trades[symbol] = 0
            symbol_pnl[symbol] += trade.net_pnl
            symbol_trades[symbol] += 1
        
        if not symbol_pnl:
            st.warning("No symbol data available")
            return
        
        # Create DataFrame
        df = pd.DataFrame({
            'Symbol': list(symbol_pnl.keys()),
            'PnL': list(symbol_pnl.values()),
            'Trades': list(symbol_trades.values())
        })
        
        # Create bar chart
        fig = px.bar(
            df,
            x='Symbol',
            y='PnL',
            color='PnL',
            color_continuous_scale='RdYlGn',
            title="PnL by Symbol",
            labels={'PnL': 'Net PnL (₹)', 'Symbol': 'Symbol'}
        )
        
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show table
        st.dataframe(df, use_container_width=True)
    
    def _render_system_tab(self) -> None:
        """
        Render system status and health tab.
        """
        st.header("System Status")
        
        # System state
        st.subheader("System State")
        if self.system_state:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f"**Mode:** {self.system_state.current_mode}")
                st.markdown(f"**Running:** {'✅ Yes' if self.system_state.is_running else '❌ No'}")
            
            with col2:
                st.markdown(f"**Paper Trading:** {'✅ Yes' if self.system_state.is_paper_trading else '❌ No'}")
                st.markdown(f"**Start Time:** {self.system_state.start_time or 'N/A'}")
            
            with col3:
                if self.system_state.start_time:
                    uptime = datetime.now() - self.system_state.start_time
                    st.markdown(f"**Uptime:** {uptime}")
        
        st.divider()
        
        # Risk state
        st.subheader("Risk Management")
        if self._risk_state_cache:
            risk_state = self._risk_state_cache
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"**Daily PnL:** ₹{risk_state.daily_pnl:.2f}")
                st.markdown(f"**Daily PnL %:** {risk_state.daily_pnl_pct:.2f}%")
            
            with col2:
                st.markdown(f"**Exposure:** ₹{risk_state.current_exposure:.2f}")
                st.markdown(f"**Exposure %:** {risk_state.current_exposure_pct:.2f}%")
            
            with col3:
                st.markdown(f"**Open Positions:** {risk_state.open_positions_count}")
                st.markdown(f"**Consecutive Failures:** {risk_state.consecutive_failures}")
            
            with col4:
                kill_status = "❌ Not Active" if not risk_state.kill_switch_activated else "✅ ACTIVE"
                st.markdown(f"**Kill Switch:** {kill_status}")
        
        st.divider()
        
        # System logs
        st.subheader("System Logs")
        st.info("System logs will appear here (implementation pending)")


def run_dashboard(config_path: str = "config.yaml"):
    """
    Run the dashboard.
    
    Args:
        config_path: Path to configuration file
    """
    # Load config
    config = load_config(config_path)
    
    # Create dashboard
    dashboard = Dashboard(config)
    
    # Start dashboard
    dashboard.start()


if __name__ == "__main__":
    run_dashboard()