# Rapid Fire Intraday Trading System

A modular intraday trading system for Indian equities (NSE) in Python: six
strategies vote on every symbol, a risk manager sizes and vets each trade, and
an execution engine manages positions with exchange-side stop-loss and target
orders. The same strategy and voting code runs in the backtester, in paper
trading and in live trading.

> **Status:** Backtesting and paper trading are complete and covered by tests.
> The Zerodha adapter maps orders onto the official `kiteconnect` SDK and is
> unit-tested against a stub, but it has **not** been run against a live Kite
> account. Paper trade first, then go live with small size. Alpaca and
> Interactive Brokers are not implemented.
>
> The bundled strategies are a starting point, not a proven edge. On synthetic
> random-walk data they lose roughly the cost of trading, as any strategy
> should. Validate and tune them on real historical data before risking money.

## What's included

| Component | File | Role |
|---|---|---|
| Strategies | `strategy_engine.py` | ORB, VWAP mean reversion, momentum burst, EMA crossover, order-flow imbalance, Supertrend |
| Signal aggregation | `signal_aggregator.py` | Weighted vote; trade only if net score ≥ threshold **and** ≥ N strategies agree, volatility regime filter, per-symbol cooldown |
| Risk | `risk_manager.py` | Risk-based sizing, leverage cap, exposure/position limits, spread & volume checks, market hours, daily loss/profit limits, kill switch (file, loss limit, order failures) |
| Execution | `execution_engine.py` | Entry → protective SL-M + target LIMIT at the broker (OCO), ATR trailing stop, time square-off, emergency flatten |
| Orders | `order_manager.py` | Retries with backoff, idempotency keys, fill reconciliation, LIMIT→MARKET fallback |
| Brokers | `brokers/paper_broker.py`, `brokers/zerodha_broker.py` | Paper broker fills at live feed prices; Zerodha via `kiteconnect` |
| Market data | `data_feed.py` | Tick ingest → 1m/5m bars, SQLite candle store, mock feed for paper mode, KiteTicker for Zerodha |
| Portfolio | `portfolio_tracker.py` | Ledger: realized/unrealized PnL, drawdown, CSV trade journal |
| Costs | `costs.py` | Brokerage (capped), exchange fees, STT, GST, stamp duty, slippage, shared by live and backtest |
| Backtester | `backtester.py` | Bar-by-bar simulation with the live strategy and voting code, metrics, CSV/PNG output, walk-forward grid search |
| Alerts | `notifier.py` | Telegram and email, per-event filters, rate limiting, daily report |
| Dashboard | `dashboard.py` | Streamlit app: live status, positions, orders, trade journal, backtest results |

## Quick start

```bash
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m pytest -q                                # 120 tests
```

### Backtest

```bash
# Synthetic data (reproducible): a quick way to see the whole pipeline work
python run_backtest.py --synthetic --days 20 --symbols RELIANCE TCS INFY

# Your own 1-minute data: data/historical/<SYMBOL>.csv
python run_backtest.py --data-dir data/historical --start 2024-01-01 --end 2024-06-30
```

CSV format (header names are case-insensitive; `datetime`/`date` also work for the time column):

```
timestamp,open,high,low,close,volume
2024-01-02 09:15:00,2580.5,2584.0,2579.1,2583.2,15230
```

Output goes to `results/`: `trades_*.csv`, `summary_*.txt`, `equity_curve_*.csv`, `equity_curve_*.png`.

Mechanics to know:
- Signals are evaluated on bar close and entries fill at the **next bar's open** (no look-ahead).
- If a bar touches both stop and target, the stop is assumed hit first (conservative).
- Positions are flattened at `square_off_time`; no new entries in the last `pre_square_off_minutes`.
- `ORDER_FLOW` needs real tick data; with 1-minute bars it is effectively inactive.

### Paper trading

```bash
# During NSE hours (mock tick feed + paper broker)
python -m intraday_system.main paper

# Outside market hours, e.g. to watch it run on mock data
python -m intraday_system.main paper --ignore-market-hours --duration 600
```

Strategies need about 20 minutes of bars before they can signal. Ctrl+C (or
SIGTERM) shuts down gracefully and flattens all positions.

### Live trading (Zerodha)

```bash
export ZERODHA_API_KEY=...
export ZERODHA_ACCESS_TOKEN=...        # Daily token from Kite login
# or: ZERODHA_REQUEST_TOKEN=... with broker.zerodha.api_secret set
# then set broker.active: "ZERODHA" in config.yaml
python -m intraday_system.main live --confirm-live
```

Live mode refuses to start without `--confirm-live`, and refuses `broker.active: PAPER`.

### Dashboard

```bash
streamlit run intraday_system/dashboard.py -- --config config.yaml
```

The dashboard is a separate process. It reads `data/status.json` (written by
the trading process every 5 s), `data/trade_journal.csv` and `results/`.

### Docker

```bash
docker build -t rapidfire .
docker run --rm -v $(pwd)/data:/app/data rapidfire                        # paper trading
docker run --rm -v $(pwd)/results:/app/results rapidfire \
    python run_backtest.py --synthetic --days 20                          # backtest
docker run --rm -p 8501:8501 -v $(pwd)/data:/app/data -v $(pwd)/results:/app/results rapidfire \
    streamlit run intraday_system/dashboard.py --server.address 0.0.0.0   # dashboard
```

## How a trade happens

```
ticks ──► DataFeed ──► 1m/5m bars ──► 6 strategies ──► StrategySignal
                                                           │
             SignalAggregator: latest signal per strategy within 5 min,
             weighted net score ≥ 0.65 AND ≥ 2 strategies agree,
             volatility regime OK, no position, not in cooldown
                                                           │
ExecutionEngine: stop = median strategy stop, clamped to [0.5%, 1%] of price,
                 target = median strategy target (≥ 1R, default 2R)
RiskManager:     qty = equity × 0.5% / stop distance, capped by leverage;
                 spread, volume, exposure, position, hours, daily limits
                                                           │
OrderManager ──► entry ──► fill ──► SL-M + LIMIT target at broker (OCO)
             trailing stop modifies the SL-M once the trade is 1R in profit
             square-off / kill switch: cancel brackets, MARKET exit
                                                           │
PortfolioTracker (journal, PnL) ◄── Trade with all charges ──► Notifier
```

## Configuration

Everything lives in `config.yaml`. Secrets should come from environment
variables, which override the file: `ZERODHA_API_KEY`, `ZERODHA_API_SECRET`,
`ZERODHA_ACCESS_TOKEN`, `ZERODHA_REQUEST_TOKEN`, `TELEGRAM_BOT_TOKEN`,
`TELEGRAM_CHAT_ID`, `SMTP_USERNAME`, `SMTP_PASSWORD`, and so on. Any other value
can be overridden with `RAPIDFIRE_` plus its path, segments separated by `__`,
e.g. `RAPIDFIRE_RISK_MANAGEMENT__DAILY__MAX_LOSS_PCT=1.5`.

Key settings:

| Setting | Default | Meaning |
|---|---|---|
| `broker.active` | `PAPER` | `PAPER` or `ZERODHA` |
| `market_data.websocket.symbols` | 16 NSE stocks | Universe (`NSE:INFY` style also accepted) |
| `strategies.weights` / `.parameters` | | Per-strategy weight, timeframe, enable flag and parameters |
| `signal_aggregation.threshold` | `0.65` | Minimum weighted net score |
| `signal_aggregation.min_agreeing_strategies` | `2` | Strategies that must agree on direction |
| `signal_aggregation.regime.volatility_threshold` | `0.75` | Block entries when ATR is above this percentile of its recent range |
| `risk_management.per_trade.max_risk_pct` | `0.5` | % of equity risked per trade |
| `risk_management.stop_loss.hard_stop_pct` / `min_stop_pct` | `1.0` / `0.5` | Stop distance bounds (% of price) |
| `risk_management.stop_loss.trailing_activation_r` | `1.0` | Start trailing after this many R of profit |
| `risk_management.limits.max_concurrent_positions` | `5` | |
| `risk_management.limits.max_exposure_pct` | `500` | Gross notional as % of equity |
| `risk_management.leverage.max_leverage` | `5` | Caps per-position notional at equity × leverage / max positions |
| `risk_management.daily.max_loss_pct` / `max_profit_pct` | `2` / `4` | Loss trips the kill switch; profit stops new entries |
| `risk_management.time_stop.square_off_time` | `15:15` | Flatten time (exchange time, `system.timezone`) |
| `risk_management.circuit_breakers.manual_kill_file` | `/tmp/KILL` | Create this file to halt and flatten |
| `execution.entry.order_type` | `LIMIT` | LIMIT at last price with MARKET fallback after `fallback_timeout_seconds` |
| `execution.exits.use_bracket_orders` | `true` | Keep SL/TP at the broker (recommended) vs. software exits |
| `backtester.costs.*` | NSE intraday | Brokerage 0.03% capped at ₹20/order, STT 0.025% sell, exchange 0.00297%, GST 18%, stamp 0.003% buy, slippage 0.05% |

## Safety

- **Kill switch**: trips on the daily loss limit, `consecutive_failures_limit`
  order failures, or the manual kill file. It cancels working orders, flattens
  every position and blocks new entries.
- **Exits are never blocked**: stop, target and square-off orders bypass entry
  risk checks. Exits always send a real order and cancel the sibling bracket.
- **Graceful shutdown** flattens positions; if anything is still open after
  10 s it is logged as CRITICAL.
- Live mode needs `--confirm-live` and a non-paper broker.

## Tests

```bash
python -m pytest -q
```

| File | Covers |
|---|---|
| `test_live_flow.py` | Signal → order → fill → brackets → OCO → exits, square-off, trailing stop, kill switch (paper broker) |
| `test_risk_manager.py` | Sizing, every pre-trade check, cooldown, daily limits, kill switch triggers |
| `test_strategies_and_aggregation.py` | Crossover/flip/breakout signal timing, voting rules, signal TTL, regime filter |
| `test_backtester.py` | Data loading, determinism, no look-ahead, ledger consistency, cost model |
| `test_zerodha_broker.py` | Kite order mapping, status/fill parsing (stubbed SDK) |
| `test_notifier_and_dashboard.py` | Alert routing and rate limits, dashboard data helpers |
| `test_models.py`, `test_config_loader.py` | Data models, config loading and overrides |

## Project layout

```
├── config.yaml              # All settings
├── run_backtest.py          # Backtest CLI
├── intraday_system/
│   ├── main.py              # Paper/live orchestrator (python -m intraday_system.main)
│   ├── strategy_engine.py   signal_aggregator.py   risk_manager.py
│   ├── execution_engine.py  order_manager.py       portfolio_tracker.py
│   ├── data_feed.py         backtester.py          costs.py
│   ├── notifier.py          dashboard.py           logger.py   config_loader.py   models.py
│   └── brokers/             # base_broker, broker_factory, paper_broker, zerodha_broker
├── tests/
├── data/                    # Runtime: candles.sqlite, trade_journal.csv, status.json, historical/
├── logs/                    # Runtime logs (JSON lines)
└── results/                 # Backtest output
```

## Disclaimer

For education and research. Trading involves substantial risk of loss. Past
performance, real or simulated, does not guarantee future results. You are
responsible for compliance with your broker's and SEBI's rules.
