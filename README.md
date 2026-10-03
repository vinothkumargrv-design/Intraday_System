# Rapid Fire Intraday Trading System

A complete, modular, production-ready intraday trading system in Python with sub-second latency on the signal side.

## Features

### Core Capabilities
- **Multi-Broker Support**: Zerodha Kite Connect, Alpaca, Interactive Brokers (pluggable architecture)
- **Real-time Data**: WebSocket tick-by-tick + 1-min OHLCV feeds
- **6 Trading Strategies**: ORB, VWAP Mean Reversion, Momentum Burst, EMA Crossover, Order Flow Imbalance, Supertrend
- **Signal Aggregation**: Weighted voting with confidence scoring and regime filtering
- **Risk Management**: Position sizing, kill-switch, exposure caps, trailing stops
- **Order Management**: Bracket orders, partial fills, retry logic, idempotency keys
- **Portfolio Tracking**: Real-time PnL, drawdown, position management

### Performance Targets
- **Latency**: < 500ms from signal to order placement
- **Concurrent Positions**: Up to 5 (configurable)
- **Daily Limits**: 2% loss limit, 4% profit limit
- **Risk per Trade**: 0.5% of equity (configurable)

### Backtesting
- Event-driven backtesting (same logic as live)
- Includes brokerage, STT, exchange fees, GST, stamp duty, slippage
- Performance metrics: CAGR, Sharpe, Sortino, max DD, win rate, profit factor
- Walk-forward optimization support

## Directory Structure

```
Intraday_System/
├── intraday_system/
│   ├── __init__.py
│   ├── config.yaml            # All parameters and settings
│   ├── models.py             # Core data structures and enums
│   ├── config_loader.py      # Configuration management
│   ├── logger.py             # Structured JSON logging
│   ├── data_feed.py          # WebSocket + REST data feeds
│   ├── strategy_engine.py    # 6 trading strategies
│   ├── signal_aggregator.py  # Multi-strategy voting
│   ├── risk_manager.py       # Risk management and safety
│   ├── order_manager.py      # Order placement and management
│   ├── execution_engine.py   # Trade execution logic
│   ├── portfolio_tracker.py  # PnL, drawdown, live positions
│   ├── backtester.py         # Event-driven backtester
│   ├── notifier.py           # Telegram/email alerts
│   └── dashboard.py          # Streamlit live dashboard
├── config.yaml              # Main configuration
├── requirements.txt         # Python dependencies
├── Dockerfile               # Docker container
├── README.md               # This file
└── results/                # Backtest results (created at runtime)
    ├── trades_*.csv
    ├── summary_*.txt
    ├── equity_curve_*.csv
    └── equity_curve_*.png
```

## Quick Start

### 1. Install Dependencies

```bash
# Clone the repository
git clone <repository-url>
cd Intraday_System

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt

# Install optional dependencies based on your broker
# For Zerodha Kite Connect
pip install kiteconnect

# For Alpaca
pip install alpaca-trade-api

# For Interactive Brokers
pip install ib_insync

# For TA-Lib (technical analysis)
pip install TA-Lib  # May require system libraries
```

### 2. Configuration

Edit `config.yaml` to configure your settings:

```yaml
# Broker Configuration
broker:
  broker_type: ZERODHA  # ZERODHA, ALPACA, INTERACTIVE_BROKERS, PAPER
  api_key: "your_api_key"
  api_secret: "your_api_secret"
  access_token: "your_access_token"  # For Zerodha
  account_id: "your_account_id"

# Trading Configuration
trading:
  paper_trading: true    # Set to false for live trading
  initial_capital: 100000
  symbols: ["NIFTY", "BANKNIFTY", "RELIANCE"]
  
# Risk Management
risk:
  max_per_trade_risk_pct: 0.5
  max_concurrent_positions: 5
  max_daily_loss_pct: 2.0
  max_daily_profit_pct: 4.0

# Strategies
strategies:
  ORB:
    enabled: true
    weight: 1.2
  VWAP_MR:
    enabled: true
    weight: 1.0
  # ... other strategies
```

### 3. Run the System

#### Paper Trading (Recommended for Testing)
```bash
python -m intraday_system.main paper
```

#### Live Trading
```bash
# WARNING: Real money at risk!
python -m intraday_system.main live
```

You will be prompted to type `CONFIRM_LIVE` to confirm live trading.

#### Backtesting
```bash
python -m intraday_system.main backtest
```

#### Streamlit Dashboard
```bash
streamlit run intraday_system/dashboard.py
```

### 4. Docker Deployment

#### Build the Docker image
```bash
docker build -t rapid-fire-trading-system .
```

#### Run with Docker
```bash
# Paper trading
mkdir -p data/historical
docker run -v $(pwd)/data:/app/data rapid-fire-trading-system paper

# Live trading (be careful!)
docker run -v $(pwd)/data:/app/data rapid-fire-trading-system live

# Backtesting
docker run -v $(pwd)/data:/app/data rapid-fire-trading-system backtest

# Dashboard
docker run -p 8501:8501 -v $(pwd)/data:/app/data rapid-fire-trading-system streamlit run intraday_system/dashboard.py
```

## Configuration Reference

### config.yaml Structure

```yaml
# Broker Settings
broker:
  broker_type: ZERODHA          # ZERODHA, ALPACA, INTERACTIVE_BROKERS, PAPER
  api_key: "your_key"
  api_secret: "your_secret"
  access_token: "your_token"   # Required for Zerodha
  account_id: "your_account"   # Required for Zerodha
  base_url: null              # Custom API URL
  websocket_url: null         # Custom WebSocket URL

# Trading Settings
trading:
  paper_trading: true          # Paper trading mode
  initial_capital: 100000      # Starting capital
  symbols:                    # List of symbols to trade
    - NIFTY
    - BANKNIFTY
    - RELIANCE
    - TCS
  
# Strategy Weights
strategy_weights:
  ORB: 1.2
  VWAP_MR: 1.0
  MOMENTUM_BURST: 0.8
  EMA_CROSSOVER: 1.1
  ORDER_FLOW: 0.9
  SUPERTREND: 1.0

# Signal Aggregation
signal_aggregation:
  threshold: 0.65             # Minimum net score to trade
  min_agreeing_strategies: 2 # Minimum strategies agreeing
  use_regime_filter: true    # Use market regime filter
  regime_volatility_threshold: 0.75

# Risk Management
risk:
  max_per_trade_risk_pct: 0.5    # 0.5% of equity per trade
  max_concurrent_positions: 5    # Maximum open positions
  max_daily_loss_pct: 2.0        # Daily loss limit (auto kill-switch)
  max_daily_profit_pct: 4.0      # Daily profit limit (auto stop)
  trailing_stop_atr_multiplier: 1.5
  hard_stop_loss_pct: 1.0       # Hard stop loss from entry
  time_stop_time: "15:15:00"    # Time to close all positions (IST)
  max_spread_pct: 0.15          # Reject trades if spread > 0.15%
  slippage_buffer_pct: 0.05    # Buffer for slippage
  cooldown_seconds: 60         # Cooldown per symbol after exit

# Execution Settings
execution:
  order_type: MARKET           # MARKET or LIMIT
  use_bracket_orders: true     # Use bracket orders (entry + SL + TP)
  retry_attempts: 3            # Order retry attempts
  retry_backoff_seconds: 1.0  # Exponential backoff base
  max_order_age_seconds: 30   # Cancel orders older than this

# Data Feed Settings
data_feed:
  websocket_enabled: true
  rest_enabled: true
  historical_data_path: data/historical
  tick_buffer_size: 1000
  ohlcv_buffer_size: 1000
  resample_ticks: true         # Resample ticks to OHLCV

# Notifications
notifications:
  telegram_enabled: false
  telegram_bot_token: ""
  telegram_chat_id: ""
  email_enabled: false
  email_smtp_server: "smtp.gmail.com"
  email_smtp_port: 587
  email_username: ""
  email_password: ""
  email_from: ""
  email_to: []
  min_level: INFO              # Minimum notification level
  trade_pnl_threshold: 0.0    # Notify on trades with PnL > this
  daily_pnl_threshold: 0.0    # Notify on daily PnL changes > this
  drawdown_threshold: 1.0     # Notify on drawdown > this percent

# Logging
logging:
  level: INFO                 # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file_path: logs/trading.log
  json_format: true          # JSON structured logs
  max_file_size: 10485760    # 10MB
  backup_count: 5

# Backtesting
backtest:
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  initial_capital: 100000
  realtime_simulation: false   # Simulate with delays

# Broker-Specific Settings
zerodha:
  api_key: ""
  api_secret: ""
  access_token: ""
  
alpaca:
  api_key: ""
  api_secret: ""
  base_url: "https://api.alpaca.markets"
  
interactive_brokers:
  account_id: ""
  client_id: 1
```

## Safety Features

### Kill Switch
The system has multiple kill-switch mechanisms:

1. **Daily Loss Limit**: Auto shut down if daily loss exceeds 2% of equity
2. **Consecutive Failures**: Auto shut down after 3 consecutive order failures
3. **WebSocket Disconnect**: Auto shut down if WebSocket disconnect > 10s
4. **Manual Override**: Create `/tmp/KILL` file to trigger emergency shutdown
5. **Time Stop**: All positions closed automatically at 15:15 IST

### Risk Management
- **Per-trade risk**: Maximum 0.5% of equity
- **Position sizing**: Calculated as `(equity * risk_pct) / |entry - SL|`
- **Trailing SL**: ATR-based (1.5x ATR)
- **Hard SL**: Always set on exchange
- **Spread check**: Reject trades if spread > 0.15%
- **Slippage buffer**: 0.05% buffer on entries

### Order Safety
- **Idempotency keys**: Prevent duplicate orders
- **Exponential backoff**: Retry failed orders with increasing delays
- **Partial fill handling**: Adjust SL quantity for partial fills
- **Order age limit**: Cancel orders older than 30 seconds

## Strategy Descriptions

### 1. Opening Range Breakout (ORB)
- **Timeframe**: 5-min candles
- **Logic**: Identifies breakout from 15-minute opening range
- **Entry**: Buy above opening range high, sell below opening range low
- **Time filter**: Only trades within first 2 hours of market open

### 2. VWAP Mean Reversion (VWAP_MR)
- **Logic**: Fades extremes beyond 2 standard deviations from VWAP
- **Entry**: Buy when price > VWAP + 2σ, sell when price < VWAP - 2σ
- **Exit**: Return to VWAP or opposite extreme
- **Timeframe**: 1-min candles

### 3. Momentum Burst
- **Logic**: Detects strong momentum with volume confirmation
- **Entry**: 3 consecutive bullish/bearish candles with volume > 2x average
- **Confirmation**: Price breaks recent high/low
- **Timeframe**: 1-min candles

### 4. EMA Crossover Scalper
- **Logic**: 9/21 EMA crossover on 1-min chart
- **Entry**: Buy when EMA9 crosses above EMA21, sell when EMA9 crosses below EMA21
- **RSI Filter**: Only trade when RSI is in favorable zone (>50 for buy, <50 for sell)
- **Timeframe**: 1-min candles

### 5. Order Flow Imbalance
- **Logic**: Analyzes bid/ask delta from tick data
- **Entry**: Buy when bid volume significantly exceeds ask volume
- **Exit**: When order flow reverses or price reaches target
- **Timeframe**: Tick data

### 6. Supertrend + ATR Trailing
- **Logic**: Trend-following with Supertrend indicator
- **Entry**: Buy when price closes above Supertrend, sell when closes below
- **Exit**: Supertrend flip or trailing stop hit
- **Trailing Stop**: 1.5x ATR
- **Regime**: Only trades in trending markets

## Performance Metrics

### Backtest Metrics
- **CAGR**: Compound Annual Growth Rate
- **Sharpe Ratio**: Risk-adjusted return (return/volatility)
- **Sortino Ratio**: Downside risk-adjusted return
- **Max Drawdown**: Maximum peak-to-trough decline
- **Win Rate**: Percentage of winning trades
- **Profit Factor**: Average win / Average loss
- **Expectancy**: Average PnL per trade
- **MAR Ratio**: CAGR / Max Drawdown

### Live Metrics
- **Real-time PnL**: Updated with each tick
- **Drawdown**: Current drawdown from peak equity
- **Position PnL**: Unrealized PnL for each open position
- **Trade PnL**: Realized PnL for each completed trade

## Parameter Tuning Guide

### General Approach

1. **Start with Paper Trading**: Always test new parameters in paper trading mode
2. **Single Parameter Changes**: Change one parameter at a time to isolate effects
3. **Use Backtesting**: Validate parameter changes with historical data
4. **Monitor Metrics**: Focus on Sharpe ratio and drawdown, not just returns

### Strategy-Specific Tuning

#### ORB Strategy
```yaml
strategies:
  ORB:
    parameters:
      range_minutes: 15      # Opening range duration
      breakout_threshold: 0.5  # Price must break by this % of range
      max_time_after_or: 120  # Minutes after opening range to trade
      atr_period: 14          # ATR period for volatility filtering
```

#### VWAP Mean Reversion
```yaml
strategies:
  VWAP_MR:
    parameters:
      lookback_period: 20    # Period for standard deviation calculation
      entry_threshold: 2.0  # Standard deviation threshold for entry
      exit_threshold: 0.5   # Standard deviation threshold for exit
      use_volume_filter: true  # Require volume confirmation
```

#### Momentum Burst
```yaml
strategies:
  MOMENTUM_BURST:
    parameters:
      consecutive_candles: 3   # Number of consecutive candles
      volume_multiplier: 2.0  # Volume must be > this * average volume
      price_break_pct: 0.5   # Price must break recent high/low by this %
      max_candle_age: 5      # Maximum age of candles to consider
```

#### EMA Crossover Scalper
```yaml
strategies:
  EMA_CROSSOVER:
    parameters:
      fast_ema_period: 9      # Fast EMA period
      slow_ema_period: 21    # Slow EMA period
      rsi_period: 14        # RSI period
      rsi_overbought: 70      # RSI overbought level
      rsi_oversold: 30        # RSI oversold level
      min_trend_strength: 0.1 # Minimum EMA separation for trade
```

#### Order Flow Imbalance
```yaml
strategies:
  ORDER_FLOW:
    parameters:
      lookback_ticks: 100    # Number of ticks to analyze
      imbalance_threshold: 0.6  # Bid/ask volume ratio threshold
      min_volume: 100       # Minimum volume for consideration
      confirmation_ticks: 5  # Number of ticks to confirm imbalance
```

#### Supertrend + ATR
```yaml
strategies:
  SUPERTREND:
    parameters:
      atr_period: 10        # ATR period for Supertrend
      multiplier: 3.0       # Supertrend multiplier
      use_atr_trailing: true # Use ATR-based trailing stop
      trailing_multiplier: 1.5 # Multiplier for trailing stop
      trend_filter_period: 50 # Period for trend filtering
```

### Risk Management Tuning

```yaml
risk:
  max_per_trade_risk_pct: 0.5  # 0.5% to 2.0% (higher = more aggressive)
  max_concurrent_positions: 5  # 1 to 10
  max_daily_loss_pct: 2.0      # 1.0% to 5.0%
  max_daily_profit_pct: 4.0    # 2.0% to 10.0%
  trailing_stop_atr_multiplier: 1.5  # 1.0 to 3.0
  max_spread_pct: 0.15         # 0.05% to 0.5%
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Install missing dependencies
   pip install -r requirements.txt
   ```

2. **Broker Connection Issues**
   - Verify API keys and credentials
   - Check network connectivity
   - Ensure broker API is accessible

3. **No Data Feeds**
   - Verify WebSocket connections
   - Check symbol names and formats
   - Ensure market is open

4. **No Trades Generated**
   - Check signal aggregation thresholds
   - Verify strategy configurations
   - Ensure market regime filter is not blocking all signals

5. **High Latency**
   - Use a VPS closer to broker servers
   - Check network latency
   - Reduce number of symbols
   - Use paper trading for testing

### Debug Mode

Enable debug logging for detailed information:

```bash
# Command line
python -m intraday_system.main paper --debug

# In config.yaml
logging:
  level: DEBUG
```

### Manual Kill Switch

To emergency stop the system:

```bash
# Create kill file
touch /tmp/KILL

# Or send SIGINT
pkill -f "python.*main.py"
```

## Monitoring

### Logs

- **Location**: `logs/trading.log`
- **Format**: JSON (structured) or text
- **Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL

### Trade Journal

- **Location**: `trade_journal.csv`
- **Columns**: trade_id, symbol, side, entry_price, exit_price, quantity, timestamps, PnL, fees, strategy, confidence, etc.

### Performance Metrics

- **Backtest Results**: `results/` directory
- **Equity Curve**: Plotted and saved as PNG
- **Trade-by-Trade**: CSV export

## Contributing

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/your-feature`)
3. **Make your changes**
4. **Run tests** (`pytest tests/`)
5. **Commit your changes** (`git commit -m 'Add your feature'`)
6. **Push to the branch** (`git push origin feature/your-feature`)
7. **Create a Pull Request**

### Code Quality Standards

- **Type Hints**: Use type hints everywhere (mypy clean)
- **Async I/O**: Use async/await for I/O-bound operations
- **Testing**: All modules must have unit tests
- **Documentation**: Include docstrings and comments for non-obvious logic
- **Error Handling**: Fail-safe defaults - if in doubt, DO NOT TRADE

## License

This project is proprietary and confidential. Do not distribute without permission.

## Disclaimer

**WARNING**: This is a trading system that can execute real trades with real money. 

- The developers are NOT responsible for any losses incurred
- Trading involves risk - only use funds you can afford to lose
- Always test thoroughly in paper trading mode before going live
- Monitor the system carefully, especially initially
- Have a kill-switch and emergency stop procedures in place

**NEVER** run live trading without:
1. ✅ Thorough backtesting
2. ✅ Paper trading validation
3. ✅ Proper risk management configuration
4. ✅ Monitoring and alerting setup
5. ✅ Emergency stop procedures

## Support

For issues or questions, please refer to the project documentation or contact the development team.

---

**Rapid Fire Intraday Trading System**
*Built for speed, precision, and profitability*