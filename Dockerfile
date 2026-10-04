# Rapid Fire Intraday Trading System
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Kolkata

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY intraday_system/ ./intraday_system/
COPY config.yaml run_backtest.py ./

RUN mkdir -p /app/data/historical /app/logs /app/results

VOLUME ["/app/data", "/app/logs", "/app/results"]

# Streamlit dashboard
EXPOSE 8501

# Healthy while the trading process keeps refreshing its status file (every 5 s)
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import os, sys, time; p='data/status.json'; sys.exit(0 if os.path.exists(p) and time.time() - os.path.getmtime(p) < 60 else 1)"

# Default: paper trading. Credentials come from environment variables
# (ZERODHA_API_KEY, ZERODHA_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, ...), never the image.
CMD ["python", "-m", "intraday_system.main", "paper"]

# Other entry points:
#   Backtest:  docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/results:/app/results IMAGE \
#                python run_backtest.py --synthetic --days 20
#   Dashboard: docker run --rm -p 8501:8501 -v $(pwd)/data:/app/data -v $(pwd)/results:/app/results IMAGE \
#                streamlit run intraday_system/dashboard.py --server.address 0.0.0.0
#   Live:      docker run -e ZERODHA_API_KEY=... -e ZERODHA_ACCESS_TOKEN=... IMAGE \
#                python -m intraday_system.main live --confirm-live
