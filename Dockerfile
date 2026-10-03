# Rapid Fire Intraday Trading System
# Python 3.11+ based Docker container for production deployment

# Use official Python image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code
COPY intraday_system/ ./intraday_system/
COPY config.yaml .

# Create necessary directories
RUN mkdir -p /app/data/historical \
    /app/data/logs \
    /app/results \
    /tmp

# Set volume for persistent data
VOLUME ["/app/data", "/app/results", "/app/logs"]

# Expose ports (for dashboard and potential APIs)
EXPOSE 8501  # Streamlit dashboard
EXPOSE 8080  # Potential REST API

# Environment variables (can be overridden with docker run -e)
ENV TRADING_MODE="PAPER" \
    LOG_LEVEL="INFO" \
    DASHBOARD_ENABLED="true"

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.path.append('intraday_system'); from logger import get_logger; logger = get_logger('healthcheck'); logger.info('Health check passed')" || exit 1

# Default command (run paper trading)
CMD ["python", "-m", "intraday_system.main", "paper"]

# Alternative commands:
# - Backtest: docker run ... python -m intraday_system.main backtest
# - Live: docker run ... python -m intraday_system.main live
# - Dashboard: docker run ... streamlit run intraday_system/dashboard.py

# Build with: docker build -t rapid-fire-trading-system .
# Run with: docker run -v $(pwd)/data:/app/data rapid-fire-trading-system

# For development with hot reloading:
# docker run -v $(pwd):/app -v $(pwd)/data:/app/data rapid-fire-trading-system