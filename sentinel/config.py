"""Shared configuration for Sentinel.

Secrets come from the environment (see .env.example); everything else has a
sensible default so the package is importable with no setup.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Binance endpoints
BINANCE_REST_URL = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")

# Credentials are only needed for authenticated endpoints; public market data
# streams work without them.
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# Symbols to watch, e.g. SENTINEL_SYMBOLS="BTCUSDT,ETHUSDT"
SYMBOLS = [s.strip().upper() for s in os.getenv("SENTINEL_SYMBOLS", "BTCUSDT").split(",") if s.strip()]

LOG_LEVEL = os.getenv("SENTINEL_LOG_LEVEL", "INFO")
