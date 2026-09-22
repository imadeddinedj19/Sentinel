"""Historical market data from the Binance REST API.

Used to backfill klines, aggregate trades and order book snapshots that the
pipeline turns into features.
"""

from typing import Any


def fetch_klines(symbol: str, interval: str = "1m", limit: int = 1000) -> list[dict[str, Any]]:
    """Fetch candlestick data for ``symbol``."""
    raise NotImplementedError


def fetch_agg_trades(symbol: str, start_ms: int | None = None, limit: int = 1000) -> list[dict[str, Any]]:
    """Fetch aggregated trades for ``symbol``."""
    raise NotImplementedError


def fetch_order_book(symbol: str, depth: int = 100) -> dict[str, Any]:
    """Fetch a point-in-time order book snapshot for ``symbol``."""
    raise NotImplementedError
