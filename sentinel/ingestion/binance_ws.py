"""Live market data from the Binance websocket streams.

Streams of interest for manipulation detection: ``@trade`` (prints),
``@depth`` (order book deltas, for spoofing/layering) and ``@kline``.
"""

from collections.abc import AsyncIterator, Iterable
from typing import Any


async def stream_trades(symbols: Iterable[str]) -> AsyncIterator[dict[str, Any]]:
    """Yield trade events for each symbol as they arrive."""
    raise NotImplementedError


async def stream_depth(symbols: Iterable[str]) -> AsyncIterator[dict[str, Any]]:
    """Yield order book diff events for each symbol as they arrive."""
    raise NotImplementedError
