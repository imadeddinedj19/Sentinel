"""Normalise raw Binance payloads into tidy frames.

Handles the awkward parts of exchange data: string-typed numerics, millisecond
timestamps, duplicate events on reconnect and out-of-order depth updates.
"""

import pandas as pd


def normalize_trades(raw: list[dict]) -> pd.DataFrame:
    """Return a trades frame indexed by event time."""
    raise NotImplementedError


def normalize_klines(raw: list[dict]) -> pd.DataFrame:
    """Return an OHLCV frame indexed by open time."""
    raise NotImplementedError
