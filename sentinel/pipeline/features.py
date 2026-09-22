"""Feature engineering for the detectors.

Features fall into three groups: price/return statistics, volume and trade
flow statistics, and order book shape statistics.
"""

import pandas as pd


def price_features(trades: pd.DataFrame, window: str = "5min") -> pd.DataFrame:
    """Rolling returns, realised volatility and price z-scores."""
    raise NotImplementedError


def volume_features(trades: pd.DataFrame, window: str = "5min") -> pd.DataFrame:
    """Rolling volume, trade count and buy/sell imbalance."""
    raise NotImplementedError


def order_book_features(depth: pd.DataFrame) -> pd.DataFrame:
    """Spread, depth imbalance and order cancellation rates."""
    raise NotImplementedError
