"""Wash trading detection.

Looks for volume that does not move price: high trade counts with near-zero
net flow, repeating sizes and tight round-trip timing.
"""

import pandas as pd

from sentinel.detectors.base import Detector, Signal


class WashTradingDetector(Detector):
    name = "wash_trading"

    def __init__(self, min_trades: int = 50, max_net_flow_pct: float = 0.02):
        self.min_trades = min_trades
        self.max_net_flow_pct = max_net_flow_pct

    def detect(self, features: pd.DataFrame) -> list[Signal]:
        raise NotImplementedError
