"""Pump-and-dump detection.

Looks for the classic shape: abnormal volume and price spike over a short
window, followed by a rapid retracement toward the pre-spike level.
"""

import pandas as pd

from sentinel.detectors.base import Detector, Signal


class PumpAndDumpDetector(Detector):
    name = "pump_and_dump"

    def __init__(self, price_sigma: float = 4.0, volume_sigma: float = 5.0, retrace_pct: float = 0.5):
        self.price_sigma = price_sigma
        self.volume_sigma = volume_sigma
        self.retrace_pct = retrace_pct

    def detect(self, features: pd.DataFrame) -> list[Signal]:
        raise NotImplementedError
