"""Spoofing and layering detection.

Looks for large resting orders that move the book's imbalance and are then
cancelled without being filled, repeatedly and on one side.
"""

import pandas as pd

from sentinel.detectors.base import Detector, Signal


class SpoofingDetector(Detector):
    name = "spoofing"

    def __init__(self, min_size_pct: float = 0.1, max_lifetime_s: float = 2.0, min_events: int = 3):
        self.min_size_pct = min_size_pct
        self.max_lifetime_s = max_lifetime_s
        self.min_events = min_events

    def detect(self, features: pd.DataFrame) -> list[Signal]:
        raise NotImplementedError
