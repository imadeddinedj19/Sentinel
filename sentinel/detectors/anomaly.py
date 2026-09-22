"""Unsupervised catch-all for patterns the rule-based detectors miss."""

import pandas as pd

from sentinel.detectors.base import Detector, Signal


class AnomalyDetector(Detector):
    """Isolation-forest style scoring over the full feature frame."""

    name = "anomaly"

    def __init__(self, contamination: float = 0.01, random_state: int = 42):
        self.contamination = contamination
        self.random_state = random_state

    def fit(self, features: pd.DataFrame) -> "AnomalyDetector":
        raise NotImplementedError

    def detect(self, features: pd.DataFrame) -> list[Signal]:
        raise NotImplementedError
