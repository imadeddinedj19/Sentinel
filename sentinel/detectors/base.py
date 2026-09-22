"""Common interface every detector implements."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd


@dataclass
class Signal:
    """A single suspected manipulation event."""

    symbol: str
    timestamp: datetime
    detector: str
    score: float  # 0-1, higher is more suspicious
    evidence: dict[str, Any] = field(default_factory=dict)


class Detector(ABC):
    """Base class for detectors.

    A detector takes an engineered feature frame and returns the events it
    considers suspicious, scored so results from different detectors can be
    ranked against each other.
    """

    name: str = "detector"

    @abstractmethod
    def detect(self, features: pd.DataFrame) -> list[Signal]:
        """Return the signals found in ``features``."""
