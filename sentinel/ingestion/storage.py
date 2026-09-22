"""Persistence for raw and processed market data."""

from pathlib import Path
from typing import Any


def write_raw(records: list[dict[str, Any]], name: str) -> Path:
    """Append ``records`` to the raw data store and return the path written."""
    raise NotImplementedError


def read_raw(name: str) -> list[dict[str, Any]]:
    """Read previously ingested raw records."""
    raise NotImplementedError
