# Notebooks

`01_ingest_historical.py` is a **Databricks notebook** (source format, `# COMMAND ----------`
cell separators) and is the entry point: it populates the Delta tables everything else reads.
Import it into a workspace or sync the repo with Databricks Repos — it will not run as a plain
script, since it needs `spark` and `dbutils`.

The numbered `.ipynb` files are exploratory scratch space. Anything reusable belongs in the
`sentinel` package.

| Notebook | Purpose |
| --- | --- |
| `01_ingest_historical.py` | Download, checksum and load Binance monthly archives into Delta |
| `02_data_exploration.ipynb` | What the data actually looks like — gaps, quirks, distributions |
| `03_feature_analysis.ipynb` | Distributions and correlations of the engineered features |
| `04_detector_tuning.ipynb` | Thresholds and validation against known manipulation events |

## 01_ingest_historical

Reads from `data.binance.vision` and writes:

- `workspace.sentinel.agg_trades` — spot aggregated trades
- `workspace.sentinel.klines_1m` — spot 1-minute candles

Both are partitioned by `(symbol, month)`, with zips retained in
`/Volumes/workspace/sentinel/raw/`. Configuration is via notebook widgets; the defaults are
BTCUSDT and ETHUSDT for 2025-01.

Two things worth knowing before widening the range:

- **Timestamp units changed.** Binance spot data switched from milliseconds to microseconds at
  2025-01. The notebook normalises by magnitude rather than by date, so mixed-era ranges load
  correctly in one pass.
- **aggTrades archives are large.** BTCUSDT 2025-01 is 756 MB zipped, ETHUSDT 557 MB, and both
  are downloaded and unzipped on the driver. Klines are ~2 MB and cheap by comparison.

Re-running is safe: verified zips are not re-downloaded and each `(symbol, month)` partition is
replaced rather than appended to.

## Local notebooks

The `.ipynb` files assume the repo root is on the path:

```python
import sys; sys.path.append("..")
from sentinel import config
```
