# Sentinel

Real-time cryptocurrency market manipulation detection using streaming data and machine learning.

Sentinel ingests public Binance market data, turns it into price, flow and order
book features, and runs a set of detectors over those features to flag suspected
pump-and-dump, spoofing and wash trading activity.

## Layout

```
sentinel/
  config.py          # paths, endpoints, symbols, env-backed settings
  ingestion/         # Binance REST backfill, websocket streams, storage
  pipeline/          # cleaning, feature engineering, run entry point
  detectors/         # one module per manipulation pattern
notebooks/           # exploratory analysis and threshold tuning
data/                # raw/ and processed/ (git-ignored)
```

Every detector implements the `Detector` interface in
[sentinel/detectors/base.py](sentinel/detectors/base.py) and returns scored
`Signal` objects, so results from different detectors can be ranked together.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
cp .env.example .env          # optional; public market data needs no keys
```

## Running

```bash
python -m sentinel.pipeline.run --symbols BTCUSDT ETHUSDT --mode backfill
```

## Status

Scaffolding. Module interfaces are defined; implementations are stubs.
