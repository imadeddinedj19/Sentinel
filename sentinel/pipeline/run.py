"""Entry point tying ingestion, features and detectors together."""

import argparse

from sentinel import config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Sentinel detection pipeline.")
    parser.add_argument("--symbols", nargs="*", default=config.SYMBOLS, help="Symbols to analyse.")
    parser.add_argument("--mode", choices=["backfill", "live"], default="backfill")
    args = parser.parse_args()

    raise NotImplementedError(f"Pipeline not implemented yet (mode={args.mode}, symbols={args.symbols}).")


if __name__ == "__main__":
    main()
