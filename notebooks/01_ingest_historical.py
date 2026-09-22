# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel — historical Binance ingest
# MAGIC
# MAGIC Downloads Binance **spot** monthly archives from `data.binance.vision`, verifies the
# MAGIC published SHA256 checksums, keeps the zips in a Unity Catalog volume, and parses them
# MAGIC into Delta tables.
# MAGIC
# MAGIC | Dataset | Source | Target table |
# MAGIC | --- | --- | --- |
# MAGIC | aggTrades | `/data/spot/monthly/aggTrades/{symbol}/` | `workspace.sentinel.agg_trades` |
# MAGIC | 1m klines | `/data/spot/monthly/klines/{symbol}/1m/` | `workspace.sentinel.klines_1m` |
# MAGIC
# MAGIC ### Timestamp units
# MAGIC
# MAGIC Binance switched spot market data timestamps from **milliseconds to microseconds** for
# MAGIC files dated 2025-01 onward. Rather than branching on the month, every timestamp column is
# MAGIC normalised by magnitude: anything above `1e14` is microseconds and gets integer-divided by
# MAGIC 1000. A millisecond epoch only crosses `1e14` in the year 5138, and a microsecond epoch was
# MAGIC already past it in 1973, so the two ranges cannot be confused for any realistic date.
# MAGIC
# MAGIC Each table keeps both the normalised `*_millis` bigint and a derived `TIMESTAMP` column.
# MAGIC
# MAGIC ### Re-running
# MAGIC
# MAGIC Safe to re-run. Zips with a matching checksum are not re-downloaded, and each
# MAGIC `(symbol, month)` partition is deleted before its rows are appended, so a partial run can
# MAGIC simply be started again.
# MAGIC
# MAGIC ### Sizing
# MAGIC
# MAGIC The klines archives are small (~2 MB/symbol/month), but monthly aggTrades are not:
# MAGIC BTCUSDT 2025-01 is **756 MB** zipped and ETHUSDT 2025-01 is **557 MB**, expanding to
# MAGIC several GB of CSV each. Downloading and unzipping both happens on the driver, so give it
# MAGIC enough local disk before widening the symbol list or date range. To smoke-test the
# MAGIC pipeline cheaply, run with `symbols = ETHUSDT` first.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("symbols", "BTCUSDT,ETHUSDT", "1. Symbols (comma separated)")
dbutils.widgets.text("start_month", "2025-01", "2. Start month (YYYY-MM)")
dbutils.widgets.text("end_month", "2025-01", "3. End month (YYYY-MM, inclusive)")
dbutils.widgets.text("catalog", "workspace", "4. Catalog")
dbutils.widgets.text("schema", "sentinel", "5. Schema")
dbutils.widgets.text("volume", "raw", "6. Raw volume")

SYMBOLS = [s.strip().upper() for s in dbutils.widgets.get("symbols").split(",") if s.strip()]
START_MONTH = dbutils.widgets.get("start_month").strip()
END_MONTH = dbutils.widgets.get("end_month").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()

RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
EXTRACT_DIR = f"{RAW_DIR}/_extracted"

AGG_TRADES_TABLE = f"{CATALOG}.{SCHEMA}.agg_trades"
KLINES_TABLE = f"{CATALOG}.{SCHEMA}.klines_1m"

KLINE_INTERVAL = "1m"
BASE_URL = "https://data.binance.vision/data/spot/monthly"

print(f"symbols  : {SYMBOLS}")
print(f"months   : {START_MONTH} .. {END_MONTH}")
print(f"raw dir  : {RAW_DIR}")
print(f"tables   : {AGG_TRADES_TABLE}, {KLINES_TABLE}")

# COMMAND ----------

# MAGIC %md ## Imports and helpers

# COMMAND ----------

import hashlib
import os
import shutil
import zipfile
from datetime import datetime

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

CHUNK_BYTES = 1 << 20  # 1 MiB
HTTP_TIMEOUT = 300

# Above this value an epoch must be microseconds; below it, milliseconds. See the header note.
MICROS_THRESHOLD = 100_000_000_000_000


def month_range(start: str, end: str) -> list:
    """Inclusive list of ``YYYY-MM`` strings from ``start`` to ``end``."""
    s = datetime.strptime(start, "%Y-%m")
    e = datetime.strptime(end, "%Y-%m")
    if (e.year, e.month) < (s.year, s.month):
        raise ValueError(f"end_month {end} is before start_month {start}")
    months, year, month = [], s.year, s.month
    while (year, month) <= (e.year, e.month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return months


def archive_url(dataset: str, symbol: str, month: str):
    """Return ``(url, filename)`` for a monthly archive."""
    if dataset == "aggTrades":
        filename = f"{symbol}-aggTrades-{month}.zip"
        return f"{BASE_URL}/aggTrades/{symbol}/{filename}", filename
    if dataset == "klines":
        filename = f"{symbol}-{KLINE_INTERVAL}-{month}.zip"
        return f"{BASE_URL}/klines/{symbol}/{KLINE_INTERVAL}/{filename}", filename
    raise ValueError(f"unknown dataset: {dataset}")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(CHUNK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_expected_sha256(url: str):
    """Read the published checksum. ``None`` means the archive is not on the server."""
    response = requests.get(f"{url}.CHECKSUM", timeout=HTTP_TIMEOUT)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    # Format is "<sha256>  <filename>"
    return response.text.split()[0].strip().lower()


def download(url: str, dest: str) -> None:
    try:
        with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as response:
            response.raise_for_status()
            with open(dest, "wb") as handle:
                for block in response.iter_content(CHUNK_BYTES):
                    handle.write(block)
    except BaseException:
        # Never leave a half-written archive behind for the next run to trust.
        if os.path.exists(dest):
            os.remove(dest)
        raise


def ensure_archive(dataset: str, symbol: str, month: str) -> dict:
    """Download and verify one archive, skipping work already done.

    Returns a status dict whose ``status`` is ``cached``, ``downloaded`` or ``unavailable``.
    """
    url, filename = archive_url(dataset, symbol, month)
    dest = f"{RAW_DIR}/{filename}"
    result = {"dataset": dataset, "symbol": symbol, "month": month, "file": filename, "path": dest}

    expected = fetch_expected_sha256(url)
    if expected is None:
        return {**result, "status": "unavailable", "bytes": 0}

    if os.path.exists(dest) and sha256_file(dest) == expected:
        return {**result, "status": "cached", "bytes": os.path.getsize(dest)}

    download(url, dest)
    actual = sha256_file(dest)
    if actual != expected:
        os.remove(dest)
        raise ValueError(f"SHA256 mismatch for {filename}: expected {expected}, got {actual}")

    return {**result, "status": "downloaded", "bytes": os.path.getsize(dest)}


def extract_csv(zip_path: str) -> str:
    """Extract the single CSV member of ``zip_path`` and return its path."""
    stem = os.path.splitext(os.path.basename(zip_path))[0]
    out_dir = f"{EXTRACT_DIR}/{stem}"
    os.makedirs(out_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one CSV in {zip_path}, found {members}")
        target = f"{out_dir}/{os.path.basename(members[0])}"
        if not os.path.exists(target):
            with archive.open(members[0]) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK_BYTES)
    return target


def has_header_row(csv_path: str) -> bool:
    """Binance added header rows to these CSVs in 2025, so detect rather than assume.

    Checking only the first field avoids false positives from the ``true``/``false``
    booleans that appear later in every aggTrades data row.
    """
    with open(csv_path, "r", encoding="utf-8") as handle:
        first_field = handle.readline().split(",")[0].strip().strip('"')
    try:
        int(first_field)
    except ValueError:
        return True
    return False


def to_millis(column: str):
    """Normalise a raw epoch column to milliseconds regardless of its source unit."""
    return F.expr(
        f"CASE WHEN {column} > {MICROS_THRESHOLD} THEN {column} div 1000 ELSE {column} END"
    ).cast("long")


def to_bool(column: str):
    """Parse a Binance CSV boolean.

    These files write Python-style ``True``/``False`` rather than the lowercase
    ``true``/``false`` a ``BooleanType`` read expects, so the columns are ingested as
    strings and converted here. Anything unrecognised becomes NULL and is caught by the
    validation cell at the end rather than silently reading as ``false``.
    """
    normalised = F.lower(F.trim(F.col(column)))
    return (
        F.when(normalised.isin("true", "1"), F.lit(True))
        .when(normalised.isin("false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )

# COMMAND ----------

# MAGIC %md ## Target schema, volume and tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {AGG_TRADES_TABLE} (
        symbol          STRING    COMMENT 'Trading pair, e.g. BTCUSDT',
        month           STRING    COMMENT 'Source archive month, YYYY-MM',
        agg_trade_id    BIGINT,
        price           DOUBLE,
        quantity        DOUBLE,
        first_trade_id  BIGINT,
        last_trade_id   BIGINT,
        transact_millis BIGINT    COMMENT 'Epoch milliseconds, normalised from ms or us source',
        transact_time   TIMESTAMP,
        is_buyer_maker  BOOLEAN,
        is_best_match   BOOLEAN
    )
    USING DELTA
    PARTITIONED BY (symbol, month)
    COMMENT 'Binance spot aggregated trades from data.binance.vision monthly archives'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {KLINES_TABLE} (
        symbol          STRING    COMMENT 'Trading pair, e.g. BTCUSDT',
        month           STRING    COMMENT 'Source archive month, YYYY-MM',
        open_millis     BIGINT    COMMENT 'Epoch milliseconds, normalised from ms or us source',
        open_time       TIMESTAMP,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE,
        volume          DOUBLE,
        close_millis    BIGINT    COMMENT 'Epoch milliseconds, normalised from ms or us source',
        close_time      TIMESTAMP,
        quote_volume    DOUBLE,
        trade_count     BIGINT,
        taker_buy_base  DOUBLE,
        taker_buy_quote DOUBLE
    )
    USING DELTA
    PARTITIONED BY (symbol, month)
    COMMENT 'Binance spot 1-minute klines from data.binance.vision monthly archives'
    """
)

print("schema, volume and tables ready")

# COMMAND ----------

# MAGIC %md ## Download and verify archives

# COMMAND ----------

MONTHS = month_range(START_MONTH, END_MONTH)
downloads = []

for symbol in SYMBOLS:
    for month in MONTHS:
        for dataset in ("aggTrades", "klines"):
            outcome = ensure_archive(dataset, symbol, month)
            downloads.append(outcome)
            size_mb = outcome["bytes"] / 1e6
            print(f"{outcome['status']:>12}  {outcome['file']:<34} {size_mb:8.1f} MB")

unavailable = [d for d in downloads if d["status"] == "unavailable"]
if unavailable:
    print(f"\n{len(unavailable)} archive(s) not published on data.binance.vision:")
    for item in unavailable:
        print(f"  - {item['file']}")

display(spark.createDataFrame(downloads))

# COMMAND ----------

# MAGIC %md ## Parse into Delta
# MAGIC
# MAGIC The CSVs are read with an explicit positional schema, since these files have no header
# MAGIC before 2025 and a header from 2025 onward. Each `(symbol, month)` partition is deleted
# MAGIC and re-appended so reruns stay idempotent.

# COMMAND ----------

AGG_TRADES_CSV_SCHEMA = StructType(
    [
        StructField("agg_trade_id", LongType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("first_trade_id", LongType()),
        StructField("last_trade_id", LongType()),
        StructField("transact_raw", LongType()),
        # Read as strings: the files contain "True"/"False", which a BooleanType read
        # would reject under FAILFAST. Converted by to_bool() below.
        StructField("is_buyer_maker_raw", StringType()),
        StructField("is_best_match_raw", StringType()),
    ]
)

KLINES_CSV_SCHEMA = StructType(
    [
        StructField("open_raw", LongType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("close_raw", LongType()),
        StructField("quote_volume", DoubleType()),
        StructField("trade_count", LongType()),
        StructField("taker_buy_base", DoubleType()),
        StructField("taker_buy_quote", DoubleType()),
        StructField("ignore", StringType()),
    ]
)


def read_csv(csv_path: str, schema: StructType):
    return (
        spark.read.option("header", has_header_row(csv_path))
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(csv_path)
    )


def load_partition(df, table: str, symbol: str, month: str) -> int:
    """Replace one ``(symbol, month)`` partition with ``df`` and return the row count.

    The count is taken from the table after the write rather than from the DataFrame.
    A monthly aggTrades file holds hundreds of millions of rows, so counting first
    would mean either scanning the CSV twice or caching a frame far too large to hold.
    """
    df = df.withColumn("symbol", F.lit(symbol)).withColumn("month", F.lit(month))
    target_columns = [field.name for field in spark.table(table).schema.fields]

    spark.sql(f"DELETE FROM {table} WHERE symbol = '{symbol}' AND month = '{month}'")
    df.select(*target_columns).write.format("delta").mode("append").saveAsTable(table)

    return spark.sql(
        f"SELECT COUNT(*) AS n FROM {table} WHERE symbol = '{symbol}' AND month = '{month}'"
    ).collect()[0]["n"]


loaded = []

for item in downloads:
    if item["status"] == "unavailable":
        continue

    symbol, month = item["symbol"], item["month"]
    csv_path = extract_csv(item["path"])

    if item["dataset"] == "aggTrades":
        raw = read_csv(csv_path, AGG_TRADES_CSV_SCHEMA)
        parsed = (
            raw.withColumn("transact_millis", to_millis("transact_raw"))
            .withColumn("transact_time", F.expr("timestamp_millis(transact_millis)"))
            .withColumn("is_buyer_maker", to_bool("is_buyer_maker_raw"))
            .withColumn("is_best_match", to_bool("is_best_match_raw"))
        )
        target_table = AGG_TRADES_TABLE
    else:
        raw = read_csv(csv_path, KLINES_CSV_SCHEMA)
        parsed = (
            raw.withColumn("open_millis", to_millis("open_raw"))
            .withColumn("close_millis", to_millis("close_raw"))
            .withColumn("open_time", F.expr("timestamp_millis(open_millis)"))
            .withColumn("close_time", F.expr("timestamp_millis(close_millis)"))
        )
        target_table = KLINES_TABLE

    rows = load_partition(parsed, target_table, symbol, month)
    loaded.append({"table": target_table, "symbol": symbol, "month": month, "rows": rows})
    print(f"loaded {rows:>12,} rows into {target_table} for {symbol} {month}")

display(spark.createDataFrame(loaded))

# COMMAND ----------

# MAGIC %md ## Validate
# MAGIC
# MAGIC The timestamp bounds are the real check on the microsecond normalisation: if a month's
# MAGIC `min`/`max` land inside that calendar month, the units were interpreted correctly. An
# MAGIC unconverted microsecond value would land some 54,000 years in the future.

# COMMAND ----------

for table in (AGG_TRADES_TABLE, KLINES_TABLE):
    time_column = "transact_time" if table == AGG_TRADES_TABLE else "open_time"
    print(f"\n=== {table} ===")
    spark.sql(
        f"""
        SELECT symbol,
               month,
               COUNT(*)           AS rows,
               MIN({time_column}) AS first_event,
               MAX({time_column}) AS last_event
        FROM {table}
        GROUP BY symbol, month
        ORDER BY symbol, month
        """
    ).show(truncate=False)

# COMMAND ----------

# Expect zero rows everywhere: a timestamp outside its own archive month means a units
# bug, and a NULL boolean means to_bool() met a spelling the source files have not used
# so far.
spark.sql(
    f"""
    SELECT 'agg_trades' AS source_table, 'timestamp outside archive month' AS check,
           symbol, month, COUNT(*) AS bad_rows
    FROM {AGG_TRADES_TABLE}
    WHERE DATE_FORMAT(transact_time, 'yyyy-MM') <> month
    GROUP BY symbol, month
    UNION ALL
    SELECT 'klines_1m', 'timestamp outside archive month', symbol, month, COUNT(*)
    FROM {KLINES_TABLE}
    WHERE DATE_FORMAT(open_time, 'yyyy-MM') <> month
    GROUP BY symbol, month
    UNION ALL
    SELECT 'agg_trades', 'unparsed boolean', symbol, month, COUNT(*)
    FROM {AGG_TRADES_TABLE}
    WHERE is_buyer_maker IS NULL OR is_best_match IS NULL
    GROUP BY symbol, month
    UNION ALL
    SELECT 'klines_1m', 'kline not 60s long', symbol, month, COUNT(*)
    FROM {KLINES_TABLE}
    WHERE close_millis - open_millis <> 59999
    GROUP BY symbol, month
    """
).show(truncate=False)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {AGG_TRADES_TABLE} ORDER BY transact_time LIMIT 20"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {KLINES_TABLE} ORDER BY open_time LIMIT 20"))
