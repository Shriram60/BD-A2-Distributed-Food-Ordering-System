"""Spark Job 1 (VM4): clean and enrich landed orders -> order_metrics.jsonl.

Usage: python vm4/spark_job1.py [--input PATH] [--output PATH]
Implements task A3 in team/02_ARNAV_VM4.md; contract in CLAUDE.md sections 4-6.
"""
import argparse
import glob
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

HOME = os.path.expanduser("~")
DEFAULT_INPUT = os.path.join(HOME, "bd-a2/data/orders_landed.jsonl")
DEFAULT_OUTPUT = os.path.join(HOME, "bd-a2/data/order_metrics.jsonl")
STATS_PATH = os.path.join(HOME, "bd-a2/stats/spark_job1_stats.json")

# Timestamp format found in task A2 (CLAUDE.md section 6, TS_FORMAT).
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

# The 14 input fields (CLAUDE.md section 5.1), all read as strings.
INPUT_FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp",
    "distance_km", "order_amount", "delivery_fee", "status",
]
TS_FIELDS = ["order_timestamp", "accepted_timestamp", "picked_up_timestamp",
             "expected_delivery_timestamp", "delivered_timestamp"]
NUM_FIELDS = ["distance_km", "order_amount", "delivery_fee"]

# The 15 output keys in contract order (CLAUDE.md section 5.2).
OUTPUT_FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_amount", "delivery_fee", "distance_km",
    "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
    "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category",
]


def build_spark():
    spark = (
        SparkSession.builder
        .appName("spark_job1")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
        # Unparseable timestamps become null instead of raising SparkUpgradeException.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def any_of(conditions):
    return reduce(lambda a, b: a | b, conditions)


def local_uri(path):
    """Force the local filesystem even if a Hadoop config sets fs.defaultFS to HDFS."""
    return "file://" + os.path.abspath(path)


def minutes_between(later, earlier):
    return (F.col(later).cast("long") - F.col(earlier).cast("long")) / 60.0


def write_single_json(df, output):
    """Write df as exactly one JSON-lines file at `output` (A3 step 12)."""
    out_dir = os.path.dirname(os.path.abspath(output))
    os.makedirs(out_dir, exist_ok=True)
    tmp_dir = os.path.join(out_dir, "_spark_job1_tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    (df.coalesce(1).write.mode("overwrite")
       .option("ignoreNullFields", "false")
       .json(local_uri(tmp_dir)))
    parts = glob.glob(os.path.join(tmp_dir, "part-*.json"))
    if len(parts) > 1:
        raise RuntimeError(f"expected one part file, found {len(parts)}")
    if parts:
        shutil.move(parts[0], output)
    else:  # no rows survived: still leave an empty output file
        open(output, "w").close()
    shutil.rmtree(tmp_dir, ignore_errors=True)


def run(input_path, output_path):
    spark = build_spark()
    stats = {}

    # 1. Read raw lines.
    raw = spark.read.text(local_uri(input_path)).cache()
    stats["total_lines"] = raw.count()

    # 2. Parse; drop lines that are not JSON objects or carry none of the 14 fields.
    schema = StructType([StructField(f, StringType(), True) for f in INPUT_FIELDS])
    parsed = raw.select(
        F.col("value"),
        # SQL form works on Spark 3.1+; F.json_object_keys only exists from PySpark 3.5.
        F.expr("json_object_keys(value)").isNull().alias("_bad_json"),
        F.from_json("value", schema).alias("r"),
    )
    all_null = reduce(lambda a, b: a & b, [F.col(f"r.{f}").isNull() for f in INPUT_FIELDS])
    malformed = F.col("r").isNull() | all_null
    counts = parsed.filter(malformed).groupBy("_bad_json").count().collect()
    stats["malformed_json_lines"] = sum(c["count"] for c in counts if c["_bad_json"])
    stats["empty_json_records"] = sum(c["count"] for c in counts if not c["_bad_json"])
    stats["dropped_malformed"] = stats["malformed_json_lines"] + stats["empty_json_records"]
    df = parsed.filter(~malformed).select("r.*").cache()
    remaining = stats["total_lines"] - stats["dropped_malformed"]

    # 3 + 4. Required fields present and non-blank (D5); numeric fields parse (D5, D9).
    blank = any_of([F.col(f).isNull() | (F.trim(F.col(f)) == "") for f in INPUT_FIELDS])
    non_numeric = any_of([F.col(f).cast("double").isNull() for f in NUM_FIELDS])
    df = df.filter(~(blank | non_numeric)).cache()
    n = df.count()
    stats["dropped_missing_or_invalid_field"] = remaining - n
    remaining = n

    # 5. Timestamps must parse with TS_FORMAT.
    for f in TS_FIELDS:
        df = df.withColumn(f"_ts_{f}", F.to_timestamp(F.col(f), TS_FORMAT))
    df = df.filter(~any_of([F.col(f"_ts_{f}").isNull() for f in TS_FIELDS])).cache()
    n = df.count()
    stats["dropped_invalid_timestamp"] = remaining - n
    remaining = n

    # 6. distance_km <= 0.
    df = df.filter(~(F.col("distance_km").cast("double") <= 0)).cache()
    n = df.count()
    stats["dropped_nonpositive_distance"] = remaining - n
    remaining = n

    # 7. order_amount <= 0.
    df = df.filter(~(F.col("order_amount").cast("double") <= 0)).cache()
    n = df.count()
    stats["dropped_nonpositive_amount"] = remaining - n
    remaining = n

    # 8. Only exactly "DELIVERED" (D6).
    df = df.filter(F.col("status") == "DELIVERED").cache()
    n = df.count()
    stats["dropped_not_delivered"] = remaining - n
    remaining = n

    # 9. Exact duplicates on the 14 original string fields (D4).
    df = df.dropDuplicates(INPUT_FIELDS).cache()
    n = df.count()
    stats["dropped_duplicates"] = remaining - n

    # 10. Enrich. Status, category and speed use unrounded values (D2, D3).
    df = (
        df.withColumn("_prep", minutes_between("_ts_picked_up_timestamp", "_ts_accepted_timestamp"))
          .withColumn("_deliv", minutes_between("_ts_delivered_timestamp", "_ts_picked_up_timestamp"))
          .withColumn("_fulfil", minutes_between("_ts_delivered_timestamp", "_ts_order_timestamp"))
          .withColumn("_delay", F.greatest(
              F.lit(0.0),
              minutes_between("_ts_delivered_timestamp", "_ts_expected_delivery_timestamp")))
          .withColumn("_speed", F.when(
              F.col("_deliv") > 0,
              F.col("distance_km").cast("double") / (F.col("_deliv") / 60.0)))
    )
    df = df.select(
        *[F.col(f) for f in ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city"]],
        F.col("order_amount").cast("double").alias("order_amount"),
        F.col("delivery_fee").cast("double").alias("delivery_fee"),
        F.col("distance_km").cast("double").alias("distance_km"),
        F.round("_prep", 2).alias("preparation_time_min"),
        F.round("_deliv", 2).alias("delivery_time_min"),
        F.round("_fulfil", 2).alias("fulfillment_time_min"),
        F.round("_speed", 2).alias("delivery_speed_kmph"),
        F.round("_delay", 2).alias("delay_min"),
        F.when(F.col("_delay") == 0, "ON_TIME").otherwise("DELAYED").alias("delivery_status"),
        F.when(F.col("_delay") == 0, "ON_TIME")
         .when(F.col("_delay") <= 15, "SLIGHT_DELAY")
         .when(F.col("_delay") <= 30, "MODERATE_DELAY")
         .otherwise("SEVERE_DELAY").alias("delay_category"),
    )

    # 11. Exact contract column order; sorted so reruns produce identical files.
    df = df.select(*OUTPUT_FIELDS).orderBy("order_id")

    # 12. One output file.
    write_single_json(df, output_path)
    with open(output_path) as fh:
        stats["output_records"] = sum(1 for line in fh if line.strip())
    spark.stop()

    # 13. Stats.
    drop_keys = ["dropped_malformed", "dropped_missing_or_invalid_field",
                 "dropped_invalid_timestamp", "dropped_nonpositive_distance",
                 "dropped_nonpositive_amount", "dropped_not_delivered", "dropped_duplicates"]
    stats["total_dropped"] = sum(stats[k] for k in drop_keys)
    stats["input_path"] = os.path.abspath(input_path)
    stats["output_path"] = os.path.abspath(output_path)
    stats["finished_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    os.makedirs(os.path.dirname(STATS_PATH), exist_ok=True)
    with open(STATS_PATH, "w") as fh:
        json.dump(stats, fh, indent=2)

    print("spark_job1 stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    assert stats["total_lines"] == stats["total_dropped"] + stats["output_records"], \
        "counts do not add up: total_lines != sum(drops) + output_records"
    print(f"OK: {stats['total_lines']} lines = {stats['total_dropped']} dropped "
          f"+ {stats['output_records']} written to {output_path}")


def main():
    ap = argparse.ArgumentParser(description="Spark Job 1: clean + enrich orders")
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    args = ap.parse_args()
    if not os.path.exists(args.input):
        sys.exit(f"input not found: {args.input}")
    run(os.path.expanduser(args.input), os.path.expanduser(args.output))


if __name__ == "__main__":
    main()
