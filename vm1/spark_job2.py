"""Spark Job 2 (VM1): order_metrics_landed.jsonl -> restaurant_summary.csv + city_summary.csv.

Usage: python vm1/spark_job2.py [--input PATH] [--outdir DIR]
Implements task M4 in team/03_MONTY_VM1.md; contract in CLAUDE.md sections 4-6.
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
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

HOME = os.path.expanduser("~")
DEFAULT_INPUT = os.path.join(HOME, "bd-a2/data/order_metrics_landed.jsonl")
DEFAULT_OUTDIR = os.path.join(HOME, "bd-a2/output")
STATS_PATH = os.path.join(HOME, "bd-a2/stats/spark_job2_stats.json")

# The 15 enriched keys in contract order (CLAUDE.md section 5.2).
STRING_FIELDS = {"order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
                 "delivery_status", "delay_category"}
FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_amount", "delivery_fee", "distance_km",
    "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
    "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category",
]
SCHEMA = StructType([
    StructField(f, StringType() if f in STRING_FIELDS else DoubleType(), True) for f in FIELDS
])

# Output columns in contract order (CLAUDE.md sections 5.3 and 5.4).
RESTAURANT_COLUMNS = ["restaurant_id", "order_count", "avg_order_amount",
                      "avg_preparation_time_min", "avg_delivery_time_min",
                      "delayed_order_count", "delay_rate_pct"]
CITY_COLUMNS = ["city", "order_count", "avg_order_amount", "avg_delivery_time_min",
                "delayed_order_count", "delay_rate_pct"]


def build_spark():
    spark = (
        SparkSession.builder
        .appName("spark_job2")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def local_uri(path):
    """Force the local filesystem even if a Hadoop config sets fs.defaultFS to HDFS."""
    return "file://" + os.path.abspath(path)


def summarize(df, key, avg_fields, columns):
    """Group by `key`: count, 2 dp averages, delayed count and delay rate (CLAUDE.md 5.3/5.4)."""
    out = df.groupBy(key).agg(
        F.count(F.lit(1)).alias("order_count"),
        *[F.avg(f).alias(f"avg_{f}") for f in avg_fields],
        F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0))
         .alias("delayed_order_count"),
    )
    out = out.withColumn("delay_rate_pct",
                         F.round(F.lit(100.0) * F.col("delayed_order_count") / F.col("order_count"), 2))
    for f in avg_fields:
        out = out.withColumn(f"avg_{f}", F.round(f"avg_{f}", 2))
    return out.select(*columns).orderBy(key)


def write_single_csv(df, output):
    """Write df as exactly one CSV file with a header at `output`."""
    out_dir = os.path.dirname(os.path.abspath(output))
    os.makedirs(out_dir, exist_ok=True)
    tmp_dir = os.path.join(out_dir, "_" + os.path.basename(output) + "_tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_uri(tmp_dir))
    parts = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"expected one part file, found {len(parts)}")
    shutil.move(parts[0], output)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def run(input_path, outdir):
    spark = build_spark()
    stats = {}

    # 1. Read lines and parse with the explicit 15-field schema.
    raw = spark.read.text(local_uri(input_path))
    parsed = raw.select(F.from_json("value", SCHEMA).alias("r"))
    all_null = reduce(lambda a, b: a & b, [F.col(f"r.{f}").isNull() for f in FIELDS])
    failed = F.col("r").isNull() | all_null
    parsed = parsed.select(failed.alias("_failed"), "r.*").cache()
    stats["input_records"] = parsed.count()
    stats["parse_failures"] = parsed.filter("_failed").count()
    df = parsed.filter(~F.col("_failed")).select(*FIELDS).cache()
    parsed_ok = df.count()

    # 2. Exact duplicates over all 15 fields (D4).
    df = df.dropDuplicates(FIELDS).cache()
    stats["records_after_dedup"] = df.count()
    stats["duplicates_removed"] = parsed_ok - stats["records_after_dedup"]

    # 3 + 4. Restaurant and city summaries.
    restaurants = summarize(df, "restaurant_id",
                            ["order_amount", "preparation_time_min", "delivery_time_min"],
                            RESTAURANT_COLUMNS).cache()
    cities = summarize(df, "city", ["order_amount", "delivery_time_min"], CITY_COLUMNS).cache()
    stats["restaurants"] = restaurants.count()
    stats["cities"] = cities.count()

    # 5 + 6. One CSV per table.
    restaurant_csv = os.path.join(outdir, "restaurant_summary.csv")
    city_csv = os.path.join(outdir, "city_summary.csv")
    write_single_csv(restaurants, restaurant_csv)
    write_single_csv(cities, city_csv)
    spark.stop()

    # 7. Stats.
    stats["input_path"] = os.path.abspath(input_path)
    stats["restaurant_summary_path"] = os.path.abspath(restaurant_csv)
    stats["city_summary_path"] = os.path.abspath(city_csv)
    stats["finished_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    os.makedirs(os.path.dirname(STATS_PATH), exist_ok=True)
    with open(STATS_PATH, "w") as fh:
        json.dump(stats, fh, indent=2)

    print("spark_job2 stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    assert stats["input_records"] == (stats["parse_failures"] + stats["duplicates_removed"]
                                      + stats["records_after_dedup"]), "counts do not add up"
    print(f"OK: {stats['records_after_dedup']} orders -> {stats['restaurants']} restaurants, "
          f"{stats['cities']} cities in {outdir}")


def main():
    ap = argparse.ArgumentParser(description="Spark Job 2: restaurant + city summaries")
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--outdir", default=DEFAULT_OUTDIR)
    args = ap.parse_args()
    input_path = os.path.expanduser(args.input)
    if not os.path.exists(input_path):
        sys.exit(f"input not found: {input_path}")
    run(input_path, os.path.expanduser(args.outdir))


if __name__ == "__main__":
    main()
