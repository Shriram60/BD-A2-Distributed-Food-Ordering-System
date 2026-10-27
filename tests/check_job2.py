"""Check Spark Job 2 against an independent plain-Python computation (task M4a).

Computes restaurant_summary.csv and city_summary.csv from the input JSONL without
Spark, then diffs them byte-for-byte against the CSVs in --actual-dir.

Usage: python tests/check_job2.py --input PATH --actual-dir DIR [--expected-dir DIR]
Exit 0 when both files match exactly.
"""
import argparse
import difflib
import json
import os
import sys
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal

FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_amount", "delivery_fee", "distance_km",
    "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
    "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category",
]


def round2(x):
    """Spark round(): HALF_UP on the double's shortest decimal representation."""
    return float(Decimal(repr(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def fmt(v):
    # Spark's CSV writer prints doubles with Java Double.toString and longs as integers;
    # for these magnitudes Python repr() gives the same text.
    return repr(v) if isinstance(v, float) else str(v)


def summarize(rows, key, avg_fields):
    groups = defaultdict(list)
    for r in rows:
        groups[r[key]].append(r)
    lines = []
    for k in sorted(groups):
        g = groups[k]
        n = len(g)
        delayed = sum(1 for r in g if r["delivery_status"] == "DELAYED")
        avgs = [round2(sum(float(r[f]) for r in g) / n) for f in avg_fields]
        lines.append(",".join(fmt(v) for v in [k, n, *avgs, delayed, round2(100.0 * delayed / n)]))
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True)
    ap.add_argument("--actual-dir", required=True)
    ap.add_argument("--expected-dir", help="also write the expected CSVs here")
    args = ap.parse_args()

    rows, seen = [], set()
    with open(args.input, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            sig = tuple(None if r[f] is None else (float(r[f]) if f not in
                        ("order_id", "customer_id", "restaurant_id", "delivery_partner_id",
                         "city", "delivery_status", "delay_category") else r[f]) for f in FIELDS)
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(r)

    expected = {
        "restaurant_summary.csv":
            ["restaurant_id,order_count,avg_order_amount,avg_preparation_time_min,"
             "avg_delivery_time_min,delayed_order_count,delay_rate_pct"]
            + summarize(rows, "restaurant_id",
                        ["order_amount", "preparation_time_min", "delivery_time_min"]),
        "city_summary.csv":
            ["city,order_count,avg_order_amount,avg_delivery_time_min,"
             "delayed_order_count,delay_rate_pct"]
            + summarize(rows, "city", ["order_amount", "delivery_time_min"]),
    }

    print(f"plain python: {len(rows)} records after dedup")
    ok = True
    for name, lines in expected.items():
        text = "\n".join(lines) + "\n"
        if args.expected_dir:
            os.makedirs(args.expected_dir, exist_ok=True)
            with open(os.path.join(args.expected_dir, name), "w") as fh:
                fh.write(text)
        with open(os.path.join(args.actual_dir, name)) as fh:
            actual = fh.read()
        diff = list(difflib.unified_diff(text.splitlines(True), actual.splitlines(True),
                                         f"expected/{name}", f"actual/{name}"))
        if diff:
            ok = False
            sys.stdout.writelines(diff)
        else:
            print(f"MATCH {name} ({len(lines) - 1} rows)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
