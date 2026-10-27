"""Generate tests/synthetic_order_metrics.jsonl for Spark Job 2 validation (task M4a).

~200 records in the CLAUDE.md 5.2 shape: a mix of ON_TIME / DELAYED, a few exact
duplicates (must be removed), a few near-duplicates differing in one field (must be
kept) and some null delivery_speed_kmph (D2). Deterministic (fixed seed).

Usage: python tests/make_synthetic_job2.py [--output PATH]
"""
import argparse
import json
import os
import random

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(HERE, "synthetic_order_metrics.jsonl")

RESTAURANTS = [f"R{i:02d}" for i in range(1, 9)]
CITIES = ["Bengaluru", "Chennai", "Delhi", "Hyderabad", "Mumbai", "Pune"]


def category(delay):
    if delay == 0:
        return "ON_TIME"
    if delay <= 15:
        return "SLIGHT_DELAY"
    if delay <= 30:
        return "MODERATE_DELAY"
    return "SEVERE_DELAY"


def record(rng, i):
    prep = round(rng.uniform(3, 30), 2)
    deliv = 0.0 if i % 37 == 0 else round(rng.uniform(5, 60), 2)
    dist = round(rng.uniform(0.5, 15), 2)
    delay = 0.0 if rng.random() < 0.4 else round(rng.uniform(0.01, 50), 2)
    return {
        "order_id": f"S{i:05d}",
        "customer_id": f"C{rng.randint(1, 80):05d}",
        "restaurant_id": rng.choice(RESTAURANTS),
        "delivery_partner_id": f"D{rng.randint(1, 15):02d}",
        "city": rng.choice(CITIES),
        "order_amount": round(rng.uniform(80, 2500), 2),
        "delivery_fee": round(rng.uniform(0, 80), 2),
        "distance_km": dist,
        "preparation_time_min": prep,
        "delivery_time_min": deliv,
        "fulfillment_time_min": round(prep + deliv + rng.uniform(0, 5), 2),
        "delivery_speed_kmph": None if deliv == 0 else round(dist / (deliv / 60), 2),
        "delay_min": delay,
        "delivery_status": "ON_TIME" if delay == 0 else "DELAYED",
        "delay_category": category(delay),
    }


def main():
    ap = argparse.ArgumentParser(description="Generate synthetic order_metrics for Job 2")
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    args = ap.parse_args()

    rng = random.Random(343)
    rows = [record(rng, i) for i in range(1, 193)]
    rows += [dict(rows[i]) for i in (0, 5, 5, 17, 42, 99, 150)]   # 7 exact duplicates
    near = dict(rows[10]); near["order_amount"] += 1.0              # differs in one field: keep
    near2 = dict(rows[20]); near2["delivery_status"] = "ON_TIME"    # differs in one field: keep
    rows += [near, near2]
    rng.shuffle(rows)

    with open(args.output, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    print(f"wrote {len(rows)} records (7 exact duplicates) to {args.output}")


if __name__ == "__main__":
    main()
