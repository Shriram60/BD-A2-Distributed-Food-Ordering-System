"""Producer 1 (VM1): publish every line of raw_orders.jsonl to topic `orders`.

Lines are sent as-is, malformed ones included; Spark Job 1 does the cleaning.
Only blank / whitespace-only lines are skipped, since they are not records.

Usage: python vm1/producer1.py [--input PATH]
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

from confluent_kafka import Producer

TOPIC = "orders"
BASE = Path.home() / "bd-a2"
DEFAULT_INPUT = BASE / "data" / "raw_orders.jsonl"
STATS_FILE = BASE / "stats" / "producer1_stats.json"


def key_for(line):
    """order_id as the message key, or None if the line has no usable order_id."""
    try:
        order_id = json.loads(line)["order_id"]
    except (ValueError, TypeError, KeyError):
        return None
    if order_id is None:
        return None
    return str(order_id).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })

    per_partition = Counter()
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))
        else:
            per_partition[msg.partition()] += 1

    attempted = skipped_blank = unkeyed = 0
    start = time.time()
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line.strip():
                skipped_blank += 1
                continue
            key = key_for(line)
            if key is None:
                unkeyed += 1
            while True:
                try:
                    producer.produce(TOPIC, value=line.encode("utf-8"), key=key,
                                     on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # local queue full: let deliveries drain, then retry
            attempted += 1
            producer.poll(0)

    remaining = producer.flush(60)
    if remaining:
        failures.append(f"{remaining} message(s) still unacknowledged after 60s flush")
    sent = sum(per_partition.values())
    failed = attempted - sent  # includes anything still queued after flush
    elapsed = round(time.time() - start, 2)

    stats = {
        "script": "producer1",
        "topic": TOPIC,
        "bootstrap": bootstrap,
        "input": args.input,
        "attempted": attempted,
        "sent": sent,
        "skipped_blank": skipped_blank,
        "unkeyed": unkeyed,
        "per_partition": {str(p): per_partition[p] for p in sorted(per_partition)},
        "failed": failed,
        "errors": sorted(set(failures)),
        "elapsed_sec": elapsed,
    }
    STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATS_FILE.write_text(json.dumps(stats, indent=2) + "\n")

    print(f"topic={TOPIC} bootstrap={bootstrap}")
    print(f"sent={sent} skipped_blank={skipped_blank} unkeyed={unkeyed} failed={failed}")
    for p in sorted(per_partition):
        print(f"  partition {p}: {per_partition[p]}")
    if len(per_partition) < 2:
        print("WARNING: fewer than 2 partitions received records")
    for e in stats["errors"]:
        print(f"  error: {e}")
    print(f"stats -> {STATS_FILE}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
