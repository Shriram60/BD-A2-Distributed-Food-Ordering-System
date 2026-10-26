"""Producer 2 (VM4): publish order_metrics.jsonl to topic `order_metrics`.

Usage: python vm4/producer2.py [--input PATH]
Implements task A4 in team/02_ARNAV_VM4.md; names in CLAUDE.md section 4.

Each non-empty line is sent unchanged as the value, keyed by its order_id.
Exits non-zero if any delivery fails.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer

HOME = os.path.expanduser("~")
TOPIC = "order_metrics"
DEFAULT_INPUT = os.path.join(HOME, "bd-a2/data/order_metrics.jsonl")
STATS_PATH = os.path.join(HOME, "bd-a2/stats/producer2_stats.json")


def run(input_path):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })
    started = time.time()
    delivered = Counter()   # partition -> successful deliveries
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(f"key={msg.key()!r}: {err}")
        else:
            delivered[msg.partition()] += 1

    lines_read = skipped_blank = unparseable = queued = 0
    with open(input_path, encoding="utf-8") as fh:
        for line in fh:
            lines_read += 1
            value = line.rstrip("\r\n")
            if not value.strip():
                skipped_blank += 1
                continue
            try:
                key = json.loads(value)["order_id"]
            except (ValueError, KeyError, TypeError) as e:
                unparseable += 1
                failures.append(f"line {lines_read}: no order_id ({e})")
                continue
            while True:
                try:
                    producer.produce(TOPIC, key=str(key).encode("utf-8"),
                                     value=value.encode("utf-8"), on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full: let deliveries drain
                    producer.poll(0.5)
            queued += 1
            producer.poll(0)

    remaining = producer.flush(60)
    if remaining:
        failures.append(f"{remaining} message(s) still undelivered after flush timeout")

    sent = sum(delivered.values())
    stats = {
        "topic": TOPIC,
        "bootstrap": bootstrap,
        "input_path": os.path.abspath(input_path),
        "lines_read": lines_read,
        "skipped_blank_lines": skipped_blank,
        "unparseable_lines": unparseable,
        "queued": queued,
        "sent": sent,
        "failed": queued - sent + unparseable,
        "per_partition_sent": {str(p): n for p, n in sorted(delivered.items())},
        "errors": failures[:20],
        "elapsed_s": round(time.time() - started, 2),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    os.makedirs(os.path.dirname(STATS_PATH), exist_ok=True)
    with open(STATS_PATH, "w") as fh:
        json.dump(stats, fh, indent=2)

    print("producer2 stats:")
    for p, n in sorted(delivered.items()):
        print(f"  partition {p}: sent {n}")
    print(f"  sent: {sent}  failed: {stats['failed']}  (lines read {lines_read}, "
          f"blank skipped {skipped_blank})")
    for e in failures[:20]:
        print(f"  error: {e}", file=sys.stderr)
    if stats["failed"] or failures:
        sys.exit(f"FAILED: {stats['failed']} record(s) not delivered to '{TOPIC}'")
    print(f"OK: published {sent} records to '{TOPIC}'")


def main():
    ap = argparse.ArgumentParser(description="Producer 2: publish order_metrics.jsonl")
    ap.add_argument("--input", default=DEFAULT_INPUT)
    args = ap.parse_args()
    path = os.path.expanduser(args.input)
    if not os.path.exists(path):
        sys.exit(f"input not found: {path}")
    run(path)


if __name__ == "__main__":
    main()
