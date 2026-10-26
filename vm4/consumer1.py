"""Consumer 1 (VM4): land topic `orders` into orders_landed.jsonl as one batch.

Usage: python vm4/consumer1.py [--output PATH]
Implements task A4 in team/02_ARNAV_VM4.md; names in CLAUDE.md section 4.

Reads every partition from the beginning up to the high watermark seen at start,
then stops. Values are written raw, one per line; cleaning is Spark Job 1's job.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition

HOME = os.path.expanduser("~")
TOPIC = "orders"
GROUP_ID = "vm4-orders-landing"
DEFAULT_OUTPUT = os.path.join(HOME, "bd-a2/data/orders_landed.jsonl")
STATS_PATH = os.path.join(HOME, "bd-a2/stats/consumer1_stats.json")
IDLE_TIMEOUT_S = 30  # safety exit if nothing arrives for this long


def run(output_path):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    started = time.time()

    # Partitions and their start-time watermarks define the batch.
    meta = consumer.list_topics(TOPIC, timeout=15)
    tmeta = meta.topics.get(TOPIC)
    if tmeta is None or tmeta.error is not None or not tmeta.partitions:
        consumer.close()
        sys.exit(f"topic '{TOPIC}' not found on {bootstrap}"
                 + (f": {tmeta.error}" if tmeta is not None and tmeta.error else ""))
    partitions = sorted(tmeta.partitions)
    low, high = {}, {}
    for p in partitions:
        low[p], high[p] = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=15)
    consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in partitions])

    next_offset = dict(low)          # next offset we expect per partition
    consumed = {p: 0 for p in partitions}
    pending = {p for p in partitions if high[p] > low[p]}
    complete = True
    last_msg = time.time()

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as out:
        while pending:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_msg > IDLE_TIMEOUT_S:
                    complete = False
                    print(f"WARNING: no messages for {IDLE_TIMEOUT_S}s; stopping early, "
                          f"partitions still short of their start watermark: {sorted(pending)}",
                          file=sys.stderr)
                    break
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                print(f"consumer error (continuing): {msg.error()}", file=sys.stderr)
                continue
            p = msg.partition()
            last_msg = time.time()
            if p not in pending:     # already reached its start-time watermark
                continue
            if msg.offset() >= high[p]:  # produced after we started: not part of this batch
                pending.discard(p)
                continue
            value = msg.value()
            out.write((value.decode("utf-8", errors="replace") if value is not None else "") + "\n")
            consumed[p] += 1
            next_offset[p] = msg.offset() + 1
            if next_offset[p] >= high[p]:
                pending.discard(p)

    # Record how far we read; harmless for reruns since we always assign at the beginning.
    if any(consumed.values()):
        consumer.commit(offsets=[TopicPartition(TOPIC, p, next_offset[p])
                                 for p in partitions if consumed[p]], asynchronous=False)
    consumer.close()

    total = sum(consumed.values())
    expected = sum(high[p] - low[p] for p in partitions)
    stats = {
        "topic": TOPIC,
        "group_id": GROUP_ID,
        "bootstrap": bootstrap,
        "partitions": {str(p): {"low_watermark": low[p], "high_watermark_at_start": high[p],
                                "consumed": consumed[p]} for p in partitions},
        "expected_records": expected,
        "total_consumed": total,
        "complete": complete and total == expected,
        "output_path": os.path.abspath(output_path),
        "elapsed_s": round(time.time() - started, 2),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    os.makedirs(os.path.dirname(STATS_PATH), exist_ok=True)
    with open(STATS_PATH, "w") as fh:
        json.dump(stats, fh, indent=2)

    print("consumer1 stats:")
    for p in partitions:
        print(f"  partition {p}: consumed {consumed[p]} (offsets {low[p]}..{high[p]})")
    print(f"  total consumed: {total} / expected {expected}")
    print(f"  written to: {output_path}")
    if not stats["complete"]:
        sys.exit("INCOMPLETE: did not reach every partition's start-time high watermark")
    print(f"OK: landed {total} records from '{TOPIC}'")


def main():
    ap = argparse.ArgumentParser(description="Consumer 1: land topic 'orders' to a file")
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    args = ap.parse_args()
    run(os.path.expanduser(args.output))


if __name__ == "__main__":
    main()
