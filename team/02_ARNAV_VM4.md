# Arnav — VM4: Consumer 1 → Spark Job 1 → Producer 2

**Read `CLAUDE.md` first.** It holds the versions, names, schemas, decisions D1–D9 and the task index this file refers to.

## Your job in one line

Land the `orders` topic into a local file, clean and enrich it with Spark Job 1, and publish the result to `order_metrics`. This is the logic-heaviest part of the project.

## What blocks you and what you block

| You wait on | For task |
|---|---|
| Nothing | A1, A2, A3, A4: start immediately |
| Shriram S1 (ZeroTier Network ID) | A5 |
| Shriram S7 (topics exist) and Monty M6 (orders published) | A6 |

| You unblock | With task |
|---|---|
| Shriram S3 | A5, your ZeroTier join |
| Monty's golden outputs (M4) | A3, by sending him `order_metrics.jsonl` from the raw-file run |
| Monty M7 | A7, publishing `order_metrics` |
| Blason's report (B8) | A8 |

**Key point:** you never wait for the cluster to write code. A3 runs on the raw dataset file, and A4 runs against local Docker Kafka.

---

## Phase 0 — kickoff call

### A1. Environment and dataset
- Follow `CLAUDE.md` §12 for setup.
- Download the team dataset from the assignment page (enter our team number).
- Save it as `~/bd-a2/data/raw_orders.jsonl`.

### A5. ZeroTier
Run the ZeroTier block in `CLAUDE.md` §12 and send your node ID and IP to Shriram.

---

## Phase 1 — build everything locally

### A2. Inspect the dataset (about 20 min, do it before writing Spark code)

Ask Claude Code, or run these yourself:

```bash
wc -l ~/bd-a2/data/raw_orders.jsonl
head -3 ~/bd-a2/data/raw_orders.jsonl
```

Then use a short Python script to tally:
- lines that fail `json.loads`
- missing keys, per key
- distinct timestamp shapes, e.g. `2026-03-01 12:30:45` vs `2026-03-01T12:30:45Z` vs garbage
- `status` values
- values of `distance_km` and `order_amount` that are ≤ 0 or non-numeric
- exact duplicate lines

**Post to the group:**
1. The file format. If it is **not** one JSON object per line, stop and tell everyone.
2. The timestamp format(s). Write it into `CLAUDE.md` §6 as `TS_FORMAT`.
3. The counts of each bad-row type. These are the expected drop counts for your stats.

If there are several valid-looking timestamp formats, raise it with Shriram for the TA rather than guessing.

### A3. `vm4/spark_job1.py` (start right after A2)

**CLI:** `python vm4/spark_job1.py [--input PATH] [--output PATH]`
- Default input: `~/bd-a2/data/orders_landed.jsonl`
- Default output: `~/bd-a2/data/order_metrics.jsonl`
- During dev, pass `--input ~/bd-a2/data/raw_orders.jsonl`.

**Spark session:** `local[*]`, plus the three settings from `CLAUDE.md` §2.

**Steps, in order.** Count rows after each step for the stats file.

1. **Read** the file with `spark.read.text(input)`, giving one column `value`. Record `total_lines`.
2. **Parse** with `from_json(value, schema)`, where the schema has all 14 fields as `StringType`. JSON numbers come through as strings, which is fine.
   - Drop rows where the struct is null or all 14 fields are null. Count them as `dropped_malformed`.
   - Keep a separate count of truly malformed JSON versus valid-JSON-but-empty if it's easy. It's for the report only, since both are dropped.
3. **Required fields (D5).** Drop the row if any of the 14 fields is null or `trim(x) == ""`. Count as `dropped_missing_or_invalid_field`.
4. **Numeric validity (D5, D9).** Drop the row if `distance_km`, `order_amount` or `delivery_fee` gives null after `cast("double")`. Add these to the same counter.
5. **Timestamps.** Parse all 5 timestamp fields with `to_timestamp(col, TS_FORMAT)`, using the format found in A2. Drop the row if any is null. Count as `dropped_invalid_timestamp`.
6. **`distance_km <= 0`**: drop and count.
7. **`order_amount <= 0`**: drop and count.
8. **`status != "DELIVERED"`**: drop and count. This is an exact match (D6).
9. **Deduplicate (D4)** with `dropDuplicates()` on the 14 original string columns. Count as `dropped_duplicates`.
10. **Enrich.** Use `secs(a,b) = unix_timestamp(a) - unix_timestamp(b)`, or cast the timestamps to long and subtract.

| Metric | Formula |
|---|---|
| `preparation_time_min` | `secs(picked_up, accepted) / 60` |
| `delivery_time_min` | `secs(delivered, picked_up) / 60` |
| `fulfillment_time_min` | `secs(delivered, order) / 60` |
| `delivery_speed_kmph` | if `delivery_time_min > 0`: `distance_km / (delivery_time_min / 60)`; else null (D2) |
| `delay_min` | `greatest(0, secs(delivered, expected_delivery) / 60)` |
| `delivery_status` | `ON_TIME` if `delay_min == 0`, else `DELAYED` |
| `delay_category` | 0 → `ON_TIME`; ≤ 15 → `SLIGHT_DELAY`; ≤ 30 → `MODERATE_DELAY`; > 30 → `SEVERE_DELAY` (D3) |

   - Compute `delivery_status`, `delay_category` and speed from the **unrounded** values.
   - Then apply `round(x, 2)` to all 5 numeric metrics (D1).
   - Do **not** filter negative durations (D8).

11. **Select** the 15 output columns in the exact order of `CLAUDE.md` §5.2:
    - The 5 ids and `city` stay as strings.
    - `order_amount`, `delivery_fee` and `distance_km` are cast to double.
12. **Write one file:**
    - `df.coalesce(1).write.mode("overwrite").option("ignoreNullFields","false").json(tmp_dir)`
    - Move the single `part-*.json` to the output path.
    - Delete `tmp_dir`.
13. **Stats.** Write `~/bd-a2/stats/spark_job1_stats.json` with every count above plus `output_records`. Also print them. Assert that `total_lines == sum(drops) + output_records`.

**Validation, before calling A3 done:**
- Make a tiny test file with one line per rule:
  - malformed JSON
  - missing key
  - empty string
  - `"abc"` as `distance_km`
  - a bad timestamp
  - `distance_km` 0
  - `order_amount` −5
  - status `CANCELLED`
  - an exact duplicate pair
  - delivery 0 min (D2)
  - delays of exactly 0, 15, 15.5, 30 and 31 minutes
- Check every expected outcome by hand.
- Check the drop counts on the raw file match what you found in A2.

**Handoff (unblocks Monty's golden step in M4):** send Monty the full `order_metrics.jsonl` produced from `raw_orders.jsonl`. The real pipeline must later produce the same records, so Monty's CSVs from this file become the golden expected output.

### A4. `vm4/consumer1.py` and `vm4/producer2.py`, against local Docker Kafka

Start Docker Kafka with `CLAUDE.md` §11. You can work on this in parallel with A3.

**`consumer1.py`**: topic `orders` → `~/bd-a2/data/orders_landed.jsonl`
- confluent-kafka `Consumer` with:
  - `bootstrap.servers` from `KAFKA_BOOTSTRAP`
  - `group.id = "vm4-orders-landing"`
  - `enable.auto.commit = False`
  - `auto.offset.reset = earliest`
- Get the partitions from `list_topics`, record each partition's **high watermark** at start, then `assign()` every partition at `OFFSET_BEGINNING`.
- Poll until every partition's position has reached its start-time high watermark. That is a deterministic batch stop.
- Add a safety exit if nothing arrives for 30 s.
- Write `msg.value().decode("utf-8", errors="replace") + "\n"` for each message, with **no parsing**, since cleaning is Spark's job. Overwrite the file each run.
- Print per-partition counts and the total. Write `consumer1_stats.json`.

**`producer2.py`**: `~/bd-a2/data/order_metrics.jsonl` → topic `order_metrics`
- confluent-kafka `Producer` with `acks=all`, `enable.idempotence=True` and `linger.ms=20`.
- For each non-empty line, use `key = order_id` (parsed from the line) and send the line unchanged as the value.
- Use a delivery callback to count successes and failures per partition. Call `flush()` at the end.
- Exit non-zero if any delivery failed.
- Print the counts and write `producer2_stats.json`.

**Local test loop:**
1. Push `raw_orders.jsonl` into local `orders`, either with a quick throwaway producer or with Monty's `producer1.py` once it exists.
2. Run `consumer1.py`, then confirm `wc -l` equals the lines sent.
3. Run `spark_job1.py`, then `producer2.py`, then check the topic's end offsets equal K.

---

## Phase 2 — real run (part of S9)

```bash
source ~/bd-a2/.venv/bin/activate
export KAFKA_BOOTSTRAP="<ZT_VM2>:9092,<ZT_VM3>:9092"
```

- **A6:** after Monty reads out N, run `python vm4/consumer1.py`. Check `wc -l orders_landed.jsonl` equals N.
- **A7:**
  1. Run `python vm4/spark_job1.py`. Read out K, the output records.
  2. Run `python vm4/producer2.py`. Confirm K sent with 0 failures.
  3. Tell Monty to go. **This unblocks M7.**
  4. Sanity check: the sorted lines of your real `order_metrics.jsonl` must equal the sorted lines of your A3 dev run.

### A8. Hand over to Blason
Send Blason:
- `stats/consumer1_stats.json`, `stats/spark_job1_stats.json` and `stats/producer2_stats.json`
- the first 5 lines of `order_metrics.jsonl`
- screenshots of the three terminal runs

Commit `vm4/` yourself.

---

## Claude Code prompts

**A2:**

> Read CLAUDE.md and team/02_ARNAV_VM4.md. Write a throwaway script `vm4/inspect_dataset.py` for task A2 and run it on ~/bd-a2/data/raw_orders.jsonl. Report the file format, timestamp formats found, status values, and counts of each bad-row type. Do not run git.

**A3:**

> Read CLAUDE.md and team/02_ARNAV_VM4.md. Implement vm4/spark_job1.py exactly per task A3, using TS_FORMAT from CLAUDE.md §6. Then create tests/fixtures_job1.jsonl covering every case in A3's validation list, run the job on it, and show me expected vs actual for each case. Then run it on raw_orders.jsonl and print the stats. Don't add business rules beyond CLAUDE.md §6. Do not run git.

**A4:**

> Read CLAUDE.md and team/02_ARNAV_VM4.md. Implement vm4/consumer1.py and vm4/producer2.py per task A4. Test them against local Docker Kafka per CLAUDE.md §11 using the local test loop in A4, and show me the counts. Do not run git.
