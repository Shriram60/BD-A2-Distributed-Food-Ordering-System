# Monty — VM1: Producer 1, Consumer 2 → Spark Job 2

**Read `CLAUDE.md` first.** It holds the versions, names, schemas, decisions D1–D9 and the task index this file refers to.

## Your job in one line

Publish the raw dataset to `orders` at the start of the pipeline. At the end, land `order_metrics` and produce the two final CSVs.

## What blocks you and what you block

| You wait on | For task |
|---|---|
| Nothing | M1, M2, M3, M4 (synthetic part): start immediately |
| Shriram S1 (ZeroTier Network ID) | M5 |
| Arnav A3 (his `order_metrics.jsonl` from the raw file) | M4, golden step only |
| Shriram S7 (topics exist) and S9 starting | M6 |
| Arnav A7 (`order_metrics` published) | M7 |

| You unblock | With task |
|---|---|
| Shriram S3 | M5, your ZeroTier join |
| Arnav A6 | M6, publishing `orders` |
| Blason's report (B8) | M8 |

**Key point:** you don't wait for Arnav to start Spark Job 2. Build it on synthetic data first, then swap in his real output when it lands.

---

## Phase 0 — kickoff call

### M1. Environment and dataset
- Follow `CLAUDE.md` §12 for setup.
- Download the team dataset (enter our team number) and save it as `~/bd-a2/data/raw_orders.jsonl`.
- VM1 needs this file for the real run.

### M5. ZeroTier
Run the ZeroTier block in `CLAUDE.md` §12 and send your node ID and IP to Shriram.

---

## Phase 1 — build everything locally

Start local Docker Kafka with `CLAUDE.md` §11.

### M2. `vm1/producer1.py`: `raw_orders.jsonl` → topic `orders`
- **CLI:** `python vm1/producer1.py [--input PATH]`, defaulting to `~/bd-a2/data/raw_orders.jsonl`.
- confluent-kafka `Producer` with:
  - `bootstrap.servers` from `KAFKA_BOOTSTRAP`
  - `acks=all`
  - `enable.idempotence=True`
  - `linger.ms=20`
- **Send every line as-is**, malformed ones included. Spark Job 1 is what drops bad records, so the producer must not filter or fix anything.
  - Skip only lines that are completely empty or whitespace-only, since they aren't records. Count them separately as `skipped_blank`.
- **Key:** try `json.loads(line)["order_id"]` as a string. If that fails, use no key. This spreads records over both partitions.
- Use a delivery callback to count successes and failures per partition. Call `flush()` at the end, and exit non-zero on any failure.
- Print `sent`, `skipped_blank`, per-partition counts and `failed`. Write `~/bd-a2/stats/producer1_stats.json`.
- **Check:** both partitions have non-zero counts.

### M3. `vm1/consumer2.py`: topic `order_metrics` → `~/bd-a2/data/order_metrics_landed.jsonl`

This is the same design as Arnav's `consumer1.py`. Coordinate with him so the two files share structure, but keep them as separate files.

- `group.id = "vm1-order-metrics-landing"`, `enable.auto.commit=False`, `auto.offset.reset=earliest`.
- At start, record each partition's high watermark, then `assign()` all partitions at `OFFSET_BEGINNING`.
- Poll until every partition reaches its start-time high watermark. Add a 30 s idle safety exit.
- Write each `msg.value()` decoded as UTF-8, plus `"\n"`, with no parsing. Overwrite the file each run.
- Print per-partition counts and the total. Write `consumer2_stats.json`.

**Local test:**
1. Produce a few lines into local `order_metrics`.
2. Run `consumer2.py`.
3. Confirm `wc -l` matches.

### M4. `vm1/spark_job2.py`

**CLI:** `python vm1/spark_job2.py [--input PATH] [--outdir DIR]`
- Default input: `~/bd-a2/data/order_metrics_landed.jsonl`
- Default output directory: `~/bd-a2/output/`

**Spark session:** `local[*]`, plus the three settings from `CLAUDE.md` §2.

**Steps:**
1. Read with `spark.read.text(input)`, then parse with `from_json` using the explicit 15-field schema from `CLAUDE.md` §5.2.
   - Ids, `city`, `delivery_status` and `delay_category` are strings. The rest are doubles.
   - Drop rows that fail to parse. There should be 0, so log the count.
2. **Deduplicate (D4)** with `dropDuplicates()` over all 15 columns. Log how many rows were removed.
3. **Restaurant performance.** Group by `restaurant_id`:
   - `order_count = count(*)`
   - `avg_order_amount = avg(order_amount)`
   - `avg_preparation_time_min = avg(preparation_time_min)`
   - `avg_delivery_time_min = avg(delivery_time_min)`
   - `delayed_order_count = sum(when(delivery_status == "DELAYED", 1).otherwise(0))`
   - `delay_rate_pct = 100 * delayed_order_count / order_count`
   - Round every average and the rate to 2 dp. Sort by `restaurant_id`.
4. **City performance.** Group by `city`:
   - `order_count`, `avg_order_amount`, `avg_delivery_time_min`, `delayed_order_count` and `delay_rate_pct`, defined the same way.
   - Round and sort by `city`.
5. Use the exact column names and order from `CLAUDE.md` §5.3 and §5.4.
6. **Write a single CSV with a header** for each table:
   - `coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)`
   - Move the `part-*.csv` to `restaurant_summary.csv` or `city_summary.csv`.
   - Delete `tmp_dir`.
7. **Stats.** Write `spark_job2_stats.json` with `input_records`, `duplicates_removed`, `restaurants` and `cities`.

**Validation, in two stages:**
- **(a) Synthetic data, no waiting.**
  1. Generate `tests/synthetic_order_metrics.jsonl`: about 200 records matching §5.2, including a few exact duplicates and a mix of ON_TIME and DELAYED.
  2. Compute the expected summaries in **plain Python**, independently of Spark.
  3. Run `spark_job2.py --input tests/synthetic_order_metrics.jsonl` and diff the two. They must match exactly.
- **(b) Golden, once Arnav finishes A3.**
  1. Run `spark_job2.py --input <Arnav's order_metrics.jsonl> --outdir ~/bd-a2/golden/`.
  2. Eyeball the results: averages are plausible, delay rates are between 0 and 100, and there are no null rows.
  3. Commit `golden/`. The real run must reproduce it byte-for-byte.

---

## Phase 2 — real run (part of S9)

```bash
source ~/bd-a2/.venv/bin/activate
export KAFKA_BOOTSTRAP="<ZT_VM2>:9092,<ZT_VM3>:9092"
```

- **M6:** once Shriram has reset the topics, run `python vm1/producer1.py`.
  - Read out N (sent) and confirm 0 failures with both partitions non-zero.
  - **This unblocks Arnav's A6.**
- **M7:** once Arnav says `order_metrics` is published:
  1. Run `python vm1/consumer2.py`. Check `wc -l` equals Arnav's K.
  2. Run `python vm1/spark_job2.py`.
  3. Run `diff ~/bd-a2/output/restaurant_summary.csv ~/bd-a2/golden/restaurant_summary.csv`, and the same for `city_summary.csv`. **There must be no output.**

### M8. Hand over to Blason
Send Blason:
- all three `stats/*.json` files
- both final CSVs
- the first 10 rows of each CSV
- screenshots of the runs

Commit `vm1/`, `tests/` and `golden/` yourself.

---

## Claude Code prompts

**M2 + M3:**

> Read CLAUDE.md and team/03_MONTY_VM1.md. Implement vm1/producer1.py and vm1/consumer2.py per tasks M2 and M3. Test both against local Docker Kafka per CLAUDE.md §11 and show me the counts. Do not run git.

**M4:**

> Read CLAUDE.md and team/03_MONTY_VM1.md. Implement vm1/spark_job2.py per task M4. Then do validation (a): generate tests/synthetic_order_metrics.jsonl, compute the expected CSVs in plain Python without Spark, run the Spark job, and show me the diff. It must be empty. Don't add business rules beyond CLAUDE.md §6. Do not run git.

**Golden step:**

> Run vm1/spark_job2.py on <path to Arnav's order_metrics.jsonl> with --outdir ~/bd-a2/golden/ and show me the first 10 rows of each CSV plus any sanity issues: nulls, delay rates outside 0–100, or negative averages.
