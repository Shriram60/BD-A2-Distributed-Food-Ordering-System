# BD Assignment 2 — Distributed Food Ordering System
## Team Plan + Shared Contract

**Course:** Big Data 2026 (UE24CS343AB2)
**Team:** Shriram (VM2) · Arnav (VM4) · Monty (VM1) · Blason (VM3)

> **Copy this file to the repo root as `CLAUDE.md`.** It is the single source of truth for versions, IPs, names, schemas and the order of work. Every Claude Code session reads it first.
>
> If something here needs to change, change it **here first** and post in the group. Never "just rename it" in your own code.
>
> The per-person files live in `team/`:
> - `team/01_SHRIRAM_VM2.md`
> - `team/02_ARNAV_VM4.md`
> - `team/03_MONTY_VM1.md`
> - `team/04_BLASON_VM3.md`

---

## 1. Who owns what

| Member | VM | Role | Folders they own | Claude Code? |
|---|---|---|---|---|
| Shriram | VM2 | KRaft controller + Broker 1, cluster lead, integration conductor, TA contact | `config/`, `scripts/`, `CLAUDE.md` | Yes |
| Arnav | VM4 | Consumer 1 → Spark Job 1 → Producer 2 | `vm4/` | Yes |
| Monty | VM1 | Producer 1, then Consumer 2 → Spark Job 2 | `vm1/` | Yes |
| Blason | VM3 | Broker 2 only, failure-test operator, evidence and report | `evidence/`, `REPORT.md` | No |

Each person edits only their own folders. That alone removes merge conflicts. Each person commits their own folder themselves; Claude Code does not run git.

---

## 2. Fixed stack (identical on every VM)

| Item | Value |
|---|---|
| OS | Ubuntu 22.04 or 24.04 |
| Java | OpenJDK 17: `sudo apt install -y openjdk-17-jdk` |
| Kafka | **3.9.1**, Scala 2.13, KRaft mode, extracted to `~/kafka` |
| Kafka download | `https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz` |
| Python | 3.10+, venv at `~/bd-a2/.venv` |
| Python packages | `confluent-kafka`, `pyspark>=4.0,<4.1` |
| Spark | Local mode `local[*]`, launched from Python. **No HDFS, no Spark cluster, no Spark–Kafka connector.** |
| Spark settings (every job) | `spark.sql.session.timeZone=UTC`, `spark.sql.ansi.enabled=false`, `spark.sql.jsonGenerator.ignoreNullFields=false` |
| Network | ZeroTier, one network created by Shriram |

**"VM" means "machine".** Anyone may run native Ubuntu (dual boot) instead of a VM; everything here works the same. For laptops:
- Boot into Ubuntu before every team session.
- Keep the charger plugged in.
- Brokers must not sleep. Start them wrapped in `systemd-inhibit --what=sleep:idle:handle-lid-switch ...`, so the laptop won't suspend, even with the lid closed, while the broker runs.
- Ask the TA to confirm that physical machines count as the "4 VMs" (question 6 in Shriram's TA message).

Why `ansi.enabled=false`: Spark 4 turns ANSI mode on by default. With it on, casting a bad string like `"abc"` to a number crashes the job instead of returning null. We need invalid values to become null so we can drop them.

VM2 and VM3 need the Kafka tarball to run brokers. VM1 and VM4 only need Java and Python, since PySpark needs Java. Extracting Kafka on VM1 and VM4 as well is optional but handy for the CLI debug tools.

---

## 3. Network table (Shriram fills this in during task S3)

| VM | Owner | ZeroTier IP | Inbound ports that must be open |
|---|---|---|---|
| VM1 | Monty | `ZT_VM1 = 10.113.232.224` | none |
| VM2 | Shriram | `ZT_VM2 = 10.113.232.128` | 9092 (broker), 9093 (controller) |
| VM3 | Blason | `ZT_VM3 = ___.___.___.___` | 9092 (broker) |
| VM4 | Arnav | `ZT_VM4 = 10.113.232.70` | none |

- **ZeroTier Network ID:** `3b19b3a71682751e`
- **Kafka Cluster ID:** `_2Xn7kSKSdOvrVoRv00JMA` (generated once by Shriram, used only on VM2 and VM3)

All Python clients read the broker list from one environment variable:

```bash
export KAFKA_BOOTSTRAP="<ZT_VM2>:9092,<ZT_VM3>:9092"
```

**Always list both brokers**, so clients keep working when VM3 goes down. For local development the default is `localhost:9092`.

---

## 4. Names (exact spelling)

| Thing | Name |
|---|---|
| Topic 1 | `orders` |
| Topic 2 | `order_metrics` |
| Topic settings | 2 partitions, replication factor 2, for **both** topics |
| Raw dataset | `~/bd-a2/data/raw_orders.jsonl` (rename the download to this) |
| VM4 landing file | `~/bd-a2/data/orders_landed.jsonl` |
| VM4 Spark Job 1 output | `~/bd-a2/data/order_metrics.jsonl` |
| VM1 landing file | `~/bd-a2/data/order_metrics_landed.jsonl` |
| VM1 final outputs | `~/bd-a2/output/restaurant_summary.csv` and `~/bd-a2/output/city_summary.csv` |
| Stats files | `~/bd-a2/stats/<script_name>_stats.json` |

---

## 5. Schemas

### 5.1 Input record (topic `orders`)

There are 14 fields, all required:

`order_id, customer_id, restaurant_id, delivery_partner_id, city, order_timestamp, accepted_timestamp, picked_up_timestamp, expected_delivery_timestamp, delivered_timestamp, distance_km, order_amount, delivery_fee, status`

### 5.2 Enriched record (file `order_metrics.jsonl` and topic `order_metrics`)

One JSON object per line, with keys in exactly this order:

| # | Key | Type |
|---|---|---|
| 1 | `order_id` | string |
| 2 | `customer_id` | string |
| 3 | `restaurant_id` | string |
| 4 | `delivery_partner_id` | string |
| 5 | `city` | string |
| 6 | `order_amount` | double |
| 7 | `delivery_fee` | double |
| 8 | `distance_km` | double |
| 9 | `preparation_time_min` | double, 2 dp |
| 10 | `delivery_time_min` | double, 2 dp |
| 11 | `fulfillment_time_min` | double, 2 dp |
| 12 | `delivery_speed_kmph` | double, 2 dp, or `null` (see D2) |
| 13 | `delay_min` | double, 2 dp |
| 14 | `delivery_status` | `ON_TIME` or `DELAYED` |
| 15 | `delay_category` | `ON_TIME`, `SLIGHT_DELAY`, `MODERATE_DELAY` or `SEVERE_DELAY` |

### 5.3 `restaurant_summary.csv`

Header row:

`restaurant_id,order_count,avg_order_amount,avg_preparation_time_min,avg_delivery_time_min,delayed_order_count,delay_rate_pct`

Rows sorted by `restaurant_id`. Every calculated number is rounded to 2 dp.

### 5.4 `city_summary.csv`

Header row:

`city,order_count,avg_order_amount,avg_delivery_time_min,delayed_order_count,delay_rate_pct`

Rows sorted by `city`. Every calculated number is rounded to 2 dp.

`delay_rate_pct = 100 × delayed_order_count / order_count`. A delayed order is one with `delivery_status == "DELAYED"`.

---

## 6. Interpretation decisions

These are the defaults we code to now. Shriram confirms them with the TA. If the TA changes one, Shriram updates this table and posts in the group.

| ID | Question | Our default until the TA says otherwise |
|---|---|---|
| D1 | Does Job 1 round its metrics too? | Yes. Job 1 rounds all computed metrics to 2 dp. Job 2 rounds its outputs to 2 dp. |
| D2 | `delivery_time_min = 0` would divide by zero in `delivery_speed_kmph` | Keep the record and write `delivery_speed_kmph` as JSON `null`. |
| D3 | Fractional delays such as 15.5 min fall between categories | 0 → ON_TIME. Over 0 up to 15 → SLIGHT. Over 15 up to 30 → MODERATE. Over 30 → SEVERE. The category is computed from the unrounded delay. |
| D4 | What counts as an "exact duplicate"? | Job 1: identical values in all 14 input fields, compared as strings after JSON parsing. Job 2: identical values in all 15 fields. |
| D5 | What makes a field "invalid"? | null, missing, an empty or whitespace-only string, or a numeric field that doesn't parse as a number. |
| D6 | How is `status` matched? | Exactly `status == "DELIVERED"`, with no trimming or case-folding. |
| D7 | The PDF writes `order amount` with a space | We use `order_amount`. |
| D8 | Negative durations, e.g. picked up before accepted | Keep the record and compute as-is. The spec forbids adding business rules. |
| D9 | Does `delivery_fee <= 0` drop a record? | No. Only `distance_km` and `order_amount` have the `<= 0` rule. `delivery_fee` must only be a valid number. |

---

## 7. Repo layout

```
bd-a2/
├── CLAUDE.md                 ← this file
├── team/                     ← the 4 per-person MD files
├── requirements.txt          ← confluent-kafka, pyspark>=4.0,<4.1
├── .gitignore                ← data/, output/, stats/, .venv/, spark-warehouse/, *.log, metastore_db/
├── config/                   (Shriram)
│   ├── vm2.properties
│   └── vm3.properties
├── scripts/                  (Shriram)
│   ├── create_topics.sh
│   ├── reset_topics.sh
│   ├── describe_topics.sh
│   └── failure_test.sh
├── vm1/                      (Monty)
│   ├── producer1.py
│   ├── consumer2.py
│   └── spark_job2.py
├── vm4/                      (Arnav)
│   ├── consumer1.py
│   ├── spark_job1.py
│   └── producer2.py
├── golden/                   (Monty) expected CSVs from the dev run, used to check the real run
├── evidence/                 (Blason) screenshots and text captures
└── REPORT.md                 (Blason)
```

Data never goes into git.

---

## 8. The pipeline: who is blocked by whom

**Core idea:** nobody waits for the real cluster to write code.
- Arnav builds Spark Job 1 directly on the raw dataset file.
- Monty builds Spark Job 2 on synthetic data, then on Arnav's real output.
- Both test their producers and consumers against a throwaway local Kafka in Docker (see section 11).
- The real 4-VM cluster is only needed for the integration run and the failure test.

```
          PHASE 0          PHASE 1 (all parallel)                                PHASE 2 (integration)       PHASE 3              PHASE 4
          ~30 min call     ~1 day                                                ~1-2 h, all online          ~45 min              ~half day
Shriram   S1 S2 S3 ──► S4 S5 ──► S6 S7 S8 ── (TA questions, scripts) ───────►    S9 conducts the run ──►     S10 failure test ──► S11 review + commit
Blason    B1 B2 ─────────► B3 B4 B5 ── (report skeleton, evidence folder) ──►    B6 VM3 up, screenshots ──►  B7 stop / restart ──► B8 report
Arnav     A1 A5 A2 ──► A3 Spark Job 1 on raw file ──► A4 local-Kafka tests ──►    A6 consume → A7 job1+publish ─────────────────► A8 stats
Monty     M1 M5 ───► M2 producer1 ► M3 consumer2 ► M4 Spark Job 2 (+golden) ──►   M6 publish … M7 consume → job2 ─────────────► M8 stats
```

### 8.1 Task index

| ID | Owner | Task | Blocked by | Unblocks |
|---|---|---|---|---|
| S1 | Shriram | Create ZeroTier network and post the Network ID | — | B1, A5, M5, S2 |
| S2 | Shriram | VM2 setup: Java, Kafka, join ZeroTier | S1 | S3 |
| S3 | Shriram | Authorize all members, fill the IP table, push the repo skeleton | B1, A5, M5 | S4, everyone's `KAFKA_BOOTSTRAP` |
| S4 | Shriram | Generate the Cluster ID, write both properties files, send VM3's file to Blason | S3 | B3, S5 |
| S5 | Shriram | Format and start VM2 (controller + broker 1) | S4 | B4 |
| S6 | Shriram | Verify both brokers are registered | B5 | S7 |
| S7 | Shriram | Create both topics and describe them | S6 | M6, A6 |
| S8 | Shriram | Confirm VM1 and VM4 can reach 9092 on both brokers | S7 | S9 |
| S9 | Shriram | Conduct the integration run, resetting topics first | S8, A4, M4 | S10 |
| S10 | Shriram | Run the failure test with Blason | S9 | B8 |
| S11 | Shriram | Final review and commits | everything | — |
| B1 | Blason | Install ZeroTier, join the network, send node ID and IP | S1 | S3 |
| B2 | Blason | Install Java 17 and download Kafka | — | B4 |
| B3 | Blason | Receive `vm3.properties` and the Cluster ID | S4 | B4 |
| B4 | Blason | Format storage and start broker 2 | B2, B3, S5 | B5 |
| B5 | Blason | Confirm broker started, tell Shriram | B4 | S6 |
| B6 | Blason | Keep VM3 up through integration and screenshot every step | S9 | — |
| B7 | Blason | Stop VM3 on cue, restart on cue | S10 | — |
| B8 | Blason | Assemble evidence and write the report | A8, M8, S10 | — |
| A1 | Arnav | Environment setup and dataset download | — | A2, A4 |
| A2 | Arnav | Inspect the dataset and post timestamp format and bad-row types | A1 | A3 |
| A3 | Arnav | Spark Job 1 on the raw file; send `order_metrics.jsonl` to Monty | A2 | M4 (golden step) |
| A4 | Arnav | `consumer1.py` and `producer2.py` against local Docker Kafka | A1 | S9 |
| A5 | Arnav | Join ZeroTier | S1 | S3 |
| A6 | Arnav | Real run: consume `orders` | S7, M6 | A7 |
| A7 | Arnav | Real run: Spark Job 1, then publish `order_metrics` | A6 | M7 |
| A8 | Arnav | Send stats and logs to Blason | A7 | B8 |
| M1 | Monty | Environment setup and dataset download | — | M2, M3, M4 |
| M2 | Monty | `producer1.py` against local Kafka | M1 | S9 |
| M3 | Monty | `consumer2.py` against local Kafka | M1 | S9 |
| M4 | Monty | Spark Job 2 on synthetic data, then on Arnav's real output, saving golden CSVs | M1 (A3 for the golden step) | S9 |
| M5 | Monty | Join ZeroTier | S1 | S3 |
| M6 | Monty | Real run: publish `orders` | S7, S9 starts | A6 |
| M7 | Monty | Real run: consume `order_metrics`, run Spark Job 2, compare with golden | A7 | M8 |
| M8 | Monty | Send stats and outputs to Blason | M7 | B8 |

### 8.2 Critical path

S1 → joins (B1/A5/M5) → S3 → S4 → S5 → B4 → S6 → S7 → integration (M6 → A6 → A7 → M7) → S10

Everything else is off the critical path. Do it while you wait.

**Phase 0 is the only moment all four people must be online together.** It's a 30-minute call where everyone joins ZeroTier while installing Java and Kafka in parallel.

---

## 9. The graded integration run (exact order)

| Step | Who | Command | Pass condition |
|---|---|---|---|
| 0 | All | `export KAFKA_BOOTSTRAP=...` and activate the venv | — |
| 1 | Shriram | `scripts/describe_topics.sh` | All 4 partitions show `Isr: 1,2` |
| 2 | Shriram | `scripts/reset_topics.sh` | Both topics recreated empty, so no leftovers from test runs |
| 3 | Monty | `python vm1/producer1.py` | Prints N sent, 0 errors, both partitions non-zero |
| 4 | Arnav | `python vm4/consumer1.py` | `orders_landed.jsonl` has N lines |
| 5 | Arnav | `python vm4/spark_job1.py` | `order_metrics.jsonl` has K lines; stats add up |
| 6 | Arnav | `python vm4/producer2.py` | Prints K sent, 0 errors |
| 7 | Monty | `python vm1/consumer2.py` | `order_metrics_landed.jsonl` has K lines |
| 8 | Monty | `python vm1/spark_job2.py` | Both CSVs written and **identical to `golden/`** |
| — | Blason | Screenshot each step's terminal | Saved into `evidence/` |

Then Phase 3, the failure test (see Shriram's and Blason's files).

The steps are strictly sequential. Each consumer reads up to the end offsets it sees when it starts, so start it only after the previous producer has printed its final count.

---

## 10. Rules for every Claude Code session

1. Read `CLAUDE.md` and your own `team/0X_*.md` file before writing code.
2. Never rename topics, files, JSON keys or CSV columns. Sections 4 and 5 are the contract.
3. Read brokers only from `KAFKA_BOOTSTRAP`, defaulting to `localhost:9092`. Never hardcode IPs in Python.
4. Spark reads only local files. No Kafka connector, no HDFS.
5. Never hardcode outputs. Everything is computed from the data.
6. Don't add business rules beyond section 6.
7. Every script prints counts at the end and writes `~/bd-a2/stats/<script>_stats.json`, which feeds the report.
8. Every script creates its output directories if missing and overwrites its output files, so reruns are clean.
9. Claude Code does not run `git add`, `git commit` or `git push`. Humans commit their own folders.

---

## 11. Local dev Kafka (Arnav and Monty, before the real cluster exists)

```bash
docker run -d --name kafka-local -p 9092:9092 apache/kafka:3.9.1
docker exec kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 2 --replication-factor 1
docker exec kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic order_metrics --partitions 2 --replication-factor 1

# wipe between tests
docker rm -f kafka-local
```

Locally the replication factor is 1 because there is only one broker. The real cluster uses RF 2.

If you have no Docker, follow the single-node KRaft quickstart in the Kafka 3.9 docs using `~/kafka`.

---

## 12. Common environment setup (Arnav, Monty, Shriram)

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk python3-venv python3-pip curl
mkdir -p ~/bd-a2/{data,output,stats}
cd ~/bd-a2            # the cloned repo
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version         # must say 17
python -c "import pyspark, confluent_kafka; print(pyspark.__version__)"
```

ZeroTier (everyone):

```bash
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <NETWORK_ID>
sudo zerotier-cli info            # send the 10-character node ID to Shriram
# after Shriram authorizes you:
sudo zerotier-cli listnetworks    # status OK and an IP
ip -4 addr | grep -A2 zt          # your ZeroTier IP, send it to Shriram
```
