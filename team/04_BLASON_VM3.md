# Blason — VM3: Kafka Broker 2, Failure-Test Operator, Evidence and Report

**You don't write any code.** Your VM runs Kafka Broker 2 using a config file Shriram sends you. You also keep the team's evidence folder and write the report.

`CLAUDE.md` in the repo has the full plan. This file is everything you personally need.

## What blocks you and what you block

| You wait on | For task |
|---|---|
| Nothing | B2: start immediately |
| Shriram S1 (ZeroTier Network ID) | B1 |
| Shriram S4 (`vm3.properties` + Cluster ID) and S5 ("VM2 is up") | B3, B4 |
| Arnav A8, Monty M8, Shriram S10 | B8 |

| You unblock | With task |
|---|---|
| Shriram S3 (he needs your ZeroTier IP) | B1 |
| Shriram S6 and S7 (cluster and topics) | B5. The real cluster can't exist without your broker. |
| The failure test | B7 |

**The most important thing:** during the integration run and the failure test, your VM must be on, connected, and running the broker.

---

## Your VM

- Ubuntu 22.04 or 24.04
- At least 2 GB RAM and 5 GB free disk
- Internet access

If it's a VirtualBox or VMware VM, the default NAT network is fine. ZeroTier works through it.

If you dual-boot Ubuntu instead of using a VM, that's fine too. Boot into Ubuntu before every session and keep the charger plugged in.

---

## B2. Install Java and Kafka (do this right now, no waiting)

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk curl netcat-openbsd
java -version          # must mention 17
cd ~
wget https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1 kafka
ls ~/kafka/bin | head  # you should see kafka-*.sh files
```

## B1. Join ZeroTier (as soon as Shriram posts the Network ID)

```bash
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <NETWORK_ID>
sudo zerotier-cli info
```

The `info` command prints a line like `200 info a1b2c3d4e5 ...`. Send the 10-character ID (`a1b2c3d4e5`) to Shriram.

Wait for Shriram to say he has authorized you, then:

```bash
sudo zerotier-cli listnetworks    # should say OK and show an IP
ip -4 addr | grep -A2 zt          # your ZeroTier IP, e.g. 10.147.x.x
```

Send that IP to Shriram. Then test that you can reach VM2, using the IP Shriram gives you:

```bash
ping -c 3 <ZT_VM2>
```

## B3. Get your config (when Shriram sends it)

Shriram sends you two things: the file `vm3.properties` and a **Cluster ID**, a long random string.

```bash
echo $HOME                         # e.g. /home/blason
mkdir -p ~/kafka-data
cp vm3.properties ~/kafka/config/vm3.properties
nano ~/kafka/config/vm3.properties
```

In `nano`:
1. Find the line `log.dirs=/home/BLASON_USER/kafka-data`.
2. Replace `/home/BLASON_USER` with what `echo $HOME` printed.
3. Save with Ctrl+O, Enter, then exit with Ctrl+X.

Change nothing else in the file.

## B4. Start your broker (after Shriram says "VM2 is up")

Open the firewall port, but only if the firewall is active:

```bash
sudo ufw status                    # if it says "active":
sudo ufw allow 9092/tcp
```

Format the storage **once**, then start:

```bash
cd ~/kafka
bin/kafka-storage.sh format -t <CLUSTER_ID> -c config/vm3.properties
systemd-inhibit --what=sleep:idle:handle-lid-switch --why="Kafka broker" \
  bin/kafka-server-start.sh config/vm3.properties
```

The `systemd-inhibit` part stops your laptop or VM from sleeping while the broker runs. Use this same start command every time you restart.

**Leave that terminal open.** It is your broker. Closing it stops the broker.

## B5. Confirm it worked

Within about 20 seconds the log should show a line containing `Kafka Server started`, with no repeating errors. Tell Shriram: **"Broker 2 is up."**

Take a screenshot and save it as `evidence/03_vm3_broker_started.png`.

### If something goes wrong

| What you see | Fix |
|---|---|
| `Invalid cluster.id` or `Unexpected cluster ID` | You formatted with the wrong ID. Run `rm -rf ~/kafka-data && mkdir ~/kafka-data`, then format again with the exact ID from Shriram. |
| Repeating `Connection to node 1 ... could not be established` | VM2 isn't reachable. Check `ping <ZT_VM2>`, check that Shriram's broker is running, and check that ZeroTier says OK. |
| `Address already in use` | A broker is already running. Run `bin/kafka-server-stop.sh`, wait 10 s, then start again. |
| Shriram says clients can't reach VM3 | Run `sudo ufw allow 9092/tcp`, and check that the `advertised.listeners` line in your file has **your** ZeroTier IP. |

**Golden rule: never run `kafka-storage.sh format` again after the first successful start.** To restart the broker, use only `bin/kafka-server-start.sh config/vm3.properties`.

---

## B6. Integration run day

Be on the call with your VM on and your broker terminal running.

Screenshot every step as teammates run it; they can share screens, or ask them for screenshots. Save into `evidence/` using the names in the checklist below.

## B7. Failure test (with Shriram, after the integration run)

1. Shriram says **"stop VM3"**. Then either:
   - **Option A**, the default: in a new terminal, run `cd ~/kafka && bin/kafka-server-stop.sh`. Your broker terminal shows it shutting down.
   - **Option B**, only if the TA said the VM must be powered off: `sudo poweroff`.
2. Wait while Shriram runs his checks, about 1–2 minutes.
3. Shriram says **"restart VM3"**.
   - With option A: `bin/kafka-server-start.sh config/vm3.properties`.
   - With option B: boot the VM, wait for ZeroTier (`sudo zerotier-cli listnetworks` shows OK), then run the same start command.
   - **Do not format.**
4. Tell Shriram when you see `Kafka Server started` again.

Screenshot your terminal at the stop and at the restart.

---

## B8. Evidence folder and report

### Start this on day 1, while waiting

Create `REPORT.md` with the outline below and fill in the parts that don't need results yet: introduction, architecture table, setup steps and team contributions.

### Evidence checklist (filenames in `evidence/`)

| # | File | Comes from |
|---|---|---|
| 01 | `01_cluster_up.txt` | Shriram (S6) |
| 02 | `02_topics_created.txt`, showing 2 partitions and RF 2 for both topics | Shriram (S7) |
| 03 | `03_vm3_broker_started.png` | You |
| 04 | `04_zerotier_members.png`, the 4 members with IPs | Shriram |
| 05 | `05_producer1_run.png` | Monty |
| 06 | `06_consumer1_run.png` and `06_orders_landed_wc.png` | Arnav |
| 07 | `07_spark_job1_run.png` and `07_order_metrics_head.png` | Arnav |
| 08 | `08_producer2_run.png` | Arnav |
| 09 | `09_consumer2_run.png` | Monty |
| 10 | `10_spark_job2_run.png`, `10_restaurant_summary_head.png` and `10_city_summary_head.png` | Monty |
| 11 | `ft_1_before.txt`, `ft_1_offsets.txt`, `ft_2_after.txt`, `ft_2_counts.txt` and `ft_3_recovered.txt` | Shriram (S10) |
| 12 | `12_vm3_stop.png` and `12_vm3_restart.png` | You |
| 13 | All `stats/*.json` files, copied in | Arnav, Monty |

### `REPORT.md` outline

1. **Introduction.** The problem, and the two-stage Kafka → Spark → Kafka → Spark pipeline. Paraphrase the assignment's intro.
2. **Architecture.** The VM role table: VM1 Monty, VM2 Shriram, VM3 Blason, VM4 Arnav. Show the data flow `orders` → VM4 → `order_metrics` → VM1, and the ZeroTier IPs.
3. **Kafka cluster setup.** KRaft with VM2 as controller and broker and VM3 as broker. The key config lines, and why `advertised.listeners` and `offsets.topic.replication.factor=2` matter.
4. **Topics.** `orders` and `order_metrics`, each with 2 partitions and RF 2. Paste the describe output.
5. **Stage 1, order enrichment.** Producer 1 counts, consumer 1 counts, then the Spark Job 1 cleaning table. Get the table from `spark_job1_stats.json`: rows dropped per rule, duplicates removed, and final records. Include sample output lines.
6. **Stage 2, operations analytics.** Consumer 2 counts, then Spark Job 2 results with the first rows of both CSVs.
7. **Broker failure test.** Before, after and recovered describe outputs. Explain that leaders moved to broker 1, the ISR shrank to `[1]`, all messages were still readable through VM2 alone, and the ISR recovered after the restart.
8. **Interpretation decisions.** Copy table D1–D9 from `CLAUDE.md` §6, with the TA's answers.
9. **Challenges and fixes.** Ask each teammate for one or two.
10. **Team contributions.** One line per member.

Ask Shriram to review it before submission. Commit `evidence/` and `REPORT.md` yourself, or send them to Shriram to commit.
