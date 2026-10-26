# Shriram — VM2: KRaft Controller + Broker 1, Cluster Lead

**Read `CLAUDE.md` first.** It holds the versions, names, schemas, decisions and the task index this file refers to.

## Your job in one line

You own the network and the Kafka cluster, you are the critical path for everyone's real runs, you conduct the integration run and the failure test, and you are the TA contact.

## What blocks you and what you block

| You wait on | For task |
|---|---|
| Blason B1, Arnav A5, Monty M5 (ZeroTier joins) | S3 |
| Blason B5 (broker 2 started) | S6 |
| Arnav A4 and Monty M2/M3/M4 (code tested locally) | S9 |

| You unblock | With task |
|---|---|
| Everyone joining ZeroTier | S1, the Network ID |
| Blason starting broker 2 | S4, `vm3.properties` + Cluster ID, and S5, VM2 running |
| Monty's and Arnav's real runs | S7, topics created |
| The whole integration run | S9 |

**Rule of thumb:** do S1 → S7 as fast as possible. Everything else of yours can be done while waiting.

---

## Phase 0 — the 30-minute kickoff call

### S1. Create the ZeroTier network (do this first)
1. Go to my.zerotier.com, then Create A Network.
2. Keep it **Private**, so members must be authorized.
3. Leave the auto-assigned IPv4 pool on.
4. Post the **Network ID** in the group chat. Everyone runs the ZeroTier block from `CLAUDE.md` §12.

### S2. Set up VM2 (in parallel with everyone else's installs)

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk python3-venv curl
cd ~ && wget https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz && mv kafka_2.13-3.9.1 kafka
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <NETWORK_ID>
```

### S3. Authorize members, fill the IP table, push the repo skeleton
1. On the network page under Members, tick **Auth** for all 4 node IDs. Give each a name like VM1-Monty.
2. Note each member's managed IP and fill in the table in `CLAUDE.md` §3.
3. From VM2, `ping -c 2` every other ZeroTier IP. All three must reply.
4. Create the repo with the layout in `CLAUDE.md` §7.
   - Copy `00_TEAM_PLAN.md` to the root as `CLAUDE.md`.
   - Put the 4 per-person files in `team/`.
   - Add `requirements.txt` and `.gitignore`.
5. Commit and push it yourself, then post the repo link. Everyone clones it.

---

## Phase 1 — bring up the cluster (critical path)

### S4. Cluster ID and broker configs

```bash
~/kafka/bin/kafka-storage.sh random-uuid      # e.g. q1Sh7_xxxxxxxxxxxxxxx
```

Put the ID into `CLAUDE.md` §3.

**`config/vm2.properties`**

```properties
# VM2: combined KRaft controller + broker 1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@<ZT_VM2>:9093

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://<ZT_VM2>:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/home/<your-user>/kafka-data

# topic defaults (topics are still created explicitly)
num.partitions=2
default.replication.factor=2
auto.create.topics.enable=false

# internal topics must survive VM3 going down
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
min.insync.replicas=1

# faster consumer startup
group.initial.rebalance.delay.ms=0
```

**`config/vm3.properties`**

```properties
# VM3: broker 2 only
process.roles=broker
node.id=2
controller.quorum.voters=1@<ZT_VM2>:9093

listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<ZT_VM3>:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/home/BLASON_USER/kafka-data

num.partitions=2
default.replication.factor=2
auto.create.topics.enable=false
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
min.insync.replicas=1
group.initial.rebalance.delay.ms=0
```

Why these settings matter:
- **`advertised.listeners` must be the ZeroTier IP.** If it is localhost or a LAN IP, clients on other VMs connect once and then fail. This is the number-one cross-VM Kafka bug.
- **`offsets.topic.replication.factor=2`.** Without it, `__consumer_offsets` may live only on VM3, and every consumer hangs during the failure test.
- **`min.insync.replicas=1`.** Writes and reads keep working with one broker alive.

Send `vm3.properties` and the Cluster ID to Blason on WhatsApp. **This unblocks B3.**

### S5. Format and start VM2

```bash
mkdir -p ~/kafka-data
cd ~/kafka
bin/kafka-storage.sh format -t <CLUSTER_ID> -c ~/bd-a2/config/vm2.properties
systemd-inhibit --what=sleep:idle:handle-lid-switch --why="Kafka broker" \
  bin/kafka-server-start.sh ~/bd-a2/config/vm2.properties   # leave this terminal open
```

You are on dual-boot Ubuntu, so `systemd-inhibit` stops the laptop suspending while the broker runs. A suspended VM2 kills the whole cluster, because it is also the controller. Keep the charger in during sessions.

If `ufw` is active, open the ports:

```bash
sudo ufw status
sudo ufw allow 9092/tcp
sudo ufw allow 9093/tcp
```

Tell Blason "VM2 is up". **This unblocks B4.**

Never run `format` again after the first time, unless you deliberately wipe `~/kafka-data`.

### S6. Verify both brokers (after Blason says B5 is done)

```bash
cd ~/kafka
bin/kafka-metadata-quorum.sh --bootstrap-server <ZT_VM2>:9092 describe --status
bin/kafka-broker-api-versions.sh --bootstrap-server <ZT_VM2>:9092 | grep "id:"
```

You should see a leader with id 1 and two brokers listed: `id: 1` at ZT_VM2 and `id: 2` at ZT_VM3. Save both outputs to `evidence/01_cluster_up.txt`.

### S7. Create the topics

Write these scripts yourself or have Claude Code do it with the prompt at the bottom. Their behavior:

- **`scripts/create_topics.sh`**: creates `orders` and `order_metrics` with `--partitions 2 --replication-factor 2 --if-not-exists`, then runs describe.
- **`scripts/describe_topics.sh`**: runs `kafka-topics.sh --describe` for both topics.
- **`scripts/reset_topics.sh`**:
  1. Deletes both topics.
  2. Loops until `--list` no longer shows them.
  3. Recreates them.
  4. Describes them.
- All scripts read `KAFKA_BOOTSTRAP` and use `~/kafka/bin`.

```bash
export KAFKA_BOOTSTRAP="<ZT_VM2>:9092,<ZT_VM3>:9092"
scripts/create_topics.sh
```

Expected: 4 partitions in total, each with `ReplicationFactor: 2` and `Isr: 1,2`, and leaders spread across 1 and 2. Save the output to `evidence/02_topics_created.txt`. Post "topics are up". **This unblocks M6 and A6.**

### S8. Reachability check

Ask Monty and Arnav to run this on their VMs:

```bash
nc -zv <ZT_VM2> 9092 && nc -zv <ZT_VM3> 9092
```

Both must succeed. If VM3 fails, it's Blason's firewall or his `advertised.listeners`.

---

## Phase 1, off the critical path

### Ask the TA (send it right after S1)

> Hi, for A2:
>
> (1) Should Spark Job 1's metrics also be rounded to 2 decimals, or only Job 2's outputs?
>
> (2) If delivery_time_min is 0, what should delivery_speed_kmph be: null, 0, or should the record be dropped?
>
> (3) For delay_category, how do fractional delays like 15.5 min map (SLIGHT or MODERATE)?
>
> (4) The expected schema says "order amount": we're assuming order_amount. Is that right?
>
> (5) For the broker-failure test, is stopping the Kafka process on VM3 enough, or should VM3 be powered off?
>
> (6) Some of us are running native Ubuntu (dual boot) instead of VMs, connected over ZeroTier. Is that acceptable for the "4 VMs"?

Update `CLAUDE.md` §6 with each answer and post the change in the group.

### Write `scripts/failure_test.sh`

It should be semi-automatic, pausing with `read -p` at each cue:

1. Run describe and save it to `evidence/ft_1_before.txt`.
2. Run `kafka-get-offsets.sh` for both topics and save the end offsets to `evidence/ft_1_offsets.txt`.
3. Pause with: "Tell Blason to stop VM3, then press Enter".
4. Sleep 20 s.
5. Run describe again plus `--under-replicated-partitions`, and save to `evidence/ft_2_after.txt`.
6. Count messages using **only** VM2 as the bootstrap:
   ```bash
   bin/kafka-console-consumer.sh --bootstrap-server <ZT_VM2>:9092 --topic orders --from-beginning --timeout-ms 15000 2>/dev/null | wc -l
   ```
   Repeat for `order_metrics`, and save the counts to `evidence/ft_2_counts.txt`.
7. Pause with: "Tell Blason to restart VM3, then press Enter".
8. Sleep 30 s, then describe and save to `evidence/ft_3_recovered.txt`.

---

## Phase 2 — S9: conduct the integration run

Run the table in `CLAUDE.md` §9 step by step on a call. You run steps 1–2, then call each step out:

- Monty runs step 3 and reads out N.
- Arnav runs steps 4–6 and reads out N and K.
- Monty runs steps 7–8 and confirms the CSVs match `golden/`.

Before you start, make sure Blason is sharing his screen or taking screenshots.

**Pass criteria:**
- The N lines published equal the N lines landed on VM4.
- The K records published equal the K lines landed on VM1.
- The final CSVs are identical to `golden/`.

---

## Phase 3 — S10: failure test, with Blason

Run this after a successful S9, so both topics hold data and `__consumer_offsets` exists.

1. `scripts/failure_test.sh`, step 1: `Isr: 1,2` everywhere.
2. Cue Blason to stop the broker or power off VM3 (B7).
3. After: every partition shows `Leader: 1` and `Isr: 1`. Partitions that broker 2 used to lead have moved to broker 1. All 4 are under-replicated.
4. Message counts read through VM2 alone must equal the end offsets from step 1. **This is the proof of availability.**
5. Cue Blason to restart. The ISR returns to `1,2`.
   - Leaders may stay on broker 1 for up to about 5 minutes, until the automatic preferred-leader rebalance runs. That's normal; say so in the report.

Out of scope: do **not** stop VM2. Controller handover is excluded by the spec.

---

## Phase 4 — S11: review and commit
- Check that `REPORT.md` covers everything in Blason's checklist.
- Check the repo has no data files, outputs or IPs inside Python code.
- Commit `config/` and `scripts/` yourself.
- Remind Arnav, Monty and Blason to commit their own folders.

---

## Claude Code prompt for your scripts

> Read CLAUDE.md and team/01_SHRIRAM_VM2.md. Write scripts/create_topics.sh, scripts/describe_topics.sh, scripts/reset_topics.sh and scripts/failure_test.sh exactly as specified in tasks S7 and "Write scripts/failure_test.sh".
>
> Use bash with `set -euo pipefail`. Read the broker list from KAFKA_BOOTSTRAP and fail with a clear message if it's unset. Use ~/kafka/bin for all Kafka CLIs. failure_test.sh additionally needs ZT_VM2 as an env var for the VM2-only consumer, and writes its captures into evidence/.
>
> Make them executable. Do not run git commands.
