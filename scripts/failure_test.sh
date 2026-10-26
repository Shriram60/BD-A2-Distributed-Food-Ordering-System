#!/usr/bin/env bash
# S10: broker-failure test. Semi-automatic — pauses at each human cue.
# RUN THIS IN YOUR OWN TERMINAL (it needs an interactive prompt).
#
# Proves availability of replicated partitions through VM2 alone while VM3 is down.
set -euo pipefail

[ -t 0 ] || { echo "ERROR: run failure_test.sh in your own terminal (it pauses for input)." >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/env.sh"

case "$KAFKA_BOOTSTRAP $ZT_VM2 $ZT_VM3" in
  *SET_ZT*) echo "ERROR: fill scripts/env.sh with the real ZeroTier IPs first." >&2; exit 1 ;;
esac

EV="$ROOT/evidence"; mkdir -p "$EV"
KT="$KAFKA_HOME/bin/kafka-topics.sh"
GO="$KAFKA_HOME/bin/kafka-get-offsets.sh"
CC="$KAFKA_HOME/bin/kafka-console-consumer.sh"
VM2_ONLY="$ZT_VM2:9092"
TOPICS=(orders order_metrics)

describe_all () {                 # $1 = bootstrap servers to query
  local bs="$1"
  for topic in "${TOPICS[@]}"; do
    echo "=== describe: $topic ==="
    "$KT" --bootstrap-server "$bs" --describe --topic "$topic"
    echo
  done
}

echo "### STEP 1: cluster healthy (expect Isr: 1,2 on all 4 partitions)"
describe_all "$KAFKA_BOOTSTRAP" | tee "$EV/ft_1_before.txt"

echo "### STEP 1a: ensure __consumer_offsets exists (RF 2) — else the VM2-only read would false-FAIL"
# The VM2-only console consumer needs a group coordinator, i.e. __consumer_offsets.
# With RF 2 that topic can't be created while VM3 is down, so warm it up now (both
# brokers up) and fail loudly if it still isn't there.
"$CC" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic orders --from-beginning \
  --max-messages 1 --timeout-ms 10000 >/dev/null 2>&1 || true
co_desc="$("$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic __consumer_offsets 2>/dev/null || true)"
if [ -z "$co_desc" ]; then
  echo "ERROR: __consumer_offsets does not exist yet. Run the integration run (S9) first, then this test." >&2
  exit 1
fi
{ echo "=== __consumer_offsets (internal, expect ReplicationFactor: 2) ==="; head -1 <<<"$co_desc"; } \
  | tee -a "$EV/ft_1_before.txt"

echo "### STEP 1b: end offsets per partition (pre-failure)"
: > "$EV/ft_1_offsets.txt"
declare -A EXPECTED
for topic in "${TOPICS[@]}"; do
  # One snapshot: write it to evidence AND sum it for the PASS/FAIL baseline.
  off="$("$GO" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" --time -1)"
  { echo "=== offsets: $topic ==="; echo "$off"; } | tee -a "$EV/ft_1_offsets.txt"
  EXPECTED[$topic]=$(awk -F: '{s+=$NF} END{print s+0}' <<<"$off")
done

read -r -p ">>> Tell Blason to STOP VM3, then press Enter... "
echo "waiting 20s for the cluster to react..."
sleep 20

echo "### STEP 2: after VM3 down (expect Leader: 1, Isr: 1, all under-replicated)"
{ describe_all "$VM2_ONLY"
  echo "=== under-replicated partitions ==="
  "$KT" --bootstrap-server "$VM2_ONLY" --describe --under-replicated-partitions
} | tee "$EV/ft_2_after.txt"

echo "### STEP 2b: messages readable through VM2 ALONE ($VM2_ONLY) — the availability proof"
: > "$EV/ft_2_counts.txt"
declare -A READ_VIA_VM2
for topic in "${TOPICS[@]}"; do
  # Isolate the pipeline: the console consumer exits non-zero on --timeout-ms, which
  # would otherwise trip `set -e`/pipefail and abort right at the proof step.
  n=$( { "$CC" --bootstrap-server "$VM2_ONLY" --topic "$topic" \
           --from-beginning --timeout-ms 15000 2>/dev/null || true; } | wc -l )
  READ_VIA_VM2[$topic]=$n
  echo "$topic: $n messages read via VM2 only" | tee -a "$EV/ft_2_counts.txt"
done

read -r -p ">>> Tell Blason to RESTART VM3, then press Enter... "
echo "polling up to 120s for ISR to return to 1,2..."
recovered=0
for ((i=1; i<=40; i++)); do
  urp="$("$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null || true)"
  if [ -z "$urp" ]; then recovered=1; break; fi
  sleep 3
done

echo "### STEP 3: recovered (expect Isr: 1,2 again)"
describe_all "$KAFKA_BOOTSTRAP" | tee "$EV/ft_3_recovered.txt"
if [ "$recovered" -eq 1 ]; then
  echo "ISR recovered: no under-replicated partitions." | tee -a "$EV/ft_3_recovered.txt"
else
  echo "NOTE: still under-replicated after 120s. Preferred-leader rebalance can take ~5 min; that's normal." \
    | tee -a "$EV/ft_3_recovered.txt"
fi

# ---- PASS/FAIL -------------------------------------------------------------
# Availability holds if, for each topic, the count read through VM2 alone equals
# the pre-failure end-offset sum. That sum == message count because reset_topics.sh
# recreates topics at offset 0 and the producers write no transactional control records.
echo
echo "================ PASS / FAIL ================"
overall=PASS
for topic in "${TOPICS[@]}"; do
  exp="${EXPECTED[$topic]}"; got="${READ_VIA_VM2[$topic]}"
  if [ "$exp" -eq "$got" ]; then
    echo "  $topic: PASS  (pre-failure offsets=$exp == read-via-VM2=$got)"
  else
    echo "  $topic: FAIL  (pre-failure offsets=$exp != read-via-VM2=$got)"
    overall=FAIL
  fi
done
echo "  OVERALL: $overall"
echo "============================================"
