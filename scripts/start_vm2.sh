#!/usr/bin/env bash
# S5: format (first run only) and start VM2 = KRaft controller + Kafka broker 1.
# Run this in its OWN terminal and leave it open — the broker runs in the foreground.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/env.sh"

CLUSTER_ID="_2Xn7kSKSdOvrVoRv00JMA"      # cluster ID from the shared contract §3 (quoted: starts with '_')
CONFIG="$ROOT/config/vm2.properties"
DATA_DIR="$HOME/kafka-data"

# Guard: config must exist and already have VM2's real ZeroTier IP, not the placeholder.
if [ ! -f "$CONFIG" ]; then
  echo "ERROR: $CONFIG not found. Write config/vm2.properties first (S4)." >&2; exit 1
fi
if grep -q '<ZT_VM2>' "$CONFIG"; then
  echo "ERROR: $CONFIG still has the <ZT_VM2> placeholder. Fill in VM2's ZeroTier IP first." >&2; exit 1
fi

# Format once only. Hard rule: never reformat once ~/kafka-data is formatted.
if [ ! -f "$DATA_DIR/meta.properties" ]; then
  echo "=== first run: formatting $DATA_DIR ==="
  mkdir -p "$DATA_DIR"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONFIG"
else
  echo "=== $DATA_DIR already formatted — skipping format ==="
fi

echo "=== starting broker (foreground; keep this terminal open) ==="
# systemd-inhibit stops the laptop suspending (even lid-closed) while the broker runs.
exec systemd-inhibit --what=sleep:idle:handle-lid-switch --why="Kafka broker VM2" \
  "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG"
