#!/usr/bin/env bash
# S9 prep: delete both topics, wait until they're really gone, recreate, describe.
# Guarantees a clean, empty start so no stale test messages leak into the graded run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/env.sh"

case "$KAFKA_BOOTSTRAP $ZT_VM2 $ZT_VM3" in
  *SET_ZT*) echo "ERROR: fill scripts/env.sh with the real ZeroTier IPs first." >&2; exit 1 ;;
esac

KT="$KAFKA_HOME/bin/kafka-topics.sh"
TOPICS=(orders order_metrics)

echo "=== deleting topics ==="
for topic in "${TOPICS[@]}"; do
  "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$topic" --if-exists
done

echo "=== waiting for deletion to finish ==="
for topic in "${TOPICS[@]}"; do
  for ((i=1; i<=30; i++)); do
    # Capture the list first (avoids SIGPIPE under pipefail), then exact-match.
    list="$("$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --list)"
    if grep -qxF "$topic" <<<"$list"; then
      if [ "$i" -eq 30 ]; then
        echo "ERROR: $topic still present after 30 tries." >&2; exit 1
      fi
      sleep 2
    else
      echo "  $topic gone"
      break
    fi
  done
done

echo "=== recreating ==="
"$ROOT/scripts/create_topics.sh"

echo "=== verifying topics are empty (run-sheet pass condition: recreated empty) ==="
GO="$KAFKA_HOME/bin/kafka-get-offsets.sh"
for topic in "${TOPICS[@]}"; do
  total=$("$GO" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" --time -1 | awk -F: '{s+=$NF} END{print s+0}')
  [ "$total" -eq 0 ] || { echo "ERROR: $topic not empty after reset (end-offset sum=$total)." >&2; exit 1; }
  echo "  $topic empty (offset sum=0)"
done
