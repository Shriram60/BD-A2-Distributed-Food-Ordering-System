#!/usr/bin/env bash
# S7: create both topics with 2 partitions, RF 2, then describe. Idempotent.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/env.sh"

case "$KAFKA_BOOTSTRAP $ZT_VM2 $ZT_VM3" in
  *SET_ZT*) echo "ERROR: fill scripts/env.sh with the real ZeroTier IPs first." >&2; exit 1 ;;
esac

KT="$KAFKA_HOME/bin/kafka-topics.sh"
for topic in orders order_metrics; do
  echo "=== create: $topic (partitions=2, RF=2) ==="
  "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --create \
    --topic "$topic" --partitions 2 --replication-factor 2 --if-not-exists
done
echo
"$ROOT/scripts/describe_topics.sh"
