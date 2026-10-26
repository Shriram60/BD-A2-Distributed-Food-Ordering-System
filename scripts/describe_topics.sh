#!/usr/bin/env bash
# Describe both topics (orders, order_metrics). Read-only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/env.sh"

# Refuse to run until env.sh holds real ZeroTier IPs (placeholders read SET_ZT_*).
case "$KAFKA_BOOTSTRAP $ZT_VM2 $ZT_VM3" in
  *SET_ZT*) echo "ERROR: fill scripts/env.sh with the real ZeroTier IPs first." >&2; exit 1 ;;
esac

KT="$KAFKA_HOME/bin/kafka-topics.sh"
for topic in orders order_metrics; do
  echo "=== describe: $topic ==="
  "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$topic"
  echo
done
