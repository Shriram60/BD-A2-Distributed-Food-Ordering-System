# scripts/env.sh — shared environment for the BD A2 cluster.
# USAGE:  source scripts/env.sh
#
# This file is SOURCED into your interactive shell. Do NOT add
# `set -euo pipefail` or any `exit` here — either one would close your
# terminal the next time a command fails. Keep it to `export`s only.

# ZeroTier IPs — filled in during S3 (Phase B). Loud placeholders until then,
# so a half-configured shell fails visibly instead of silently producing
# an empty KAFKA_BOOTSTRAP like ":9092,:9092".
export ZT_VM1="${ZT_VM1:-10.113.232.224}"   # Monty   (VM1)
export ZT_VM2="${ZT_VM2:-10.113.232.128}"   # Shriram (VM2) — this machine
export ZT_VM3="${ZT_VM3:-SET_ZT_VM3}"   # Blason  (VM3)
export ZT_VM4="${ZT_VM4:-10.113.232.70}"   # Arnav   (VM4)

# Kafka install location (per the shared contract §2).
export KAFKA_HOME="$HOME/kafka"

# All Python clients read the broker list from here. Always list BOTH brokers
# so clients keep working when VM3 goes down (shared contract §3).
export KAFKA_BOOTSTRAP="$ZT_VM2:9092,$ZT_VM3:9092"
