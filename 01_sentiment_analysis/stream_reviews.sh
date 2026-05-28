#!/usr/bin/env bash
set -euo pipefail

DATA_FILE="$(dirname "$0")/reviews.jsonl"

echo "Streaming reviews data from $DATA_FILE -> topic: customer-reviews"

count=0
while IFS= read -r line; do
  count=$((count + 1))
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic customer-reviews \
    <<< "$line"
  echo "  -> sent: $(echo "$line" | grep -o '"review_id":"[^"]*"')"
  sleep 2
done < "$DATA_FILE"

echo "Done. Produced $count record(s)."
