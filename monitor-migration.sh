#!/bin/bash

if [ -z "${ENVIRONMENT:-}" ]; then
  echo 'ENVIRONMENT is required. Use "stage" or "prod".'
  exit 1
fi

case "$ENVIRONMENT" in
  prod|stage) ;;
  *)
    echo "Unsupported ENVIRONMENT=\"$ENVIRONMENT\". Use \"stage\" or \"prod\"."
    exit 1
    ;;
esac

STATE_ROOT="migration-state/$ENVIRONMENT"
SUMMARY_PATH="$STATE_ROOT/summary.json"
BATCHES_PATH="$STATE_ROOT/affiliate_notes/batches.jsonl"

# Monitor migration progress
echo "=== Migration Progress Monitor ==="
echo "Environment: $ENVIRONMENT"
echo ""

while true; do
  clear
  echo "=== Affiliate Notes Migration Progress ==="
  echo ""
  echo "📊 Local State Files:"
  
  if [ -f "$SUMMARY_PATH" ]; then
    echo "Summary:"
    cat "$SUMMARY_PATH" | jq '.'
  fi
  
  echo ""
  echo "📝 Recent Batches (last 5):"
  if [ -f "$BATCHES_PATH" ]; then
    tail -5 "$BATCHES_PATH" | while read line; do
      echo "$line" | jq -r '"  Batch \(.batch_number): \(.rows_fetched) rows, \(.success) success, \(.failed) failed - \(.timestamp)"'
    done
  fi
  
  echo ""
  echo "Press Ctrl+C to stop monitoring"
  echo "Refreshing in 5 seconds..."
  
  sleep 5
done

