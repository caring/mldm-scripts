#!/bin/bash

# Monitor migration progress
echo "=== Migration Progress Monitor ==="
echo ""

while true; do
  clear
  echo "=== Affiliate Notes Migration Progress ==="
  echo ""
  echo "📊 Local State Files:"
  
  if [ -f "migration-state/affiliate_notes/summary.json" ]; then
    echo "Summary:"
    cat migration-state/affiliate_notes/summary.json | jq '.'
  fi
  
  echo ""
  echo "📝 Recent Batches (last 5):"
  if [ -f "migration-state/affiliate_notes/batches.jsonl" ]; then
    tail -5 migration-state/affiliate_notes/batches.jsonl | while read line; do
      echo "$line" | jq -r '"  Batch \(.batch_number): \(.rows_fetched) rows, \(.success) success, \(.failed) failed - \(.timestamp)"'
    done
  fi
  
  echo ""
  echo "Press Ctrl+C to stop monitoring"
  echo "Refreshing in 5 seconds..."
  
  sleep 5
done

