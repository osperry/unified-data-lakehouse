#!/bin/bash
# Auto-restart Dagster if it crashes
# Run this instead of dagster dev directly

LOGFILE="/app/orchestrate/dagster.log"
CMD="dagster dev -f /app/orchestrate/pipeline.py --host 0.0.0.0 --port 3000"

echo "Starting Dagster auto-restart wrapper..."

while true; do
    echo "[$(date)] Starting Dagster..." | tee -a "$LOGFILE"
    $CMD >> "$LOGFILE" 2>&1
    EXIT_CODE=$?
    echo "[$(date)] Dagster exited with code $EXIT_CODE. Restarting in 10 seconds..." | tee -a "$LOGFILE"
    sleep 10
done
