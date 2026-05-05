#!/bin/bash
# Unified Data Lakehouse - Safe Supervisor Script
# Starts FastAPI, Streamlit, and Dagster with port checks and PID tracking

LOGFILE="/app/orchestrate/startup.log"
mkdir -p /app/orchestrate

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"
}

wait_for_port() {
    local port=$1
    local max_wait=30
    local count=0
    while lsof -i :$port > /dev/null 2>&1; do
        if [ $count -ge $max_wait ]; then
            log "Port $port still in use after ${max_wait}s, killing occupant..."
            fuser -k $port/tcp 2>/dev/null
            sleep 2
            break
        fi
        log "Waiting for port $port to free up... ($count/$max_wait)"
        sleep 1
        count=$((count + 1))
    done
}

log "=== Starting Unified Data Lakehouse ==="

# Clear any stale port occupants
wait_for_port 8000
wait_for_port 8501
wait_for_port 3000

# Start FastAPI
log "Starting FastAPI on port 8000..."
uvicorn api.main:app --host 0.0.0.0 --port 8000 >> "$LOGFILE" 2>&1 &
FASTAPI_PID=$!
log "FastAPI PID: $FASTAPI_PID"

# Start Streamlit
log "Starting Streamlit on port 8501..."
streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --browser.gatherUsageStats false \
    >> "$LOGFILE" 2>&1 &
STREAMLIT_PID=$!
log "Streamlit PID: $STREAMLIT_PID"

sleep 5

# Start Dagster with auto-restart in foreground
log "Starting Dagster on port 3000..."
while true; do
    log "Launching Dagster..."
    dagster dev -f /app/orchestrate/pipeline.py \
        --host 0.0.0.0 \
        --port 3000 \
        >> "$LOGFILE" 2>&1
    EXIT=$?
    log "Dagster exited with code $EXIT. Restarting in 15 seconds..."
    sleep 15
done