#!/bin/bash
# Unified Data Lakehouse startup script

echo "[$(date)] Starting Unified Data Lakehouse..."

# Start FastAPI in background
echo "[$(date)] Starting FastAPI..."
uvicorn api.main:app --host 0.0.0.0 --port 8000 &

# Start Streamlit in background
echo "[$(date)] Starting Streamlit..."
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0 &

# Start Dagster with existing auto-restart wrapper in foreground
echo "[$(date)] Starting Dagster via start_dagster.sh..."
bash /app/orchestrate/start_dagster.sh