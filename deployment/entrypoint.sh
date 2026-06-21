#!/bin/bash
set -e

cleanup() { kill 0; }
trap cleanup EXIT

cd /app

echo "=== NL2SQL Agent Starting ==="

# Start API server
echo "[1/3] API server → :8000"
python -m uvicorn api.app:app --host 0.0.0.0 --port 8000 --log-level warning &
sleep 2

# Start Worker (Kafka consumer)
echo "[2/3] Worker starting..."
python -m worker.main &
sleep 1

# Start Streamlit (foreground — container stays alive)
echo "[3/3] Streamlit → :8501"
exec streamlit run ui/app.py --server.port 8501 --server.address 0.0.0.0
