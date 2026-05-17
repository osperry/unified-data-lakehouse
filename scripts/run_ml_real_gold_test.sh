#!/bin/zsh
set -euo pipefail

REPO_ROOT="/Users/operry/Projects/osp-lakehouse"
LOG_DIR="$REPO_ROOT/logs"
RUN_TS="$(date -u +"%Y%m%dT%H%M%SZ")"
LOG_FILE="$LOG_DIR/ml_real_gold_test_${RUN_TS}.log"

mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

echo "==================================================" | tee -a "$LOG_FILE"
echo "Starting ML real Gold validation test: $RUN_TS" | tee -a "$LOG_FILE"
echo "Repo: $REPO_ROOT" | tee -a "$LOG_FILE"
echo "==================================================" | tee -a "$LOG_FILE"

# Run the automated flow:
# 1. Export local NYC 311 Gold Parquet
# 2. Run ML validation
# 3. Run health check
/usr/bin/make ml-real-gold-test 2>&1 | tee -a "$LOG_FILE"

echo "==================================================" | tee -a "$LOG_FILE"
echo "Completed ML real Gold validation test: $(date -u +"%Y%m%dT%H%M%SZ")" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "==================================================" | tee -a "$LOG_FILE"
