#!/bin/sh
# Starts the API. WEB_CONCURRENCY=N runs N uvicorn worker processes (use ~1 per vCPU);
# Prometheus counters are then aggregated across workers via PROMETHEUS_MULTIPROC_DIR.
set -e
WORKERS="${WEB_CONCURRENCY:-1}"
if [ "$WORKERS" -gt 1 ]; then
  export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom-multiproc}"
  rm -rf "$PROMETHEUS_MULTIPROC_DIR" && mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi
# Large backlog so a connection stampede queues in the kernel instead of being refused.
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8080}" --workers "$WORKERS" \
  --backlog 4096 --timeout-keep-alive 30 --no-access-log --proxy-headers --forwarded-allow-ips='*'
