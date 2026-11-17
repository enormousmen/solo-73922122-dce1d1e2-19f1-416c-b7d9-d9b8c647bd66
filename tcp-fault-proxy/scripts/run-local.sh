#!/usr/bin/env bash
# Run all three components locally without Docker.
# Usage: scripts/run-local.sh [venv-dir]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="${1:-$ROOT/.venv}"
LOG_DIR="${LOG_DIR:-/tmp/fp-logs}"
mkdir -p "$LOG_DIR"

if [ ! -x "$VENV/bin/python" ]; then
  echo "creating venv at $VENV (needs python3 with venv support)"
  python3 -m venv --without-pip "$VENV"
  if [ ! -f /tmp/get-pip.py ]; then
    curl -sS -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py
  fi
  "$VENV/bin/python" -I /tmp/get-pip.py
  "$VENV/bin/pip" install -r "$ROOT/requirements.txt"
fi

cd "$ROOT"
nohup "$VENV/bin/python" -m receiver.app \
  > "$LOG_DIR/receiver.log" 2>&1 & echo $! > "$LOG_DIR/receiver.pid"

TARGET_HOST=127.0.0.1 TARGET_PORT=9100 PROXY_PORT=9000 \
  nohup "$VENV/bin/python" -m uvicorn proxy.app:app --host 127.0.0.1 --port 8080 \
  > "$LOG_DIR/proxy.log" 2>&1 & echo $! > "$LOG_DIR/proxy.pid"

SENDER_TARGET_HOST=127.0.0.1 SENDER_TARGET_PORT=9000 \
  nohup "$VENV/bin/python" -m uvicorn sender.app:app --host 127.0.0.1 --port 8081 \
  > "$LOG_DIR/sender.log" 2>&1 & echo $! > "$LOG_DIR/sender.pid"

sleep 2
echo "receiver : TCP 127.0.0.1:9100, receipts API http://127.0.0.1:9101 (pid $(cat "$LOG_DIR/receiver.pid"))"
echo "proxy    : TCP 127.0.0.1:9000, control/docs  http://127.0.0.1:8080 (pid $(cat "$LOG_DIR/proxy.pid"))"
echo "sender   : control/docs        http://127.0.0.1:8081 (pid $(cat "$LOG_DIR/sender.pid"))"
echo "logs:     $LOG_DIR/{receiver,proxy,sender}.log"
