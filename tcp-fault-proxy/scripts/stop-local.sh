#!/usr/bin/env bash
# Stop the components started by run-local.sh.
set -euo pipefail
LOG_DIR="${LOG_DIR:-/tmp/fp-logs}"
for name in receiver proxy sender; do
  pidfile="$LOG_DIR/$name.pid"
  if [ -f "$pidfile" ]; then
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" && echo "stopped $name (pid $pid)"
    fi
    rm -f "$pidfile"
  fi
done
