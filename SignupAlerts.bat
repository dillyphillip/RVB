#!/bin/bash
# ensure_signupalerts.sh
set -euo pipefail

PY="/home/ec2-user/miniconda3/bin/python"
SCRIPT="/home/ec2-user/SignupAlerts.py"
LOG="$HOME/signupalerts.log"

# Sanity: make sure the interpreter exists
if [ ! -x "$PY" ]; then
  echo "$(date -Is) ERROR: Python not found at $PY"
  exit 2
fi

# Is SignupAlerts.py already running? (substring check via ps|grep)
if ps aux | grep -F "$SCRIPT" | grep -v grep | grep -v "$0" > /dev/null; then
  echo "$(date -Is) OK: $SCRIPT already running."
  exit 0
fi

# Start it with the explicit Python path (no conda activation needed)
# cd to the script’s directory in case it has relative imports/files
cd "$(dirname "$SCRIPT")"

nohup "$PY" "$SCRIPT" >> "$LOG" 2>&1 &
echo "$(date -Is) STARTED: $SCRIPT (PID $!)" | tee -a "$LOG"
