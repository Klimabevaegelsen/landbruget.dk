#!/bin/bash

echo "[run.sh] Starting Xvfb on $DISPLAY ..."
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp &
XVFB_PID=$!

# Optional wait to ensure Xvfb is fully started
sleep 2

echo "[run.sh] Running pipeline (main.py) ..."
cd /app
python main.py

# Kill Xvfb cleanly if still running
kill $XVFB_PID
