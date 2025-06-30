#!/bin/bash

echo "[run.sh] Starting Xvfb on $DISPLAY ..."
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp &
XVFB_PID=$!

# Optional wait to ensure Xvfb is fully started
sleep 2

echo "[run.sh] Running pipeline (main.py) ..."
cd /app

# Check if GCS_BUCKET is set and not already in PIPELINE_ARGS
if [ -n "$GCS_BUCKET" ] && [[ ! "$PIPELINE_ARGS" =~ --gcs-bucket ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --gcs-bucket $GCS_BUCKET"
fi

# Check if PIPELINE_ARGS environment variable is set
if [ -n "$PIPELINE_ARGS" ]; then
    echo "[run.sh] Running pipeline with arguments: $PIPELINE_ARGS"
    python main.py $PIPELINE_ARGS
else
    # Default command (runs all stages)
    echo "[run.sh] Running pipeline with default arguments"
    python main.py
fi

# Example with parameters (commented out)
# Uncomment and modify as needed:
# python main.py --start-date 2025-01-01 --end-date 2025-05-01 --log-level DEBUG --stage silver --gcs-bucket your-gcs-bucket-name

# Kill Xvfb cleanly if still running
kill $XVFB_PID
