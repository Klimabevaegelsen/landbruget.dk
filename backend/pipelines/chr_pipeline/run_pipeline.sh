#!/bin/bash
# Run the CHR pipeline

echo "Starting CHR pipeline..."
docker compose down
docker compose build
docker compose up

echo "Pipeline completed."
