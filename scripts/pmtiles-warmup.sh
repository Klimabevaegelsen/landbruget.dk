#!/bin/bash

# PMTiles Cache Warmup Script
# Run this as a cron job: 0 3 * * 1 /path/to/pmtiles-warmup.sh

set -e

# Configuration
FRONTEND_URL="${FRONTEND_URL:-https://landbruget.dk}"
PMTILES_BASE_URL="https://data.pesticidkortet.dk"
LOG_FILE="/var/log/pmtiles-warmup.log"
MAX_PARALLEL=3

# PMTiles files to warm (in priority order)
PMTILES_FILES=(
    # Current year files (highest priority)
    "field_analysis_2023.pmtiles"
    "field_analysis_2024.pmtiles"

    # Background layers (used by all users)
    "bnbo_areas.pmtiles"
    "buildings_proximity_2024.pmtiles"

    # Historical years
    "field_analysis_2022.pmtiles"
    "field_analysis_2021.pmtiles"
    "field_analysis_2020.pmtiles"
    "field_analysis_2019.pmtiles"
    "field_analysis_2018.pmtiles"
    "field_analysis_2017.pmtiles"
    "field_analysis_2016.pmtiles"
    "field_analysis_2015.pmtiles"

    # Large files (lower priority)
    "wetlands_all_2024.pmtiles"
    "water_projects_2024.pmtiles"
)

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to warm a single PMTiles file
warm_pmtiles() {
    local filename=$1
    local proxy_url="${FRONTEND_URL}/api/pmtiles/pmtiles/${filename}"
    local direct_url="${PMTILES_BASE_URL}/pmtiles/${filename}"

    log "🔥 Warming: ${filename}"

    # First, check if file exists
    if ! curl -sf --head "$direct_url" >/dev/null 2>&1; then
        log "❌ File not found: ${filename}"
        return 1
    fi

    # Get file size for logging
    local file_size=$(curl -sI "$direct_url" | grep -i content-length | cut -d' ' -f2 | tr -d '\r')

    # Warm the cache by requesting the PMTiles header (first 16KB)
    local start_time=$(date +%s.%N)

    local response=$(curl -s -w "%{http_code}|%{size_download}|%{time_total}" \
        -H "Range: bytes=0-16383" \
        -H "User-Agent: PMTiles-Warmup-Script/1.0" \
        -H "X-Cache-Warming: true" \
        -o /dev/null \
        "$proxy_url" 2>/dev/null || echo "000|0|0")

    local http_code=$(echo "$response" | cut -d'|' -f1)
    local downloaded_size=$(echo "$response" | cut -d'|' -f2)
    local time_total=$(echo "$response" | cut -d'|' -f3)

    if [[ $http_code -eq 206 || $http_code -eq 200 ]]; then
        log "✅ Warmed: ${filename} (${file_size} bytes total, ${downloaded_size} bytes cached, ${time_total}s)"
        return 0
    else
        log "❌ Failed: ${filename} (HTTP ${http_code})"
        return 1
    fi
}

# Function to warm files in parallel batches
warm_batch() {
    local batch=("$@")
    local pids=()

    log "📦 Processing batch: ${batch[*]}"

    # Start warming files in parallel
    for filename in "${batch[@]}"; do
        warm_pmtiles "$filename" &
        pids+=($!)
    done

    # Wait for all files in batch to complete
    local success_count=0
    local error_count=0

    for pid in "${pids[@]}"; do
        if wait "$pid"; then
            ((success_count++))
        else
            ((error_count++))
        fi
    done

    log "📊 Batch completed: ${success_count} success, ${error_count} errors"
    return $error_count
}

# Main warmup function
main() {
    log "🚀 Starting PMTiles cache warmup..."
    log "📋 Files to warm: ${#PMTILES_FILES[@]}"

    local total_success=0
    local total_errors=0
    local start_time=$(date +%s)

    # Process files in batches
    for ((i=0; i<${#PMTILES_FILES[@]}; i+=MAX_PARALLEL)); do
        local batch=("${PMTILES_FILES[@]:i:MAX_PARALLEL}")

        if warm_batch "${batch[@]}"; then
            ((total_success += ${#batch[@]}))
        else
            local batch_errors=$?
            ((total_errors += batch_errors))
            ((total_success += ${#batch[@]} - batch_errors))
        fi

        # Small delay between batches to be nice to the server
        if [[ $((i + MAX_PARALLEL)) -lt ${#PMTILES_FILES[@]} ]]; then
            log "⏳ Waiting 2 seconds before next batch..."
            sleep 2
        fi
    done

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log "🎯 PMTiles cache warmup completed!"
    log "📊 Final results: ${total_success} success, ${total_errors} errors"
    log "⏱️ Total duration: ${duration} seconds"

    # Calculate failure rate
    local total_files=${#PMTILES_FILES[@]}
    local failure_rate=$((total_errors * 100 / total_files))

    if [[ $failure_rate -gt 20 ]]; then
        log "❌ High failure rate: ${failure_rate}% (${total_errors}/${total_files})"
        return 1
    fi

    log "✅ Warmup successful with ${failure_rate}% failure rate"
    return 0
}

# Verification function
verify_cache() {
    log "🔍 Verifying cache status..."

    local key_files=("field_analysis_2023.pmtiles" "bnbo_areas.pmtiles")
    local verification_errors=0

    for filename in "${key_files[@]}"; do
        local url="${FRONTEND_URL}/api/pmtiles/pmtiles/${filename}"

        if curl -sf --head "$url" >/dev/null 2>&1; then
            log "✅ Verified: ${filename}"
        else
            log "❌ Verification failed: ${filename}"
            ((verification_errors++))
        fi
    done

    if [[ $verification_errors -eq 0 ]]; then
        log "✅ All key files verified successfully"
        return 0
    else
        log "❌ ${verification_errors} verification failures"
        return 1
    fi
}

# Error handling
cleanup() {
    log "🧹 Cleaning up background processes..."
    jobs -p | xargs -r kill 2>/dev/null || true
}

trap cleanup EXIT

# Create log directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Run the warmup
if main && verify_cache; then
    log "🎉 PMTiles cache warmup completed successfully"
    exit 0
else
    log "💥 PMTiles cache warmup failed"
    exit 1
fi
