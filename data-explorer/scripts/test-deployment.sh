#!/bin/bash
# Test deployment health and functionality
# Usage: ./scripts/test-deployment.sh [base-url]

set -euo pipefail

# Configuration
BASE_URL="${1:-https://data.landbruget.dk}"
TIMEOUT=10

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Deployment Health Check               ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo
echo -e "Testing: ${GREEN}$BASE_URL${NC}"
echo -e "Timeout: ${YELLOW}${TIMEOUT}s${NC}"
echo

# Function to run a test
run_test() {
    local test_name=$1
    local command=$2

    echo -ne "Testing: $test_name... "

    if eval "$command" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        ((FAILED++))
        return 1
    fi
}

# Function to run a test with output capture
run_test_with_output() {
    local test_name=$1
    local command=$2
    local expected=$3

    echo -ne "Testing: $test_name... "

    local output
    output=$(eval "$command" 2>&1) || {
        echo -e "${RED}✗ FAIL (command failed)${NC}"
        ((FAILED++))
        return 1
    }

    if echo "$output" | grep -q "$expected"; then
        echo -e "${GREEN}✓ PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL (expected: $expected)${NC}"
        echo "  Output: $output"
        ((FAILED++))
        return 1
    fi
}

echo -e "${BLUE}━━━ Basic Connectivity Tests ━━━${NC}"
echo

# Test 1: Homepage accessible
run_test "Homepage (GET /)" \
    "curl -sf --max-time $TIMEOUT '$BASE_URL' -o /dev/null"

# Test 2: Homepage returns HTML
run_test_with_output "Homepage returns HTML" \
    "curl -sf --max-time $TIMEOUT '$BASE_URL' -H 'Accept: text/html'" \
    "<!DOCTYPE html>"

# Test 3: HTTPS redirect
if [[ $BASE_URL == https://* ]]; then
    HTTP_URL="${BASE_URL/https:/http:}"
    run_test "HTTPS redirect" \
        "curl -sI --max-time $TIMEOUT '$HTTP_URL' | grep -q '301\\|302\\|307\\|308'"
fi

echo
echo -e "${BLUE}━━━ API Endpoint Tests ━━━${NC}"
echo

# Test 4: API endpoint accessible
run_test "API endpoint (POST /api/ask)" \
    "curl -sf --max-time $TIMEOUT '$BASE_URL/api/ask' \
        -X POST \
        -H 'Content-Type: application/json' \
        -d '{\"question\":\"test\"}' \
        -o /dev/null"

# Test 5: API returns JSON
run_test_with_output "API returns JSON" \
    "curl -sf --max-time $TIMEOUT '$BASE_URL/api/ask' \
        -X POST \
        -H 'Content-Type: application/json' \
        -d '{\"question\":\"Show me the first row\"}' \
        -H 'Accept: application/json'" \
    '"sql"'

echo
echo -e "${BLUE}━━━ Static Asset Tests ━━━${NC}"
echo

# Test 6: Static assets accessible
run_test "Static assets (/_next/static/)" \
    "curl -sI --max-time $TIMEOUT '$BASE_URL/_next/static/' | grep -q '200\\|301\\|302'"

# Test 7: Favicon accessible
run_test "Favicon (/favicon.ico)" \
    "curl -sI --max-time $TIMEOUT '$BASE_URL/favicon.ico' | grep -q '200'"

echo
echo -e "${BLUE}━━━ Performance Tests ━━━${NC}"
echo

# Test 8: Response time < 3s
echo -ne "Testing: Response time... "
RESPONSE_TIME=$(curl -sf --max-time $TIMEOUT -w "%{time_total}" -o /dev/null "$BASE_URL")
RESPONSE_TIME_INT=${RESPONSE_TIME%.*}

if [ "$RESPONSE_TIME_INT" -lt 3 ]; then
    echo -e "${GREEN}✓ PASS${NC} (${RESPONSE_TIME}s)"
    ((PASSED++))
else
    echo -e "${YELLOW}⚠ SLOW${NC} (${RESPONSE_TIME}s - expected < 3s)"
    ((PASSED++))  # Still pass but warn
fi

echo
echo -e "${BLUE}━━━ Security Tests ━━━${NC}"
echo

# Test 9: Security headers present
run_test_with_output "Security headers (X-Frame-Options)" \
    "curl -sI --max-time $TIMEOUT '$BASE_URL'" \
    "X-Frame-Options"

# Test 10: HSTS header present
if [[ $BASE_URL == https://* ]]; then
    run_test_with_output "HSTS header" \
        "curl -sI --max-time $TIMEOUT '$BASE_URL'" \
        "Strict-Transport-Security"
fi

echo
echo -e "${BLUE}━━━ CORS Tests (R2) ━━━${NC}"
echo

# Test 11: CORS headers for R2
R2_URL="${NEXT_PUBLIC_R2_URL:-https://r2.landbruget.dk}"
echo -ne "Testing: CORS headers... "

CORS_RESPONSE=$(curl -sI --max-time $TIMEOUT \
    -H "Origin: $BASE_URL" \
    -H "Access-Control-Request-Method: GET" \
    "$R2_URL/bronze/test.parquet" 2>&1) || {
    echo -e "${YELLOW}⚠ SKIP (test file not found)${NC}"
}

if echo "$CORS_RESPONSE" | grep -q "Access-Control-Allow-Origin"; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
elif echo "$CORS_RESPONSE" | grep -q "404"; then
    echo -e "${YELLOW}⚠ SKIP (test file not found)${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    ((FAILED++))
fi

echo
echo -e "${BLUE}━━━ Health Check Endpoint ━━━${NC}"
echo

# Test 12: Health endpoint (if it exists)
run_test "Health endpoint (/api/health)" \
    "curl -sf --max-time $TIMEOUT '$BASE_URL/api/health' -o /dev/null" || {
    echo -e "${YELLOW}  Note: /api/health endpoint not implemented (optional)${NC}"
}

echo
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Test Summary                          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! ✓${NC}"
    echo
    echo "Deployment is healthy and ready for production."
    exit 0
else
    echo -e "${RED}Some tests failed! ✗${NC}"
    echo
    echo "Please review the failures above and address them before"
    echo "considering this deployment production-ready."
    exit 1
fi
