#!/bin/bash
# =================================================================
# test_common.sh - Common functions for test suites
# =================================================================

# Colors
export RED='\033[0;31m'
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export CYAN='\033[0;36m'
export MAGENTA='\033[0;35m'
export NC='\033[0m'

# Configuration
export TD_HOST="${TD_HOST:-192.168.137.128}"
export TD_USER="${TD_USER:-dbc}"
export TD_PASS="${TD_PASS:-dbc}"
export TD_HOME="${TD_HOME:-/opt/teradata/client/20.00}"
export PATH=$PATH:$TD_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TD_HOME/lib64:$TD_HOME/lib

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Logging
log() {
    echo -e "$1"
}

# Run SQL and return raw output (DO NOT set separator)
run_sql() {
    local sql="$1"
    cat << EOF | bteq 2>&1
.LOGON $TD_HOST/$TD_USER,$TD_PASS
.SET WIDTH 300
DATABASE TrinoExport;
$sql
.QUIT
EOF
}

# Test pass
test_pass() {
    log "  ${GREEN}✓ PASS${NC}: $1"
    ((TESTS_PASSED++))
}

# Test fail
test_fail() {
    log "  ${RED}✗ FAIL${NC}: $1"
    if [ -n "$2" ]; then
        log "    Expected: $2"
    fi
    if [ -n "$3" ]; then
        log "    Actual: $3"
    fi
    ((TESTS_FAILED++))
}

# Assert equals
assert_equals() {
    local description="$1"
    local expected="$2"
    local actual="$3"
    
    if [ "$expected" == "$actual" ]; then
        test_pass "$description"
        return 0
    else
        test_fail "$description" "$expected" "$actual"
        return 1
    fi
}

# Assert greater than or equal
assert_gte() {
    local description="$1"
    local expected="$2"
    local actual="$3"
    
    # Handle empty actual
    if [ -z "$actual" ]; then
        actual=0
    fi
    
    if [ "$actual" -ge "$expected" ] 2>/dev/null; then
        test_pass "$description"
        return 0
    else
        test_fail "$description" ">= $expected" "$actual"
        return 1
    fi
}

# Assert contains
assert_contains() {
    local description="$1"
    local expected="$2"
    local actual="$3"
    
    # Use -- to stop option processing (handles patterns starting with -)
    if echo "$actual" | grep -F -- "$expected" > /dev/null 2>&1; then
        test_pass "$description"
        return 0
    else
        test_fail "$description" "contains '$expected'" "(not found)"
        return 1
    fi
}

# Extract total rows from ExportToTrino output
get_total_rows() {
    local result="$1"
    echo "$result" | grep "SUCCESS" | awk '{sum+=$2} END {print sum+0}'
}

# Extract total bytes from ExportToTrino output
get_total_bytes() {
    local result="$1"
    echo "$result" | grep "SUCCESS" | awk '{sum+=$3} END {print sum+0}'
}

# Extract total nulls from ExportToTrino output
get_total_nulls() {
    local result="$1"
    echo "$result" | grep "SUCCESS" | awk '{sum+=$4} END {print sum+0}'
}

# Extract input columns from ExportToTrino output (column 6, avoiding trailing text)
get_input_columns() {
    local result="$1"
    echo "$result" | grep "SUCCESS" | head -1 | awk '{print $6}' | grep -oE '^[0-9]+'
}

# Print test summary
print_summary() {
    local suite_name="$1"
    log ""
    log "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "${BLUE}  $suite_name Summary${NC}"
    log "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "  ${GREEN}Passed: $TESTS_PASSED${NC}"
    log "  ${RED}Failed: $TESTS_FAILED${NC}"
    
    if [ $TESTS_FAILED -eq 0 ]; then
        log ""
        log "  ${GREEN}✓ All tests passed!${NC}"
        return 0
    else
        log ""
        log "  ${RED}✗ Some tests failed${NC}"
        return 1
    fi
}
