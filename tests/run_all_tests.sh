#!/bin/bash
# =================================================================
# run_all_tests.sh - Master Test Runner
# =================================================================
# Runs all test suites and produces a comprehensive report
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
SUITES_DIR="$SCRIPT_DIR/suites"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Results tracking
TOTAL_SUITES=0
PASSED_SUITES=0
FAILED_SUITES=0
RESULTS=()

log() {
    echo -e "$1"
}

# =================================================================
# Header
# =================================================================

log ""
log "${BLUE}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
log "${BLUE}║           Teradata Table Operator - Complete Test Suite                  ║${NC}"
log "${BLUE}║                    Data Accuracy Validation                              ║${NC}"
log "${BLUE}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
log ""
log "Started: $(date)"
log "Test Suites Directory: $SUITES_DIR"
log ""

# =================================================================
# Run Individual Suites
# =================================================================

run_suite() {
    local suite_name="$1"
    local suite_file="$SUITES_DIR/$2"
    
    log "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "${CYAN}  Running: $suite_name${NC}"
    log "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log ""
    
    ((TOTAL_SUITES++))
    
    if [ -f "$suite_file" ]; then
        chmod +x "$suite_file"
        bash "$suite_file"
        
        if [ $? -eq 0 ]; then
            ((PASSED_SUITES++))
            RESULTS+=("${GREEN}✓${NC} $suite_name")
        else
            ((FAILED_SUITES++))
            RESULTS+=("${RED}✗${NC} $suite_name")
        fi
    else
        log "${RED}Suite file not found: $suite_file${NC}"
        ((FAILED_SUITES++))
        RESULTS+=("${RED}✗${NC} $suite_name (file not found)")
    fi
    
    log ""
}

# Run each test suite
run_suite "Integer Types" "test_integer_types.sh"
run_suite "Decimal/Number Types" "test_decimal_types.sh"
run_suite "Date/Time Types" "test_datetime_types.sh"
run_suite "Character Types" "test_character_types.sh"
run_suite "NULL Handling" "test_null_handling.sh"
run_suite "Edge Cases" "test_edge_cases.sh"

# =================================================================
# Final Summary
# =================================================================

log ""
log "${BLUE}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
log "${BLUE}║                           FINAL REPORT                                   ║${NC}"
log "${BLUE}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
log ""
log "Completed: $(date)"
log ""
log "${YELLOW}Test Suites:${NC}"
for result in "${RESULTS[@]}"; do
    log "  $result"
done
log ""
log "${YELLOW}Summary:${NC}"
log "  Total Suites:  $TOTAL_SUITES"
log "  ${GREEN}Passed:        $PASSED_SUITES${NC}"
log "  ${RED}Failed:        $FAILED_SUITES${NC}"
log ""

if [ $FAILED_SUITES -eq 0 ]; then
    log "${GREEN}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
    log "${GREEN}║                    ✓ ALL TEST SUITES PASSED!                             ║${NC}"
    log "${GREEN}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    log "${RED}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
    log "${RED}║               ✗ SOME TEST SUITES FAILED                                  ║${NC}"
    log "${RED}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
    exit 1
fi
