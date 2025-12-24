#!/bin/bash
# =================================================================
# test_edge_cases.sh - Edge Case and Special Scenario Tests
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: Edge Cases and Special Scenarios${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: Empty Table
# =================================================================

log "${MAGENTA}[Test 1] Empty Table${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Empty;
CREATE TABLE Test_Empty (id INTEGER, name VARCHAR(50));
-- No inserts

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Empty)) AS t;
")

assert_contains "Empty table: Query completed" "Query completed" "$RESULT"

ROWS=$(get_total_rows "$RESULT")
if [ "$ROWS" -eq 0 ] 2>/dev/null || [ -z "$ROWS" ]; then
    test_pass "Empty table: 0 rows processed"
else
    test_fail "Empty table" "0 rows" "$ROWS rows"
fi

run_sql "DROP TABLE Test_Empty;" > /dev/null 2>&1

# =================================================================
# Test 2: Single Row
# =================================================================

log ""
log "${MAGENTA}[Test 2] Single Row${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Single;
CREATE TABLE Test_Single (id INTEGER, name VARCHAR(50));
INSERT INTO Test_Single VALUES (1, 'Only Row');

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Single)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Single row: 1 row exported" "1" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Single row: 2 columns detected" "2" "$COLS"

run_sql "DROP TABLE Test_Single;" > /dev/null 2>&1

# =================================================================
# Test 3: Single Column
# =================================================================

log ""
log "${MAGENTA}[Test 3] Single Column${NC}"

RESULT=$(run_sql "
DROP TABLE Test_SingleCol;
CREATE TABLE Test_SingleCol (only_col INTEGER);
INSERT INTO Test_SingleCol VALUES (1);
INSERT INTO Test_SingleCol VALUES (2);
INSERT INTO Test_SingleCol VALUES (3);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_SingleCol)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Single column: 3 rows exported" "3" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Single column: 1 column detected" "1" "$COLS"

run_sql "DROP TABLE Test_SingleCol;" > /dev/null 2>&1

# =================================================================
# Test 4: Wide Table (30 columns)
# =================================================================

log ""
log "${MAGENTA}[Test 4] Wide Table (30 columns)${NC}"

# Build column list
COLS_DDL=""
VALS=""
for i in $(seq 1 30); do
    COLS_DDL="${COLS_DDL}col_${i} INTEGER"
    VALS="${VALS}${i}"
    if [ $i -lt 30 ]; then
        COLS_DDL="${COLS_DDL}, "
        VALS="${VALS}, "
    fi
done

RESULT=$(run_sql "
DROP TABLE Test_Wide;
CREATE TABLE Test_Wide ($COLS_DDL);
INSERT INTO Test_Wide VALUES ($VALS);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Wide)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Wide table: 1 row exported" "1" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Wide table: 30 columns detected" "30" "$COLS"

BYTES=$(get_total_bytes "$RESULT")
# 30 integers = 30 * 4 = 120 bytes
assert_gte "Wide table: Bytes >= 100" "100" "$BYTES"

run_sql "DROP TABLE Test_Wide;" > /dev/null 2>&1

# =================================================================
# Test 5: Large Dataset (1000+ rows)
# =================================================================

log ""
log "${MAGENTA}[Test 5] Large Dataset (1000+ rows)${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Large;
CREATE TABLE Test_Large (id INTEGER, data VARCHAR(50), amount DECIMAL(10,2));

INSERT INTO Test_Large 
SELECT ROW_NUMBER() OVER (ORDER BY 1) as id,
       'Row' || TRIM(CAST(ROW_NUMBER() OVER (ORDER BY 1) AS VARCHAR(10))) as data,
       CAST(ROW_NUMBER() OVER (ORDER BY 1) * 1.5 AS DECIMAL(10,2)) as amount
FROM sys_calendar.calendar
WHERE calendar_date BETWEEN DATE '2020-01-01' AND DATE '2022-09-27';

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Large)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_gte "Large dataset: >= 1000 rows" "1000" "$ROWS"

BYTES=$(get_total_bytes "$RESULT")
assert_gte "Large dataset: Bytes > 50000" "50000" "$BYTES"

assert_contains "Large dataset: SUCCESS" "SUCCESS" "$RESULT"

log "    Rows: $ROWS, Bytes: $BYTES"

run_sql "DROP TABLE Test_Large;" > /dev/null 2>&1

# =================================================================
# Test 6: Parallel Execution
# =================================================================

log ""
log "${MAGENTA}[Test 6] Parallel Execution (AMP distribution)${NC}"

# Count distinct AMP outputs from previous result
AMP_COUNT=$(echo "$RESULT" | grep "SUCCESS" | wc -l)
assert_gte "Parallel: >= 1 AMP participated" "1" "$AMP_COUNT"

log "    AMPs: $AMP_COUNT"

# =================================================================
# Test 7: Configuration Display
# =================================================================

log ""
log "${MAGENTA}[Test 7] Configuration in Status${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Config;
CREATE TABLE Test_Config (val INTEGER);
INSERT INTO Test_Config VALUES (1);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Config)) AS t;
")

assert_contains "Config: IP displayed" "127.0.0.1" "$RESULT"
assert_contains "Config: Port displayed" "50051" "$RESULT"

run_sql "DROP TABLE Test_Config;" > /dev/null 2>&1

# =================================================================
# Test 8: Varying String Lengths
# =================================================================

log ""
log "${MAGENTA}[Test 8] Varying String Lengths${NC}"

RESULT=$(run_sql "
DROP TABLE Test_VaryLen;
CREATE TABLE Test_VaryLen (id INTEGER, val VARCHAR(500));
INSERT INTO Test_VaryLen VALUES (1, 'A');
INSERT INTO Test_VaryLen VALUES (2, 'ABC');
INSERT INTO Test_VaryLen VALUES (3, REPEAT('X', 100));
INSERT INTO Test_VaryLen VALUES (4, REPEAT('Y', 400));

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_VaryLen)) AS t;

DROP TABLE Test_VaryLen;
")

ROWS=$(get_total_rows "$RESULT")
assert_gte "Varying lengths: >= 2 rows exported" "2" "$ROWS"

BYTES=$(get_total_bytes "$RESULT")
# Variable depending on which AMPs got which rows
assert_gte "Varying lengths: Bytes >= 100" "100" "$BYTES"

assert_contains "Varying lengths: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Summary
# =================================================================

print_summary "Edge Cases"
exit $?
