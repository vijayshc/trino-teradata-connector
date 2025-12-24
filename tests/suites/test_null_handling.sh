#!/bin/bash
# =================================================================
# test_null_handling.sh - NULL Value Handling Tests
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: NULL Value Handling${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: Single Row All NULLs
# =================================================================

log "${MAGENTA}[Test 1] Single Row - All NULLs${NC}"

RESULT=$(run_sql "
DROP TABLE Test_AllNull1;
CREATE TABLE Test_AllNull1 (col1 INTEGER, col2 VARCHAR(50), col3 DATE);
INSERT INTO Test_AllNull1 VALUES (NULL, NULL, NULL);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_AllNull1)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "All NULL 1 row: 1 row exported" "1" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "All NULL 1 row: 3 NULLs detected (1 row x 3 cols)" "3" "$NULLS"

BYTES=$(get_total_bytes "$RESULT")
assert_equals "All NULL 1 row: 0 bytes (no data)" "0" "$BYTES"

run_sql "DROP TABLE Test_AllNull1;" > /dev/null 2>&1

# =================================================================
# Test 2: Multiple Rows All NULLs
# =================================================================

log ""
log "${MAGENTA}[Test 2] Multiple Rows - All NULLs${NC}"

RESULT=$(run_sql "
DROP TABLE Test_AllNull5;
CREATE TABLE Test_AllNull5 (id INTEGER, col1 INTEGER, col2 VARCHAR(50));
INSERT INTO Test_AllNull5 VALUES (1, NULL, NULL);
INSERT INTO Test_AllNull5 VALUES (2, NULL, NULL);
INSERT INTO Test_AllNull5 VALUES (3, NULL, NULL);
INSERT INTO Test_AllNull5 VALUES (4, NULL, NULL);
INSERT INTO Test_AllNull5 VALUES (5, NULL, NULL);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_AllNull5)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "All NULL 5 rows: 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
# 2 NULL columns per row x 5 rows = 10 NULLs  
assert_equals "All NULL 5 rows: 10 NULLs detected (5 x 2)" "10" "$NULLS"

run_sql "DROP TABLE Test_AllNull5;" > /dev/null 2>&1

# =================================================================
# Test 3: No NULLs (NOT NULL columns)
# =================================================================

log ""
log "${MAGENTA}[Test 3] No NULLs (NOT NULL Columns)${NC}"

RESULT=$(run_sql "
DROP TABLE Test_NoNull;
CREATE TABLE Test_NoNull (
    col1 INTEGER NOT NULL,
    col2 VARCHAR(20) NOT NULL,
    col3 DECIMAL(5,2) NOT NULL
);
INSERT INTO Test_NoNull VALUES (1, 'A', 1.00);
INSERT INTO Test_NoNull VALUES (2, 'B', 2.00);
INSERT INTO Test_NoNull VALUES (3, 'C', 3.00);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_NoNull)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "No NULL: 3 rows exported" "3" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "No NULL: 0 NULLs detected" "0" "$NULLS"

BYTES=$(get_total_bytes "$RESULT")
assert_gte "No NULL: Bytes > 0 (has data)" "10" "$BYTES"

run_sql "DROP TABLE Test_NoNull;" > /dev/null 2>&1

# =================================================================
# Test 4: Mixed NULL Pattern
# =================================================================

log ""
log "${MAGENTA}[Test 4] Mixed NULL Pattern${NC}"

RESULT=$(run_sql "
DROP TABLE Test_MixNull;
CREATE TABLE Test_MixNull (col1 INTEGER, col2 VARCHAR(20), col3 DECIMAL(5,2));
INSERT INTO Test_MixNull VALUES (1, 'A', 1.00);        -- 0 NULLs
INSERT INTO Test_MixNull VALUES (2, NULL, 2.00);       -- 1 NULL
INSERT INTO Test_MixNull VALUES (NULL, NULL, 3.00);    -- 2 NULLs
INSERT INTO Test_MixNull VALUES (NULL, NULL, NULL);    -- 3 NULLs

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_MixNull)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Mixed NULL: 4 rows exported" "4" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
# 0 + 1 + 2 + 3 = 6 NULLs
assert_equals "Mixed NULL: 6 NULLs (0+1+2+3)" "6" "$NULLS"

run_sql "DROP TABLE Test_MixNull;" > /dev/null 2>&1

# =================================================================
# Test 5: First Column NULL
# =================================================================

log ""
log "${MAGENTA}[Test 5] First Column Always NULL${NC}"

RESULT=$(run_sql "
DROP TABLE Test_FirstNull;
CREATE TABLE Test_FirstNull (col1 INTEGER, col2 VARCHAR(20), col3 DECIMAL(5,2));
INSERT INTO Test_FirstNull VALUES (NULL, 'A', 1.00);
INSERT INTO Test_FirstNull VALUES (NULL, 'B', 2.00);
INSERT INTO Test_FirstNull VALUES (NULL, 'C', 3.00);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_FirstNull)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "First NULL: 3 rows exported" "3" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "First NULL: 3 NULLs (1 per row)" "3" "$NULLS"

run_sql "DROP TABLE Test_FirstNull;" > /dev/null 2>&1

# =================================================================
# Test 6: Last Column NULL
# =================================================================

log ""
log "${MAGENTA}[Test 6] Last Column Always NULL${NC}"

RESULT=$(run_sql "
DROP TABLE Test_LastNull;
CREATE TABLE Test_LastNull (col1 INTEGER, col2 VARCHAR(20), col3 DECIMAL(5,2));
INSERT INTO Test_LastNull VALUES (1, 'A', NULL);
INSERT INTO Test_LastNull VALUES (2, 'B', NULL);
INSERT INTO Test_LastNull VALUES (3, 'C', NULL);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_LastNull)) AS t;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Last NULL: 3 rows exported" "3" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "Last NULL: 3 NULLs (1 per row)" "3" "$NULLS"

run_sql "DROP TABLE Test_LastNull;" > /dev/null 2>&1

# =================================================================
# Test 7: NULL Across Different Types
# =================================================================

log ""
log "${MAGENTA}[Test 7] NULL Across All Data Types${NC}"

RESULT=$(run_sql "
DROP TABLE Test_NullTypes;
CREATE TABLE Test_NullTypes (
    col_int INTEGER,
    col_bigint BIGINT,
    col_dec DECIMAL(10,2),
    col_date DATE,
    col_ts TIMESTAMP,
    col_char CHAR(10),
    col_varchar VARCHAR(50)
);
INSERT INTO Test_NullTypes VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_NullTypes)) AS t;
")

NULLS=$(get_total_nulls "$RESULT")
assert_equals "NULL types: 7 NULLs (all types)" "7" "$NULLS"

COLS=$(get_input_columns "$RESULT")
assert_equals "NULL types: 7 columns detected" "7" "$COLS"

run_sql "DROP TABLE Test_NullTypes;" > /dev/null 2>&1

# =================================================================
# Summary
# =================================================================

print_summary "NULL Handling"
exit $?
