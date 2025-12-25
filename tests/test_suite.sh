#!/bin/bash
# =================================================================
# test_suite.sh - Comprehensive Test Suite for Teradata Table Operator
# =================================================================
#
# Tests the ExportToTrino Table Operator with:
# 1. All major data types (INT, BIGINT, DECIMAL, DATE, TIMESTAMP, VARCHAR, etc.)
# 2. NULL value handling
# 3. Large datasets
# 4. Edge cases
# 5. Parallel execution
#
# Output Schema (7 columns):
# Column_1: amp_id
# Column_2: rows_processed
# Column_3: bytes_sent
# Column_4: null_count
# Column_5: batches_sent
# Column_6: input_columns
# Column_7: status
#

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")

# Configuration
TD_HOST="${TD_HOST:-192.168.137.129}"
TD_USER="${TD_USER:-dbc}"
TD_PASS="${TD_PASS:-dbc}"

export TD_HOME="${TD_HOME:-/opt/teradata/client/20.00}"
export PATH=$PATH:$TD_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TD_HOME/lib64:$TD_HOME/lib

# Test results
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Log file
LOG_FILE="$PROJECT_DIR/test_results_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# Helper Functions
# ============================================================

log() {
    echo -e "$1" | tee -a "$LOG_FILE"
}

run_sql() {
    local sql="$1"
    local result
    result=$(cat << EOF | bteq 2>&1
.LOGON $TD_HOST/$TD_USER,$TD_PASS
.SET WIDTH 300
DATABASE TrinoExport;
$sql
.QUIT
EOF
)
    echo "$result"
}

test_pass() {
    log "  ${GREEN}✓ PASS${NC}: $1"
    ((TESTS_PASSED++))
}

test_fail() {
    log "  ${RED}✗ FAIL${NC}: $1"
    log "    Reason: $2"
    ((TESTS_FAILED++))
}

test_skip() {
    log "  ${YELLOW}○ SKIP${NC}: $1"
    log "    Reason: $2"
    ((TESTS_SKIPPED++))
}

# ============================================================
# Test Suite Header
# ============================================================

log ""
log "${BLUE}╔════════════════════════════════════════════════════════════════════════╗${NC}"
log "${BLUE}║         Teradata Table Operator - Comprehensive Test Suite             ║${NC}"
log "${BLUE}║                     Testing All Major Data Types                       ║${NC}"
log "${BLUE}╚════════════════════════════════════════════════════════════════════════╝${NC}"
log ""
log "Test Started: $(date)"
log "Teradata Host: $TD_HOST"
log "Log File: $LOG_FILE"
log ""

# ============================================================
# Test 0: Prerequisites Check
# ============================================================

log "${CYAN}[Test 0] Prerequisites Check${NC}"

# Check BTEQ
if ! command -v bteq &> /dev/null; then
    log "  ${RED}ERROR: bteq not found in PATH${NC}"
    exit 1
fi
log "  bteq: OK"

# Check Teradata connection
CONN_TEST=$(run_sql "SELECT 1 as test;")
if echo "$CONN_TEST" | grep -q "Query completed"; then
    log "  Database connection: OK"
else
    log "  ${RED}ERROR: Cannot connect to Teradata${NC}"
    exit 1
fi

# Check Table Operator registration
REG_TEST=$(run_sql "SELECT COUNT(*) FROM DBC.FunctionsV WHERE FunctionName = 'EXPORTTOTRINO';")
if echo "$REG_TEST" | grep -q "1"; then
    log "  ExportToTrino registration: OK"
else
    log "  ${RED}ERROR: ExportToTrino not registered${NC}"
    log "  Run: bteq < scripts/register.bteq"
    exit 1
fi
log ""

# ============================================================
# Test 1: INTEGER Types
# ============================================================

log "${CYAN}[Test 1] INTEGER Data Types${NC}"
log "  Testing: INTEGER, BIGINT, SMALLINT, BYTEINT"

SETUP_SQL="
DROP TABLE Test_Integers;
CREATE TABLE Test_Integers (
    col_integer INTEGER,
    col_bigint BIGINT,
    col_smallint SMALLINT,
    col_byteint BYTEINT
);

INSERT INTO Test_Integers VALUES (2147483647, 9223372036854775807, 32767, 127);
INSERT INTO Test_Integers VALUES (-2147483648, -9223372036854775808, -32768, -128);
INSERT INTO Test_Integers VALUES (0, 0, 0, 0);
INSERT INTO Test_Integers VALUES (NULL, NULL, NULL, NULL);
INSERT INTO Test_Integers VALUES (12345, 123456789012345, 1234, 12);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Integers)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    NULLS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$4} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    
    if [ "$ROWS" -eq 5 ]; then
        test_pass "INTEGER types - 5 rows processed"
    else
        test_fail "INTEGER types" "Expected 5 rows, got $ROWS"
    fi
    
    if [ "$NULLS" -eq 4 ]; then
        test_pass "INTEGER types - 4 NULL values detected"
    else
        test_fail "INTEGER types NULL handling" "Expected 4 NULLs, got $NULLS"
    fi
    
    if [ "$COLS" -eq 4 ]; then
        test_pass "INTEGER types - 4 columns detected"
    else
        test_fail "INTEGER types column detection" "Expected 4 columns, got $COLS"
    fi
else
    test_fail "INTEGER types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 2: DECIMAL/NUMBER Types
# ============================================================

log "${CYAN}[Test 2] DECIMAL/NUMBER Data Types${NC}"
log "  Testing: DECIMAL(5,2), DECIMAL(18,4), DECIMAL(38,10), NUMBER"

SETUP_SQL="
DROP TABLE Test_Decimals;
CREATE TABLE Test_Decimals (
    col_dec_small DECIMAL(5,2),
    col_dec_medium DECIMAL(18,4),
    col_dec_large DECIMAL(38,10),
    col_number NUMBER(15,3)
);

INSERT INTO Test_Decimals VALUES (123.45, 12345678901234.5678, 1234567890123456789012345678.1234567890, 123456789012.345);
INSERT INTO Test_Decimals VALUES (-999.99, -99999999999999.9999, -9999999999999999999999999999.9999999999, -999999999999.999);
INSERT INTO Test_Decimals VALUES (0.00, 0.0000, 0.0000000000, 0.000);
INSERT INTO Test_Decimals VALUES (NULL, NULL, NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Decimals)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    
    if [ "$ROWS" -eq 4 ]; then
        test_pass "DECIMAL types - 4 rows processed"
    else
        test_fail "DECIMAL types" "Expected 4 rows, got $ROWS"
    fi
    
    if [ "$COLS" -eq 4 ]; then
        test_pass "DECIMAL types - 4 columns (varying precisions)"
    else
        test_fail "DECIMAL types column detection" "Expected 4 columns, got $COLS"
    fi
else
    test_fail "DECIMAL types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 3: FLOAT/REAL Types
# ============================================================

log "${CYAN}[Test 3] FLOAT/REAL Data Types${NC}"
log "  Testing: FLOAT, REAL, DOUBLE PRECISION"

SETUP_SQL="
DROP TABLE Test_Floats;
CREATE TABLE Test_Floats (
    col_float FLOAT,
    col_real REAL,
    col_double DOUBLE PRECISION
);

INSERT INTO Test_Floats VALUES (3.14159265358979, 2.71828, 1.41421356);
INSERT INTO Test_Floats VALUES (-123.456, 789.012, 0.0);
INSERT INTO Test_Floats VALUES (NULL, NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Floats)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    
    if [ "$ROWS" -ge 3 ]; then
        test_pass "FLOAT types - $ROWS rows processed"
    else
        test_fail "FLOAT types" "Expected >= 3 rows, got $ROWS"
    fi
    
    if [ "$COLS" -eq 3 ]; then
        test_pass "FLOAT types - 3 columns detected"
    else
        test_fail "FLOAT types column detection" "Expected 3 columns, got $COLS"
    fi
else
    test_fail "FLOAT types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 4: CHARACTER Types
# ============================================================

log "${CYAN}[Test 4] CHARACTER Data Types${NC}"
log "  Testing: CHAR, VARCHAR, LONG VARCHAR"

SETUP_SQL="
DROP TABLE Test_Chars;
CREATE TABLE Test_Chars (
    col_char CHAR(10),
    col_varchar VARCHAR(100),
    col_long_varchar VARCHAR(32000)
);

INSERT INTO Test_Chars VALUES ('Hello     ', 'Hello World', 'This is a longer text string for testing.');
INSERT INTO Test_Chars VALUES ('Test      ', 'Testing 123', 'Another row of test data.');
INSERT INTO Test_Chars VALUES ('ABCDEFGHIJ', 'Special chars', 'More test data here.');
INSERT INTO Test_Chars VALUES (NULL, NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Chars)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    
    if [ "$ROWS" -ge 4 ]; then
        test_pass "CHARACTER types - $ROWS rows processed"
    else
        test_fail "CHARACTER types" "Expected >= 4 rows, got $ROWS"
    fi
    
    if [ "$COLS" -eq 3 ]; then
        test_pass "CHARACTER types - CHAR/VARCHAR/LONG VARCHAR columns"
    else
        test_fail "CHARACTER types column detection" "Expected 3 columns, got $COLS"
    fi
else
    test_fail "CHARACTER types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 5: DATE Type
# ============================================================

log "${CYAN}[Test 5] DATE Data Type${NC}"
log "  Testing: DATE with various values"

SETUP_SQL="
DROP TABLE Test_Dates;
CREATE TABLE Test_Dates (
    col_date DATE
);

INSERT INTO Test_Dates VALUES (DATE '2024-12-23');
INSERT INTO Test_Dates VALUES (DATE '1970-01-01');
INSERT INTO Test_Dates VALUES (DATE '2099-12-31');
INSERT INTO Test_Dates VALUES (DATE '1900-01-01');
INSERT INTO Test_Dates VALUES (NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Dates)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    NULLS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$4} END {print sum}')
    
    if [ "$ROWS" -eq 5 ]; then
        test_pass "DATE type - 5 rows processed"
    else
        test_fail "DATE type" "Expected 5 rows, got $ROWS"
    fi
    
    if [ "$NULLS" -eq 1 ]; then
        test_pass "DATE type - 1 NULL value detected"
    else
        test_fail "DATE type NULL handling" "Expected 1 NULL, got $NULLS"
    fi
else
    test_fail "DATE type" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 6: TIME Type
# ============================================================

log "${CYAN}[Test 6] TIME Data Type${NC}"
log "  Testing: TIME with fractional seconds"

SETUP_SQL="
DROP TABLE Test_Times;
CREATE TABLE Test_Times (
    col_time TIME(6)
);

INSERT INTO Test_Times VALUES (TIME '14:30:45.123456');
INSERT INTO Test_Times VALUES (TIME '00:00:00.000000');
INSERT INTO Test_Times VALUES (TIME '23:59:59.999999');
INSERT INTO Test_Times VALUES (NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Times)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    
    if [ "$ROWS" -eq 4 ]; then
        test_pass "TIME type - 4 rows processed"
    else
        test_fail "TIME type" "Expected 4 rows, got $ROWS"
    fi
else
    test_fail "TIME type" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 7: TIMESTAMP Type
# ============================================================

log "${CYAN}[Test 7] TIMESTAMP Data Type${NC}"
log "  Testing: TIMESTAMP with fractional seconds"

SETUP_SQL="
DROP TABLE Test_Timestamps;
CREATE TABLE Test_Timestamps (
    col_timestamp TIMESTAMP(6)
);

INSERT INTO Test_Timestamps VALUES (TIMESTAMP '2024-12-23 14:30:45.123456');
INSERT INTO Test_Timestamps VALUES (TIMESTAMP '1970-01-01 00:00:00.000000');
INSERT INTO Test_Timestamps VALUES (TIMESTAMP '2099-12-31 23:59:59.999999');
INSERT INTO Test_Timestamps VALUES (NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Timestamps)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    
    if [ "$ROWS" -eq 4 ]; then
        test_pass "TIMESTAMP type - 4 rows processed"
    else
        test_fail "TIMESTAMP type" "Expected 4 rows, got $ROWS"
    fi
else
    test_fail "TIMESTAMP type" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 8: BINARY Types
# ============================================================

log "${CYAN}[Test 8] BINARY Data Types${NC}"
log "  Testing: BYTE, VARBYTE"

SETUP_SQL="
DROP TABLE Test_Binary;
CREATE TABLE Test_Binary (
    col_byte BYTE(10),
    col_varbyte VARBYTE(100)
);

INSERT INTO Test_Binary VALUES ('AABBCCDDEE'XB, '0123456789ABCDEF'XB);
INSERT INTO Test_Binary VALUES ('0000000000'XB, ''XB);
INSERT INTO Test_Binary VALUES (NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Binary)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    
    if [ "$ROWS" -eq 3 ]; then
        test_pass "BINARY types - 3 rows processed"
    else
        test_fail "BINARY types" "Expected 3 rows, got $ROWS"
    fi
    
    if [ "$COLS" -eq 2 ]; then
        test_pass "BINARY types - BYTE/VARBYTE columns"
    else
        test_fail "BINARY types column detection" "Expected 2 columns, got $COLS"
    fi
else
    test_fail "BINARY types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 9: INTERVAL Types
# ============================================================

log "${CYAN}[Test 9] INTERVAL Data Types${NC}"
log "  Testing: INTERVAL YEAR, INTERVAL DAY TO HOUR"

SETUP_SQL="
DROP TABLE Test_Intervals;
CREATE TABLE Test_Intervals (
    col_interval_year INTERVAL YEAR,
    col_interval_day INTERVAL DAY TO HOUR
);

INSERT INTO Test_Intervals VALUES (INTERVAL '5' YEAR, INTERVAL '10 12' DAY TO HOUR);
INSERT INTO Test_Intervals VALUES (INTERVAL '1' YEAR, INTERVAL '1 01' DAY TO HOUR);
INSERT INTO Test_Intervals VALUES (NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Intervals)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    
    if [ "$ROWS" -ge 3 ]; then
        test_pass "INTERVAL types - $ROWS rows processed"
    else
        test_fail "INTERVAL types" "Expected >= 3 rows, got $ROWS"
    fi
else
    test_fail "INTERVAL types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 10: Mixed Types (Real-world Table)
# ============================================================

log "${CYAN}[Test 10] Mixed Data Types (Real-world Scenario)${NC}"
log "  Testing: Customer table with 10 columns of mixed types"

SETUP_SQL="
DROP TABLE Test_RealWorld;
CREATE TABLE Test_RealWorld (
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(255),
    balance DECIMAL(15,2),
    credit_limit DECIMAL(15,2),
    created_date DATE,
    last_login TIMESTAMP(6),
    is_active BYTEINT,
    loyalty_points BIGINT,
    account_type CHAR(1)
);

INSERT INTO Test_RealWorld VALUES (1, 'John Doe', 'john.doe@example.com', 1234.56, 5000.00, DATE '2020-01-15', TIMESTAMP '2024-12-22 10:30:00', 1, 15000, 'P');
INSERT INTO Test_RealWorld VALUES (2, 'Jane Smith', 'jane.smith@example.com', -500.00, 10000.00, DATE '2019-06-20', TIMESTAMP '2024-12-21 14:45:30', 1, 25000, 'G');
INSERT INTO Test_RealWorld VALUES (3, 'Bob Wilson', NULL, 0.00, 2500.00, DATE '2021-03-10', NULL, 0, 0, 'S');
INSERT INTO Test_RealWorld VALUES (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO Test_RealWorld VALUES (5, 'Alice Brown', 'alice@test.org', 9999.99, 15000.00, DATE '2018-11-05', TIMESTAMP '2024-12-23 08:00:00', 1, 50000, 'P');
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_RealWorld)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    NULLS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$4} END {print sum}')
    
    if [ "$ROWS" -eq 5 ]; then
        test_pass "Mixed types - 5 rows processed"
    else
        test_fail "Mixed types" "Expected 5 rows, got $ROWS"
    fi
    
    if [ "$COLS" -eq 10 ]; then
        test_pass "Mixed types - 10 columns detected"
    else
        test_fail "Mixed types column detection" "Expected 10 columns, got $COLS"
    fi
    
    # Row 3 has 2 NULLs, Row 4 has 9 NULLs (customer_id NOT NULL)
    if [ "$NULLS" -eq 11 ]; then
        test_pass "Mixed types - 11 NULL values detected"
    else
        test_fail "Mixed types NULL handling" "Expected 11 NULLs, got $NULLS"
    fi
else
    test_fail "Mixed types" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 11: Large Dataset
# ============================================================

log "${CYAN}[Test 11] Large Dataset (1000+ rows)${NC}"
log "  Testing: Performance with large row count"

SETUP_SQL="
DROP TABLE Test_Large;
CREATE TABLE Test_Large (
    id INTEGER,
    data VARCHAR(100),
    amount DECIMAL(10,2)
);

INSERT INTO Test_Large 
SELECT ROW_NUMBER() OVER (ORDER BY 1) as id,
       'Row ' || TRIM(CAST(ROW_NUMBER() OVER (ORDER BY 1) AS VARCHAR(10))) as data,
       CAST(ROW_NUMBER() OVER (ORDER BY 1) * 1.5 AS DECIMAL(10,2)) as amount
FROM sys_calendar.calendar
WHERE calendar_date BETWEEN DATE '2020-01-01' AND DATE '2022-09-27';
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Large)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    BYTES=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$3} END {print sum}')
    BATCHES=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$5} END {print sum}')
    
    if [ "$ROWS" -ge 1000 ]; then
        test_pass "Large dataset - $ROWS rows processed"
    else
        test_fail "Large dataset" "Expected >= 1000 rows, got $ROWS"
    fi
    
    log "    Bytes sent: $BYTES"
    log "    Batches: $BATCHES"
else
    test_fail "Large dataset" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 12: Empty Table
# ============================================================

log "${CYAN}[Test 12] Empty Table${NC}"
log "  Testing: Table with no rows"

SETUP_SQL="
DROP TABLE Test_Empty;
CREATE TABLE Test_Empty (
    id INTEGER,
    name VARCHAR(50)
);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Empty)) AS t;")

if echo "$RESULT" | grep -q "Query completed"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    if [ -z "$ROWS" ] || [ "$ROWS" -eq 0 ]; then
        test_pass "Empty table - 0 rows correctly reported"
    else
        test_fail "Empty table" "Expected 0 rows, got $ROWS"
    fi
else
    test_fail "Empty table" "Table Operator failed"
fi

log ""

# ============================================================
# Test 13: All NULL Values
# ============================================================

log "${CYAN}[Test 13] All NULL Values${NC}"
log "  Testing: Table where every column is NULL"

SETUP_SQL="
DROP TABLE Test_AllNulls;
CREATE TABLE Test_AllNulls (
    col1 INTEGER,
    col2 VARCHAR(50),
    col3 DECIMAL(10,2),
    col4 DATE,
    col5 TIMESTAMP
);

INSERT INTO Test_AllNulls VALUES (NULL, NULL, NULL, NULL, NULL);
INSERT INTO Test_AllNulls VALUES (NULL, NULL, NULL, NULL, NULL);
INSERT INTO Test_AllNulls VALUES (NULL, NULL, NULL, NULL, NULL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_AllNulls)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    ROWS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    NULLS=$(echo "$RESULT" | grep "SUCCESS" | awk '{sum+=$4} END {print sum}')
    
    # Each row has 5 NULL columns, total rows processed times 5
    EXPECTED_NULLS=$((ROWS * 5))
    
    if [ "$NULLS" -eq "$EXPECTED_NULLS" ]; then
        test_pass "All NULLs - $NULLS NULL values detected ($ROWS rows x 5 columns)"
    else
        test_fail "All NULLs" "Expected $EXPECTED_NULLS NULLs ($ROWS rows x 5), got $NULLS"
    fi
else
    test_fail "All NULLs" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 14: Wide Table (50 columns)
# ============================================================

log "${CYAN}[Test 14] Wide Table (50 columns)${NC}"
log "  Testing: Dynamic column handling"

# Generate CREATE TABLE with 50 columns
COLS_DDL=""
for i in $(seq 1 50); do
    COLS_DDL="${COLS_DDL}col_${i} INTEGER"
    if [ $i -lt 50 ]; then
        COLS_DDL="${COLS_DDL}, "
    fi
done

SETUP_SQL="
DROP TABLE Test_Wide;
CREATE TABLE Test_Wide ($COLS_DDL);
"

run_sql "$SETUP_SQL" > /dev/null 2>&1

# Generate INSERT with 50 values
VALS=""
for i in $(seq 1 50); do
    VALS="${VALS}${i}"
    if [ $i -lt 50 ]; then
        VALS="${VALS}, "
    fi
done

run_sql "INSERT INTO Test_Wide VALUES ($VALS);" > /dev/null 2>&1
run_sql "INSERT INTO Test_Wide VALUES ($VALS);" > /dev/null 2>&1

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Wide)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    COLS=$(echo "$RESULT" | grep "SUCCESS" | head -1 | awk '{print $6}')
    if [ "$COLS" -eq 50 ]; then
        test_pass "Wide table - 50 columns processed"
    else
        test_fail "Wide table" "Expected 50 columns, got $COLS"
    fi
else
    test_fail "Wide table" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 15: Parallel Execution (AMP Distribution)
# ============================================================

log "${CYAN}[Test 15] Parallel Execution (AMP Distribution)${NC}"
log "  Testing: Data distribution across AMPs"

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Large)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    AMP_COUNT=$(echo "$RESULT" | grep "SUCCESS" | wc -l)
    if [ "$AMP_COUNT" -ge 1 ]; then
        test_pass "Parallel execution - $AMP_COUNT AMPs participated"
        
        # Show AMP distribution
        log "    AMP Distribution:"
        echo "$RESULT" | grep "SUCCESS" | while read line; do
            AMP=$(echo "$line" | awk '{print $1}')
            ROWS=$(echo "$line" | awk '{print $2}')
            log "      AMP $AMP: $ROWS rows"
        done
    else
        test_fail "Parallel execution" "No AMPs reported"
    fi
else
    test_fail "Parallel execution" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Test 16: Configuration Display
# ============================================================

log "${CYAN}[Test 16] Configuration Display${NC}"
log "  Testing: Status shows configuration"

RESULT=$(run_sql "SELECT * FROM ExportToTrino(ON (SELECT 1)) AS t;")

if echo "$RESULT" | grep -q "SUCCESS"; then
    if echo "$RESULT" | grep -q "\[127.0.0.1:50051\]"; then
        test_pass "Configuration displayed in status"
    else
        test_fail "Configuration display" "Expected [IP:PORT] in status"
    fi
else
    test_fail "Configuration display" "Table Operator did not return SUCCESS"
fi

log ""

# ============================================================
# Cleanup
# ============================================================

log "${CYAN}[Cleanup] Removing test tables${NC}"

CLEANUP_SQL="
DROP TABLE Test_Integers;
DROP TABLE Test_Decimals;
DROP TABLE Test_Floats;
DROP TABLE Test_Chars;
DROP TABLE Test_Dates;
DROP TABLE Test_Times;
DROP TABLE Test_Timestamps;
DROP TABLE Test_Binary;
DROP TABLE Test_Intervals;
DROP TABLE Test_RealWorld;
DROP TABLE Test_Large;
DROP TABLE Test_Empty;
DROP TABLE Test_AllNulls;
DROP TABLE Test_Wide;
"

run_sql "$CLEANUP_SQL" > /dev/null 2>&1
log "  Test tables cleaned up"
log ""

# ============================================================
# Summary
# ============================================================

log "${BLUE}════════════════════════════════════════════════════════════════════════${NC}"
log "${BLUE}                           TEST SUMMARY                                  ${NC}"
log "${BLUE}════════════════════════════════════════════════════════════════════════${NC}"
log ""
log "  Data Types Tested:"
log "    ${MAGENTA}• INTEGER, BIGINT, SMALLINT, BYTEINT${NC}"
log "    ${MAGENTA}• DECIMAL (5,2), (18,4), (38,10), NUMBER${NC}"
log "    ${MAGENTA}• FLOAT, REAL, DOUBLE PRECISION${NC}"
log "    ${MAGENTA}• CHAR, VARCHAR, LONG VARCHAR${NC}"
log "    ${MAGENTA}• DATE, TIME, TIMESTAMP${NC}"
log "    ${MAGENTA}• BYTE, VARBYTE${NC}"
log "    ${MAGENTA}• INTERVAL YEAR, INTERVAL DAY${NC}"
log ""
log "  Results:"
log "    ${GREEN}Passed:  $TESTS_PASSED${NC}"
log "    ${RED}Failed:  $TESTS_FAILED${NC}"
log "    ${YELLOW}Skipped: $TESTS_SKIPPED${NC}"
log ""
log "  Total Tests: $((TESTS_PASSED + TESTS_FAILED + TESTS_SKIPPED))"
log ""

if [ $TESTS_FAILED -eq 0 ]; then
    log "${GREEN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    log "${GREEN}║                    ✓ ALL TESTS PASSED!                                 ║${NC}"
    log "${GREEN}╚════════════════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    log "${RED}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    log "${RED}║              ✗ SOME TESTS FAILED - Check log for details               ║${NC}"
    log "${RED}╚════════════════════════════════════════════════════════════════════════╝${NC}"
    exit 1
fi
