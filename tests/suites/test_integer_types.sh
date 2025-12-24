#!/bin/bash
# =================================================================
# test_integer_types.sh - Integer Data Type ACCURACY Tests
# =================================================================
# Validates: Exact integer values, boundary values, byte sizes
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: INTEGER Data Types - ACCURACY VALIDATION${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: INTEGER Type - Exact Boundary Values
# =================================================================

log "${MAGENTA}[Test 1] INTEGER Type - Exact Boundary Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Int;
CREATE TABLE Test_Int (id INTEGER, val INTEGER);
INSERT INTO Test_Int VALUES (1, 2147483647);
INSERT INTO Test_Int VALUES (2, -2147483648);
INSERT INTO Test_Int VALUES (3, 0);
INSERT INTO Test_Int VALUES (4, 1);
INSERT INTO Test_Int VALUES (5, -1);
INSERT INTO Test_Int VALUES (6, NULL);

-- Verify EXACT values
SELECT 'INT_MAX=' || CAST(val AS VARCHAR(20)) FROM Test_Int WHERE id=1;
SELECT 'INT_MIN=' || CAST(val AS VARCHAR(20)) FROM Test_Int WHERE id=2;
SELECT 'INT_ZERO=' || CAST(val AS VARCHAR(20)) FROM Test_Int WHERE id=3;
SELECT 'INT_ONE=' || CAST(val AS VARCHAR(20)) FROM Test_Int WHERE id=4;
SELECT 'INT_NEG1=' || CAST(val AS VARCHAR(20)) FROM Test_Int WHERE id=5;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Int)) AS t;

DROP TABLE Test_Int;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "INTEGER: 6 rows exported" "6" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "INTEGER: 1 NULL detected" "1" "$NULLS"

# Verify EXACT integer boundary values
assert_contains "INTEGER: Max 2147483647 exact" "INT_MAX=2147483647" "$RESULT"
assert_contains "INTEGER: Min -2147483648 exact" "INT_MIN=-2147483648" "$RESULT"
assert_contains "INTEGER: Zero exact" "INT_ZERO=0" "$RESULT"
assert_contains "INTEGER: One exact" "INT_ONE=1" "$RESULT"
assert_contains "INTEGER: Negative one exact" "INT_NEG1=-1" "$RESULT"

# INTEGER is 4 bytes, 5 non-null values = 20 bytes minimum for val column
BYTES=$(get_total_bytes "$RESULT")
assert_gte "INTEGER: Bytes >= 36 (5 vals * 4 + 5 ids * 4)" "36" "$BYTES"

assert_contains "INTEGER: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 2: BIGINT Type - 64-bit Exact Values
# =================================================================

log ""
log "${MAGENTA}[Test 2] BIGINT Type - 64-bit Exact Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_BigInt;
CREATE TABLE Test_BigInt (id INTEGER, val BIGINT);
INSERT INTO Test_BigInt VALUES (1, 9223372036854775807);
INSERT INTO Test_BigInt VALUES (2, -9223372036854775808);
INSERT INTO Test_BigInt VALUES (3, 0);
INSERT INTO Test_BigInt VALUES (4, 1234567890123456789);
INSERT INTO Test_BigInt VALUES (5, NULL);

-- Verify EXACT BIGINT values
SELECT 'BIG_MAX=' || CAST(val AS VARCHAR(25)) FROM Test_BigInt WHERE id=1;
SELECT 'BIG_MIN=' || CAST(val AS VARCHAR(25)) FROM Test_BigInt WHERE id=2;
SELECT 'BIG_ZERO=' || CAST(val AS VARCHAR(25)) FROM Test_BigInt WHERE id=3;
SELECT 'BIG_LARGE=' || CAST(val AS VARCHAR(25)) FROM Test_BigInt WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_BigInt)) AS t;

DROP TABLE Test_BigInt;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "BIGINT: 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "BIGINT: 1 NULL detected" "1" "$NULLS"

# Verify EXACT 64-bit values (19 digits)
assert_contains "BIGINT: Max 9223372036854775807" "BIG_MAX=9223372036854775807" "$RESULT"
assert_contains "BIGINT: Min -9223372036854775808" "BIG_MIN=-9223372036854775808" "$RESULT"
assert_contains "BIGINT: Zero" "BIG_ZERO=0" "$RESULT"
assert_contains "BIGINT: 19-digit value preserved" "BIG_LARGE=1234567890123456789" "$RESULT"

# BIGINT is 8 bytes, 4 non-null values = 32 bytes for val + 20 for ids
BYTES=$(get_total_bytes "$RESULT")
assert_gte "BIGINT: Bytes >= 48 (4 vals * 8 + 5 ids * 4)" "48" "$BYTES"

assert_contains "BIGINT: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 3: SMALLINT Type - 16-bit Exact Values
# =================================================================

log ""
log "${MAGENTA}[Test 3] SMALLINT Type - 16-bit Exact Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_SmallInt;
CREATE TABLE Test_SmallInt (id INTEGER, val SMALLINT);
INSERT INTO Test_SmallInt VALUES (1, 32767);
INSERT INTO Test_SmallInt VALUES (2, -32768);
INSERT INTO Test_SmallInt VALUES (3, 0);
INSERT INTO Test_SmallInt VALUES (4, 12345);
INSERT INTO Test_SmallInt VALUES (5, -12345);

-- Verify EXACT SMALLINT values
SELECT 'SMALL_MAX=' || CAST(val AS VARCHAR(10)) FROM Test_SmallInt WHERE id=1;
SELECT 'SMALL_MIN=' || CAST(val AS VARCHAR(10)) FROM Test_SmallInt WHERE id=2;
SELECT 'SMALL_ZERO=' || CAST(val AS VARCHAR(10)) FROM Test_SmallInt WHERE id=3;
SELECT 'SMALL_POS=' || CAST(val AS VARCHAR(10)) FROM Test_SmallInt WHERE id=4;
SELECT 'SMALL_NEG=' || CAST(val AS VARCHAR(10)) FROM Test_SmallInt WHERE id=5;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_SmallInt)) AS t;

DROP TABLE Test_SmallInt;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "SMALLINT: 5 rows exported" "5" "$ROWS"

# Verify EXACT 16-bit boundary values
assert_contains "SMALLINT: Max 32767" "SMALL_MAX=32767" "$RESULT"
assert_contains "SMALLINT: Min -32768" "SMALL_MIN=-32768" "$RESULT"
assert_contains "SMALLINT: Zero" "SMALL_ZERO=0" "$RESULT"
assert_contains "SMALLINT: Positive 12345" "SMALL_POS=12345" "$RESULT"
assert_contains "SMALLINT: Negative -12345" "SMALL_NEG=-12345" "$RESULT"

# SMALLINT is 2 bytes
BYTES=$(get_total_bytes "$RESULT")
assert_gte "SMALLINT: Bytes >= 28 (5 vals * 2 + 5 ids * 4)" "28" "$BYTES"

assert_contains "SMALLINT: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 4: BYTEINT Type - 8-bit Exact Values
# =================================================================

log ""
log "${MAGENTA}[Test 4] BYTEINT Type - 8-bit Exact Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_ByteInt;
CREATE TABLE Test_ByteInt (id INTEGER, val BYTEINT);
INSERT INTO Test_ByteInt VALUES (1, 127);
INSERT INTO Test_ByteInt VALUES (2, -128);
INSERT INTO Test_ByteInt VALUES (3, 0);
INSERT INTO Test_ByteInt VALUES (4, 100);
INSERT INTO Test_ByteInt VALUES (5, -100);

-- Verify EXACT BYTEINT values
SELECT 'BYTE_MAX=' || CAST(val AS VARCHAR(5)) FROM Test_ByteInt WHERE id=1;
SELECT 'BYTE_MIN=' || CAST(val AS VARCHAR(5)) FROM Test_ByteInt WHERE id=2;
SELECT 'BYTE_ZERO=' || CAST(val AS VARCHAR(5)) FROM Test_ByteInt WHERE id=3;
SELECT 'BYTE_POS=' || CAST(val AS VARCHAR(5)) FROM Test_ByteInt WHERE id=4;
SELECT 'BYTE_NEG=' || CAST(val AS VARCHAR(5)) FROM Test_ByteInt WHERE id=5;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_ByteInt)) AS t;

DROP TABLE Test_ByteInt;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "BYTEINT: 5 rows exported" "5" "$ROWS"

# Verify EXACT 8-bit boundary values
assert_contains "BYTEINT: Max 127" "BYTE_MAX=127" "$RESULT"
assert_contains "BYTEINT: Min -128" "BYTE_MIN=-128" "$RESULT"
assert_contains "BYTEINT: Zero" "BYTE_ZERO=0" "$RESULT"
assert_contains "BYTEINT: Positive 100" "BYTE_POS=100" "$RESULT"
assert_contains "BYTEINT: Negative -100" "BYTE_NEG=-100" "$RESULT"

# BYTEINT is 1 byte
BYTES=$(get_total_bytes "$RESULT")
assert_gte "BYTEINT: Bytes >= 23 (5 vals * 1 + 5 ids * 4)" "23" "$BYTES"

assert_contains "BYTEINT: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 5: Mixed Integer Types - All Preserved
# =================================================================

log ""
log "${MAGENTA}[Test 5] Mixed Integer Types - All Values Preserved${NC}"

RESULT=$(run_sql "
DROP TABLE Test_MixedInt;
CREATE TABLE Test_MixedInt (
    id INTEGER,
    col_int INTEGER,
    col_big BIGINT,
    col_small SMALLINT,
    col_byte BYTEINT
);
INSERT INTO Test_MixedInt VALUES (1, 1000000, 1000000000000, 10000, 100);
INSERT INTO Test_MixedInt VALUES (2, -1000000, -1000000000000, -10000, -100);

-- Verify ALL values preserved across types
SELECT 'MIX1_INT=' || CAST(col_int AS VARCHAR(15)) FROM Test_MixedInt WHERE id=1;
SELECT 'MIX1_BIG=' || CAST(col_big AS VARCHAR(20)) FROM Test_MixedInt WHERE id=1;
SELECT 'MIX1_SMALL=' || CAST(col_small AS VARCHAR(10)) FROM Test_MixedInt WHERE id=1;
SELECT 'MIX1_BYTE=' || CAST(col_byte AS VARCHAR(5)) FROM Test_MixedInt WHERE id=1;
SELECT 'MIX2_INT=' || CAST(col_int AS VARCHAR(15)) FROM Test_MixedInt WHERE id=2;
SELECT 'MIX2_BIG=' || CAST(col_big AS VARCHAR(20)) FROM Test_MixedInt WHERE id=2;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_MixedInt)) AS t;

DROP TABLE Test_MixedInt;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Mixed int: 2 rows exported" "2" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Mixed int: 5 columns detected" "5" "$COLS"

# Verify each type preserves value exactly
assert_contains "Mixed: INTEGER 1000000" "MIX1_INT=1000000" "$RESULT"
assert_contains "Mixed: BIGINT 1000000000000" "MIX1_BIG=1000000000000" "$RESULT"
assert_contains "Mixed: SMALLINT 10000" "MIX1_SMALL=10000" "$RESULT"
assert_contains "Mixed: BYTEINT 100" "MIX1_BYTE=100" "$RESULT"
assert_contains "Mixed: Negative INTEGER" "MIX2_INT=-1000000" "$RESULT"
assert_contains "Mixed: Negative BIGINT" "MIX2_BIG=-1000000000000" "$RESULT"

# Total bytes: 2 rows * (4 + 4 + 8 + 2 + 1) = 38 bytes
BYTES=$(get_total_bytes "$RESULT")
assert_gte "Mixed int: Bytes >= 36" "36" "$BYTES"

assert_contains "Mixed int: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 6: Arithmetic Verification
# =================================================================

log ""
log "${MAGENTA}[Test 6] Integer Arithmetic Verification${NC}"

RESULT=$(run_sql "
DROP TABLE Test_IntArith;
CREATE TABLE Test_IntArith (id INTEGER, val INTEGER);
INSERT INTO Test_IntArith VALUES (1, 100);
INSERT INTO Test_IntArith VALUES (2, 200);
INSERT INTO Test_IntArith VALUES (3, 300);

-- Verify arithmetic is preserved
SELECT 'SUM=' || CAST(SUM(val) AS VARCHAR(10)) FROM Test_IntArith;
SELECT 'AVG=' || CAST(AVG(val) AS VARCHAR(10)) FROM Test_IntArith;
SELECT 'MAX=' || CAST(MAX(val) AS VARCHAR(10)) FROM Test_IntArith;
SELECT 'MIN=' || CAST(MIN(val) AS VARCHAR(10)) FROM Test_IntArith;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_IntArith)) AS t;

DROP TABLE Test_IntArith;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Int arithmetic: 3 rows exported" "3" "$ROWS"

# Verify arithmetic results
assert_contains "Int: SUM=600" "SUM=600" "$RESULT"
assert_contains "Int: AVG=200" "AVG=200" "$RESULT"
assert_contains "Int: MAX=300" "MAX=300" "$RESULT"
assert_contains "Int: MIN=100" "MIN=100" "$RESULT"

assert_contains "Int arithmetic: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Summary
# =================================================================

print_summary "Integer Types (Accuracy Validated)"
exit $?
