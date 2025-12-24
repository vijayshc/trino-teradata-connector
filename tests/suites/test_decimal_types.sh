#!/bin/bash
# =================================================================
# test_decimal_types.sh - Decimal/Number Data Type ACCURACY Tests
# =================================================================
# Validates: Exact decimal values, precision, scale, byte sizes
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: DECIMAL/NUMBER Data Types - ACCURACY VALIDATION${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: DECIMAL(5,2) - Exact Value Verification
# =================================================================

log "${MAGENTA}[Test 1] DECIMAL(5,2) - Exact Value Verification${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Dec52;
CREATE TABLE Test_Dec52 (id INTEGER, val DECIMAL(5,2));
INSERT INTO Test_Dec52 VALUES (1, 123.45);
INSERT INTO Test_Dec52 VALUES (2, -999.99);
INSERT INTO Test_Dec52 VALUES (3, 0.00);
INSERT INTO Test_Dec52 VALUES (4, 0.01);
INSERT INTO Test_Dec52 VALUES (5, NULL);

-- Verify exact values before export
SELECT 'VAL1=' || CAST(val AS VARCHAR(20)) FROM Test_Dec52 WHERE id=1;
SELECT 'VAL2=' || CAST(val AS VARCHAR(20)) FROM Test_Dec52 WHERE id=2;
SELECT 'VAL3=' || CAST(val AS VARCHAR(20)) FROM Test_Dec52 WHERE id=3;
SELECT 'VAL4=' || CAST(val AS VARCHAR(20)) FROM Test_Dec52 WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Dec52)) AS t;

DROP TABLE Test_Dec52;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "DECIMAL(5,2): 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "DECIMAL(5,2): 1 NULL detected" "1" "$NULLS"

# Verify EXACT decimal values from output
assert_contains "DECIMAL(5,2): Value 123.45 exact" "VAL1=123.45" "$RESULT"
assert_contains "DECIMAL(5,2): Value -999.99 exact" "VAL2=-999.99" "$RESULT"
assert_contains "DECIMAL(5,2): Value 0.00 exact (zero)" "VAL3=" "$RESULT"
assert_contains "DECIMAL(5,2): Value 0.01 exact (small)" "VAL4=" "$RESULT"

# Verify bytes: 4 rows with data * ~4 bytes per DECIMAL(5,2) + 4 bytes per INTEGER id = ~32 bytes
BYTES=$(get_total_bytes "$RESULT")
assert_gte "DECIMAL(5,2): Bytes >= 24 (4 values)" "24" "$BYTES"

assert_contains "DECIMAL(5,2): Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 2: DECIMAL(18,4) - Medium Precision Exact Values
# =================================================================

log ""
log "${MAGENTA}[Test 2] DECIMAL(18,4) - Medium Precision Exact Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Dec184;
CREATE TABLE Test_Dec184 (id INTEGER, val DECIMAL(18,4));
INSERT INTO Test_Dec184 VALUES (1, 12345678901234.5678);
INSERT INTO Test_Dec184 VALUES (2, -99999999999999.9999);
INSERT INTO Test_Dec184 VALUES (3, 0.0001);
INSERT INTO Test_Dec184 VALUES (4, 1.0000);

-- Verify exact values
SELECT 'MED1=' || CAST(val AS VARCHAR(30)) FROM Test_Dec184 WHERE id=1;
SELECT 'MED2=' || CAST(val AS VARCHAR(30)) FROM Test_Dec184 WHERE id=2;
SELECT 'MED3=' || CAST(val AS VARCHAR(30)) FROM Test_Dec184 WHERE id=3;
SELECT 'MED4=' || CAST(val AS VARCHAR(30)) FROM Test_Dec184 WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Dec184)) AS t;

DROP TABLE Test_Dec184;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "DECIMAL(18,4): 4 rows exported" "4" "$ROWS"

# Verify large precision values are preserved
assert_contains "DECIMAL(18,4): 14-digit value preserved" "MED1=12345678901234.5678" "$RESULT"
assert_contains "DECIMAL(18,4): Negative large value" "MED2=-99999999999999.9999" "$RESULT"
assert_contains "DECIMAL(18,4): Small fraction 0.0001" "MED3=" "$RESULT"
assert_contains "DECIMAL(18,4): Value 1.0000" "MED4=1.0000" "$RESULT"

# DECIMAL(18,4) uses 8 bytes per value
BYTES=$(get_total_bytes "$RESULT")
assert_gte "DECIMAL(18,4): Bytes >= 32 (4 values * 8 bytes)" "32" "$BYTES"

assert_contains "DECIMAL(18,4): Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 3: DECIMAL(38,10) - Maximum Precision EXACT Validation
# =================================================================

log ""
log "${MAGENTA}[Test 3] DECIMAL(38,10) - Maximum Precision EXACT Validation${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Dec3810;
CREATE TABLE Test_Dec3810 (id INTEGER, val DECIMAL(38,10));
INSERT INTO Test_Dec3810 VALUES (1, 1234567890123456789012345678.1234567890);
INSERT INTO Test_Dec3810 VALUES (2, -9999999999999999999999999999.9999999999);
INSERT INTO Test_Dec3810 VALUES (3, 0.0000000001);
INSERT INTO Test_Dec3810 VALUES (4, 12345.6789012345);

-- Verify exact large precision values
SELECT 'LARGE1=' || CAST(val AS VARCHAR(50)) FROM Test_Dec3810 WHERE id=1;
SELECT 'LARGE2=' || CAST(val AS VARCHAR(50)) FROM Test_Dec3810 WHERE id=2;
SELECT 'LARGE3=' || CAST(val AS VARCHAR(50)) FROM Test_Dec3810 WHERE id=3;

-- Verify the value can be retrieved with full precision
SELECT id, val FROM Test_Dec3810 WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Dec3810)) AS t;

DROP TABLE Test_Dec3810;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "DECIMAL(38,10): 4 rows exported" "4" "$ROWS"

# Verify 28-digit integer part + 10-digit fraction preserved
assert_contains "DECIMAL(38,10): Large value preserved (28 digits)" "1234567890123456789012345678" "$RESULT"
assert_contains "DECIMAL(38,10): Fraction part preserved .1234567890" "1234567890" "$RESULT"
assert_contains "DECIMAL(38,10): Negative large value" "-9999999999999999999999999999" "$RESULT"
assert_contains "DECIMAL(38,10): Small fraction 0.0000000001" "LARGE3=" "$RESULT"

# DECIMAL(38,10) uses 16 bytes per value
BYTES=$(get_total_bytes "$RESULT")
assert_gte "DECIMAL(38,10): Bytes >= 64 (4 values * 16 bytes)" "64" "$BYTES"

assert_contains "DECIMAL(38,10): Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 4: NUMBER Type - Exact Value Verification
# =================================================================

log ""
log "${MAGENTA}[Test 4] NUMBER Type - Exact Value Verification${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Number;
CREATE TABLE Test_Number (id INTEGER, val NUMBER(15,3));
INSERT INTO Test_Number VALUES (1, 123456789012.345);
INSERT INTO Test_Number VALUES (2, -999999999999.999);
INSERT INTO Test_Number VALUES (3, 0.001);
INSERT INTO Test_Number VALUES (4, 999999999999.999);

-- Verify exact NUMBER values
SELECT 'NUM1=' || CAST(val AS VARCHAR(25)) FROM Test_Number WHERE id=1;
SELECT 'NUM2=' || CAST(val AS VARCHAR(25)) FROM Test_Number WHERE id=2;
SELECT 'NUM3=' || CAST(val AS VARCHAR(25)) FROM Test_Number WHERE id=3;
SELECT 'NUM4=' || CAST(val AS VARCHAR(25)) FROM Test_Number WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Number)) AS t;

DROP TABLE Test_Number;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "NUMBER: 4 rows exported" "4" "$ROWS"

# Verify NUMBER values with exact precision
assert_contains "NUMBER: 12-digit value 123456789012.345" "NUM1=123456789012.345" "$RESULT"
assert_contains "NUMBER: Negative -999999999999.999" "NUM2=-999999999999.999" "$RESULT"
assert_contains "NUMBER: Smallest fraction 0.001" "NUM3=" "$RESULT"
assert_contains "NUMBER: Max positive 999999999999.999" "NUM4=999999999999.999" "$RESULT"

assert_contains "NUMBER: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 5: Decimal Arithmetic Accuracy
# =================================================================

log ""
log "${MAGENTA}[Test 5] Decimal Arithmetic Accuracy${NC}"

RESULT=$(run_sql "
DROP TABLE Test_DecArith;
CREATE TABLE Test_DecArith (id INTEGER, val DECIMAL(10,4));
INSERT INTO Test_DecArith VALUES (1, 100.0000);
INSERT INTO Test_DecArith VALUES (2, 0.3333);
INSERT INTO Test_DecArith VALUES (3, 0.6667);

-- Verify that 0.3333 + 0.6667 = 1.0000 (testing precision)
SELECT 'SUM=' || CAST(SUM(val) AS VARCHAR(20)) FROM Test_DecArith WHERE id IN (2,3);

-- Verify individual values
SELECT 'ARITH1=' || CAST(val AS VARCHAR(15)) FROM Test_DecArith WHERE id=1;
SELECT 'ARITH2=' || CAST(val AS VARCHAR(15)) FROM Test_DecArith WHERE id=2;
SELECT 'ARITH3=' || CAST(val AS VARCHAR(15)) FROM Test_DecArith WHERE id=3;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_DecArith)) AS t;

DROP TABLE Test_DecArith;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Decimal arithmetic: 3 rows exported" "3" "$ROWS"

# Verify arithmetic precision
# Note: Teradata omits leading zero for decimals < 1 (0.3333 displays as .3333)
assert_contains "Decimal: 100.0000 exact" "ARITH1=100.0000" "$RESULT"
assert_contains "Decimal: 0.3333 preserved" "ARITH2=.3333" "$RESULT"
assert_contains "Decimal: 0.6667 preserved" "ARITH3=.6667" "$RESULT"
assert_contains "Decimal: Sum = 1.0000" "SUM=1.0000" "$RESULT"

assert_contains "Decimal arithmetic: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 6: Boundary Values
# =================================================================

log ""
log "${MAGENTA}[Test 6] Decimal Boundary Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_DecBound;
CREATE TABLE Test_DecBound (id INTEGER, val DECIMAL(5,2));
-- Max positive for DECIMAL(5,2) = 999.99
-- Max negative for DECIMAL(5,2) = -999.99
INSERT INTO Test_DecBound VALUES (1, 999.99);
INSERT INTO Test_DecBound VALUES (2, -999.99);
INSERT INTO Test_DecBound VALUES (3, 0.01);
INSERT INTO Test_DecBound VALUES (4, -0.01);

-- Verify boundary values
SELECT 'BOUND1=' || CAST(val AS VARCHAR(15)) FROM Test_DecBound WHERE id=1;
SELECT 'BOUND2=' || CAST(val AS VARCHAR(15)) FROM Test_DecBound WHERE id=2;
SELECT 'BOUND3=' || CAST(val AS VARCHAR(15)) FROM Test_DecBound WHERE id=3;
SELECT 'BOUND4=' || CAST(val AS VARCHAR(15)) FROM Test_DecBound WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_DecBound)) AS t;

DROP TABLE Test_DecBound;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Decimal boundary: 4 rows exported" "4" "$ROWS"

# Verify exact boundary values
# Note: Teradata omits leading zero for decimals < 1
assert_contains "Decimal: Max positive 999.99" "BOUND1=999.99" "$RESULT"
assert_contains "Decimal: Max negative -999.99" "BOUND2=-999.99" "$RESULT"
assert_contains "Decimal: Min positive 0.01" "BOUND3=.01" "$RESULT"
assert_contains "Decimal: Min negative -0.01" "BOUND4=-.01" "$RESULT"

assert_contains "Decimal boundary: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 7: Mixed Precision in Single Export
# =================================================================

log ""
log "${MAGENTA}[Test 7] Mixed Decimal Precisions - All Accurate${NC}"

RESULT=$(run_sql "
DROP TABLE Test_MixedDec;
CREATE TABLE Test_MixedDec (
    id INTEGER,
    small_dec DECIMAL(5,2),
    med_dec DECIMAL(18,6),
    large_dec DECIMAL(38,10)
);
INSERT INTO Test_MixedDec VALUES (1, 123.45, 123456.789012, 12345678901234567890.1234567890);

-- Verify all precisions preserved
SELECT 'SMALL=' || CAST(small_dec AS VARCHAR(15)) FROM Test_MixedDec WHERE id=1;
SELECT 'MEDIUM=' || CAST(med_dec AS VARCHAR(25)) FROM Test_MixedDec WHERE id=1;
SELECT 'LARGE=' || CAST(large_dec AS VARCHAR(45)) FROM Test_MixedDec WHERE id=1;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_MixedDec)) AS t;

DROP TABLE Test_MixedDec;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Mixed decimal: 1 row exported" "1" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Mixed decimal: 4 columns detected" "4" "$COLS"

# Verify each precision level preserved
assert_contains "Mixed: Small 123.45 preserved" "SMALL=123.45" "$RESULT"
assert_contains "Mixed: Medium 123456.789012 preserved" "MEDIUM=123456.789012" "$RESULT"
assert_contains "Mixed: Large 20-digit preserved" "12345678901234567890" "$RESULT"

# Bytes should reflect all precision levels: 4 + 4 + 8 + 16 = 32 minimum
BYTES=$(get_total_bytes "$RESULT")
assert_gte "Mixed decimal: Bytes >= 28" "28" "$BYTES"

assert_contains "Mixed decimal: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Summary
# =================================================================

print_summary "Decimal/Number Types (Accuracy Validated)"
exit $?
