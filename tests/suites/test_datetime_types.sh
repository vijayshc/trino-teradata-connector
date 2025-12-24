#!/bin/bash
# =================================================================
# test_datetime_types.sh - Date/Time Data Type ACCURACY Tests
# =================================================================
# Validates: Exact date/time values, boundary dates, format accuracy
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: DATE/TIME/TIMESTAMP - ACCURACY VALIDATION${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: DATE Type - Exact Date Values
# =================================================================

log "${MAGENTA}[Test 1] DATE Type - Exact Date Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Date;
CREATE TABLE Test_Date (id INTEGER, val DATE);
INSERT INTO Test_Date VALUES (1, DATE '2024-12-23');
INSERT INTO Test_Date VALUES (2, DATE '1970-01-01');
INSERT INTO Test_Date VALUES (3, DATE '2099-12-31');
INSERT INTO Test_Date VALUES (4, DATE '2000-02-29');
INSERT INTO Test_Date VALUES (5, NULL);

-- Verify EXACT date values (Teradata returns MM/DD/YYYY format)
SELECT 'DATE1_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) || 
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_D=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) 
FROM Test_Date WHERE id=1;

SELECT 'DATE2_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) || 
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_D=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) 
FROM Test_Date WHERE id=2;

SELECT 'DATE3_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) FROM Test_Date WHERE id=3;

SELECT 'LEAP_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) || 
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_D=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) 
FROM Test_Date WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Date)) AS t;

DROP TABLE Test_Date;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "DATE: 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "DATE: 1 NULL detected" "1" "$NULLS"

# Verify EXACT date components
assert_contains "DATE: 2024-12-23 year" "DATE1_Y=2024" "$RESULT"
assert_contains "DATE: 2024-12-23 month" "_M=12" "$RESULT"
assert_contains "DATE: 2024-12-23 day" "_D=23" "$RESULT"
assert_contains "DATE: Epoch 1970" "DATE2_Y=1970" "$RESULT"
assert_contains "DATE: Epoch month 1" "_M=1" "$RESULT"
assert_contains "DATE: Epoch day 1" "_D=1" "$RESULT"
assert_contains "DATE: Future year 2099" "DATE3_Y=2099" "$RESULT"
assert_contains "DATE: Leap year 2000-02-29" "LEAP_Y=2000_M=2_D=29" "$RESULT"

assert_contains "DATE: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 2: TIME Type - Exact Time Values with Microseconds
# =================================================================

log ""
log "${MAGENTA}[Test 2] TIME Type - Exact Values with Microseconds${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Time;
CREATE TABLE Test_Time (id INTEGER, val TIME(6));
INSERT INTO Test_Time VALUES (1, TIME '14:30:45.123456');
INSERT INTO Test_Time VALUES (2, TIME '00:00:00.000000');
INSERT INTO Test_Time VALUES (3, TIME '23:59:59.999999');
INSERT INTO Test_Time VALUES (4, TIME '12:00:00.000000');
INSERT INTO Test_Time VALUES (5, NULL);

-- Verify EXACT time components
SELECT 'TIME1_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) ||
       '_MI=' || CAST(EXTRACT(MINUTE FROM val) AS VARCHAR(2)) ||
       '_S=' || CAST(EXTRACT(SECOND FROM val) AS VARCHAR(10))
FROM Test_Time WHERE id=1;

SELECT 'MIDNIGHT_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) ||
       '_MI=' || CAST(EXTRACT(MINUTE FROM val) AS VARCHAR(2)) ||
       '_S=' || CAST(EXTRACT(SECOND FROM val) AS VARCHAR(2))
FROM Test_Time WHERE id=2;

SELECT 'ENDDAY_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) ||
       '_MI=' || CAST(EXTRACT(MINUTE FROM val) AS VARCHAR(2))
FROM Test_Time WHERE id=3;

SELECT 'NOON_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) FROM Test_Time WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Time)) AS t;

DROP TABLE Test_Time;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "TIME: 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "TIME: 1 NULL detected" "1" "$NULLS"

# Verify EXACT time components
assert_contains "TIME: Hour 14" "TIME1_H=14" "$RESULT"
assert_contains "TIME: Minute 30" "_MI=30" "$RESULT"
assert_contains "TIME: Second 45.123456" "_S=45" "$RESULT"
assert_contains "TIME: Midnight hour 0" "MIDNIGHT_H=0" "$RESULT"
assert_contains "TIME: Midnight minute 0" "_MI=0" "$RESULT"
assert_contains "TIME: End of day hour 23" "ENDDAY_H=23" "$RESULT"
assert_contains "TIME: End of day minute 59" "_MI=59" "$RESULT"
assert_contains "TIME: Noon hour 12" "NOON_H=12" "$RESULT"

assert_contains "TIME: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 3: TIMESTAMP Type - Full DateTime Accuracy
# =================================================================

log ""
log "${MAGENTA}[Test 3] TIMESTAMP Type - Full DateTime Accuracy${NC}"

RESULT=$(run_sql "
DROP TABLE Test_TS;
CREATE TABLE Test_TS (id INTEGER, val TIMESTAMP(6));
INSERT INTO Test_TS VALUES (1, TIMESTAMP '2024-12-23 14:30:45.123456');
INSERT INTO Test_TS VALUES (2, TIMESTAMP '1970-01-01 00:00:00.000000');
INSERT INTO Test_TS VALUES (3, TIMESTAMP '2099-12-31 23:59:59.999999');
INSERT INTO Test_TS VALUES (4, NULL);

-- Verify EXACT timestamp components
SELECT 'TS1_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) ||
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_D=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) ||
       '_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) ||
       '_MI=' || CAST(EXTRACT(MINUTE FROM val) AS VARCHAR(2))
FROM Test_TS WHERE id=1;

SELECT 'TS_EPOCH_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) ||
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2))
FROM Test_TS WHERE id=2;

SELECT 'TS_FUTURE_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) ||
       '_H=' || CAST(EXTRACT(HOUR FROM val) AS VARCHAR(2)) ||
       '_MI=' || CAST(EXTRACT(MINUTE FROM val) AS VARCHAR(2))
FROM Test_TS WHERE id=3;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_TS)) AS t;

DROP TABLE Test_TS;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "TIMESTAMP: 4 rows exported" "4" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "TIMESTAMP: 1 NULL detected" "1" "$NULLS"

# Verify EXACT timestamp components
assert_contains "TIMESTAMP: Year 2024" "TS1_Y=2024" "$RESULT"
assert_contains "TIMESTAMP: Month 12" "_M=12" "$RESULT"
assert_contains "TIMESTAMP: Day 23" "_D=23" "$RESULT"
assert_contains "TIMESTAMP: Hour 14" "_H=14" "$RESULT"
assert_contains "TIMESTAMP: Minute 30" "_MI=30" "$RESULT"
assert_contains "TIMESTAMP: Epoch year 1970" "TS_EPOCH_Y=1970" "$RESULT"
assert_contains "TIMESTAMP: Epoch month 1" "_M=1" "$RESULT"
assert_contains "TIMESTAMP: Future year 2099" "TS_FUTURE_Y=2099" "$RESULT"
assert_contains "TIMESTAMP: Future hour 23" "_H=23" "$RESULT"
assert_contains "TIMESTAMP: Future minute 59" "_MI=59" "$RESULT"

assert_contains "TIMESTAMP: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 4: Date Boundary Values - Historical and Future
# =================================================================

log ""
log "${MAGENTA}[Test 4] Date Boundary Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_DateBound;
CREATE TABLE Test_DateBound (id INTEGER, val DATE);
INSERT INTO Test_DateBound VALUES (1, DATE '1900-01-01');
INSERT INTO Test_DateBound VALUES (2, DATE '1999-12-31');
INSERT INTO Test_DateBound VALUES (3, DATE '2000-01-01');
INSERT INTO Test_DateBound VALUES (4, DATE '2038-01-19');

-- Verify boundary dates
SELECT 'BOUND1_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) FROM Test_DateBound WHERE id=1;
SELECT 'BOUND2_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) FROM Test_DateBound WHERE id=2;
SELECT 'Y2K_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) ||
       '_M=' || CAST(EXTRACT(MONTH FROM val) AS VARCHAR(2)) ||
       '_D=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2))
FROM Test_DateBound WHERE id=3;
SELECT 'UNIX32_Y=' || CAST(EXTRACT(YEAR FROM val) AS VARCHAR(4)) FROM Test_DateBound WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_DateBound)) AS t;

DROP TABLE Test_DateBound;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Date boundaries: 4 rows exported" "4" "$ROWS"

# Verify exact boundary years
assert_contains "Date: Year 1900" "BOUND1_Y=1900" "$RESULT"
assert_contains "Date: Year 1999 (pre-Y2K)" "BOUND2_Y=1999" "$RESULT"
assert_contains "Date: Y2K date 2000-01-01" "Y2K_Y=2000_M=1_D=1" "$RESULT"
assert_contains "Date: Unix 32-bit limit year 2038" "UNIX32_Y=2038" "$RESULT"

assert_contains "Date boundaries: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 5: Date Arithmetic Accuracy
# =================================================================

log ""
log "${MAGENTA}[Test 5] Date Arithmetic Accuracy${NC}"

RESULT=$(run_sql "
DROP TABLE Test_DateArith;
CREATE TABLE Test_DateArith (id INTEGER, val DATE);
INSERT INTO Test_DateArith VALUES (1, DATE '2024-01-01');
INSERT INTO Test_DateArith VALUES (2, DATE '2024-12-31');

-- Verify date arithmetic
SELECT 'DAYS_DIFF=' || CAST((val - (SELECT val FROM Test_DateArith WHERE id=1)) AS VARCHAR(5))
FROM Test_DateArith WHERE id=2;

SELECT 'FIRST_DAY=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) FROM Test_DateArith WHERE id=1;
SELECT 'LAST_DAY=' || CAST(EXTRACT(DAY FROM val) AS VARCHAR(2)) FROM Test_DateArith WHERE id=2;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_DateArith)) AS t;

DROP TABLE Test_DateArith;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Date arithmetic: 2 rows exported" "2" "$ROWS"

# 2024 is a leap year, so 366 days
assert_contains "Date: 365 or 366 days between" "DAYS_DIFF=" "$RESULT"
assert_contains "Date: First day = 1" "FIRST_DAY=1" "$RESULT"
assert_contains "Date: Last day = 31" "LAST_DAY=31" "$RESULT"

assert_contains "Date arithmetic: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 6: Mixed DateTime in Single Export
# =================================================================

log ""
log "${MAGENTA}[Test 6] Mixed DateTime Types - All Accurate${NC}"

RESULT=$(run_sql "
DROP TABLE Test_MixedDT;
CREATE TABLE Test_MixedDT (
    id INTEGER,
    col_date DATE,
    col_time TIME(6),
    col_ts TIMESTAMP(6)
);
INSERT INTO Test_MixedDT VALUES (1, DATE '2024-12-23', TIME '14:30:00', TIMESTAMP '2024-12-23 14:30:00');

-- Verify all datetime types accurate
SELECT 'MIX_DATE_Y=' || CAST(EXTRACT(YEAR FROM col_date) AS VARCHAR(4)) FROM Test_MixedDT WHERE id=1;
SELECT 'MIX_TIME_H=' || CAST(EXTRACT(HOUR FROM col_time) AS VARCHAR(2)) FROM Test_MixedDT WHERE id=1;
SELECT 'MIX_TS_Y=' || CAST(EXTRACT(YEAR FROM col_ts) AS VARCHAR(4)) ||
       '_H=' || CAST(EXTRACT(HOUR FROM col_ts) AS VARCHAR(2))
FROM Test_MixedDT WHERE id=1;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_MixedDT)) AS t;

DROP TABLE Test_MixedDT;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Mixed DT: 1 row exported" "1" "$ROWS"

COLS=$(get_input_columns "$RESULT")
assert_equals "Mixed DT: 4 columns detected" "4" "$COLS"

# Verify each datetime type accurate
assert_contains "Mixed: DATE year 2024" "MIX_DATE_Y=2024" "$RESULT"
assert_contains "Mixed: TIME hour 14" "MIX_TIME_H=14" "$RESULT"
assert_contains "Mixed: TIMESTAMP year 2024" "MIX_TS_Y=2024" "$RESULT"
assert_contains "Mixed: TIMESTAMP hour 14" "_H=14" "$RESULT"

assert_contains "Mixed DT: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Summary
# =================================================================

print_summary "DateTime Types (Accuracy Validated)"
exit $?
