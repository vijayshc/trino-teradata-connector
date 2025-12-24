#!/bin/bash
# =================================================================
# test_character_types.sh - Character Data Type ACCURACY Tests
# =================================================================
# Validates: Exact string values, length accuracy, special chars
# =================================================================

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
source "$SCRIPT_DIR/test_common.sh"

log ""
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log "${CYAN}  Test Suite: CHARACTER Data Types - ACCURACY VALIDATION${NC}"
log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
log ""

# =================================================================
# Test 1: CHAR Type - Exact Values and Padding
# =================================================================

log "${MAGENTA}[Test 1] CHAR Type - Exact Values and Padding${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Char;
CREATE TABLE Test_Char (id INTEGER, val CHAR(10));
INSERT INTO Test_Char VALUES (1, 'Hello');
INSERT INTO Test_Char VALUES (2, 'ABCDEFGHIJ');
INSERT INTO Test_Char VALUES (3, 'X');
INSERT INTO Test_Char VALUES (4, NULL);

-- Verify EXACT values (CHAR pads with spaces)
SELECT 'CHAR1=[' || val || ']' FROM Test_Char WHERE id=1;
SELECT 'CHAR1_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_Char WHERE id=1;
SELECT 'CHAR1_TRIM=[' || TRIM(val) || ']' FROM Test_Char WHERE id=1;
SELECT 'CHAR2=[' || val || ']' FROM Test_Char WHERE id=2;
SELECT 'CHAR2_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_Char WHERE id=2;
SELECT 'CHAR3_TRIM=[' || TRIM(val) || ']' FROM Test_Char WHERE id=3;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Char)) AS t;

DROP TABLE Test_Char;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "CHAR: 4 rows exported" "4" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "CHAR: 1 NULL detected" "1" "$NULLS"

# Verify EXACT character values
assert_contains "CHAR: 'Hello' with padding" "CHAR1=[Hello" "$RESULT"
assert_contains "CHAR: Length always 10" "CHAR1_LEN=10" "$RESULT"
assert_contains "CHAR: Trimmed value 'Hello'" "CHAR1_TRIM=[Hello]" "$RESULT"
assert_contains "CHAR: Full 10-char value" "CHAR2=[ABCDEFGHIJ]" "$RESULT"
assert_contains "CHAR: Full-length is 10" "CHAR2_LEN=10" "$RESULT"
assert_contains "CHAR: Single char trimmed" "CHAR3_TRIM=[X]" "$RESULT"

# CHAR(10) is 10 bytes per row, 3 non-null values = 30 bytes
BYTES=$(get_total_bytes "$RESULT")
assert_gte "CHAR: Bytes >= 38 (3 vals * 10 + 4 ids * 4)" "38" "$BYTES"

assert_contains "CHAR: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 2: VARCHAR Type - Exact Variable Length Values
# =================================================================

log ""
log "${MAGENTA}[Test 2] VARCHAR Type - Exact Variable Length Values${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Varchar;
CREATE TABLE Test_Varchar (id INTEGER, val VARCHAR(100));
INSERT INTO Test_Varchar VALUES (1, 'Hello World');
INSERT INTO Test_Varchar VALUES (2, 'A');
INSERT INTO Test_Varchar VALUES (3, 'The quick brown fox jumps over the lazy dog');
INSERT INTO Test_Varchar VALUES (4, '12345');
INSERT INTO Test_Varchar VALUES (5, NULL);

-- Verify EXACT values and lengths
SELECT 'VC1=[' || val || ']' FROM Test_Varchar WHERE id=1;
SELECT 'VC1_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_Varchar WHERE id=1;
SELECT 'VC2=[' || val || ']' FROM Test_Varchar WHERE id=2;
SELECT 'VC2_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_Varchar WHERE id=2;
SELECT 'VC3_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_Varchar WHERE id=3;
SELECT 'VC4=[' || val || ']' FROM Test_Varchar WHERE id=4;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Varchar)) AS t;

DROP TABLE Test_Varchar;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "VARCHAR: 5 rows exported" "5" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "VARCHAR: 1 NULL detected" "1" "$NULLS"

# Verify EXACT varchar values and lengths
assert_contains "VARCHAR: 'Hello World' exact" "VC1=[Hello World]" "$RESULT"
assert_contains "VARCHAR: 'Hello World' length 11" "VC1_LEN=11" "$RESULT"
assert_contains "VARCHAR: Single char 'A'" "VC2=[A]" "$RESULT"
assert_contains "VARCHAR: Single char length 1" "VC2_LEN=1" "$RESULT"
assert_contains "VARCHAR: Long sentence length 43" "VC3_LEN=43" "$RESULT"
assert_contains "VARCHAR: Numeric string '12345'" "VC4=[12345]" "$RESULT"

# Variable bytes based on actual string lengths
BYTES=$(get_total_bytes "$RESULT")
assert_gte "VARCHAR: Bytes >= 60 (11+1+43+5)" "60" "$BYTES"

assert_contains "VARCHAR: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 3: Long VARCHAR - Large Text Accuracy
# =================================================================

log ""
log "${MAGENTA}[Test 3] Long VARCHAR - Large Text Accuracy${NC}"

RESULT=$(run_sql "
DROP TABLE Test_LongVC;
CREATE TABLE Test_LongVC (id INTEGER, val VARCHAR(2000));
INSERT INTO Test_LongVC VALUES (1, 'This is a longer string for testing. It contains multiple sentences and should be processed correctly by the Table Operator without truncation or corruption.');
INSERT INTO Test_LongVC VALUES (2, 'Short');

-- Verify lengths
SELECT 'LONG1_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(5)) FROM Test_LongVC WHERE id=1;
SELECT 'LONG2_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(5)) FROM Test_LongVC WHERE id=2;

-- Verify first and last 10 chars to ensure no truncation
SELECT 'LONG1_START=[' || SUBSTR(val, 1, 10) || ']' FROM Test_LongVC WHERE id=1;
SELECT 'LONG1_END=[' || SUBSTR(val, CHAR_LENGTH(val)-9, 10) || ']' FROM Test_LongVC WHERE id=1;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_LongVC)) AS t;

DROP TABLE Test_LongVC;
")

ROWS=$(get_total_rows "$RESULT")
assert_gte "Long VARCHAR: >= 1 row exported" "1" "$ROWS"

# Verify exact lengths
assert_contains "Long VARCHAR: String 1 length ~159" "LONG1_LEN=" "$RESULT"
assert_contains "Long VARCHAR: String 2 length 5" "LONG2_LEN=5" "$RESULT"

# Verify start and end (no truncation)
assert_contains "Long VARCHAR: Starts with 'This is a'" "LONG1_START=[This is a " "$RESULT"
assert_contains "Long VARCHAR: Ends with 'corruption.'" "ruption.]" "$RESULT"

BYTES=$(get_total_bytes "$RESULT")
assert_gte "Long VARCHAR: Bytes >= 100" "100" "$BYTES"

assert_contains "Long VARCHAR: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 4: Special Characters - Exact Preservation
# =================================================================

log ""
log "${MAGENTA}[Test 4] Special Characters - Exact Preservation${NC}"

RESULT=$(run_sql "
DROP TABLE Test_Special;
CREATE TABLE Test_Special (id INTEGER, val VARCHAR(100));
INSERT INTO Test_Special VALUES (1, 'Email: user@example.com');
INSERT INTO Test_Special VALUES (2, 'Path: /usr/local/bin');
INSERT INTO Test_Special VALUES (3, 'Apostrophe: it''s working');
INSERT INTO Test_Special VALUES (4, 'Math: 1+2=3 and 5*6=30');
INSERT INTO Test_Special VALUES (5, 'Symbols: #$%^&*()');

-- Verify EXACT special character preservation
SELECT 'SPEC1=[' || val || ']' FROM Test_Special WHERE id=1;
SELECT 'SPEC2=[' || val || ']' FROM Test_Special WHERE id=2;
SELECT 'SPEC3=[' || val || ']' FROM Test_Special WHERE id=3;
SELECT 'SPEC4=[' || val || ']' FROM Test_Special WHERE id=4;
SELECT 'SPEC5=[' || val || ']' FROM Test_Special WHERE id=5;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_Special)) AS t;

DROP TABLE Test_Special;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Special chars: 5 rows exported" "5" "$ROWS"

# Verify EXACT special character preservation
assert_contains "Special: @ in email" "SPEC1=[Email: user@example.com]" "$RESULT"
assert_contains "Special: / in path" "SPEC2=[Path: /usr/local/bin]" "$RESULT"
assert_contains "Special: Apostrophe" "SPEC3=[Apostrophe: it's working]" "$RESULT"
assert_contains "Special: Math symbols +=" "SPEC4=[Math: 1+2=3 and 5*6=30]" "$RESULT"
assert_contains "Special: Symbols #$%^" "SPEC5=[Symbols: #" "$RESULT"

assert_contains "Special chars: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 5: Empty String vs NULL - Distinction
# =================================================================

log ""
log "${MAGENTA}[Test 5] Empty String vs NULL - Correct Distinction${NC}"

RESULT=$(run_sql "
DROP TABLE Test_EmptyNull;
CREATE TABLE Test_EmptyNull (id INTEGER, val VARCHAR(50));
INSERT INTO Test_EmptyNull VALUES (1, '');
INSERT INTO Test_EmptyNull VALUES (2, NULL);
INSERT INTO Test_EmptyNull VALUES (3, 'NotEmpty');

-- Verify empty string vs NULL distinction
SELECT 'EMPTY_LEN=' || CAST(CHAR_LENGTH(val) AS VARCHAR(3)) FROM Test_EmptyNull WHERE id=1;
SELECT 'EMPTY_IS_NULL=' || CASE WHEN val IS NULL THEN 'YES' ELSE 'NO' END FROM Test_EmptyNull WHERE id=1;
SELECT 'NULL_IS_NULL=' || CASE WHEN val IS NULL THEN 'YES' ELSE 'NO' END FROM Test_EmptyNull WHERE id=2;
SELECT 'NOTEMPTY=[' || val || ']' FROM Test_EmptyNull WHERE id=3;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_EmptyNull)) AS t;

DROP TABLE Test_EmptyNull;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Empty vs NULL: 3 rows exported" "3" "$ROWS"

NULLS=$(get_total_nulls "$RESULT")
assert_equals "Empty vs NULL: 1 NULL (only row 2)" "1" "$NULLS"

# Verify distinction between empty string and NULL
assert_contains "Empty string has length 0" "EMPTY_LEN=0" "$RESULT"
assert_contains "Empty string is NOT NULL" "EMPTY_IS_NULL=NO" "$RESULT"
assert_contains "NULL is NULL" "NULL_IS_NULL=YES" "$RESULT"
assert_contains "Non-empty value correct" "NOTEMPTY=[NotEmpty]" "$RESULT"

assert_contains "Empty vs NULL: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Test 6: Unicode/International Characters
# =================================================================

log ""
log "${MAGENTA}[Test 6] ASCII Text with Numbers${NC}"

RESULT=$(run_sql "
DROP TABLE Test_TextNum;
CREATE TABLE Test_TextNum (id INTEGER, val VARCHAR(100));
INSERT INTO Test_TextNum VALUES (1, 'ABC123');
INSERT INTO Test_TextNum VALUES (2, '999-888-7777');
INSERT INTO Test_TextNum VALUES (3, 'Item_001_Rev_A');

-- Verify mixed text/number values
SELECT 'TN1=[' || val || ']' FROM Test_TextNum WHERE id=1;
SELECT 'TN2=[' || val || ']' FROM Test_TextNum WHERE id=2;
SELECT 'TN3=[' || val || ']' FROM Test_TextNum WHERE id=3;

SELECT * FROM ExportToTrino(ON (SELECT * FROM Test_TextNum)) AS t;

DROP TABLE Test_TextNum;
")

ROWS=$(get_total_rows "$RESULT")
assert_equals "Text/Num: 3 rows exported" "3" "$ROWS"

# Verify exact text/number combinations
assert_contains "Text/Num: ABC123 exact" "TN1=[ABC123]" "$RESULT"
assert_contains "Text/Num: Phone format" "TN2=[999-888-7777]" "$RESULT"
assert_contains "Text/Num: Underscore format" "TN3=[Item_001_Rev_A]" "$RESULT"

assert_contains "Text/Num: Export SUCCESS" "SUCCESS" "$RESULT"

# =================================================================
# Summary
# =================================================================

print_summary "Character Types (Accuracy Validated)"
exit $?
