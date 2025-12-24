#!/bin/bash
# ============================================================
# Quick Test Runner - Execute specific SQL queries against Trino
# ============================================================
# Usage: ./quick_test.sh "SQL_QUERY" [expected_value]
#
# Examples:
#   ./quick_test.sh "SELECT COUNT(*) FROM test_integer_types"
#   ./quick_test.sh "SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 1" "0.00"
# ============================================================

JAVA_HOME="${JAVA_HOME:-/home/vijay/tdconnector/trino_server/jdk-25.0.1}"
TRINO_CLI="${TRINO_CLI:-/home/vijay/tdconnector/trino_server/trino-cli-479}"
TRINO_HOST="${TRINO_HOST:-localhost:8080}"
TRINO_CATALOG="${TRINO_CATALOG:-tdexport}"
TRINO_SCHEMA="${TRINO_SCHEMA:-trinoexport}"

if [ -z "$1" ]; then
    echo "Usage: $0 \"SQL_QUERY\" [expected_value]"
    echo ""
    echo "Examples:"
    echo "  $0 \"SELECT COUNT(*) FROM test_integer_types\""
    echo "  $0 \"SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 1\" \"0.00\""
    exit 1
fi

QUERY="$1"
EXPECTED="$2"

echo "Query: $QUERY"
echo "---"

RESULT=$($JAVA_HOME/bin/java -jar "$TRINO_CLI" \
    --server "http://$TRINO_HOST" \
    --catalog "$TRINO_CATALOG" \
    --schema "$TRINO_SCHEMA" \
    --execute "$QUERY" 2>&1 | grep -v "^WARNING:" | grep -v "^Dec" | grep -v "org.jline" | sed 's/"//g')

echo "Result: $RESULT"

if [ -n "$EXPECTED" ]; then
    # Normalize for comparison
    EXPECTED_NORM=$(echo "$EXPECTED" | tr -d '[:space:]')
    RESULT_NORM=$(echo "$RESULT" | tr -d '[:space:]')
    
    if [ "$RESULT_NORM" = "$EXPECTED_NORM" ]; then
        echo -e "\033[0;32m[PASS]\033[0m Expected: $EXPECTED"
    else
        echo -e "\033[0;31m[FAIL]\033[0m Expected: $EXPECTED, Got: $RESULT"
        exit 1
    fi
fi
